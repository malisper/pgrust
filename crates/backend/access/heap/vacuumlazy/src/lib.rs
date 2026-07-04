//! vacuumlazy.c phases I (scan/prune), II (index vacuum via ambulkdelete),
//! III (mark LP_UNUSED), and end-of-vacuum rel truncation, single-table lane.
//! Loud named panics:
//! lazy_scan_noprune, parallel vacuum, eager scanning. C divergences (recorded): no HEAP_PAGE_PRUNE_FREEZE (pruneheap's
//! freeze lane is loud), so page all-visibility is recomputed by
//! heap_page_is_all_visible after pruning — the shape C asserts equivalent;
//! relfrozenxid advancement and pg_class relstats writes are skipped
//! (inplace-update lane unported); the read stream is collapsed to sync
//! per-block reads (bitmap precedent); dead items are a flat tid vec, not
//! C 17+'s radix-tree TidStore (rock recorded in CATALOG).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::commands_vacuum::{
    vac_bulkdel_one_index, vac_cleanup_one_index, vac_close_indexes, vac_estimate_reltuples,
    vac_open_indexes, vacuum_delay_point, vacuum_get_cutoffs, vacuum_xid_failsafe_check,
    SetVacuumFailsafeActive, VacuumFailsafeActive,
};
use ::nbtree::IndexVacuumInfo;
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_rel::lock::{NoLock, RowExclusiveLock};
use ::types_rel::Relation;
use ::mcx::{Mcx, PgVec};
use ::tableam_vocab::{
    VacOptValue, VacuumCutoffs, VacuumParams, VACOPT_DISABLE_PAGE_SKIPPING, VACOPT_VERBOSE,
};
use ::types_core::xact::{
    InvalidTransactionId, TransactionIdIsNormal, TransactionIdIsValid, TransactionIdPrecedes,
};
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, GlobalVisStateHandle, InvalidBlockNumber, OffsetNumber, Size,
    TransactionId, BLCKSZ,
};
use ::types_error::PgResult;
use ::types_rel::RelationData;
use ::types_snapshot::HTSV_Result;
use ::types_storage::buf::BufferAccessStrategy;
use ::types_storage::bufpage::{
    MaxHeapTuplesPerPage, PageMut, PageRef, SizeOfPageHeaderData,
};
use ::types_storage::ReadBufferMode;
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleData, InvalidOffsetNumber, ItemPointerData,
    ItemPointerGetBlockNumberNoCheck, ItemPointerGetOffsetNumberNoCheck,
};

use ::bufmgr_seams::{BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK};
use ::pruneheap::{
    heap_page_prune_and_freeze, log_heap_prune_and_freeze, PruneFreezeResult, PruneReason,
    HEAP_PAGE_PRUNE_MARK_UNUSED_NOW,
};
use ::visibilitymap::{
    visibilitymap_clear, visibilitymap_count, visibilitymap_get_status, visibilitymap_pin,
    visibilitymap_set, vm_all_frozen, VmBuffer, VISIBILITYMAP_ALL_FROZEN,
    VISIBILITYMAP_ALL_VISIBLE, VISIBILITYMAP_VALID_BITS,
};

const SKIP_PAGES_THRESHOLD: BlockNumber = 32;
const FAILSAFE_EVERY_PAGES: BlockNumber = ((4u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) as BlockNumber;
const VACUUM_FSM_EVERY_PAGES: BlockNumber = ((8u64 * 1024 * 1024 * 1024) / BLCKSZ as u64) as BlockNumber;
const REL_TRUNCATE_MINIMUM: BlockNumber = 1000;
const REL_TRUNCATE_FRACTION: BlockNumber = 16;
const BYPASS_THRESHOLD_PAGES: f64 = 0.02;

pub struct LVRelState<'a, 'mcx> {
    mcx: Mcx<'mcx>,
    rel: &'a RelationData<'mcx>,
    indrels: ::mcx::PgVec<'mcx, Relation<'mcx>>,
    indstats: ::mcx::PgVec<'mcx, Option<IndexBulkDeleteResult>>,
    nindexes: usize,
    bstrategy: BufferAccessStrategy,

    aggressive: bool,
    skipwithvm: bool,
    consider_bypass_optimization: bool,
    do_index_vacuuming: bool,
    do_index_cleanup: bool,
    do_rel_truncate: bool,

    cutoffs: VacuumCutoffs,
    vistest: GlobalVisStateHandle,
    skippedallvis: bool,

    rel_pages: BlockNumber,
    scanned_pages: BlockNumber,
    new_frozen_tuple_pages: BlockNumber,
    lpdead_item_pages: BlockNumber,
    nonempty_pages: BlockNumber,

    dead_items: PgVec<'mcx, ItemPointerData>,
    dead_items_max_bytes: usize,

    num_index_scans: i64,
    tuples_deleted: i64,
    tuples_frozen: i64,
    lpdead_items: i64,
    live_tuples: i64,
    recently_dead_tuples: i64,
    missed_dead_tuples: i64,

    vm_new_visible_pages: BlockNumber,
    vm_new_visible_frozen_pages: BlockNumber,
    vm_new_frozen_pages: BlockNumber,

    new_rel_tuples: f64,
    new_live_tuples: f64,

    // Error-context bookkeeping (C's vacrel->offnum).
    offnum: OffsetNumber,

    current_block: BlockNumber,
    next_unskippable_block: BlockNumber,
    next_unskippable_allvis: bool,
    next_unskippable_vmbuffer: VmBuffer,
}

pub fn heap_vacuum_rel<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'mcx>,
    params: &VacuumParams,
    bstrategy: BufferAccessStrategy,
) -> PgResult<()> {
    if params.options & VACOPT_VERBOSE != 0 {
        unported("heap_vacuum_rel: VERBOSE instrumentation");
    }
    debug_assert!(params.index_cleanup != VacOptValue::Unspecified);
    debug_assert!(!matches!(params.truncate, VacOptValue::Unspecified | VacOptValue::Auto));

    let indrels = vac_open_indexes(mcx, rel, RowExclusiveLock)?;
    let nindexes = indrels.len();
    let mut indstats = ::mcx::PgVec::with_capacity_in(nindexes, mcx);
    for _ in 0..nindexes {
        indstats.push(None);
    }

    SetVacuumFailsafeActive(false);
    let mut do_index_vacuuming = true;
    let mut do_index_cleanup = true;
    let mut consider_bypass_optimization = true;
    match params.index_cleanup {
        VacOptValue::Disabled => {
            do_index_vacuuming = false;
            do_index_cleanup = false;
        }
        VacOptValue::Enabled => consider_bypass_optimization = false,
        _ => debug_assert!(params.index_cleanup == VacOptValue::Auto),
    }

    let (mut aggressive, cutoffs) = vacuum_get_cutoffs(rel, params)?;
    let rel_pages =
        bufmgr_seams::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
    let orig_rel_pages = rel_pages;
    let vistest = procarray_seams::global_vis_test_for::call(rel);

    let mut skipwithvm = true;
    if params.options & VACOPT_DISABLE_PAGE_SKIPPING != 0 {
        aggressive = true;
        skipwithvm = false;
    }

    // C divergence (recorded): heap_vacuum_eager_scan_setup is elided; eager
    // scanning stays disabled (find_next_unskippable_block skips all-visible
    // pages exactly as a normal vacuum with the failure cap exhausted).

    let mut vacrel = LVRelState {
        mcx,
        rel,
        indrels,
        indstats,
        nindexes,
        bstrategy,
        aggressive,
        skipwithvm,
        consider_bypass_optimization,
        do_index_vacuuming,
        do_index_cleanup,
        do_rel_truncate: params.truncate != VacOptValue::Disabled,
        cutoffs,
        vistest,
        skippedallvis: false,
        rel_pages,
        scanned_pages: 0,
        new_frozen_tuple_pages: 0,
        lpdead_item_pages: 0,
        nonempty_pages: 0,
        dead_items: PgVec::new_in(mcx),
        dead_items_max_bytes: init_small::globals::maintenance_work_mem() as usize * 1024,
        num_index_scans: 0,
        tuples_deleted: 0,
        tuples_frozen: 0,
        lpdead_items: 0,
        live_tuples: 0,
        recently_dead_tuples: 0,
        missed_dead_tuples: 0,
        vm_new_visible_pages: 0,
        vm_new_visible_frozen_pages: 0,
        vm_new_frozen_pages: 0,
        new_rel_tuples: 0.0,
        new_live_tuples: 0.0,
        offnum: InvalidOffsetNumber,
        current_block: InvalidBlockNumber,
        next_unskippable_block: InvalidBlockNumber,
        next_unskippable_allvis: false,
        next_unskippable_vmbuffer: VmBuffer::new(),
    };

    lazy_check_wraparound_failsafe(&mut vacrel)?;
    if params.nworkers > 0 {
        unported("dead_items_alloc: parallel vacuum (vacuumparallel.c)");
    }

    lazy_scan_heap(&mut vacrel, mcx)?;

    if vacrel.do_index_cleanup {
        update_relstats_all_indexes(&mut vacrel)?;
    }

    let indrels = core::mem::replace(&mut vacrel.indrels, ::mcx::PgVec::new_in(mcx));
    vac_close_indexes(indrels, NoLock)?;

    if should_attempt_truncation(&vacrel) {
        lazy_truncate_heap(&mut vacrel)?;
    }

    if vacrel.skippedallvis {
        debug_assert!(!vacrel.aggressive);
    }

    let new_rel_pages = vacrel.rel_pages;
    let (mut new_rel_allvisible, mut new_rel_allfrozen) = visibilitymap_count(rel)?;
    if new_rel_allvisible > new_rel_pages {
        new_rel_allvisible = new_rel_pages;
    }
    if new_rel_allfrozen > new_rel_allvisible {
        new_rel_allfrozen = new_rel_allvisible;
    }
    let _ = orig_rel_pages;
    // C divergence (recorded): relfrozenxid/relminmxid advancement skipped
    // (freeze lane); pgstat_report_vacuum skipped (cumulative-stats lane).
    vacuum_seams::vac_update_relstats::call(
        rel,
        new_rel_pages,
        vacrel.new_live_tuples,
        new_rel_allvisible,
        new_rel_allfrozen,
        vacrel.nindexes > 0,
        false,
    )?;
    Ok(())
}

fn dead_items_add(
    vacrel: &mut LVRelState<'_, '_>,
    blkno: BlockNumber,
    offsets: &[OffsetNumber],
) -> PgResult<()> {
    vacrel.dead_items.reserve(offsets.len());
    for &off in offsets {
        let tid = ItemPointerData::new(blkno, off);
        // The flat TidStore substitute relies on append-in-TID-order.
        debug_assert!(vacrel
            .dead_items
            .last()
            .is_none_or(|prev| ::types_tuple::itemptr::ItemPointerCompare(prev, &tid) < 0));
        vacrel.dead_items.push(tid);
    }
    Ok(())
}

fn dead_items_memory(vacrel: &LVRelState<'_, '_>) -> usize {
    vacrel.dead_items.len() * core::mem::size_of::<ItemPointerData>()
}

fn lazy_scan_heap(vacrel: &mut LVRelState<'_, '_>, mcx: Mcx<'_>) -> PgResult<()> {
    let rel_pages = vacrel.rel_pages;
    let mut blkno: BlockNumber = 0;
    let mut next_fsm_block_to_vacuum: BlockNumber = 0;
    let mut vmbuffer = VmBuffer::new();

    vacrel.current_block = InvalidBlockNumber;
    vacrel.next_unskippable_block = InvalidBlockNumber;
    vacrel.next_unskippable_allvis = false;

    loop {
        vacuum_delay_point(false)?;

        if vacrel.scanned_pages > 0 && vacrel.scanned_pages % FAILSAFE_EVERY_PAGES == 0 {
            lazy_check_wraparound_failsafe(vacrel)?;
        }

        if !vacrel.dead_items.is_empty() && dead_items_memory(vacrel) > vacrel.dead_items_max_bytes
        {
            vmbuffer.release();
            vacrel.consider_bypass_optimization = false;
            lazy_vacuum(vacrel)?;
            freespace::FreeSpaceMapVacuumRange(vacrel.rel, next_fsm_block_to_vacuum, blkno + 1)?;
            next_fsm_block_to_vacuum = blkno;
        }

        let Some((next_blkno, all_visible_according_to_vm)) = heap_vac_scan_next_block(vacrel)?
        else {
            break;
        };
        blkno = next_blkno;

        let buf = bufmgr_seams::read_buffer_extended::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vacrel.bstrategy.clone(),
        )?;
        vacrel.scanned_pages += 1;

        visibilitymap_pin(vacrel.rel, blkno, &mut vmbuffer)?;

        if !bufmgr_seams::conditional_lock_buffer_for_cleanup::call(buf)? {
            // C settles for lazy_scan_noprune under a share lock; single
            // pinner cannot fail the conditional cleanup lock today.
            unported("lazy_scan_noprune (cleanup-lock contention lane)");
        }

        // SAFETY: buffer pinned + cleanup-locked above.
        let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };

        if lazy_scan_new_or_empty(vacrel, buf, blkno, page, &vmbuffer)? {
            continue;
        }

        let mut has_lpdead_items = false;
        let ndeleted = lazy_scan_prune(
            vacrel,
            buf,
            blkno,
            page,
            &mut vmbuffer,
            all_visible_according_to_vm,
            &mut has_lpdead_items,
        )?;

        if vacrel.nindexes == 0 || !vacrel.do_index_vacuuming || !has_lpdead_items {
            let freespace = page.heap_free_space();
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::release_buffer::call(buf)?;
            freespace::RecordPageWithFreeSpace(vacrel.rel, blkno, freespace)?;

            if vacrel.nindexes == 0
                && ndeleted > 0
                && blkno - next_fsm_block_to_vacuum >= VACUUM_FSM_EVERY_PAGES
            {
                freespace::FreeSpaceMapVacuumRange(vacrel.rel, next_fsm_block_to_vacuum, blkno)?;
                next_fsm_block_to_vacuum = blkno;
            }
        } else {
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::release_buffer::call(buf)?;
        }
    }

    vacrel.current_block = InvalidBlockNumber;
    vmbuffer.release();
    vacrel.next_unskippable_vmbuffer.release();

    vacrel.new_live_tuples = vac_estimate_reltuples(
        vacrel.rel,
        rel_pages,
        vacrel.scanned_pages,
        vacrel.live_tuples as f64,
    );
    vacrel.new_rel_tuples = vacrel.new_live_tuples.max(0.0)
        + vacrel.recently_dead_tuples as f64
        + vacrel.missed_dead_tuples as f64;

    if !vacrel.dead_items.is_empty() {
        lazy_vacuum(vacrel)?;
    }

    if rel_pages > next_fsm_block_to_vacuum {
        freespace::FreeSpaceMapVacuumRange(vacrel.rel, next_fsm_block_to_vacuum, rel_pages)?;
    }

    if vacrel.nindexes > 0 && vacrel.do_index_cleanup {
        lazy_cleanup_all_indexes(vacrel)?;
    }
    Ok(())
}

/// The read-stream callback collapsed to a direct call: returns the next
/// block to scan and its VM status, or None at end of relation.
fn heap_vac_scan_next_block(
    vacrel: &mut LVRelState<'_, '_>,
) -> PgResult<Option<(BlockNumber, bool)>> {
    let mut next_block = vacrel.current_block.wrapping_add(1);

    if next_block >= vacrel.rel_pages {
        vacrel.next_unskippable_vmbuffer.release();
        return Ok(None);
    }

    if vacrel.next_unskippable_block == InvalidBlockNumber
        || next_block > vacrel.next_unskippable_block
    {
        let skipsallvis = find_next_unskippable_block(vacrel)?;
        if vacrel.next_unskippable_block - next_block >= SKIP_PAGES_THRESHOLD {
            next_block = vacrel.next_unskippable_block;
            if skipsallvis {
                vacrel.skippedallvis = true;
            }
        }
    }

    if next_block < vacrel.next_unskippable_block {
        vacrel.current_block = next_block;
        Ok(Some((next_block, true)))
    } else {
        debug_assert!(next_block == vacrel.next_unskippable_block);
        vacrel.current_block = next_block;
        Ok(Some((next_block, vacrel.next_unskippable_allvis)))
    }
}

fn find_next_unskippable_block(vacrel: &mut LVRelState<'_, '_>) -> PgResult<bool> {
    let rel_pages = vacrel.rel_pages;
    let mut next_unskippable_block = vacrel.next_unskippable_block.wrapping_add(1);
    let mut skipsallvis = false;

    loop {
        let mapbits = visibilitymap_get_status(
            vacrel.rel,
            next_unskippable_block,
            &mut vacrel.next_unskippable_vmbuffer,
        )?;
        let next_unskippable_allvis = mapbits & VISIBILITYMAP_ALL_VISIBLE != 0;

        if !next_unskippable_allvis {
            debug_assert!(mapbits & VISIBILITYMAP_ALL_FROZEN == 0);
            vacrel.next_unskippable_allvis = false;
            break;
        }
        // The last block is always scanned (truncation opportunity check).
        if next_unskippable_block == rel_pages - 1 {
            vacrel.next_unskippable_allvis = true;
            break;
        }
        if !vacrel.skipwithvm {
            vacrel.next_unskippable_allvis = true;
            break;
        }
        if mapbits & VISIBILITYMAP_ALL_FROZEN != 0 {
            next_unskippable_block += 1;
            continue;
        }
        if vacrel.aggressive {
            vacrel.next_unskippable_allvis = true;
            break;
        }
        skipsallvis = true;
        next_unskippable_block += 1;
    }

    vacrel.next_unskippable_block = next_unskippable_block;
    Ok(skipsallvis)
}

fn page_is_empty(page: PageRef<'_>) -> bool {
    (page.pd_lower() as usize) <= SizeOfPageHeaderData
}

fn lazy_scan_new_or_empty(
    vacrel: &mut LVRelState<'_, '_>,
    buf: Buffer,
    blkno: BlockNumber,
    page: PageRef<'_>,
    vmbuffer: &VmBuffer,
) -> PgResult<bool> {
    if page.is_new() {
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
        if freespace::GetRecordedFreeSpace(vacrel.rel, blkno)? == 0 {
            let freespace: Size = BLCKSZ - SizeOfPageHeaderData;
            freespace::RecordPageWithFreeSpace(vacrel.rel, blkno, freespace)?;
        }
        return Ok(true);
    }

    if page_is_empty(page) {
        // Caller always holds the cleanup lock here (share-lock escalation
        // lane is unreachable: lazy_scan_noprune is loud).
        if !page.is_all_visible() {
            bufmgr_seams::mark_buffer_dirty::call(buf)?;

            if relation_needs_wal(vacrel.rel)
                && bufmgr_seams::buffer_page_get_lsn::call(buf) == 0
            {
                xloginsert_seams::log_newpage_buffer::call(buf, true)?;
            }

            // SAFETY: pinned + cleanup-locked by the scan loop.
            let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
            pm.set_all_visible();
            visibilitymap_set(
                vacrel.rel,
                blkno,
                buf,
                0,
                vmbuffer,
                InvalidTransactionId,
                VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
            )?;
            vacrel.vm_new_visible_pages += 1;
            vacrel.vm_new_visible_frozen_pages += 1;
        }

        let freespace = page.heap_free_space();
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
        freespace::RecordPageWithFreeSpace(vacrel.rel, blkno, freespace)?;
        return Ok(true);
    }

    Ok(false)
}

#[allow(clippy::too_many_arguments)]
fn lazy_scan_prune(
    vacrel: &mut LVRelState<'_, '_>,
    buf: Buffer,
    blkno: BlockNumber,
    page: PageRef<'_>,
    vmbuffer: &mut VmBuffer,
    all_visible_according_to_vm: bool,
    has_lpdead_items: &mut bool,
) -> PgResult<i32> {
    // C divergence (recorded): HEAP_PAGE_PRUNE_FREEZE is not passed — the
    // freeze lane is loud in pruneheap — so presult never reports visibility;
    // all_visible/all_frozen come from heap_page_is_all_visible below, the
    // recheck C's own assertion holds equivalent to the prune-time answer.
    let mut prune_options = 0;
    if vacrel.nindexes == 0 {
        prune_options |= HEAP_PAGE_PRUNE_MARK_UNUSED_NOW;
    }

    let mut presult = PruneFreezeResult::default();
    heap_page_prune_and_freeze(
        vacrel.rel,
        buf,
        vacrel.vistest,
        prune_options,
        &mut presult,
        PruneReason::PruneVacuumScan,
        &mut vacrel.offnum,
    )?;

    if presult.nfrozen > 0 {
        vacrel.new_frozen_tuple_pages += 1;
    }

    if presult.lpdead_items > 0 {
        vacrel.lpdead_item_pages += 1;
        let deadoffsets = &mut presult.deadoffsets[..presult.lpdead_items as usize];
        deadoffsets.sort_unstable();
        dead_items_add(vacrel, blkno, deadoffsets)?;
    }

    vacrel.tuples_deleted += presult.ndeleted as i64;
    vacrel.tuples_frozen += presult.nfrozen as i64;
    vacrel.lpdead_items += presult.lpdead_items as i64;
    vacrel.live_tuples += presult.live_tuples as i64;
    vacrel.recently_dead_tuples += presult.recently_dead_tuples as i64;

    if presult.hastup {
        vacrel.nonempty_pages = blkno + 1;
    }

    *has_lpdead_items = presult.lpdead_items > 0;

    let (all_visible, all_frozen, vm_conflict_horizon) = if presult.lpdead_items > 0 {
        (false, false, InvalidTransactionId)
    } else {
        let (av, af, cutoff) = heap_page_is_all_visible(vacrel, buf)?;
        (av, af, cutoff)
    };

    if !all_visible_according_to_vm && all_visible {
        let mut flags = VISIBILITYMAP_ALL_VISIBLE;
        if all_frozen {
            debug_assert!(!TransactionIdIsValid(vm_conflict_horizon));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }

        // PD_ALL_VISIBLE before the VM bit, as C (the reverse is corruption).
        // SAFETY: pinned + cleanup-locked by the scan loop.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
        pm.set_all_visible();
        bufmgr_seams::mark_buffer_dirty::call(buf)?;
        let old_vmbits =
            visibilitymap_set(vacrel.rel, blkno, buf, 0, vmbuffer, vm_conflict_horizon, flags)?;

        if old_vmbits & VISIBILITYMAP_ALL_VISIBLE == 0 {
            vacrel.vm_new_visible_pages += 1;
            if all_frozen {
                vacrel.vm_new_visible_frozen_pages += 1;
            }
        } else if old_vmbits & VISIBILITYMAP_ALL_FROZEN == 0 && all_frozen {
            vacrel.vm_new_frozen_pages += 1;
        }
    } else if all_visible_according_to_vm
        && !page.is_all_visible()
        && visibilitymap_get_status(vacrel.rel, blkno, vmbuffer)? != 0
    {
        // VM bit set while the page-level bit is clear: repair, as C (WARNING
        // elided).
        visibilitymap_clear(vacrel.rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS)?;
    } else if presult.lpdead_items > 0 && page.is_all_visible() {
        // SAFETY: pinned + cleanup-locked by the scan loop.
        let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
        pm.clear_all_visible();
        bufmgr_seams::mark_buffer_dirty::call(buf)?;
        visibilitymap_clear(vacrel.rel, blkno, vmbuffer, VISIBILITYMAP_VALID_BITS)?;
    } else if all_visible_according_to_vm
        && all_visible
        && all_frozen
        && !vm_all_frozen(vacrel.rel, blkno, vmbuffer)?
    {
        if !page.is_all_visible() {
            // SAFETY: pinned + cleanup-locked by the scan loop.
            let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
            pm.set_all_visible();
            bufmgr_seams::mark_buffer_dirty::call(buf)?;
        }
        debug_assert!(!TransactionIdIsValid(vm_conflict_horizon));
        let old_vmbits = visibilitymap_set(
            vacrel.rel,
            blkno,
            buf,
            0,
            vmbuffer,
            InvalidTransactionId,
            VISIBILITYMAP_ALL_VISIBLE | VISIBILITYMAP_ALL_FROZEN,
        )?;
        if old_vmbits & VISIBILITYMAP_ALL_VISIBLE == 0 {
            vacrel.vm_new_visible_pages += 1;
            vacrel.vm_new_visible_frozen_pages += 1;
        } else {
            vacrel.vm_new_frozen_pages += 1;
        }
    }

    Ok(presult.ndeleted)
}

fn lazy_vacuum(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    debug_assert!(vacrel.nindexes > 0);
    debug_assert!(vacrel.lpdead_item_pages > 0);

    if !vacrel.do_index_vacuuming {
        debug_assert!(!vacrel.do_index_cleanup);
        vacrel.dead_items.clear();
        return Ok(());
    }

    let mut bypass = false;
    if vacrel.consider_bypass_optimization && vacrel.rel_pages > 0 {
        debug_assert!(vacrel.num_index_scans == 0);
        debug_assert!(vacrel.lpdead_items == vacrel.dead_items.len() as i64);
        let threshold = vacrel.rel_pages as f64 * BYPASS_THRESHOLD_PAGES;
        bypass = (vacrel.lpdead_item_pages as f64) < threshold
            && dead_items_memory(vacrel) < 32 * 1024 * 1024;
    }

    if bypass {
        vacrel.do_index_vacuuming = false;
    } else if lazy_vacuum_all_indexes(vacrel)? {
        lazy_vacuum_heap_rel(vacrel)?;
    } else {
        debug_assert!(VacuumFailsafeActive());
    }

    vacrel.dead_items.clear();
    Ok(())
}

/// lazy_vacuum_all_indexes: one ambulkdelete round over every index. `false`
/// only in the wraparound-failsafe case.
fn lazy_vacuum_all_indexes(vacrel: &mut LVRelState<'_, '_>) -> PgResult<bool> {
    let mut allindexes = true;
    let old_live_tuples = vacrel.rel.rd_rel.reltuples as f64;

    debug_assert!(vacrel.nindexes > 0);
    debug_assert!(vacrel.do_index_vacuuming);
    debug_assert!(vacrel.do_index_cleanup);

    if lazy_check_wraparound_failsafe(vacrel)? {
        return Ok(false);
    }

    for idx in 0..vacrel.nindexes {
        let istat = vacrel.indstats[idx].take();
        let new_istat = {
            let ivinfo = IndexVacuumInfo {
                index: &vacrel.indrels[idx],
                heaprel: vacrel.rel,
                analyze_only: false,
                estimated_count: true,
                num_heap_tuples: old_live_tuples,
                strategy: vacrel.bstrategy.clone(),
            };
            vac_bulkdel_one_index(vacrel.mcx, &ivinfo, istat, &vacrel.dead_items)?
        };
        vacrel.indstats[idx] = Some(new_istat);

        if lazy_check_wraparound_failsafe(vacrel)? {
            allindexes = false;
            break;
        }
    }

    debug_assert!(
        vacrel.num_index_scans > 0 || vacrel.dead_items.len() as i64 == vacrel.lpdead_items
    );
    debug_assert!(allindexes || VacuumFailsafeActive());

    vacrel.num_index_scans += 1;
    Ok(allindexes)
}

/// lazy_cleanup_all_indexes: amvacuumcleanup for every index.
fn lazy_cleanup_all_indexes(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    debug_assert!(vacrel.do_index_cleanup);
    debug_assert!(vacrel.nindexes > 0);

    let reltuples = vacrel.new_rel_tuples;
    let estimated_count = vacrel.scanned_pages < vacrel.rel_pages;

    for idx in 0..vacrel.nindexes {
        let istat = vacrel.indstats[idx].take();
        let new_istat = {
            let ivinfo = IndexVacuumInfo {
                index: &vacrel.indrels[idx],
                heaprel: vacrel.rel,
                analyze_only: false,
                estimated_count,
                num_heap_tuples: reltuples,
                strategy: vacrel.bstrategy.clone(),
            };
            vac_cleanup_one_index(vacrel.mcx, &ivinfo, istat)?
        };
        vacrel.indstats[idx] = new_istat;
    }
    Ok(())
}

/// update_relstats_all_indexes: index pg_class stats where accurate.
fn update_relstats_all_indexes(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    debug_assert!(vacrel.do_index_cleanup);
    for idx in 0..vacrel.nindexes {
        let Some(istat) = &vacrel.indstats[idx] else {
            continue;
        };
        if istat.estimated_count {
            continue;
        }
        vacuum_seams::vac_update_relstats::call(
            &vacrel.indrels[idx],
            istat.num_pages,
            istat.num_index_tuples,
            0,
            0,
            false,
            false,
        )?;
    }
    Ok(())
}

/// Phase III driver (C lazy_vacuum_heap_rel): reap the collected LP_DEAD tids
/// block by block. Reached from lazy_vacuum only after index vacuuming (loud
/// today); exercised directly by tests.
pub fn lazy_vacuum_heap_rel(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    let mut vacuumed_pages: BlockNumber = 0;
    let mut vmbuffer = VmBuffer::new();

    let mut i = 0usize;
    while i < vacrel.dead_items.len() {
        vacuum_delay_point(false)?;

        let blkno = ItemPointerGetBlockNumberNoCheck(&vacrel.dead_items[i]);
        let mut offsets = [InvalidOffsetNumber; MaxHeapTuplesPerPage];
        let mut num_offsets = 0usize;
        while i < vacrel.dead_items.len()
            && ItemPointerGetBlockNumberNoCheck(&vacrel.dead_items[i]) == blkno
        {
            offsets[num_offsets] = ItemPointerGetOffsetNumberNoCheck(&vacrel.dead_items[i]);
            num_offsets += 1;
            i += 1;
        }

        let buf = bufmgr_seams::read_buffer_extended::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vacrel.bstrategy.clone(),
        )?;
        visibilitymap_pin(vacrel.rel, blkno, &mut vmbuffer)?;
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
        lazy_vacuum_heap_page(vacrel, blkno, buf, &offsets[..num_offsets], &vmbuffer)?;

        // SAFETY: pinned; freespace read before unlock, as C.
        let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
        let freespace = page.heap_free_space();
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;
        freespace::RecordPageWithFreeSpace(vacrel.rel, blkno, freespace)?;
        vacuumed_pages += 1;
    }
    debug_assert!(
        vacrel.num_index_scans > 1
            || (vacrel.dead_items.len() as i64 == vacrel.lpdead_items
                && vacuumed_pages == vacrel.lpdead_item_pages)
    );
    vacrel.dead_items.clear();

    vmbuffer.release();
    Ok(())
}

fn lazy_vacuum_heap_page(
    vacrel: &mut LVRelState<'_, '_>,
    blkno: BlockNumber,
    buffer: Buffer,
    deadoffsets: &[OffsetNumber],
    vmbuffer: &VmBuffer,
) -> PgResult<()> {
    // SAFETY: caller holds pin + exclusive content lock.
    let mut pm = unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) };
    let mut unused = [InvalidOffsetNumber; MaxHeapTuplesPerPage];
    let mut nunused = 0usize;

    for &toff in deadoffsets {
        let mut itemid = pm.as_ref().item_id(toff);
        debug_assert!(itemid.is_dead() && !itemid.has_storage());
        itemid.set_unused();
        pm.set_item_id(toff, itemid);
        unused[nunused] = toff;
        nunused += 1;
    }
    debug_assert!(nunused > 0);

    pm.truncate_line_pointer_array();

    bufmgr_seams::mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(vacrel.rel) {
        log_heap_prune_and_freeze(
            vacrel.rel,
            buffer,
            InvalidTransactionId,
            false,
            PruneReason::PruneVacuumCleanup,
            &[],
            &[],
            &unused[..nunused],
        )?;
    }

    debug_assert!(!pm.as_ref().is_all_visible());
    let (all_visible, all_frozen, visibility_cutoff_xid) =
        heap_page_is_all_visible(vacrel, buffer)?;
    if all_visible {
        let mut flags = VISIBILITYMAP_ALL_VISIBLE;
        if all_frozen {
            debug_assert!(!TransactionIdIsValid(visibility_cutoff_xid));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }
        pm.set_all_visible();
        visibilitymap_set(
            vacrel.rel,
            blkno,
            buffer,
            0,
            vmbuffer,
            visibility_cutoff_xid,
            flags,
        )?;
        vacrel.vm_new_visible_pages += 1;
        if all_frozen {
            vacrel.vm_new_visible_frozen_pages += 1;
        }
    }
    Ok(())
}

/// Returns (all_visible, all_frozen, visibility_cutoff_xid).
fn heap_page_is_all_visible(
    vacrel: &mut LVRelState<'_, '_>,
    buf: Buffer,
) -> PgResult<(bool, bool, TransactionId)> {
    // SAFETY: caller holds pin + content lock; HTSV hint-bit stores land in
    // the page, as C.
    let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };
    let blockno = bufmgr_seams::buffer_get_block_number::call(buf);
    let mut visibility_cutoff_xid = InvalidTransactionId;
    let mut all_frozen = true;
    let mut all_visible = true;

    let maxoff = page.max_offset_number();
    let mut offnum = FirstOffsetNumber;
    while offnum <= maxoff && all_visible {
        vacrel.offnum = offnum;
        let itemid = page.item_id(offnum);

        if !itemid.is_used() || itemid.is_redirected() {
            offnum += 1;
            continue;
        }

        if itemid.is_dead() {
            all_visible = false;
            all_frozen = false;
            break;
        }

        debug_assert!(itemid.is_normal());
        // SAFETY: LP_NORMAL item within the locked page image.
        let (ptr, len) = unsafe { page.item_raw_unchecked(itemid) };
        // SAFETY: in-page tuple image; the pin outlives this scope.
        let mut tuple = unsafe {
            HeapTupleData::from_raw_parts(
                ptr,
                len,
                ItemPointerData::new(blockno, offnum),
                vacrel.rel.rd_id,
            )
        };

        match heapam_visibility_seams::heap_tuple_satisfies_vacuum::call(
            &mut tuple,
            vacrel.cutoffs.OldestXmin,
            buf,
        )? {
            HTSV_Result::HEAPTUPLE_LIVE => {
                let hdr = tuple.t_data();
                if !hdr.xmin_committed() {
                    all_visible = false;
                    all_frozen = false;
                    break;
                }
                let xmin = hdr.xmin();
                if !TransactionIdPrecedes(xmin, vacrel.cutoffs.OldestXmin) {
                    all_visible = false;
                    all_frozen = false;
                    break;
                }
                if TransactionIdIsNormal(xmin)
                    && (visibility_cutoff_xid == InvalidTransactionId
                        || TransactionIdPrecedes(visibility_cutoff_xid, xmin))
                {
                    visibility_cutoff_xid = xmin;
                }
                if all_frozen && heapam::heap_tuple_needs_eventual_freeze(hdr) {
                    all_frozen = false;
                }
            }
            HTSV_Result::HEAPTUPLE_DEAD
            | HTSV_Result::HEAPTUPLE_RECENTLY_DEAD
            | HTSV_Result::HEAPTUPLE_INSERT_IN_PROGRESS
            | HTSV_Result::HEAPTUPLE_DELETE_IN_PROGRESS => {
                all_visible = false;
                all_frozen = false;
            }
        }
        offnum += 1;
    }

    vacrel.offnum = InvalidOffsetNumber;
    Ok((all_visible, all_frozen, visibility_cutoff_xid))
}

fn lazy_check_wraparound_failsafe(vacrel: &mut LVRelState<'_, '_>) -> PgResult<bool> {
    if !vacuum_xid_failsafe_check(&vacrel.cutoffs)? {
        return Ok(false);
    }
    SetVacuumFailsafeActive(true);
    unported("lazy_check_wraparound_failsafe: failsafe triggered (cost/parallel teardown)");
}

const VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL_MS: u64 = 50;
const VACUUM_TRUNCATE_LOCK_TIMEOUT_MS: u64 = 5000;
const VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL_MS: u128 = 20;

// lazy_truncate_heap (vacuumlazy.c). The "stopping/suspending truncate" and
// "truncated N to M pages" messages are DEBUG2 without VERBOSE (loud
// upstream), so none are emitted.
fn lazy_truncate_heap(vacrel: &mut LVRelState<'_, '_>) -> PgResult<()> {
    let mut orig_rel_pages = vacrel.rel_pages;
    loop {
        let mut lock_retry = 0u64;
        loop {
            if lmgr::ConditionalLockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)? {
                break;
            }
            postgres_seams::check_for_interrupts::call()?;
            lock_retry += 1;
            if lock_retry
                > VACUUM_TRUNCATE_LOCK_TIMEOUT_MS / VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL_MS
            {
                return Ok(());
            }
            // C: WaitLatch(MyLatch, WL_TIMEOUT, 50ms) — no latch wakeups
            // here, so a plain timed sleep (worst case the same 50ms).
            std::thread::sleep(std::time::Duration::from_millis(
                VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL_MS,
            ));
        }

        // If the rel grew while we vacuumed under a weaker lock, the new
        // pages presumably hold live tuples: give up without updating
        // rel_pages (the old density estimate stays).
        let new_rel_pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
        )?;
        if new_rel_pages != orig_rel_pages {
            lmgr::UnlockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)?;
            return Ok(());
        }

        let mut lock_waiter_detected = false;
        let new_rel_pages = count_nondeletable_pages(vacrel, &mut lock_waiter_detected)?;
        vacrel.current_block = new_rel_pages;

        if new_rel_pages >= orig_rel_pages {
            lmgr::UnlockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)?;
            return Ok(());
        }

        catalog_storage::RelationTruncate(vacrel.rel, new_rel_pages)?;

        // Other backends can't touch the rel until they process the smgr
        // inval smgrtruncate sent, which happens once they take their lock.
        lmgr::UnlockRelation(vacrel.rel, types_rel::lock::AccessExclusiveLock)?;

        // rel_pages shrinks without touching reltuples: the truncated pages
        // held no tuples.
        vacrel.rel_pages = new_rel_pages;
        orig_rel_pages = new_rel_pages;

        if !(new_rel_pages > vacrel.nonempty_pages && lock_waiter_detected) {
            return Ok(());
        }
    }
}

// count_nondeletable_pages (vacuumlazy.c). C's OS-readahead prefetch loop is
// skipped (no PrefetchBuffer surface); advisory only.
fn count_nondeletable_pages(
    vacrel: &mut LVRelState<'_, '_>,
    lock_waiter_detected: &mut bool,
) -> PgResult<BlockNumber> {
    let mut starttime = std::time::Instant::now();
    let mut blkno = vacrel.rel_pages;
    while blkno > vacrel.nonempty_pages {
        // Waiters queue behind our AccessExclusiveLock; probe at most every
        // VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL, checked once per 32 blocks.
        if blkno % 32 == 0 {
            let currenttime = std::time::Instant::now();
            if currenttime.duration_since(starttime).as_millis()
                >= VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL_MS
            {
                if lmgr::LockHasWaitersRelation(
                    vacrel.rel,
                    types_rel::lock::AccessExclusiveLock,
                )? {
                    *lock_waiter_detected = true;
                    return Ok(blkno);
                }
                starttime = currenttime;
            }
        }

        // No vacuum delay point under the exclusive lock; interrupts only.
        postgres_seams::check_for_interrupts::call()?;

        blkno -= 1;

        let buf = bufmgr_seams::read_buffer_extended::call(
            vacrel.rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vacrel.bstrategy.clone(),
        )?;
        bufmgr_seams::lock_buffer::call(buf, ::bufmgr_seams::BUFFER_LOCK_SHARE)?;
        // SAFETY: buffer pinned + share-locked above.
        let page = unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buf)) };

        if page.is_new() || page_is_empty(page) {
            bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            bufmgr_seams::release_buffer::call(buf)?;
            continue;
        }

        let mut hastup = false;
        let maxoff = page.max_offset_number();
        for offnum in FirstOffsetNumber..=maxoff {
            // Any non-unused item keeps the page: even LP_DEAD makes
            // truncation unsafe, its index entries may not be cleaned out.
            if page.item_id(offnum).is_used() {
                hastup = true;
                break;
            }
        }
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        bufmgr_seams::release_buffer::call(buf)?;

        if hastup {
            return Ok(blkno + 1);
        }
    }
    Ok(vacrel.nonempty_pages)
}

fn should_attempt_truncation(vacrel: &LVRelState<'_, '_>) -> bool {
    if !vacrel.do_rel_truncate || VacuumFailsafeActive() {
        return false;
    }
    let possibly_freeable = vacrel.rel_pages - vacrel.nonempty_pages;
    possibly_freeable > 0
        && (possibly_freeable >= REL_TRUNCATE_MINIMUM
            || possibly_freeable >= vacrel.rel_pages / REL_TRUNCATE_FRACTION)
}

fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
}

pub fn init_seams() {
    // rd_tableam->relation_vacuum: installed here, not by tableam — heap is
    // the only AM and a tableam-side install cycles through heapam_handler.
    tableam_seams::table_relation_vacuum::set(|mcx, rel, params, bstrategy| {
        heap_vacuum_rel(mcx, rel, params, bstrategy)
    });
}

#[cold]
#[inline(never)]
fn unported(unit: &'static str) -> ! {
    panic!("unported callee reached from vacuumlazy.c: {unit}");
}

#[cfg(test)]
mod tests;
