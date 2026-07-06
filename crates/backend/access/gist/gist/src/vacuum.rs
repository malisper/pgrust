//! gistvacuum.c: gistbulkdelete/gistvacuumcleanup — physical-order leaf
//! vacuum, empty-leaf-page deletion, deleted-page recycling. The bulkdelete
//! callback is monomorphized to the sorted dead-TID slice (vac_tid_reaped) or
//! validate_index's never-delete collect callback, as nbtree renders it.
use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::types_core::xact::FullTransactionId;
use ::types_core::{BlockNumber, ForkNumber, InvalidBlockNumber, OffsetNumber, XLogRecPtr};
use ::types_error::PgResult;
use ::types_gist::{
    page_opaque_update, GistFollowRight, GistPageGetNSN, GistPageIsDeleted, GistPageIsLeaf,
    GistPageSetDeleted, F_TUPLES_DELETED, GIST_ROOT_BLKNO,
};
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::{
    InvalidOffsetNumber, ItemPointerCompare, ItemPointerData, ItemPointerGetBlockNumber,
};

use crate::util::{
    gistGetFakeLSN, gistPageRecyclable, gist_tuple_is_invalid, gistcheckpage, itup_get_tid,
    page_item, FirstOffsetNumber,
};
use crate::wal::{gistXLogPageDelete, gistXLogUpdate};

pub use ::nbtree::IndexVacuumInfo;

const GIST_SHARE: i32 = bufmgr::BUFFER_LOCK_SHARE;
const GIST_EXCLUSIVE: i32 = bufmgr::BUFFER_LOCK_EXCLUSIVE;
const GIST_UNLOCK: i32 = bufmgr::BUFFER_LOCK_UNLOCK;

struct GistVacState<'a, 'cb, 'mcx> {
    info: &'a IndexVacuumInfo<'a, 'mcx>,
    stats: &'a mut IndexBulkDeleteResult,
    dead_items: Option<&'a [ItemPointerData]>,
    collect: Option<&'a mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + 'cb)>,
    start_nsn: XLogRecPtr,
    // C's IntegerSets; iteration order stays ascending like intset.
    internal_page_set: Vec<BlockNumber>,
    empty_leaf_set: std::collections::BTreeSet<BlockNumber>,
}

fn vacuum_delay_point() -> PgResult<()> {
    crate::check_for_interrupts();
    if init_small::globals::VacuumCostActive() {
        vacuum_seams::vacuum_delay_point::call(false)?;
    }
    Ok(())
}

// vac_tid_reaped over the sorted dead-TID image.
fn tid_is_dead(dead_items: &[ItemPointerData], tid: &ItemPointerData) -> bool {
    dead_items
        .binary_search_by(|probe| ItemPointerCompare(probe, tid).cmp(&0))
        .is_ok()
}

/// gistbulkdelete.
pub fn gistbulkdelete<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    let mut stats = istat.unwrap_or_default();
    gistvacuumscan(info, &mut stats, Some(dead_items), None)?;
    Ok(stats)
}

/// gistbulkdelete with C's collect-only callback shape (validate_index).
pub fn gistbulkdelete_collect<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    let mut stats = IndexBulkDeleteResult::default();
    gistvacuumscan(info, &mut stats, None, Some(callback))?;
    Ok(stats)
}

/// gistvacuumcleanup.
pub fn gistvacuumcleanup<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    if info.analyze_only {
        return Ok(istat);
    }
    let mut stats = match istat {
        Some(s) => s,
        None => {
            let mut s = IndexBulkDeleteResult::default();
            gistvacuumscan(info, &mut s, None, None)?;
            s
        }
    };
    // Concurrent page splits can double-count tuples; disbelieve totals
    // beyond the heap's own count when that count is accurate.
    if !info.estimated_count && stats.num_index_tuples > info.num_heap_tuples {
        stats.num_index_tuples = info.num_heap_tuples;
    }
    Ok(Some(stats))
}

/// gistvacuumscan: physical-order scan; the relation length is rechecked so
/// leaf pages added by concurrent splits are visited.
/// LockRelationForExtension around the length check: single-backend no-op.
fn gistvacuumscan<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: &mut IndexBulkDeleteResult,
    dead_items: Option<&[ItemPointerData]>,
    collect: Option<&mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_)>,
) -> PgResult<()> {
    let rel = info.index;

    stats.num_pages = 0;
    stats.estimated_count = false;
    stats.num_index_tuples = 0.0;
    stats.pages_deleted = 0;
    stats.pages_free = 0;

    let start_nsn = if crate::relation_needs_wal(rel) {
        transam_xlog::GetInsertRecPtr()
    } else {
        gistGetFakeLSN(rel)?
    };
    let mut vstate = GistVacState {
        info,
        stats,
        dead_items,
        collect,
        start_nsn,
        internal_page_set: Vec::new(),
        empty_leaf_set: std::collections::BTreeSet::new(),
    };

    let mut current: BlockNumber = GIST_ROOT_BLKNO;
    let mut num_pages;
    loop {
        num_pages =
            bufmgr::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
        if current >= num_pages {
            break;
        }
        while current < num_pages {
            vacuum_delay_point()?;
            let pin = read_pin(info, current)?;
            gistvacuumpage(&mut vstate, pin, current)?;
            current += 1;
        }
    }

    if vstate.stats.pages_free > 0 {
        freespace::IndexFreeSpaceMapVacuum(rel)?;
    }
    vstate.stats.num_pages = num_pages;

    gistvacuum_delete_empty_pages(&mut vstate)?;
    Ok(())
}

fn read_pin<'mcx>(info: &IndexVacuumInfo<'_, 'mcx>, blkno: BlockNumber) -> PgResult<BufferPin> {
    Ok(BufferPin::adopt(bufmgr::read_buffer_extended::call(
        info.index,
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::Normal,
        info.strategy.clone(),
    )?)
    .expect("ReadBufferExtended returned InvalidBuffer"))
}

/// gistvacuumpage. The rightlink recursion (page split after the scan started
/// moved tuples to a lower-numbered page) is C's `goto restart`.
fn gistvacuumpage(
    vstate: &mut GistVacState<'_, '_, '_>,
    mut pin: BufferPin,
    orig_blkno: BlockNumber,
) -> PgResult<()> {
    let rel = vstate.info.index;
    let mut blkno = orig_blkno;
    loop {
        let mut recurse_to = InvalidBlockNumber;
        bufmgr::lock_buffer::call(pin.buffer(), GIST_EXCLUSIVE)?;
        {
            let page = pin.page();
            if gistPageRecyclable(vstate.info.heaprel, &page)? {
                freespace::RecordFreeIndexPage(rel, blkno)?;
                vstate.stats.pages_deleted += 1;
                vstate.stats.pages_free += 1;
            } else if GistPageIsDeleted(&page) {
                // Already deleted, but not recyclable yet.
                vstate.stats.pages_deleted += 1;
            } else if GistPageIsLeaf(&page) {
                let opaque = ::types_gist::page_opaque(&page);
                if (GistFollowRight(&page) || vstate.start_nsn < GistPageGetNSN(&page))
                    && opaque.rightlink != InvalidBlockNumber
                    && opaque.rightlink < orig_blkno
                {
                    recurse_to = opaque.rightlink;
                }

                let mut maxoff = page.max_offset_number();
                let mut todelete: Vec<OffsetNumber> = Vec::new();
                for off in FirstOffsetNumber..=maxoff {
                    let itup = page_item(&page, off);
                    // SAFETY: page item under our exclusive content lock.
                    let tid = unsafe { itup_get_tid(itup) };
                    if let Some(cb) = vstate.collect.as_deref_mut() {
                        cb(&tid)?;
                    } else if let Some(dead) = vstate.dead_items {
                        if tid_is_dead(dead, &tid) {
                            todelete.push(off);
                        }
                    }
                }

                if !todelete.is_empty() {
                    // One WAL record per page, C-exact.
                    bufmgr::mark_buffer_dirty::call(pin.buffer())?;
                    {
                        let mut pm = crate::buf_page_mut(pin.buffer());
                        pm.index_multi_delete(&todelete);
                        page_opaque_update(&mut pm, |o| o.flags |= F_TUPLES_DELETED);
                    }
                    let recptr = if crate::relation_needs_wal(rel) {
                        gistXLogUpdate(&pin, &todelete, &[], None)?
                    } else {
                        gistGetFakeLSN(rel)?
                    };
                    crate::buf_page_mut(pin.buffer()).set_lsn(recptr);

                    vstate.stats.tuples_removed += todelete.len() as f64;
                    maxoff = pin.page().max_offset_number();
                }

                let nremain = maxoff as i64 - FirstOffsetNumber as i64 + 1;
                if nremain == 0 {
                    // Deletion happens in the second stage; skip when
                    // recursing, matching intset's ascending-order rule —
                    // the next VACUUM picks it up.
                    if blkno == orig_blkno {
                        vstate.empty_leaf_set.insert(blkno);
                    }
                } else {
                    vstate.stats.num_index_tuples += nremain as f64;
                }
            } else {
                // Invalid tuples are pre-9.1 incomplete-split leftovers.
                for off in FirstOffsetNumber..=page.max_offset_number() {
                    let itup = page_item(&page, off);
                    // SAFETY: page item under our exclusive content lock.
                    if unsafe { gist_tuple_is_invalid(itup) } {
                        elog::ereport(::types_error::LOG)
                            .errmsg(format!(
                                "index \"{}\" contains an inner tuple marked as invalid",
                                rel.name()
                            ))
                            .errdetail(
                                "This is caused by an incomplete page split at crash \
                                 recovery before upgrading to PostgreSQL 9.1.",
                            )
                            .errhint("Please REINDEX it.")
                            .finish(::types_error::ErrorLocation::new(
                                "gistvacuum.c",
                                0,
                                "gistvacuumpage",
                            ))?;
                    }
                }
                if blkno == orig_blkno {
                    vstate.internal_page_set.push(blkno);
                }
            }
        }
        bufmgr::lock_buffer::call(pin.buffer(), GIST_UNLOCK)?;
        drop(pin);

        if recurse_to == InvalidBlockNumber {
            return Ok(());
        }
        blkno = recurse_to;
        vacuum_delay_point()?;
        pin = read_pin(vstate.info, blkno)?;
    }
}

/// gistvacuum_delete_empty_pages: rescan the remembered internal pages and
/// unlink their empty children.
fn gistvacuum_delete_empty_pages(vstate: &mut GistVacState<'_, '_, '_>) -> PgResult<()> {
    let info = vstate.info;
    let rel = info.index;
    let mut empty_pages_remaining = vstate.empty_leaf_set.len();

    for i in 0..vstate.internal_page_set.len() {
        if empty_pages_remaining == 0 {
            break;
        }
        let blkno = vstate.internal_page_set[i];
        let parent = read_pin(info, blkno)?;
        bufmgr::lock_buffer::call(parent.buffer(), GIST_SHARE)?;

        let mut todelete: Vec<(OffsetNumber, BlockNumber)> = Vec::new();
        {
            let page = parent.page();
            if page.is_new() || GistPageIsDeleted(&page) || GistPageIsLeaf(&page) {
                // Was an internal page earlier; shouldn't change.
                debug_assert!(false);
                bufmgr::lock_buffer::call(parent.buffer(), GIST_UNLOCK)?;
                continue;
            }
            let maxoff = page.max_offset_number();
            for off in FirstOffsetNumber..=maxoff {
                if todelete.len() >= maxoff.saturating_sub(1) as usize {
                    break;
                }
                let itup = page_item(&page, off);
                // SAFETY: page item under our share lock.
                let tid = unsafe { itup_get_tid(itup) };
                let leafblk = ItemPointerGetBlockNumber(&tid);
                if vstate.empty_leaf_set.contains(&leafblk) {
                    todelete.push((off, leafblk));
                }
            }
        }

        // Child before parent for deadlock avoidance: release the parent
        // lock, take the child's, retake the parent's; gistdeletepage
        // re-checks everything that can move meanwhile.
        bufmgr::lock_buffer::call(parent.buffer(), GIST_UNLOCK)?;

        let mut deleted: OffsetNumber = 0;
        for &(off, leafblk) in &todelete {
            // Never remove the last downlink; that would confuse insertion.
            if parent.page().max_offset_number() == FirstOffsetNumber {
                break;
            }
            let leaf = read_pin(info, leafblk)?;
            bufmgr::lock_buffer::call(leaf.buffer(), GIST_EXCLUSIVE)?;
            gistcheckpage(rel, &leaf)?;

            bufmgr::lock_buffer::call(parent.buffer(), GIST_EXCLUSIVE)?;
            if gistdeletepage(vstate.info, vstate.stats, &parent, off - deleted, &leaf)? {
                deleted += 1;
            }
            bufmgr::lock_buffer::call(parent.buffer(), GIST_UNLOCK)?;
            bufmgr::lock_buffer::call(leaf.buffer(), GIST_UNLOCK)?;
            drop(leaf);
        }
        drop(parent);

        // The scan can stop once all downlinks were seen, even if not all
        // could be removed.
        empty_pages_remaining = empty_pages_remaining.saturating_sub(todelete.len());
    }
    Ok(())
}

/// gistdeletepage: both pages locked; re-checks deletability, then marks the
/// leaf deleted and removes the downlink. false = a concurrent update won.
fn gistdeletepage<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: &mut IndexBulkDeleteResult,
    parent: &BufferPin,
    downlink: OffsetNumber,
    leaf: &BufferPin,
) -> PgResult<bool> {
    {
        let leaf_page = leaf.page();
        if !GistPageIsLeaf(&leaf_page) {
            debug_assert!(false, "leaf page became non-leaf");
            return Ok(false);
        }
        if GistFollowRight(&leaf_page) {
            return Ok(false);
        }
        if leaf_page.max_offset_number() != InvalidOffsetNumber {
            return Ok(false);
        }

        let parent_page = parent.page();
        if parent_page.is_new()
            || GistPageIsDeleted(&parent_page)
            || GistPageIsLeaf(&parent_page)
        {
            debug_assert!(false, "internal pages are never deleted");
            return Ok(false);
        }
        if parent_page.max_offset_number() < downlink
            || parent_page.max_offset_number() <= FirstOffsetNumber
        {
            return Ok(false);
        }
        let itup = page_item(&parent_page, downlink);
        // SAFETY: page item under our exclusive content lock.
        let tid = unsafe { itup_get_tid(itup) };
        if leaf.block_number() != ItemPointerGetBlockNumber(&tid) {
            return Ok(false);
        }
    }

    // The page cannot be recycled until in-progress scans that saw the
    // downlink have ended; stamp it with the next XID as that horizon.
    let txid: FullTransactionId = varsup::ReadNextFullTransactionId()?;

    bufmgr::mark_buffer_dirty::call(leaf.buffer())?;
    GistPageSetDeleted(&mut crate::buf_page_mut(leaf.buffer()), txid);
    stats.pages_newly_deleted += 1;
    stats.pages_deleted += 1;

    bufmgr::mark_buffer_dirty::call(parent.buffer())?;
    crate::buf_page_mut(parent.buffer()).index_tuple_delete(downlink);

    let recptr = if crate::relation_needs_wal(info.index) {
        gistXLogPageDelete(leaf, txid, parent, downlink)?
    } else {
        gistGetFakeLSN(info.index)?
    };
    crate::buf_page_mut(parent.buffer()).set_lsn(recptr);
    crate::buf_page_mut(leaf.buffer()).set_lsn(recptr);

    Ok(true)
}
