//! blvacuum.c: blbulkdelete (page-compacting rewrite + notFullPage rebuild)
//! and blvacuumcleanup (deleted/new pages into the FSM).

use crate::state::{buf_page_bytes, init_bloom_state};
use bufmgr::{LockBuffer, UnlockReleaseBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE};
use generic_xlog::{GenericXLogAbort, GenericXLogFinish, GenericXLogStart};
use nbtree::IndexVacuumInfo;
use types_bloom::*;
use types_core::{BlockNumber, ForkNumber};
use types_error::PgResult;
use types_nbtree::genam::IndexBulkDeleteResult;
use types_tuple::itemptr::{
    ItemPointerData, ItemPointerGetBlockNumberNoCheck, ItemPointerGetOffsetNumberNoCheck,
};

fn tid_cmp(a: &ItemPointerData, b: &ItemPointerData) -> core::cmp::Ordering {
    (
        ItemPointerGetBlockNumberNoCheck(a),
        ItemPointerGetOffsetNumberNoCheck(a),
    )
        .cmp(&(
            ItemPointerGetBlockNumberNoCheck(b),
            ItemPointerGetOffsetNumberNoCheck(b),
        ))
}

// The C callback: vacuum's dead-TID list membership.
fn tid_reaped(dead_items: &[ItemPointerData], tid: &ItemPointerData) -> bool {
    dead_items
        .binary_search_by(|probe| tid_cmp(probe, tid))
        .is_ok()
}

fn tuple_heap_ptr(tuple: &[u8]) -> ItemPointerData {
    let hi = u16::from_ne_bytes([tuple[0], tuple[1]]) as u32;
    let lo = u16::from_ne_bytes([tuple[2], tuple[3]]) as u32;
    let off = u16::from_ne_bytes([tuple[4], tuple[5]]);
    ItemPointerData::new((hi << 16) | lo, off)
}

/// blbulkdelete with the reaped-set shape shared by this port's AMs
/// (dead_items replaces C's callback; same membership test).
pub fn blbulkdelete<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    bulkdelete_common(info, istat, &mut |tid| Ok(tid_reaped(dead_items, tid)))
}

/// blbulkdelete with C's collect-only callback shape (validate_index):
/// report every TID, delete nothing.
pub fn blbulkdelete_collect<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    bulkdelete_common(info, None, &mut |tid| {
        callback(tid)?;
        Ok(false)
    })
}

fn bulkdelete_common<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    callback: &mut dyn FnMut(&ItemPointerData) -> PgResult<bool>,
) -> PgResult<IndexBulkDeleteResult> {
    let index = info.index;
    let mut stats = istat.unwrap_or_default();
    let state = init_bloom_state(index)?;
    let size = state.size_of_bloom_tuple;

    let scratch = mcx::MemoryContext::new_bump("blbulkdelete xlog");
    let smcx = scratch.mcx();

    // notFullPage accumulator (C: FreeBlockNumberArray on the stack).
    let mut not_full_page: Vec<BlockNumber> = Vec::new();

    // Iterate over the pages; concurrently-added pages can't hold deletable
    // tuples, so a start-of-scan block count is enough.
    let npages = bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)?;
    for blkno in BLOOM_HEAD_BLKNO..npages {
        vacuum_seams::vacuum_delay_point::call(false)?;

        let buffer = bufmgr::ReadBufferExtended(
            index,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            info.strategy.clone(),
        )?;
        LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;
        let mut gxlog = GenericXLogStart(smcx, index)?;
        let page = gxlog.register_buffer(buffer, 0)?;

        // Ignore empty/deleted pages until blvacuumcleanup().
        if page_is_new(page) || page_is_deleted(page) {
            UnlockReleaseBuffer(buffer)?;
            GenericXLogAbort(gxlog);
            continue;
        }

        // Compact live tuples in place: itup scans, itup_ptr receives.
        let maxoff = opaque_maxoff(page);
        let mut itup_ptr: u16 = 1; // next slot to save a surviving tuple into
        let mut deleted_here = false;
        for offset in 1..=maxoff {
            let src = tuple_off(size, offset);
            let tid = tuple_heap_ptr(&page[src..src + size]);
            if callback(&tid)? {
                // Deleted: don't advance itup_ptr; maxoff shrinks at the end.
                stats.tuples_removed += 1.0;
                deleted_here = true;
            } else {
                if itup_ptr != offset {
                    let dst = tuple_off(size, itup_ptr);
                    page.copy_within(src..src + size, dst);
                }
                itup_ptr += 1;
            }
        }
        let new_maxoff = itup_ptr - 1;
        set_opaque_maxoff(page, new_maxoff);

        // Add page to the new notFullPage list if we won't mark it deleted
        // and it has free space.
        if new_maxoff != 0
            && page_free_space(size, new_maxoff) >= size as isize
            && not_full_page.len() < BLOOM_META_BLOCK_N
        {
            not_full_page.push(blkno);
        }

        if deleted_here {
            // Now-empty page is marked deleted.
            if new_maxoff == 0 {
                page_set_deleted(page);
            }
            // Adjust pd_lower.
            set_pd_lower(page, tuple_off(size, new_maxoff + 1) as u16);
            GenericXLogFinish(gxlog)?;
        } else {
            // Nothing changed: abort WAL-logging.
            GenericXLogAbort(gxlog);
        }
        UnlockReleaseBuffer(buffer)?;
    }

    // Rewrite the metapage's notFullPage list with what we found. Our info
    // may already be stale, but blinsert() copes.
    let buffer = bufmgr::ReadBuffer(index, BLOOM_METAPAGE_BLKNO)?;
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE)?;
    let mut gxlog = GenericXLogStart(smcx, index)?;
    let page = gxlog.register_buffer(buffer, 0)?;
    for (i, &b) in not_full_page.iter().enumerate() {
        meta_set_notfull(page, i, b);
    }
    meta_set_nstart(page, 0);
    meta_set_nend(page, not_full_page.len() as u16);
    GenericXLogFinish(gxlog)?;
    UnlockReleaseBuffer(buffer)?;

    Ok(stats)
}

/// blvacuumcleanup.
pub fn blvacuumcleanup<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let index = info.index;

    if info.analyze_only {
        return Ok(istat);
    }

    let mut stats = istat.unwrap_or_default();

    // Iterate over the pages: deleted pages go to the FSM; count the rest.
    let npages = bufmgr::RelationGetNumberOfBlocksInFork(index, ForkNumber::MAIN_FORKNUM)?;
    stats.num_pages = npages;
    stats.pages_free = 0;
    stats.num_index_tuples = 0.0;
    for blkno in BLOOM_HEAD_BLKNO..npages {
        vacuum_seams::vacuum_delay_point::call(false)?;

        let buffer = bufmgr::ReadBufferExtended(
            index,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            types_storage::storage::ReadBufferMode::Normal,
            info.strategy.clone(),
        )?;
        LockBuffer(buffer, BUFFER_LOCK_SHARE)?;
        let page = buf_page_bytes(buffer);

        if page_is_new(page) || page_is_deleted(page) {
            freespace::RecordFreeIndexPage(index, blkno)?;
            stats.pages_free += 1;
        } else {
            stats.num_index_tuples += opaque_maxoff(page) as f64;
        }

        UnlockReleaseBuffer(buffer)?;
    }

    freespace::IndexFreeSpaceMapVacuum(index)?;

    Ok(Some(stats))
}
