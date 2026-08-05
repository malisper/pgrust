//! ginvacuum.c: ginbulkdelete/ginvacuumcleanup — entry-tree posting-list
//! vacuum, posting-tree leaf vacuum + empty-page deletion, full-relation
//! cleanup stats scan. The bulkdelete callback is monomorphized to the sorted
//! dead-TID slice, as nbtree renders it.

use ::bufmgr_seams as bm;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::nbtree::itup;
use ::types_core::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, BLCKSZ};

use ::types_error::PgResult;
use ::types_nbtree::IndexBulkDeleteResult;
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef, PageTemp};
use ::types_core::OffsetNumber;
use ::types_tuple::itemptr::{
    FirstOffsetNumber, InvalidOffsetNumber, ItemPointerCompare, ItemPointerData,
};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_FORCE_IMAGE, REGBUF_STANDARD};

use crate::datapage::{
    gin_data_leaf_page_is_empty, gin_page_delete_posting_item, ginVacuumPostingTreeLeaf,
    posting_item_at,
};
use crate::entrypage::{
    gin_get_downlink, gin_get_nposting, gin_is_posting_tree, gintuple_get_attrnum,
    gintuple_get_key, ginReadTuple, GinFormTuple, ITup,
};
use crate::fast::ginInsertCleanup;
use crate::util::{gin_page_is_recyclable, initGinState, ginUpdateStats};
use crate::{
    page_bytes, page_mut, page_opaque, page_ref, relation_needs_wal, GinPageIsData,
    GinPageIsLeaf, GinPageIsList, GinPageRightMost, GIN_EXCLUSIVE, GIN_SHARE, GIN_UNLOCK, RM_GIN,
};

pub use ::nbtree::IndexVacuumInfo;

// The bulkdelete callback, monomorphized to its two producers: vac_tid_reaped
// (sorted dead-TID slice) and validate_index's never-delete collector.
pub(crate) enum GinVacDelete<'a> {
    DeadItems(&'a [ItemPointerData]),
    Collect(&'a mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + 'a)),
}

pub(crate) struct GinVacuumState<'a, 'cb, 'r, 'st> {
    pub rel: &'a Relation<'r>,
    pub state: &'st GinState,
    pub delete: GinVacDelete<'cb>,
    pub stats: &'a mut IndexBulkDeleteResult,
}

fn am_autovacuum_worker() -> bool {
    miscinit::GetMyBackendType() == ::types_core::BackendType::AutovacWorker
}

fn vacuum_delay_point() -> PgResult<()> {
    crate::check_for_interrupts()?;
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

/// ginVacuumItemPointers: None when nothing needs removal, otherwise the
/// surviving items (possibly empty).
pub(crate) fn ginVacuumItemPointers<'s>(
    mcx: Mcx<'s>,
    gvs: &mut GinVacuumState<'_, '_, '_, '_>,
    items: &[ItemPointerData],
) -> PgResult<Option<PgVec<'s, ItemPointerData>>> {
    let mut tmpitems: Option<PgVec<'s, ItemPointerData>> = None;
    for (i, item) in items.iter().enumerate() {
        let dead = match &mut gvs.delete {
            GinVacDelete::DeadItems(dead_items) => tid_is_dead(dead_items, item),
            GinVacDelete::Collect(callback) => {
                callback(item)?;
                false
            }
        };
        if dead {
            gvs.stats.tuples_removed += 1.0;
            if tmpitems.is_none() {
                let mut v = mcx::vec_with_capacity_in(mcx, items.len())?;
                crate::vec_append(&mut v, &items[..i])?;
                tmpitems = Some(v);
            }
        } else {
            gvs.stats.num_index_tuples += 1.0;
            if let Some(v) = tmpitems.as_mut() {
                v.push(*item);
            }
        }
    }
    Ok(tmpitems)
}

/// xlogVacuumPage: full-image WAL record for an entry-tree leaf.
fn xlog_vacuum_page(rel: &Relation<'_>, buffer: Buffer) -> PgResult<()> {
    if !relation_needs_wal(rel) {
        return Ok(());
    }
    let recptr = ::xloginsert_seams::xlog_insert_record::call(
        RM_GIN,
        XLOG_GIN_VACUUM_PAGE,
        0,
        &[],
        &[XLogRegBuf {
            block_id: 0,
            buffer,
            flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
            bufdata: &[],
        }],
    )?;
    // SAFETY: pin + exclusive lock held.
    unsafe { page_mut(buffer) }.set_lsn(recptr);
    Ok(())
}

/// ginDeletePage. All three pages are already exclusively locked by
/// ginScanToDelete's stack; this function adds pins only.
fn ginDeletePage(
    gvs: &mut GinVacuumState<'_, '_, '_, '_>,
    delete_blkno: BlockNumber,
    left_blkno: BlockNumber,
    parent_blkno: BlockNumber,
    myoff: OffsetNumber,
) -> PgResult<()> {
    let rel = gvs.rel;
    let l_buffer = bm::read_buffer::call(rel, left_blkno)?;
    let d_buffer = bm::read_buffer::call(rel, delete_blkno)?;
    let p_buffer = bm::read_buffer::call(rel, parent_blkno)?;

    // SAFETY: pin + exclusive lock held (scan stack).
    let rightlink = { page_opaque(&unsafe { page_ref(d_buffer) }).rightlink };

    predicate_seams::predicate_lock_page_combine::call(rel, delete_blkno, rightlink)?;

    {
        // SAFETY: pin + exclusive lock held.
        let mut lpage = unsafe { page_mut(l_buffer) };
        let mut lopaque = page_opaque(&lpage.as_ref());
        lopaque.rightlink = rightlink;
        crate::write_opaque(&mut lpage, &lopaque);
    }
    {
        // SAFETY: pin + exclusive lock held.
        let mut ppage = unsafe { page_mut(p_buffer) };
        // SAFETY: borrow confined here.
        let bytes = unsafe { crate::page_bytes_mut(&mut ppage) };
        debug_assert!(
            PostingItemGetBlockNumber(&posting_item_at(bytes, myoff)) == delete_blkno
        );
        gin_page_delete_posting_item(bytes, myoff);
    }
    let delete_xid = varsup_seams::read_next_transaction_id::call()?;
    {
        // GinPageSetDeleted wipes the flags; GinPageSetDeleteXid = pd_prune_xid.
        // SAFETY: pin + exclusive lock held.
        let mut dpage = unsafe { page_mut(d_buffer) };
        let mut dopaque = page_opaque(&dpage.as_ref());
        dopaque.flags = GIN_DELETED;
        crate::write_opaque(&mut dpage, &dopaque);
        dpage.set_prune_xid(delete_xid);
    }

    bm::mark_buffer_dirty::call(p_buffer)?;
    bm::mark_buffer_dirty::call(l_buffer)?;
    bm::mark_buffer_dirty::call(d_buffer)?;

    if relation_needs_wal(rel) {
        let data = crate::wal::ginxlog_delete_page(myoff, rightlink, delete_xid);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_GIN,
            XLOG_GIN_DELETE_PAGE,
            0,
            &[&data],
            &[
                XLogRegBuf { block_id: 0, buffer: d_buffer, flags: 0, bufdata: &[] },
                XLogRegBuf { block_id: 1, buffer: p_buffer, flags: REGBUF_STANDARD, bufdata: &[] },
                XLogRegBuf { block_id: 2, buffer: l_buffer, flags: 0, bufdata: &[] },
            ],
        )?;
        // SAFETY: pins + exclusive locks held.
        unsafe {
            page_mut(d_buffer).set_lsn(recptr);
            page_mut(p_buffer).set_lsn(recptr);
            page_mut(l_buffer).set_lsn(recptr);
        }
    }

    bm::release_buffer::call(p_buffer)?;
    bm::release_buffer::call(l_buffer)?;
    bm::release_buffer::call(d_buffer)?;

    gvs.stats.pages_newly_deleted += 1;
    gvs.stats.pages_deleted += 1;
    Ok(())
}

// DataPageDeleteStack level; left_buffer persists across sibling visits.
struct DeleteLevel {
    blkno: BlockNumber,
    left_buffer: Buffer,
}

/// ginScanToDelete at one page; returns true when the page was deleted.
fn ginScanToDelete(
    gvs: &mut GinVacuumState<'_, '_, '_, '_>,
    blkno: BlockNumber,
    depth: usize,
    levels: &mut Vec<DeleteLevel>,
    myoff: OffsetNumber,
) -> PgResult<bool> {
    let is_root = depth == 0;
    if levels.len() <= depth {
        levels.push(DeleteLevel {
            blkno: InvalidBlockNumber,
            left_buffer: InvalidBuffer,
        });
    }

    let buffer = bm::read_buffer::call(gvs.rel, blkno)?;
    if !is_root {
        bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
    }

    // SAFETY: pin held; root cleanup-locked by the caller, others exclusive.
    let is_leaf = {
        let opaque = page_opaque(&unsafe { page_ref(buffer) });
        debug_assert!(GinPageIsData(&opaque));
        GinPageIsLeaf(&opaque)
    };

    if !is_leaf {
        levels[depth].blkno = blkno;
        let mut i = FirstOffsetNumber;
        loop {
            // Re-read maxoff every iteration: deleting a child removes its
            // posting item from this page.
            // SAFETY: as above.
            let maxoff = { page_opaque(&unsafe { page_ref(buffer) }).maxoff };
            if i > maxoff {
                break;
            }
            let child = {
                // SAFETY: as above.
                let page = unsafe { page_ref(buffer) };
                PostingItemGetBlockNumber(&posting_item_at(page_bytes(&page), i))
            };
            if !ginScanToDelete(gvs, child, depth + 1, levels, i)? {
                i += 1;
            }
        }
        // SAFETY: as above.
        let rightmost = { GinPageRightMost(&page_opaque(&unsafe { page_ref(buffer) })) };
        if rightmost && levels.len() > depth + 1 && levels[depth + 1].left_buffer != InvalidBuffer
        {
            let lb = levels[depth + 1].left_buffer;
            bm::lock_buffer::call(lb, GIN_UNLOCK)?;
            bm::release_buffer::call(lb)?;
            levels[depth + 1].left_buffer = InvalidBuffer;
        }
    }

    // SAFETY: as above.
    let (isempty, rightmost) = {
        let page = unsafe { page_ref(buffer) };
        let opaque = page_opaque(&page);
        let isempty = if GinPageIsLeaf(&opaque) {
            gin_data_leaf_page_is_empty(page_bytes(&page))
        } else {
            opaque.maxoff < FirstOffsetNumber
        };
        (isempty, GinPageRightMost(&opaque))
    };

    let mut me_delete = false;
    if isempty && levels[depth].left_buffer != InvalidBuffer && !rightmost {
        debug_assert!(!is_root);
        let left_blkno = bm::buffer_get_block_number::call(levels[depth].left_buffer);
        let parent_blkno = levels[depth - 1].blkno;
        ginDeletePage(gvs, blkno, left_blkno, parent_blkno, myoff)?;
        me_delete = true;
    }

    if !me_delete {
        if levels[depth].left_buffer != InvalidBuffer {
            let lb = levels[depth].left_buffer;
            bm::lock_buffer::call(lb, GIN_UNLOCK)?;
            bm::release_buffer::call(lb)?;
        }
        levels[depth].left_buffer = buffer;
    } else {
        if !is_root {
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        }
        bm::release_buffer::call(buffer)?;
    }

    if is_root {
        // The caller holds the cleanup lock + its own pin; drop only ours.
        // (The root level's left_buffer keeps no pin of its own here.)
        if levels[depth].left_buffer == buffer {
            levels[depth].left_buffer = InvalidBuffer;
        }
        bm::release_buffer::call(buffer)?;
    }

    Ok(me_delete)
}

/// ginVacuumPostingTreeLeaves: leftmost descent, then rightlink walk vacuuming
/// each leaf. Returns true when at least one leaf came out empty.
fn ginVacuumPostingTreeLeaves(
    gvs: &mut GinVacuumState<'_, '_, '_, '_>,
    root_blkno: BlockNumber,
) -> PgResult<bool> {
    let rel = gvs.rel;
    let mut blkno = root_blkno;
    let mut buffer;
    loop {
        buffer = bm::read_buffer::call(rel, blkno)?;
        bm::lock_buffer::call(buffer, GIN_SHARE)?;
        // SAFETY: pin + share lock held.
        let (is_leaf, first_child) = {
            let page = unsafe { page_ref(buffer) };
            let opaque = page_opaque(&page);
            debug_assert!(GinPageIsData(&opaque));
            if GinPageIsLeaf(&opaque) {
                (true, InvalidBlockNumber)
            } else {
                let pitem = posting_item_at(page_bytes(&page), FirstOffsetNumber);
                (false, PostingItemGetBlockNumber(&pitem))
            }
        };
        if is_leaf {
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
            bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
            break;
        }
        debug_assert!(first_child != InvalidBlockNumber);
        blkno = first_child;
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::release_buffer::call(buffer)?;
    }

    let mut has_void_page = false;
    loop {
        let mut tmp_ctx = MemoryContext::new_bump("Gin vacuum temporary context");
        ginVacuumPostingTreeLeaf(tmp_ctx.mcx(), gvs, buffer)?;
        // SAFETY: pin + exclusive lock held.
        let (empty, rightlink) = {
            let page = unsafe { page_ref(buffer) };
            (
                gin_data_leaf_page_is_empty(page_bytes(&page)),
                page_opaque(&page).rightlink,
            )
        };
        if empty {
            has_void_page = true;
        }
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::release_buffer::call(buffer)?;
        tmp_ctx.reset();

        if rightlink == InvalidBlockNumber {
            break;
        }
        buffer = bm::read_buffer::call(rel, rightlink)?;
        bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
    }
    Ok(has_void_page)
}

/// ginVacuumPostingTree.
fn ginVacuumPostingTree(
    gvs: &mut GinVacuumState<'_, '_, '_, '_>,
    root_blkno: BlockNumber,
) -> PgResult<()> {
    if !ginVacuumPostingTreeLeaves(gvs, root_blkno)? {
        return Ok(());
    }
    // At least one empty leaf: rescan the tree deleting empty pages under a
    // cleanup lock on the root.
    let buffer = bm::read_buffer::call(gvs.rel, root_blkno)?;
    bm::lock_buffer_for_cleanup::call(buffer)?;

    let mut levels: Vec<DeleteLevel> = Vec::new();
    ginScanToDelete(gvs, root_blkno, 0, &mut levels, InvalidOffsetNumber)?;
    debug_assert!(levels.iter().all(|l| l.left_buffer == InvalidBuffer));

    bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(buffer)?;
    Ok(())
}

/// ginVacuumEntryPage: copy-on-write vacuum of one entry-tree leaf; collects
/// posting-tree roots for deferred processing. None when nothing changed.
fn ginVacuumEntryPage<'s>(
    scratch: Mcx<'s>,
    gvs: &mut GinVacuumState<'_, '_, '_, '_>,
    buffer: Buffer,
    roots: &mut Vec<BlockNumber>,
) -> PgResult<Option<PageTemp>> {
    roots.clear();
    let rel = gvs.rel;
    // SAFETY: pin + exclusive lock held by the caller.
    let orig = unsafe { page_ref(buffer) };
    let maxoff = orig.max_offset_number();
    let mut tmp: Option<PageTemp> = None;

    let mut i = FirstOffsetNumber;
    while i <= maxoff {
        let itup: ITup = {
            let get = |page: &PageRef<'_>| {
                let id = page.item_id(i);
                page.item_raw(id).0
            };
            match tmp.as_mut() {
                // SAFETY: owned temp image.
                Some(t) => get(&unsafe {
                    PageRef::from_raw(
                        core::ptr::NonNull::new(t.as_mut_bytes().as_mut_ptr()).unwrap(),
                    )
                }),
                None => get(&orig),
            }
        };

        // SAFETY: tuple bytes live in the (pinned page / owned temp) image.
        unsafe {
            if gin_is_posting_tree(itup) {
                // Deferred: vacuuming the tree now risks deadlock with scans.
                roots.push(gin_get_downlink(itup));
            } else if gin_get_nposting(itup) > 0 {
                let mut items: PgVec<'_, ItemPointerData> = mcx::vec_new_in(scratch);
                ginReadTuple(scratch, itup, &mut items)?;

                let Some(cleaned) = ginVacuumItemPointers(scratch, gvs, items.as_slice())?
                else {
                    i += 1;
                    continue;
                };

                if tmp.is_none() {
                    let mut t = PageTemp::new(BLCKSZ)?;
                    t.as_mut_bytes().copy_from_slice(page_bytes(&orig));
                    tmp = Some(t);
                }
                let t = tmp.as_mut().unwrap();
                // Re-resolve itup inside the temp image.
                let itup: ITup = {
                    let page = PageRef::from_raw(
                        core::ptr::NonNull::new(t.as_mut_bytes().as_mut_ptr()).unwrap(),
                    );
                    let id = page.item_id(i);
                    page.item_raw(id).0
                };

                let (plist, nitems) = if !cleaned.is_empty() {
                    let n = cleaned.len();
                    let (packed, npacked) =
                        crate::postinglist::ginCompressPostingList(
                            scratch,
                            cleaned.as_slice(),
                            GinMaxItemSize,
                        )?;
                    debug_assert!(npacked == n);
                    (packed, n)
                } else {
                    (mcx::vec_new_in(scratch), 0)
                };

                // Form the replacement before deleting: key borrows the old
                // tuple's bytes (C keeps the same order).
                let attnum = gintuple_get_attrnum(gvs.state, itup);
                let mut category = GIN_CAT_NORM_KEY;
                let key = gintuple_get_key(scratch, rel, gvs.state, itup, &mut category)?;
                let newtup = GinFormTuple(
                    scratch,
                    rel,
                    gvs.state,
                    attnum,
                    key,
                    category,
                    plist.as_slice(),
                    plist.len(),
                    nitems,
                    true,
                )?
                .expect("errorTooBig");

                // SAFETY: owned temp image.
                let mut pm = PageMut::from_raw(
                    core::ptr::NonNull::new(t.as_mut_bytes().as_mut_ptr()).unwrap(),
                );
                pm.index_tuple_delete(i);
                let bytes =
                    core::slice::from_raw_parts(newtup.as_ptr(), itup::index_tuple_size(newtup.as_ptr()));
                if pm.add_item(bytes, i, 0) != Some(i) {
                    panic!("failed to add item to index page in \"{}\"", rel.name());
                }
            }
        }
        i += 1;
    }

    Ok(tmp)
}

/// ginbulkdelete.
pub fn ginbulkdelete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    ginbulkdelete_guts(mcx, info, stats, GinVacDelete::DeadItems(dead_items))
}

/// ginbulkdelete with C's collect-only callback shape (validate_index).
pub fn ginbulkdelete_collect<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    ginbulkdelete_guts(mcx, info, None, GinVacDelete::Collect(callback))
}

fn ginbulkdelete_guts<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    delete: GinVacDelete<'_>,
) -> PgResult<IndexBulkDeleteResult> {
    let rel = info.index;
    let state = initGinState(rel)?;

    let mut stats = match stats {
        Some(s) => s,
        None => {
            // First time through: clean up pending insertions.
            let mut s = IndexBulkDeleteResult::default();
            ginInsertCleanup(
                mcx,
                rel,
                &state,
                !am_autovacuum_worker(),
                false,
                true,
                Some(&mut s),
            )?;
            s
        }
    };
    stats.num_index_tuples = 0.0;

    let mut gvs = GinVacuumState {
        rel,
        state: &state,
        delete,
        stats: &mut stats,
    };

    // Find the leftmost leaf of the entry tree.
    let mut blkno = GIN_ROOT_BLKNO;
    let mut buffer = bm::read_buffer::call(rel, blkno)?;
    loop {
        bm::lock_buffer::call(buffer, GIN_SHARE)?;
        // SAFETY: pin + share lock held.
        let (is_leaf, downlink) = {
            let page = unsafe { page_ref(buffer) };
            let opaque = page_opaque(&page);
            debug_assert!(!GinPageIsData(&opaque));
            if GinPageIsLeaf(&opaque) {
                (true, InvalidBlockNumber)
            } else {
                let id = page.item_id(FirstOffsetNumber);
                // SAFETY: as above.
                (false, unsafe { gin_get_downlink(page.item_raw(id).0) })
            }
        };
        if is_leaf {
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
            bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
            if blkno == GIN_ROOT_BLKNO {
                // SAFETY: pin + exclusive lock held.
                let still_leaf =
                    { GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buffer) })) };
                if !still_leaf {
                    bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
                    continue;
                }
            }
            break;
        }
        debug_assert!(downlink != InvalidBlockNumber);
        blkno = downlink;
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::release_buffer::call(buffer)?;
        buffer = bm::read_buffer::call(rel, blkno)?;
    }

    let mut roots: Vec<BlockNumber> = Vec::new();
    loop {
        let mut scratch = MemoryContext::new_bump("Gin vacuum temporary context");
        let res_page = ginVacuumEntryPage(scratch.mcx(), &mut gvs, buffer, &mut roots)?;

        // SAFETY: pin + exclusive lock held.
        blkno = { page_opaque(&unsafe { page_ref(buffer) }).rightlink };

        if let Some(tmp) = res_page {
            {
                // PageRestoreTempPage.
                // SAFETY: pin + exclusive lock held.
                let mut page = unsafe { page_mut(buffer) };
                // SAFETY: borrow confined here.
                let bytes = unsafe { crate::page_bytes_mut(&mut page) };
                bytes.copy_from_slice(tmp.as_bytes());
            }
            bm::mark_buffer_dirty::call(buffer)?;
            xlog_vacuum_page(rel, buffer)?;
        }
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::release_buffer::call(buffer)?;

        vacuum_delay_point()?;

        for ri in 0..roots.len() {
            ginVacuumPostingTree(&mut gvs, roots[ri])?;
            vacuum_delay_point()?;
        }
        scratch.reset();

        if blkno == InvalidBlockNumber {
            break;
        }
        buffer = bm::read_buffer::call(rel, blkno)?;
        bm::lock_buffer::call(buffer, GIN_EXCLUSIVE)?;
    }

    Ok(stats)
}

/// ginvacuumcleanup.
pub fn ginvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    let rel = info.index;

    if info.analyze_only {
        // Autovacuum analyze cleans up pending insertions; plain ANALYZE is a
        // no-op.
        if am_autovacuum_worker() {
            let state = initGinState(rel)?;
            let mut s = stats.unwrap_or_default();
            ginInsertCleanup(mcx, rel, &state, false, true, true, Some(&mut s))?;
            return Ok(Some(s));
        }
        return Ok(stats);
    }

    let mut stats = match stats {
        Some(s) => s,
        None => {
            let state = initGinState(rel)?;
            let mut s = IndexBulkDeleteResult::default();
            ginInsertCleanup(
                mcx,
                rel,
                &state,
                !am_autovacuum_worker(),
                false,
                true,
                Some(&mut s),
            )?;
            s
        }
    };

    // XXX (C): report the heap tuple count as the index entry count.
    stats.num_index_tuples = info.num_heap_tuples.max(0.0);
    stats.estimated_count = info.estimated_count;

    // LockRelationForExtension: single-backend no-op.
    let npages = bm::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;

    let mut idx_stat = GinStatsData::default();
    let mut tot_free_pages: BlockNumber = 0;

    for blkno in GIN_ROOT_BLKNO..npages {
        vacuum_delay_point()?;

        let buffer = bm::read_buffer::call(rel, blkno)?;
        bm::lock_buffer::call(buffer, GIN_SHARE)?;
        // SAFETY: pin + share lock held.
        {
            let page = unsafe { page_ref(buffer) };
            let opaque = page_opaque(&page);
            if gin_page_is_recyclable(buffer)? {
                debug_assert!(blkno != GIN_ROOT_BLKNO);
                freespace::RecordFreeIndexPage(rel, blkno)?;
                tot_free_pages += 1;
            } else if GinPageIsData(&opaque) {
                idx_stat.nDataPages += 1;
            } else if !GinPageIsList(&opaque) {
                idx_stat.nEntryPages += 1;
                if GinPageIsLeaf(&opaque) {
                    idx_stat.nEntries += page.max_offset_number() as i64;
                }
            }
        }
        bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
        bm::release_buffer::call(buffer)?;
    }

    idx_stat.nTotalPages = npages;
    ginUpdateStats(rel, &idx_stat, false)?;

    freespace::IndexFreeSpaceMapVacuum(rel)?;

    stats.pages_free = tot_free_pages;
    stats.num_pages =
        bm::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;

    Ok(Some(stats))
}
