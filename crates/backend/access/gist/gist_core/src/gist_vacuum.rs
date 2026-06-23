//! GiST VACUUM layer (`access/gist/gistvacuum.c`): the AM bulk-delete and
//! cleanup callbacks (`gistbulkdelete` / `gistvacuumcleanup`), the physical
//! page scan (`gistvacuumscan` / `gistvacuumpage`), and the second-stage empty
//! leaf-page deletion + downlink removal (`gistvacuum_delete_empty_pages` /
//! `gistdeletepage`).
//!
//! Model notes (owned tree vs. C):
//!   * The `GistVacState` working state is a Rust struct. The C
//!     `page_set_context` GenerationContext that backs the two `IntegerSet`s is
//!     subsumed by Rust ownership — the [`IntegerSet`]s own their allocations
//!     and are dropped at the end of `gistvacuumscan`.
//!   * The C `ReadStream` over `block_range_read_stream_cb` is a read-ahead
//!     optimization over a growing physical block range. We mirror its exact
//!     semantics with the same outer relation-length re-check loop and an inner
//!     per-block `ReadBufferExtended` walk (the read stream only ever returns
//!     buffers for blocks in `[current_blocknum, last_exclusive)` in ascending
//!     order, then `InvalidBuffer`). The VACUUM buffer-access strategy is held
//!     by the bufmgr seam provider.
//!   * Page bytes are reached through the bufmgr seam: a snapshot read via a
//!     `with_buffer_page` copy-out, in-place writes via `with_buffer_page`.
//!   * The dead-TID test (the `IndexBulkDeleteCallback`) is the
//!     `vacuum_tid_is_dead` seam keyed by the `callback_state` handle; `None`
//!     is the C NULL callback (cleanup-only pass).
//!   * The WAL emission (`gistXLogUpdate` / `gistXLogPageDelete` /
//!     `gistGetFakeLSN`) crosses into the unported gistxlog (F7) lane through
//!     `backend-access-gist-core-seams`; those panic until F7 lands.

#![allow(clippy::too_many_arguments)]

use alloc::vec::Vec;

use integerset::{intset_create, IntegerSet};
use mcx::Mcx;

use bufmgr_seams as bufmgr;
use page::{
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIndexMultiDelete, PageIndexTupleDelete,
    PageIsNew, PageMut, PageRef,
};
use utils_error::PgResult;
use types_error::error::LOG;

use types_core::primitive::{BlockNumber, InvalidBlockNumber, OffsetNumber};
use types_core::xact::FullTransactionId;
use rel::Relation;
use types_storage::buf::BufferIsValid;
use types_storage::bufpage::MaxOffsetNumber;
use types_storage::Buffer;
use types_tableam::genam::{IndexBulkDeleteResult, IndexVacuumInfo};
use types_tuple::heaptuple::{FIRST_OFFSET_NUMBER, INVALID_OFFSET_NUMBER};
use gist::{GistNSN, GIST_ROOT_BLKNO};

use crate::gist_page::{
    gist_page_get_nsn, gist_page_rightlink, GistFollowRight, GistMarkTuplesDeleted, GistPageIsDeleted,
    GistPageIsLeaf, GistPageSetDeleted,
};
use crate::gist_page::gistcheckpage;
use crate::gistutil::{gist_page_recyclable, gist_tuple_is_invalid, itup_block_number};

use crate::gistxlog as xlog;
use elog_seams as elog;

// ===========================================================================
// Buffer-lock modes (gist_private.h: GIST_* = BUFFER_LOCK_*).
// ===========================================================================

/// `GIST_UNLOCK` = `BUFFER_LOCK_UNLOCK` (bufmgr.h).
const GIST_UNLOCK: i32 = 0;
/// `GIST_SHARE` = `BUFFER_LOCK_SHARE`.
const GIST_SHARE: i32 = 1;
/// `GIST_EXCLUSIVE` = `BUFFER_LOCK_EXCLUSIVE`.
const GIST_EXCLUSIVE: i32 = 2;

// ===========================================================================
// GistVacState (gistvacuum.c:29).
// ===========================================================================

/// `GistVacState` (gistvacuum.c:29): working state carried through a
/// bulk-delete pass.
struct GistVacState<'a, 'mcx> {
    /// `info` — a borrow of the [`IndexVacuumInfo`].
    info: &'a IndexVacuumInfo<'mcx>,
    /// `stats` — the running [`IndexBulkDeleteResult`].
    stats: IndexBulkDeleteResult,
    /// The `callback_state` handle keying the `vacuum_tid_is_dead` seam, or
    /// `None` for the C NULL callback (a cleanup-only pass).
    callback_state: Option<u64>,
    /// `startNSN`.
    start_nsn: GistNSN,
    /// `internal_page_set` — every internal page seen, in ascending order.
    internal_page_set: IntegerSet,
    /// `empty_leaf_set` — every empty leaf page seen, in ascending order.
    empty_leaf_set: IntegerSet,
}

// ---------------------------------------------------------------------------
// Substrate seam helpers (thin name-faithful wrappers).
// ---------------------------------------------------------------------------

fn read_buffer_extended<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    bufmgr::read_buffer_extended::call(rel, blkno)
}
fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, mode)
}
fn unlock_release_buffer(buffer: Buffer) {
    bufmgr::unlock_release_buffer::call(buffer)
}
fn release_buffer(buffer: Buffer) {
    bufmgr::release_buffer::call(buffer)
}
fn mark_buffer_dirty(buffer: Buffer) {
    bufmgr::mark_buffer_dirty::call(buffer)
}
fn buffer_get_block_number(buffer: Buffer) -> BlockNumber {
    bufmgr::buffer_get_block_number::call(buffer)
}
fn page_set_lsn(buffer: Buffer, lsn: u64) -> PgResult<()> {
    bufmgr::page_set_lsn::call(buffer, lsn)
}

/// `BufferGetPage(buffer)` copied out as an owned image.
fn page_bytes(buffer: Buffer) -> PgResult<Vec<u8>> {
    let mut out = Vec::new();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        out = page.to_vec();
        Ok(())
    })?;
    Ok(out)
}

fn relation_needs_wal<'mcx>(rel: &Relation<'mcx>) -> bool {
    relcache_seams::relation_needs_wal::call(rel)
}
fn relation_is_local<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool> {
    hio_seams::relation_is_local::call(rel.rd_id)
}
fn relation_get_number_of_blocks<'mcx>(rel: &Relation<'mcx>) -> PgResult<BlockNumber> {
    relcache_seams::relation_get_number_of_blocks::call(rel)
}
fn lock_relation_for_extension<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    // The C explicitly UnlockRelationForExtension; leak the RAII guard so the
    // explicit unlock seam performs the release (mirror GIN's vacuum).
    let guard = lmgr_seams::lock_relation_for_extension::call(rel)?;
    core::mem::forget(guard);
    Ok(())
}
fn unlock_relation_for_extension<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    lmgr_seams::unlock_relation_for_extension::call(rel.rd_locator.dbOid, rel.rd_id)
}
fn record_free_index_page<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<()> {
    freespace_seams::record_free_index_page::call(rel, blkno)
}
fn index_free_space_map_vacuum<'mcx>(rel: &Relation<'mcx>) -> PgResult<()> {
    freespace_seams::index_free_space_map_vacuum::call(rel)
}
fn vacuum_delay_point() -> PgResult<()> {
    vacuumlazy_seams::vacuum_delay_point::call(false)
}
fn read_next_full_transaction_id() -> FullTransactionId {
    varsup_seams::read_next_full_transaction_id::call()
}
fn get_insert_rec_ptr() -> u64 {
    transam_xlog_seams::get_insert_rec_ptr::call()
}
fn gist_get_fake_lsn<'mcx>(rel: &Relation<'mcx>) -> PgResult<u64> {
    xlog::gist_get_fake_lsn(rel)
}

/// The index-vacuum dead-TID test (the `IndexBulkDeleteCallback`).
fn vacuum_tid_is_dead(
    tid: types_tuple::heaptuple::ItemPointerData,
    callback_state: u64,
) -> bool {
    vacuum_seams::vacuum_tid_is_dead::call(tid, callback_state)
}

// ===========================================================================
// gistbulkdelete (gistvacuum.c:59) — VACUUM bulkdelete stage.
// ===========================================================================

/// `gistbulkdelete(info, stats, callback, callback_state)` (gistvacuum.c:59):
/// remove index entries. Allocates `stats` if first time through (the C
/// `palloc0`), then drives `gistvacuumscan`.
pub fn gistbulkdelete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    callback_state: Option<u64>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    // allocate stats if first time through, else re-use existing struct.
    let stats = stats.unwrap_or_default();

    let stats = gistvacuumscan(mcx, info, stats, callback_state)?;

    Ok(Some(stats))
}

// ===========================================================================
// gistvacuumcleanup (gistvacuum.c:74) — VACUUM cleanup stage.
// ===========================================================================

/// `gistvacuumcleanup(info, stats)` (gistvacuum.c:74): delete empty pages and
/// update index statistics.
pub fn gistvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    // No-op in ANALYZE ONLY mode.
    if info.analyze_only {
        return Ok(stats);
    }

    // If gistbulkdelete was called, we need not do anything, just return the
    // stats from the latest gistbulkdelete call. If it wasn't called, we still
    // need to do a pass over the index, to obtain index statistics.
    let mut stats = match stats {
        Some(s) => s,
        None => {
            let s = IndexBulkDeleteResult::default();
            gistvacuumscan(mcx, info, s, None)?
        }
    };

    // It's quite possible for us to be fooled by concurrent page splits into
    // double-counting some index tuples, so disbelieve any total that exceeds
    // the underlying heap's count ... if we know that accurately. Otherwise
    // this might just make matters worse.
    if !info.estimated_count {
        if stats.num_index_tuples > info.num_heap_tuples {
            stats.num_index_tuples = info.num_heap_tuples;
        }
    }

    Ok(Some(stats))
}

// ===========================================================================
// gistvacuumscan (gistvacuum.c:124).
// ===========================================================================

/// `gistvacuumscan(info, stats, callback, callback_state)` (gistvacuum.c:124):
/// scan the index for VACUUMing purposes, deleting dead leaf tuples, noting
/// empty leaf and internal pages, then deleting the empty leaf pages.
fn gistvacuumscan<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    mut stats: IndexBulkDeleteResult,
    callback_state: Option<u64>,
) -> PgResult<IndexBulkDeleteResult> {
    let rel = &info.index;

    // Reset fields that track information about the entire index now. (Avoid
    // resetting tuples_removed and pages_newly_deleted, which last across each
    // gistvacuumscan() call. pages_free is whole-index state and is reset here,
    // matching the C — RecordFreeIndexPage is idempotent.)
    stats.num_pages = 0;
    stats.estimated_count = false;
    stats.num_index_tuples = 0.0;
    stats.pages_deleted = 0;
    stats.pages_free = 0;

    let mut vstate = GistVacState {
        info,
        stats,
        callback_state,
        // startNSN is filled below.
        start_nsn: 0,
        internal_page_set: intset_create()?,
        empty_leaf_set: intset_create()?,
    };

    if relation_needs_wal(rel) {
        vstate.start_nsn = get_insert_rec_ptr();
    } else {
        vstate.start_nsn = gist_get_fake_lsn(rel)?;
    }

    // We can skip locking for new or temp relations.
    let need_lock = !relation_is_local(rel)?;

    // current_blocknum starts at the root.
    let mut current_blocknum: BlockNumber = GIST_ROOT_BLKNO;
    // num_pages is (re)read inside the loop before any use; the loop always runs
    // at least once, so this initial value is never observed.
    let mut num_pages: BlockNumber;

    // The outer loop iterates over all index pages, in physical order, in a
    // growing block range (re-checking the relation length each round under the
    // relation-extension lock — see the long comment in the C). The read-stream
    // optimization is mirrored as a plain ascending per-block walk.
    loop {
        // Get the current relation length.
        if need_lock {
            lock_relation_for_extension(rel)?;
        }
        num_pages = relation_get_number_of_blocks(rel)?;
        if need_lock {
            unlock_relation_for_extension(rel)?;
        }

        // Quit if we've scanned the whole relation.
        if current_blocknum >= num_pages {
            break;
        }

        let last_exclusive = num_pages;

        // Iterate over pages, then loop back to recheck relation length.
        while current_blocknum < last_exclusive {
            // call vacuum_delay_point while not holding any buffer lock.
            vacuum_delay_point()?;

            let buf = read_buffer_extended(rel, current_blocknum)?;
            current_blocknum += 1;

            if !BufferIsValid(buf) {
                break;
            }

            gistvacuumpage(mcx, &mut vstate, buf)?;
        }
    }

    // If we found any recyclable pages (and recorded them in the FSM), then
    // forcibly update the upper-level FSM pages so searchers can find them.
    if vstate.stats.pages_free > 0 {
        index_free_space_map_vacuum(rel)?;
    }

    // update statistics.
    vstate.stats.num_pages = num_pages;

    // If we saw any empty pages, try to unlink them from the tree so that they
    // can be reused.
    gistvacuum_delete_empty_pages(mcx, &mut vstate)?;

    // The internal and empty page sets are dropped here (the C deletes
    // page_set_context).
    Ok(vstate.stats)
}

// ===========================================================================
// gistvacuumpage (gistvacuum.c:307).
// ===========================================================================

/// `gistvacuumpage(vstate, buffer)` (gistvacuum.c:307): VACUUM one page.
/// `orig_blkno` is the highest block number reached by the outer scan; it
/// equals `blkno` unless we recurse to reexamine a previous page (after a
/// concurrent split). The C tail-recurses via a goto loop; we mirror that with
/// a `loop`.
fn gistvacuumpage<'mcx>(
    mcx: Mcx<'mcx>,
    vstate: &mut GistVacState<'_, 'mcx>,
    mut buffer: Buffer,
) -> PgResult<()> {
    let rel = &vstate.info.index;
    // orig_blkno is the highest block number reached by the outer scan loop;
    // it equals blkno unless we recurse to reexamine a previous page.
    let orig_blkno = buffer_get_block_number(buffer);
    let mut blkno = orig_blkno;

    loop {
        let mut recurse_to: BlockNumber = InvalidBlockNumber;

        // We are not going to stay here for a long time, aggressively grab an
        // exclusive lock.
        lock_buffer(buffer, GIST_EXCLUSIVE)?;
        let mut page = page_bytes(buffer)?;

        if gist_page_recyclable(&page)? {
            // Okay to recycle this page.
            record_free_index_page(rel, blkno)?;
            vstate.stats.pages_deleted += 1;
            vstate.stats.pages_free += 1;
        } else if GistPageIsDeleted(&page)? {
            // Already deleted, but can't recycle yet.
            vstate.stats.pages_deleted += 1;
        } else if GistPageIsLeaf(&page)? {
            let mut todelete: Vec<OffsetNumber> = Vec::new();
            let opaque_rightlink = gist_page_rightlink(&page)?;
            let mut maxoff = {
                let pref = PageRef::new(&page)?;
                PageGetMaxOffsetNumber(&pref)
            };

            // Check whether we need to recurse back to earlier pages, in case a
            // page split since the scan started moved tuples to a lower page.
            if (GistFollowRight(&page)? || vstate.start_nsn < gist_page_get_nsn(&page)?)
                && (opaque_rightlink != InvalidBlockNumber)
                && (opaque_rightlink < orig_blkno)
            {
                recurse_to = opaque_rightlink;
            }

            // Scan over all items to see which need to be deleted per the
            // callback.
            if let Some(cs) = vstate.callback_state {
                let pref = PageRef::new(&page)?;
                let mut off = FIRST_OFFSET_NUMBER;
                while off <= maxoff {
                    let iid = PageGetItemId(&pref, off)?;
                    let idxtuple = PageGetItem(&pref, &iid)?;
                    // callback(&(idxtuple->t_tid), ...).
                    let t_tid = itup_heap_tid(idxtuple);
                    if vacuum_tid_is_dead(t_tid, cs) {
                        todelete.push(off);
                    }
                    off += 1;
                }
            }

            // Apply any needed deletes (one WAL record per page).
            if !todelete.is_empty() {
                // START_CRIT_SECTION();
                mark_buffer_dirty(buffer);

                {
                    let mut pmut = PageMut::new(&mut page)?;
                    PageIndexMultiDelete(&mut pmut, &todelete)?;
                }
                GistMarkTuplesDeleted(&mut page)?;

                let recptr = if relation_needs_wal(rel) {
                    xlog::gist_xlog_update(buffer, &todelete, &[], 0)?
                } else {
                    gist_get_fake_lsn(rel)?
                };
                // Write the page image back and stamp the LSN.
                write_page(buffer, &page)?;
                page_set_lsn(buffer, recptr)?;
                // END_CRIT_SECTION();

                vstate.stats.tuples_removed += todelete.len() as f64;
                // must recompute maxoff.
                let pref = PageRef::new(&page)?;
                maxoff = PageGetMaxOffsetNumber(&pref);
            }

            let nremain = maxoff as i32 - FIRST_OFFSET_NUMBER as i32 + 1;
            if nremain == 0 {
                // The page is now completely empty. Remember its block number
                // for the second stage. Skip when recursing (IntegerSet
                // requires ascending insertion; the next VACUUM picks it up).
                if blkno == orig_blkno {
                    vstate.empty_leaf_set.add_member(blkno as u64)?;
                }
            } else {
                vstate.stats.num_index_tuples += nremain as f64;
            }
        } else {
            // Internal page: check for "invalid tuples" left by an incomplete
            // 9.0-or-below page split.
            let pref = PageRef::new(&page)?;
            let maxoff = PageGetMaxOffsetNumber(&pref);
            let mut off = FIRST_OFFSET_NUMBER;
            while off <= maxoff {
                let iid = PageGetItemId(&pref, off)?;
                let idxtuple = PageGetItem(&pref, &iid)?;
                if gist_tuple_is_invalid(idxtuple) {
                    // ereport(LOG, errmsg/errdetail/errhint). A LOG report never
                    // propagates an error, so we ignore the result (mirror
                    // nbtree's log_message). The hint is folded into the detail
                    // since ereport_msg carries only msg + detail.
                    let _ = elog::ereport_msg::call(
                        LOG,
                        alloc::format!(
                            "index \"{}\" contains an inner tuple marked as invalid",
                            rel.name()
                        ),
                        Some(alloc::string::String::from(
                            "This is caused by an incomplete page split at crash recovery before upgrading to PostgreSQL 9.1. Please REINDEX it.",
                        )),
                    );
                }
                off += 1;
            }

            // Remember this internal page so the second stage can revisit it
            // searching for parents of empty leaf pages.
            if blkno == orig_blkno {
                vstate.internal_page_set.add_member(blkno as u64)?;
            }
        }

        unlock_release_buffer(buffer);

        // Hand-optimized tail recursion (avoids large per-level stack from the
        // deletable[] array in the C).
        if recurse_to != InvalidBlockNumber {
            blkno = recurse_to;

            // check for vacuum delay while not holding any buffer lock.
            vacuum_delay_point()?;

            buffer = read_buffer_extended(rel, blkno)?;
            // goto restart.
            continue;
        }

        let _ = mcx;
        return Ok(());
    }
}

// ===========================================================================
// gistvacuum_delete_empty_pages (gistvacuum.c:503).
// ===========================================================================

/// `gistvacuum_delete_empty_pages(info, vstate)` (gistvacuum.c:503): scan all
/// internal pages and try to delete their empty child pages.
fn gistvacuum_delete_empty_pages<'mcx>(
    mcx: Mcx<'mcx>,
    vstate: &mut GistVacState<'_, 'mcx>,
) -> PgResult<()> {
    let rel = &vstate.info.index;

    // Rescan all inner pages to find those that have empty child pages.
    let mut empty_pages_remaining = vstate.empty_leaf_set.num_entries();
    vstate.internal_page_set.begin_iterate();

    while empty_pages_remaining > 0 {
        let blkno = match vstate.internal_page_set.iterate_next() {
            Some(b) => b,
            None => break,
        };

        let buffer = read_buffer_extended(rel, blkno as BlockNumber)?;

        lock_buffer(buffer, GIST_SHARE)?;
        let page = page_bytes(buffer)?;

        if PageIsNew(&PageRef::new(&page)?) || GistPageIsDeleted(&page)? || GistPageIsLeaf(&page)? {
            // This page was an internal page earlier, but now it's something
            // else. Shouldn't happen... (Assert(false) in C).
            unlock_release_buffer(buffer);
            continue;
        }

        // Scan all the downlinks, and see if any of them point to empty leaf
        // pages.
        let maxoff = {
            let pref = PageRef::new(&page)?;
            PageGetMaxOffsetNumber(&pref)
        };
        let mut todelete: Vec<OffsetNumber> = Vec::new();
        let mut leafs_to_delete: Vec<BlockNumber> = Vec::new();
        {
            let pref = PageRef::new(&page)?;
            let mut off = FIRST_OFFSET_NUMBER;
            // off <= maxoff && ntodelete < maxoff - 1
            while off <= maxoff && (todelete.len() as i32) < (maxoff as i32 - 1) {
                let iid = PageGetItemId(&pref, off)?;
                let idxtuple = PageGetItem(&pref, &iid)?;
                let leafblk = itup_block_number(idxtuple);
                if vstate.empty_leaf_set.is_member(leafblk as u64) {
                    leafs_to_delete.push(leafblk);
                    todelete.push(off);
                }
                off += 1;
            }
        }
        let ntodelete = todelete.len();

        // To avoid deadlock, child must be locked before parent: release the
        // parent lock, lock the child, then re-acquire the parent. The downlink
        // might have moved, so gistdeletepage re-checks all the conditions.
        lock_buffer(buffer, GIST_UNLOCK)?;

        let mut deleted: usize = 0;
        for i in 0..ntodelete {
            // Don't remove the last downlink from the parent — that would
            // confuse the insertion code.
            {
                let cur = page_bytes(buffer)?;
                let cur_max = {
                    let pref = PageRef::new(&cur)?;
                    PageGetMaxOffsetNumber(&pref)
                };
                if cur_max == FIRST_OFFSET_NUMBER {
                    break;
                }
            }

            let leafbuf = read_buffer_extended(rel, leafs_to_delete[i])?;
            lock_buffer(leafbuf, GIST_EXCLUSIVE)?;
            gistcheckpage(rel.name(), leafbuf)?;

            lock_buffer(buffer, GIST_EXCLUSIVE)?;
            if gistdeletepage(
                mcx,
                vstate.info,
                &mut vstate.stats,
                buffer,
                (todelete[i] as i32 - deleted as i32) as OffsetNumber,
                leafbuf,
            )? {
                deleted += 1;
            }
            lock_buffer(buffer, GIST_UNLOCK)?;

            unlock_release_buffer(leafbuf);
        }

        release_buffer(buffer);

        // We can stop the scan as soon as we have seen the downlinks, even if
        // we were not able to remove them all.
        empty_pages_remaining = empty_pages_remaining.saturating_sub(ntodelete as u64);
    }

    Ok(())
}

// ===========================================================================
// gistdeletepage (gistvacuum.c:630).
// ===========================================================================

/// `gistdeletepage(info, stats, parentBuffer, downlink, leafBuffer)`
/// (gistvacuum.c:630): try to delete the leaf page `leafBuffer` and remove its
/// downlink (at offset `downlink`) from `parentBuffer`. Both pages must be
/// locked. Re-checks all the deletability conditions, since a concurrent
/// inserter might have changed things. Returns `true` if the page was deleted.
fn gistdeletepage<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'mcx>,
    stats: &mut IndexBulkDeleteResult,
    parent_buffer: Buffer,
    downlink: OffsetNumber,
    leaf_buffer: Buffer,
) -> PgResult<bool> {
    let mut parent_page = page_bytes(parent_buffer)?;
    let mut leaf_page = page_bytes(leaf_buffer)?;

    // Check that the leaf is still empty and deletable.
    if !GistPageIsLeaf(&leaf_page)? {
        // a leaf page should never become a non-leaf page (Assert(false)).
        return Ok(false);
    }

    if GistFollowRight(&leaf_page)? {
        return Ok(false); // don't mess with a concurrent page split.
    }

    {
        let pref = PageRef::new(&leaf_page)?;
        if PageGetMaxOffsetNumber(&pref) != INVALID_OFFSET_NUMBER {
            return Ok(false); // not empty anymore.
        }
    }

    // Is the downlink in the parent page still valid? It might have been moved
    // by a concurrent insert. Keep it simple and just give up if not; the next
    // VACUUM will pick it up.
    if PageIsNew(&PageRef::new(&parent_page)?)
        || GistPageIsDeleted(&parent_page)?
        || GistPageIsLeaf(&parent_page)?
    {
        // shouldn't happen, internal pages are never deleted (Assert(false)).
        return Ok(false);
    }

    {
        let pref = PageRef::new(&parent_page)?;
        let parent_max = PageGetMaxOffsetNumber(&pref);
        if parent_max < downlink || parent_max <= FIRST_OFFSET_NUMBER {
            return Ok(false);
        }
    }

    {
        let pref = PageRef::new(&parent_page)?;
        let iid = PageGetItemId(&pref, downlink)?;
        let idxtuple = PageGetItem(&pref, &iid)?;
        if buffer_get_block_number(leaf_buffer) != itup_block_number(idxtuple) {
            return Ok(false);
        }
    }

    // All good, proceed with the deletion.
    //
    // The page cannot be immediately recycled: mark it with the current
    // next-XID counter so we know when in-progress scans must have ended.
    let txid = read_next_full_transaction_id();

    // START_CRIT_SECTION();

    // mark the page as deleted.
    mark_buffer_dirty(leaf_buffer);
    GistPageSetDeleted(&mut leaf_page, txid)?;
    stats.pages_newly_deleted += 1;
    stats.pages_deleted += 1;

    // remove the downlink from the parent.
    mark_buffer_dirty(parent_buffer);
    {
        let mut pmut = PageMut::new(&mut parent_page)?;
        PageIndexTupleDelete(&mut pmut, downlink)?;
    }

    let recptr = if relation_needs_wal(&info.index) {
        xlog::gist_xlog_page_delete(leaf_buffer, txid, parent_buffer, downlink)?
    } else {
        gist_get_fake_lsn(&info.index)?
    };

    // Write both page images back and stamp the LSN.
    write_page(leaf_buffer, &leaf_page)?;
    write_page(parent_buffer, &parent_page)?;
    page_set_lsn(parent_buffer, recptr)?;
    page_set_lsn(leaf_buffer, recptr)?;

    // END_CRIT_SECTION();

    let _ = mcx;
    Ok(true)
}

// ---------------------------------------------------------------------------
// Local helpers.
// ---------------------------------------------------------------------------

/// Write a full page image back into the buffer (the C mutates the pinned page
/// in place under the buffer lock; we copy the worked-on image back).
fn write_page(buffer: Buffer, page: &[u8]) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buffer, &mut |dst: &mut [u8]| {
        dst.copy_from_slice(page);
        Ok(())
    })
}

/// `idxtuple->t_tid` — the leading `ItemPointerData` of an on-disk index tuple.
fn itup_heap_tid(itup: &[u8]) -> types_tuple::heaptuple::ItemPointerData {
    types_tuple::heaptuple::ItemPointerData {
        ip_blkid: types_tuple::heaptuple::BlockIdData {
            bi_hi: u16::from_ne_bytes([itup[0], itup[1]]),
            bi_lo: u16::from_ne_bytes([itup[2], itup[3]]),
        },
        ip_posid: u16::from_ne_bytes([itup[4], itup[5]]),
    }
}

// The C `todelete[MaxOffsetNumber]` / `leafs_to_delete[MaxOffsetNumber]` fixed
// arrays are growable `Vec`s here; this documents the C upper bound.
const _: OffsetNumber = MaxOffsetNumber;
