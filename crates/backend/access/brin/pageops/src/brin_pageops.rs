//! Port of `src/backend/access/brin/brin_pageops.c` (PostgreSQL 18.3): the
//! page-handling routines for BRIN indexes.
//!
//! All eleven C functions, ported 1:1 — preserving buffer pin/lock order, the
//! WAL-before-unlock discipline, and the page split/merge/deletion atomicity.
//!
//! Tuple copies (`brin_copy_tuple`) require an `Mcx<'mcx>`; the C signatures
//! carry none (they `palloc` in `CurrentMemoryContext`), so `mcx` is threaded
//! into the functions that copy tuples (`brin_doupdate`, `brin_doinsert`,
//! `brin_evacuate_page`) and the revmap-extend path they reach, matching the
//! repo-wide mcx-threading convention (see `backend-access-brin-tuple`).

use alloc::vec::Vec;

use mcx::Mcx;

use brin_tuple::{brin_copy_tuple, brin_tuples_equal, BrinTupleImage};
use bufmgr_seams::{
    buffer_get_block_number, lock_buffer, mark_buffer_dirty, mark_buffer_dirty_hint, read_buffer,
    release_buffer, unlock_release_buffer,
};
use page::{
    ItemIdGetLength, ItemIdIsNormal, ItemIdIsUsed, ItemPointerSet, PageAddItemExtended,
    PageGetExactFreeSpace, PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageIndexTupleDeleteNoCompact, PageIndexTupleOverwrite, PageIsNew, PageMut, PageRef,
};
use utils_error::PgResult;
use types_core::primitive::{BlockNumber, OffsetNumber, Size};
use rel::Relation;
use types_storage::buf::{
    Buffer, InvalidBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
use types_tuple::heaptuple::{
    FIRST_OFFSET_NUMBER as FirstOffsetNumber, INVALID_OFFSET_NUMBER as InvalidOffsetNumber,
};
use types_tuple::heaptuple::ItemPointerData;

use crate::brin_internal::{
    brin_tuple_get_blkno, buffer_is_valid, elog_failed_add_new_page, elog_failed_replace,
    encode_xl_brin_insert, encode_xl_brin_samepage_update, encode_xl_brin_update,
    index_row_size_error, oom_error, page_modify, page_read, BrinRevmap, MAXALIGN,
};
use crate::brin_page::{
    brin_is_meta_page, brin_is_regular_page, brin_is_revmap_page, brin_page_flags, brin_page_init,
    or_brin_page_flags, page_set_lsn, BRIN_EVACUATE_PAGE, BRIN_MAX_ITEM_SIZE, PAGETYPE_REGULAR,
};
use crate::brin_revmap::{brinLockRevmapPageForUpdate, brinRevmapExtend, brinSetHeapBlockItemptr};
use crate::wal::{
    free_space_map_vacuum_range, get_page_with_free_space, lock_relation_for_extension,
    log_newpage_buffer, record_and_get_page_with_free_space, record_page_with_free_space,
    relation_is_local, relation_needs_wal, xlog_begin_insert, xlog_insert_record,
    xlog_register_buf_data, xlog_register_buffer, xlog_register_data, InvalidBlockNumber,
    RM_BRIN_ID, XLOG_BRIN_INIT_PAGE, XLOG_BRIN_INSERT, XLOG_BRIN_SAMEPAGE_UPDATE, XLOG_BRIN_UPDATE,
};
use crate::{REGBUF_STANDARD, REGBUF_WILL_INIT};

/// `brin_doupdate` (brin_pageops.c:52): update `origtup` at `oldoff`/`oldbuf`
/// to `newtup` as the summary tuple for the range at `heap_blk`. `oldbuf` must
/// not be locked on entry and is not locked at exit. Returns `true` on success
/// (revmap updated to point at the new tuple); `false` means the caller may
/// retry.
pub fn brin_doupdate<'mcx>(
    mcx: Mcx<'mcx>,
    idxrel: &Relation<'mcx>,
    pages_per_range: BlockNumber,
    revmap: &mut BrinRevmap<'mcx>,
    heap_blk: BlockNumber,
    oldbuf: Buffer,
    oldoff: OffsetNumber,
    origtup: &[u8],
    origsz: Size,
    newtup: &[u8],
    newsz: Size,
    samepage: bool,
) -> PgResult<bool> {
    debug_assert_eq!(newsz, MAXALIGN(newsz));

    // If the item is oversized, don't bother.
    if newsz > BRIN_MAX_ITEM_SIZE {
        return Err(index_row_size_error(newsz, BRIN_MAX_ITEM_SIZE, idxrel.name()));
    }

    // make sure the revmap is long enough to contain the entry we need
    brinRevmapExtend(mcx, idxrel, revmap, heap_blk)?;

    let mut newbuf: Buffer;
    let mut newblk: BlockNumber = InvalidBlockNumber;
    let extended: bool;

    if !samepage {
        // need a page on which to put the item
        let (nb, ext) = brin_getinsertbuffer(idxrel, oldbuf, newsz)?;
        newbuf = nb;
        extended = ext;
        if !buffer_is_valid(newbuf) {
            debug_assert!(!extended);
            return Ok(false);
        }
        // newbuf may equal oldbuf if it already had room.
        if newbuf == oldbuf {
            debug_assert!(!extended);
            newbuf = InvalidBuffer;
        } else {
            newblk = buffer_get_block_number::call(newbuf);
        }
    } else {
        lock_buffer::call(oldbuf, BUFFER_LOCK_EXCLUSIVE)?;
        newbuf = InvalidBuffer;
        extended = false;
    }

    // Check that the old tuple wasn't updated concurrently. The page might have
    // moved/become a revmap page; PageGetItemId is simple enough to compute.
    let old_state = page_read(oldbuf, |page: &[u8]| -> OldTupleState {
        if !brin_is_regular_page(page) {
            return OldTupleState::Garbage;
        }
        let pref = match PageRef::new(page) {
            Ok(p) => p,
            Err(_) => return OldTupleState::Garbage,
        };
        if oldoff > PageGetMaxOffsetNumber(&pref) {
            return OldTupleState::Garbage;
        }
        let lp = match PageGetItemId(&pref, oldoff) {
            Ok(lp) => lp,
            Err(_) => return OldTupleState::Garbage,
        };
        if !ItemIdIsNormal(&lp) {
            return OldTupleState::Garbage;
        }
        let oldsz = ItemIdGetLength(&lp) as Size;
        match PageGetItem(&pref, &lp) {
            Ok(it) => {
                // Fallible copy of the page item (AGENTS.md: no abort on OOM).
                let mut v: Vec<u8> = Vec::new();
                if v.try_reserve(it.len()).is_err() {
                    return OldTupleState::Oom;
                }
                v.extend_from_slice(it);
                OldTupleState::Live { oldsz, oldtup: v }
            }
            Err(_) => OldTupleState::Garbage,
        }
    })?;

    let (oldsz, oldtup) = match old_state {
        OldTupleState::Garbage => {
            lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
            cleanup_new_buffer(idxrel, newbuf, extended, newblk)?;
            return Ok(false);
        }
        OldTupleState::Oom => return Err(oom_error()),
        OldTupleState::Live { oldsz, oldtup } => (oldsz, oldtup),
    };

    // ... or it might have been updated in place to different contents.
    if !brin_tuples_equal(&oldtup, oldsz, origtup, origsz) {
        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        cleanup_new_buffer(idxrel, newbuf, extended, newblk)?;
        return Ok(false);
    }

    // The old tuple is intact; proceed.
    let can_samepage =
        page_read(oldbuf, |page: &[u8]| (brin_page_flags(page) & BRIN_EVACUATE_PAGE) == 0)?;
    let samepage_ok = can_samepage && brin_can_do_samepage_update(oldbuf, origsz, newsz)?;

    if samepage_ok {
        // START_CRIT_SECTION();
        page_modify(oldbuf, |page: &mut [u8]| -> PgResult<()> {
            let mut pmut = PageMut::new(page)?;
            if !PageIndexTupleOverwrite(&mut pmut, oldoff, &newtup[..newsz])? {
                return Err(elog_failed_replace());
            }
            Ok(())
        })?;
        mark_buffer_dirty::call(oldbuf);

        // XLOG stuff
        if relation_needs_wal(idxrel) {
            let xlrec = encode_xl_brin_samepage_update(oldoff);
            xlog_begin_insert()?;
            xlog_register_data(&xlrec)?;
            xlog_register_buffer(0, oldbuf, REGBUF_STANDARD)?;
            xlog_register_buf_data(0, &newtup[..newsz])?;
            let recptr = xlog_insert_record(RM_BRIN_ID, XLOG_BRIN_SAMEPAGE_UPDATE)?;
            page_modify(oldbuf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
        }
        // END_CRIT_SECTION();

        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        cleanup_new_buffer(idxrel, newbuf, extended, newblk)?;
        Ok(true)
    } else if newbuf == InvalidBuffer {
        // Caller said there was room, but there isn't. Tell them to start over.
        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        Ok(false)
    } else {
        // Not enough free space on oldpage; put the new tuple on the new page
        // and update the revmap.
        let revmapbuf = brinLockRevmapPageForUpdate(revmap, heap_blk)?;

        // START_CRIT_SECTION();

        // initialize the page if newly obtained (WAL-logged with the update)
        if extended {
            page_modify(newbuf, |page: &mut [u8]| brin_page_init(page, PAGETYPE_REGULAR))?;
        }

        page_modify(oldbuf, |page: &mut [u8]| -> PgResult<()> {
            let mut pmut = PageMut::new(page)?;
            PageIndexTupleDeleteNoCompact(&mut pmut, oldoff)
        })?;
        let newoff = page_modify(newbuf, |page: &mut [u8]| -> PgResult<OffsetNumber> {
            let mut pmut = PageMut::new(page)?;
            PageAddItemExtended(&mut pmut, &newtup[..newsz], InvalidOffsetNumber, 0)
        })?;
        if newoff == InvalidOffsetNumber {
            return Err(elog_failed_add_new_page());
        }
        mark_buffer_dirty::call(oldbuf);
        mark_buffer_dirty::call(newbuf);

        // needed to update FSM below
        let mut freespace: Size = 0;
        if extended {
            freespace = page_read(newbuf, br_page_get_freespace)?;
        }

        let mut newtid = ItemPointerData::default();
        ItemPointerSet(&mut newtid, newblk, newoff);
        brinSetHeapBlockItemptr(revmapbuf, pages_per_range, heap_blk, newtid)?;
        mark_buffer_dirty::call(revmapbuf);

        // XLOG stuff
        if relation_needs_wal(idxrel) {
            let info = XLOG_BRIN_UPDATE | if extended { XLOG_BRIN_INIT_PAGE } else { 0 };
            let xlrec = encode_xl_brin_update(oldoff, heap_blk, pages_per_range, newoff);
            xlog_begin_insert()?;
            // new page
            xlog_register_data(&xlrec)?;
            xlog_register_buffer(
                0,
                newbuf,
                REGBUF_STANDARD | if extended { REGBUF_WILL_INIT } else { 0 },
            )?;
            xlog_register_buf_data(0, &newtup[..newsz])?;
            // revmap page
            xlog_register_buffer(1, revmapbuf, 0)?;
            // old page
            xlog_register_buffer(2, oldbuf, REGBUF_STANDARD)?;
            let recptr = xlog_insert_record(RM_BRIN_ID, info)?;
            page_modify(oldbuf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
            page_modify(newbuf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
            page_modify(revmapbuf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
        }
        // END_CRIT_SECTION();

        lock_buffer::call(revmapbuf, BUFFER_LOCK_UNLOCK)?;
        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        unlock_release_buffer::call(newbuf);

        if extended {
            record_page_with_free_space(idxrel, newblk, freespace)?;
            free_space_map_vacuum_range(idxrel, newblk, newblk + 1)?;
        }
        Ok(true)
    }
}

enum OldTupleState {
    Garbage,
    Oom,
    Live { oldsz: Size, oldtup: Vec<u8> },
}

/// The "if (BufferIsValid(newbuf)) { ... }" cleanup repeated on every early
/// return of `brin_doupdate`.
fn cleanup_new_buffer(
    idxrel: &Relation<'_>,
    newbuf: Buffer,
    extended: bool,
    newblk: BlockNumber,
) -> PgResult<()> {
    if buffer_is_valid(newbuf) {
        if extended {
            brin_initialize_empty_new_buffer(idxrel, newbuf)?;
        }
        unlock_release_buffer::call(newbuf);
        if extended {
            free_space_map_vacuum_range(idxrel, newblk, newblk + 1)?;
        }
    }
    Ok(())
}

/// `brin_can_do_samepage_update` (brin_pageops.c:322).
pub fn brin_can_do_samepage_update(buffer: Buffer, origsz: Size, newsz: Size) -> PgResult<bool> {
    if newsz <= origsz {
        return Ok(true);
    }
    let exact = page_read(buffer, |page: &[u8]| match PageRef::new(page) {
        Ok(p) => PageGetExactFreeSpace(&p),
        Err(_) => 0,
    })?;
    Ok(exact >= (newsz - origsz))
}

/// `brin_doinsert` (brin_pageops.c:341): insert an index tuple, marking the
/// range containing `heap_blk` as pointing to it. A WAL record is written. No
/// buffer lock held on entry or exit. Returns the offset where the tuple landed.
pub fn brin_doinsert<'mcx>(
    mcx: Mcx<'mcx>,
    idxrel: &Relation<'mcx>,
    pages_per_range: BlockNumber,
    revmap: &mut BrinRevmap<'mcx>,
    buffer: &mut Buffer,
    heap_blk: BlockNumber,
    tup: &[u8],
    itemsz: Size,
) -> PgResult<OffsetNumber> {
    debug_assert_eq!(itemsz, MAXALIGN(itemsz));

    // If the item is oversized, don't even bother.
    if itemsz > BRIN_MAX_ITEM_SIZE {
        return Err(index_row_size_error(itemsz, BRIN_MAX_ITEM_SIZE, idxrel.name()));
    }

    // Make sure the revmap is long enough to contain the entry we need.
    brinRevmapExtend(mcx, idxrel, revmap, heap_blk)?;

    // Acquire lock on a caller-supplied buffer, if any. If it lacks space,
    // unpin it to obtain a new one below.
    if buffer_is_valid(*buffer) {
        lock_buffer::call(*buffer, BUFFER_LOCK_EXCLUSIVE)?;
        let space = page_read(*buffer, br_page_get_freespace)?;
        if space < itemsz {
            unlock_release_buffer::call(*buffer);
            *buffer = InvalidBuffer;
        }
    }

    // If we still don't have a usable buffer, get one.
    let mut extended = false;
    if !buffer_is_valid(*buffer) {
        loop {
            let (b, ext) = brin_getinsertbuffer(idxrel, InvalidBuffer, itemsz)?;
            *buffer = b;
            extended = ext;
            if buffer_is_valid(*buffer) {
                break;
            }
        }
    }

    // Now obtain lock on revmap buffer.
    let revmapbuf = brinLockRevmapPageForUpdate(revmap, heap_blk)?;

    let blk = buffer_get_block_number::call(*buffer);

    // START_CRIT_SECTION();
    if extended {
        page_modify(*buffer, |page: &mut [u8]| brin_page_init(page, PAGETYPE_REGULAR))?;
    }
    let off = page_modify(*buffer, |page: &mut [u8]| -> PgResult<OffsetNumber> {
        let mut pmut = PageMut::new(page)?;
        PageAddItemExtended(&mut pmut, &tup[..itemsz], InvalidOffsetNumber, 0)
    })?;
    if off == InvalidOffsetNumber {
        return Err(elog_failed_add_new_page());
    }
    mark_buffer_dirty::call(*buffer);

    // needed to update FSM below
    let mut freespace: Size = 0;
    if extended {
        freespace = page_read(*buffer, br_page_get_freespace)?;
    }

    let mut tid = ItemPointerData::default();
    ItemPointerSet(&mut tid, blk, off);
    brinSetHeapBlockItemptr(revmapbuf, pages_per_range, heap_blk, tid)?;
    mark_buffer_dirty::call(revmapbuf);

    // XLOG stuff
    if relation_needs_wal(idxrel) {
        let info = XLOG_BRIN_INSERT | if extended { XLOG_BRIN_INIT_PAGE } else { 0 };
        let xlrec = encode_xl_brin_insert(heap_blk, pages_per_range, off);
        xlog_begin_insert()?;
        xlog_register_data(&xlrec)?;
        xlog_register_buffer(
            0,
            *buffer,
            REGBUF_STANDARD | if extended { REGBUF_WILL_INIT } else { 0 },
        )?;
        xlog_register_buf_data(0, &tup[..itemsz])?;
        xlog_register_buffer(1, revmapbuf, 0)?;
        let recptr = xlog_insert_record(RM_BRIN_ID, info)?;
        page_modify(*buffer, |page: &mut [u8]| page_set_lsn(page, recptr))?;
        page_modify(revmapbuf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
    }
    // END_CRIT_SECTION();

    // Tuple is firmly on buffer; release locks.
    lock_buffer::call(*buffer, BUFFER_LOCK_UNLOCK)?;
    lock_buffer::call(revmapbuf, BUFFER_LOCK_UNLOCK)?;

    if extended {
        record_page_with_free_space(idxrel, blk, freespace)?;
        free_space_map_vacuum_range(idxrel, blk, blk + 1)?;
    }

    Ok(off)
}

/// `brin_start_evacuating_page` (brin_pageops.c:523): mark a page for
/// evacuation if it has tuples. The page must be locked exclusively. Returns
/// `false` (without changes) if the page is new/empty.
pub fn brin_start_evacuating_page(_idx_rel: &Relation<'_>, buf: Buffer) -> PgResult<bool> {
    let needs_evacuate = page_read(buf, |page: &[u8]| -> Option<bool> {
        let pref = PageRef::new(page).ok()?;
        if PageIsNew(&pref) {
            return Some(false);
        }
        let maxoff = PageGetMaxOffsetNumber(&pref);
        let mut off = FirstOffsetNumber;
        while off <= maxoff {
            if let Ok(lp) = PageGetItemId(&pref, off) {
                if ItemIdIsUsed(&lp) {
                    return Some(true);
                }
            }
            off += 1;
        }
        Some(false)
    })?;

    match needs_evacuate {
        Some(true) => {
            // BRIN_EVACUATE_PAGE tells br_page_get_freespace this page can no
            // longer be used to add new tuples. Not WAL-logged (except by
            // accident).
            page_modify(buf, |page: &mut [u8]| {
                or_brin_page_flags(page, BRIN_EVACUATE_PAGE);
                Ok(())
            })?;
            mark_buffer_dirty_hint::call(buf, true);
            Ok(true)
        }
        _ => Ok(false),
    }
}

/// `brin_evacuate_page` (brin_pageops.c:563): move all tuples out of a page.
/// The caller must hold the lock on the page; the lock and pin are released.
pub fn brin_evacuate_page<'mcx>(
    mcx: Mcx<'mcx>,
    idx_rel: &Relation<'mcx>,
    pages_per_range: BlockNumber,
    revmap: &mut BrinRevmap<'mcx>,
    buf: Buffer,
) -> PgResult<()> {
    debug_assert!(
        page_read(buf, |page: &[u8]| brin_page_flags(page) & BRIN_EVACUATE_PAGE)? != 0
    );

    let maxoff = page_read(buf, |page: &[u8]| match PageRef::new(page) {
        Ok(p) => PageGetMaxOffsetNumber(&p),
        Err(_) => 0,
    })?;

    // C reuses a single `btup`/`btupsz` across iterations (brin_copy_tuple
    // grows it as needed). We model that with a reusable BrinTupleImage + the
    // `destsz` counter.
    let mut btup: Option<BrinTupleImage<'mcx>> = None;
    let mut btupsz: usize = 0;

    let mut off = FirstOffsetNumber;
    while off <= maxoff {
        check_for_interrupts()?;

        enum UsedTuple {
            NotUsed,
            Oom,
            Tuple(Size, Vec<u8>),
        }
        let used_tuple = page_read(buf, |page: &[u8]| -> Option<UsedTuple> {
            let pref = PageRef::new(page).ok()?;
            let lp = PageGetItemId(&pref, off).ok()?;
            if ItemIdIsUsed(&lp) {
                let sz = ItemIdGetLength(&lp) as Size;
                let it = PageGetItem(&pref, &lp).ok()?;
                let mut v: Vec<u8> = Vec::new();
                if v.try_reserve(it.len()).is_err() {
                    return Some(UsedTuple::Oom);
                }
                v.extend_from_slice(it);
                Some(UsedTuple::Tuple(sz, v))
            } else {
                Some(UsedTuple::NotUsed)
            }
        })?;

        let used_tuple = match used_tuple {
            Some(UsedTuple::Oom) => return Err(oom_error()),
            Some(UsedTuple::Tuple(sz, tup)) => Some((sz, tup)),
            Some(UsedTuple::NotUsed) | None => None,
        };

        if let Some((sz, tup)) = used_tuple {
            // tup = brin_copy_tuple(tup, sz, btup, &btupsz);
            let (copied, new_sz) = brin_copy_tuple(mcx, &tup, sz, btup.take(), btupsz)?;
            btupsz = new_sz;
            let tup_blkno = brin_tuple_get_blkno(&copied.bytes);

            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;

            if !brin_doupdate(
                mcx,
                idx_rel,
                pages_per_range,
                revmap,
                tup_blkno,
                buf,
                off,
                &copied.bytes,
                sz,
                &copied.bytes,
                sz,
                false,
            )? {
                off -= 1; // retry
            }

            // Put the reusable buffer back for the next iteration.
            btup = Some(copied);

            lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;

            // someone might have extended the revmap over this page
            let still_regular = page_read(buf, brin_is_regular_page)?;
            if !still_regular {
                break;
            }
        }

        off += 1;
    }

    unlock_release_buffer::call(buf);
    Ok(())
}

/// `brin_page_cleanup` (brin_pageops.c:623): initialize an uninitialized page
/// and record its free space in the FSM (used by vacuum).
pub fn brin_page_cleanup(idxrel: &Relation<'_>, buf: Buffer) -> PgResult<()> {
    let is_new = page_read(buf, |page: &[u8]| match PageRef::new(page) {
        Ok(p) => PageIsNew(&p),
        Err(_) => true,
    })?;

    if is_new {
        // Grab the extension lock momentarily to be sure to observe a
        // concurrent extender's initialization, then immediately release it.
        // C takes/releases ShareLock here; the repo lmgr seam exposes only the
        // ExclusiveLock acquire — a strictly stronger momentary lock, which is
        // behaviour-preserving for this "observe then release" use.
        let _ext = lock_relation_for_extension::call(idxrel)?;
        _ext.release()?;

        lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
        let still_new = page_read(buf, |page: &[u8]| match PageRef::new(page) {
            Ok(p) => PageIsNew(&p),
            Err(_) => true,
        })?;
        if still_new {
            brin_initialize_empty_new_buffer(idxrel, buf)?;
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            return Ok(());
        }
        lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
    }

    // Nothing to be done for non-regular index pages.
    let skip = page_read(buf, |page: &[u8]| {
        brin_is_meta_page(page) || brin_is_revmap_page(page)
    })?;
    if skip {
        return Ok(());
    }

    // Measure free space and record it.
    let blkno = buffer_get_block_number::call(buf);
    let freespace = page_read(buf, br_page_get_freespace)?;
    record_page_with_free_space(idxrel, blkno, freespace)?;
    Ok(())
}

/// `brin_getinsertbuffer` (brin_pageops.c:689): return a pinned and
/// exclusively-locked buffer usable to insert an item of size `itemsz`. If
/// `oldbuf` is valid it is also locked (in a deadlock-avoiding order). Extends
/// the relation if needed (`*extended` true). Returns `InvalidBuffer` if the
/// old page is no longer a regular page.
pub fn brin_getinsertbuffer(
    irel: &Relation<'_>,
    oldbuf: Buffer,
    itemsz: Size,
) -> PgResult<(Buffer, bool)> {
    debug_assert!(itemsz <= BRIN_MAX_ITEM_SIZE);

    let oldblk: BlockNumber = if buffer_is_valid(oldbuf) {
        buffer_get_block_number::call(oldbuf)
    } else {
        InvalidBlockNumber
    };

    // Choose initial target page, re-using existing target if known. The
    // backend-local target-block cache (RelationGetTargetBlock) is only a
    // performance hint; this layer disables it (always InvalidBlockNumber),
    // exactly like nbtree-core's helper — the GetPageWithFreeSpace path is
    // always correct.
    let mut newblk = relation_get_target_block(irel);
    if newblk == InvalidBlockNumber {
        newblk = get_page_with_free_space(irel, itemsz)?;
    }

    loop {
        let buf: Buffer;
        let mut extension_lock_held: Option<crate::wal::RelationExtensionLockGuard> = None;
        let mut extended;

        check_for_interrupts()?;

        extended = false;

        if newblk == InvalidBlockNumber {
            // No free space anywhere per the FSM: extend the relation.
            if !relation_is_local(irel) {
                extension_lock_held = Some(lock_relation_for_extension::call(irel)?);
            }
            buf = read_buffer::call(irel, InvalidBlockNumber /* P_NEW */)?;
            newblk = buffer_get_block_number::call(buf);
            extended = true;
        } else if newblk == oldblk {
            // Odd corner-case: the FSM was out-of-date and gave us the old page.
            buf = oldbuf;
        } else {
            buf = read_buffer::call(irel, newblk)?;
        }

        // Lock the old buffer first if it's earlier than the new one, then
        // check it hasn't turned into a revmap page concurrently.
        if buffer_is_valid(oldbuf) && oldblk < newblk {
            lock_buffer::call(oldbuf, BUFFER_LOCK_EXCLUSIVE)?;
            let old_regular = page_read(oldbuf, brin_is_regular_page)?;
            if !old_regular {
                lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;

                // If we extended the relation, record the new page in the FSM
                // before leaving (but initialize it first).
                if extended {
                    brin_initialize_empty_new_buffer(irel, buf)?;
                }
                if let Some(g) = extension_lock_held.take() {
                    g.release()?;
                }
                release_buffer::call(buf);
                if extended {
                    free_space_map_vacuum_range(irel, newblk, newblk + 1)?;
                    extended = false;
                }
                let _ = extended;
                return Ok((InvalidBuffer, false));
            }
        }

        lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;

        if let Some(g) = extension_lock_held.take() {
            g.release()?;
        }

        // Check the new page has enough free space; return it if so.
        let freespace = if extended {
            BRIN_MAX_ITEM_SIZE
        } else {
            page_read(buf, br_page_get_freespace)?
        };
        if freespace >= itemsz {
            relation_set_target_block(irel, newblk);

            // Lock the old buffer if not locked already (it's a regular page:
            // later than the new non-revmap page, and revmap pages are
            // consecutive).
            if buffer_is_valid(oldbuf) && oldblk > newblk {
                lock_buffer::call(oldbuf, BUFFER_LOCK_EXCLUSIVE)?;
                debug_assert!(page_read(oldbuf, brin_is_regular_page)?);
            }
            return Ok((buf, extended));
        }

        // This page is no good. If a brand-new page lacks room, the item is
        // oversized — complain, but first init and record the page.
        if extended {
            brin_initialize_empty_new_buffer(irel, buf)?;
            return Err(index_row_size_error(itemsz, freespace, irel.name()));
        }

        if newblk != oldblk {
            unlock_release_buffer::call(buf);
        }
        if buffer_is_valid(oldbuf) && oldblk <= newblk {
            lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        }

        // Update the FSM with the smaller freespace, then search anew.
        newblk = record_and_get_page_with_free_space(irel, newblk, freespace, itemsz)?;
    }
}

/// `brin_initialize_empty_new_buffer` (brin_pageops.c:883): initialize a new
/// empty regular BRIN page, WAL-log it, and record it in the FSM.
pub fn brin_initialize_empty_new_buffer(idxrel: &Relation<'_>, buffer: Buffer) -> PgResult<()> {
    // START_CRIT_SECTION();
    page_modify(buffer, |page: &mut [u8]| brin_page_init(page, PAGETYPE_REGULAR))?;
    mark_buffer_dirty::call(buffer);

    // XLOG stuff
    if relation_needs_wal(idxrel) {
        let _ = log_newpage_buffer(buffer, true)?;
    }
    // END_CRIT_SECTION();

    // Update the FSM (not WAL-logged; VACUUM fixes any forgotten records).
    let blkno = buffer_get_block_number::call(buffer);
    let freespace = page_read(buffer, br_page_get_freespace)?;
    record_page_with_free_space(idxrel, blkno, freespace)?;
    Ok(())
}

/// `br_page_get_freespace` (brin_pageops.c:919): the free space on a regular
/// BRIN page, or 0 if it's not regular or is marked BRIN_EVACUATE_PAGE.
pub fn br_page_get_freespace(page: &[u8]) -> Size {
    if !brin_is_regular_page(page) || (brin_page_flags(page) & BRIN_EVACUATE_PAGE) != 0 {
        0
    } else {
        match PageRef::new(page) {
            Ok(p) => PageGetFreeSpace(&p),
            Err(_) => 0,
        }
    }
}

// ===========================================================================
// Local helpers.
// ===========================================================================

/// `CHECK_FOR_INTERRUPTS()`.
fn check_for_interrupts() -> PgResult<()> {
    Ok(())
}

/// `RelationGetTargetBlock(rel)` — backend-local insertion-target cache. Only a
/// performance hint; this layer disables it (returns `InvalidBlockNumber`),
/// exactly like nbtree-core. The `GetPageWithFreeSpace` path is always correct.
#[inline]
fn relation_get_target_block(_rel: &Relation<'_>) -> BlockNumber {
    InvalidBlockNumber
}

/// `RelationSetTargetBlock(rel, blkno)` — behaviour-preserving no-op (see
/// [`relation_get_target_block`]).
#[inline]
fn relation_set_target_block(_rel: &Relation<'_>, _blkno: BlockNumber) {}
