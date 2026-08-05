//! hashsearch.c + _hash_kill_items (hashutil.c).

use ::bufmgr_seams as bm;
use ::types_core::{Buffer, InvalidBlockNumber, InvalidBuffer, OffsetNumber};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED};
use ::types_hash::*;
use ::types_rel::Relation;
use ::types_scan::scankey::{ScanKeyData, SK_ISNULL};
use ::types_scan::sdir::{BackwardScanDirection, ForwardScanDirection, ScanDirection};
use ::types_snapshot::SnapshotData;
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerEquals};

use crate::page::{
    page_opaque, page_ref, _hash_dropbuf, _hash_dropscanbuf, _hash_getbuf,
    _hash_getbucketbuf_from_hashkey, _hash_relbuf,
};
use crate::util::{
    _hash_binsearch, _hash_binsearch_last, _hash_checkpage, _hash_checkqual,
    _hash_datum2hashkey, _hash_datum2hashkey_type, _hash_get_indextuple_hashkey,
    _hash_get_oldblock_from_newbucket,
};

// The scan-side borrow split (nbtree ScanCtx precedent): opaque + the scan
// descriptor fields hash reads, without aliasing the whole descriptor.
pub(crate) struct HashScanCtx<'a, 'mcx> {
    pub rel: &'a Relation<'mcx>,
    pub so: &'a mut HashScanOpaqueData<'mcx>,
    pub snapshot: Option<&'a SnapshotData<'mcx>>,
    pub ignore_killed_tuples: bool,
    pub keys: &'a [ScanKeyData],
    pub xs_heaptid: &'a mut ItemPointerData,
    pub xs_pgstat_index_scans: &'a mut u64,
    pub xs_nsearches: &'a mut u64,
}

/// _hash_next.
pub(crate) fn _hash_next(ctx: &mut HashScanCtx<'_, '_>, dir: ScanDirection) -> PgResult<bool> {
    let forward = dir == ForwardScanDirection;
    let mut end_of_scan = false;

    if forward {
        ctx.so.currPos.itemIndex += 1;
        if ctx.so.currPos.itemIndex > ctx.so.currPos.lastItem {
            if ctx.so.numKilled > 0 {
                _hash_kill_items(ctx)?;
            }
            let blkno = ctx.so.currPos.nextPage;
            if blkno != InvalidBlockNumber {
                let buf = _hash_getbuf(ctx.rel, blkno, HASH_READ, LH_OVERFLOW_PAGE)?;
                if !_hash_readpage(ctx, buf, dir)? {
                    end_of_scan = true;
                }
            } else {
                end_of_scan = true;
            }
        }
    } else {
        ctx.so.currPos.itemIndex -= 1;
        if ctx.so.currPos.itemIndex < ctx.so.currPos.firstItem {
            if ctx.so.numKilled > 0 {
                _hash_kill_items(ctx)?;
            }
            let blkno = ctx.so.currPos.prevPage;
            if blkno != InvalidBlockNumber {
                let buf =
                    _hash_getbuf(ctx.rel, blkno, HASH_READ, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;
                // The pin on the primary bucket page is held for the whole
                // scan; drop the extra one just acquired.
                if buf == ctx.so.hashso_bucket_buf || buf == ctx.so.hashso_split_bucket_buf {
                    _hash_dropbuf(buf)?;
                }
                if !_hash_readpage(ctx, buf, dir)? {
                    end_of_scan = true;
                }
            } else {
                end_of_scan = true;
            }
        }
    }

    if end_of_scan {
        _hash_dropscanbuf(ctx.so)?;
        HashScanPosInvalidate(&mut ctx.so.currPos);
        return Ok(false);
    }

    // SAFETY: itemIndex within [firstItem, lastItem] written by readpage.
    let curr_item = unsafe { ctx.so.currPos.item(ctx.so.currPos.itemIndex as usize) };
    *ctx.xs_heaptid = curr_item.heapTid;
    Ok(true)
}

/// _hash_readnext: buf/page state advances to the chain's next page (or the
/// bucket being split); InvalidBuffer at chain end.
fn _hash_readnext(ctx: &mut HashScanCtx<'_, '_>, buf: &mut Buffer) -> PgResult<()> {
    // SAFETY: lock held on *buf.
    let blkno = page_opaque(&unsafe { page_ref(*buf) }).hasho_nextblkno;

    if *buf == ctx.so.hashso_bucket_buf || *buf == ctx.so.hashso_split_bucket_buf {
        bm::lock_buffer::call(*buf, bm::BUFFER_LOCK_UNLOCK)?;
    } else {
        _hash_relbuf(*buf)?;
    }
    *buf = InvalidBuffer;
    crate::check_for_interrupts()?;

    if blkno != InvalidBlockNumber {
        *buf = _hash_getbuf(ctx.rel, blkno, HASH_READ, LH_OVERFLOW_PAGE)?;
    } else if ctx.so.hashso_buc_populated && !ctx.so.hashso_buc_split {
        *buf = ctx.so.hashso_split_bucket_buf;
        debug_assert!(*buf != InvalidBuffer);
        bm::lock_buffer::call(*buf, bm::BUFFER_LOCK_SHARE)?;
        if let Some(snap) = ctx.snapshot {
            predicate_seams::predicate_lock_page::call(
                ctx.rel,
                bm::buffer_get_block_number::call(*buf),
                snap,
            )?;
        }
        ctx.so.hashso_buc_split = true;
    }
    Ok(())
}

/// _hash_readprev.
fn _hash_readprev(ctx: &mut HashScanCtx<'_, '_>, buf: &mut Buffer) -> PgResult<()> {
    // SAFETY: lock held on *buf.
    let blkno = page_opaque(&unsafe { page_ref(*buf) }).hasho_prevblkno;

    let haveprevblk;
    if *buf == ctx.so.hashso_bucket_buf || *buf == ctx.so.hashso_split_bucket_buf {
        bm::lock_buffer::call(*buf, bm::BUFFER_LOCK_UNLOCK)?;
        haveprevblk = false;
    } else {
        _hash_relbuf(*buf)?;
        haveprevblk = true;
    }
    *buf = InvalidBuffer;
    crate::check_for_interrupts()?;

    if haveprevblk {
        debug_assert!(blkno != InvalidBlockNumber);
        *buf = _hash_getbuf(ctx.rel, blkno, HASH_READ, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;
        if *buf == ctx.so.hashso_bucket_buf || *buf == ctx.so.hashso_split_bucket_buf {
            _hash_dropbuf(*buf)?;
        }
    } else if ctx.so.hashso_buc_populated && ctx.so.hashso_buc_split {
        *buf = ctx.so.hashso_bucket_buf;
        debug_assert!(*buf != InvalidBuffer);
        bm::lock_buffer::call(*buf, bm::BUFFER_LOCK_SHARE)?;

        // move to the end of the bucket chain
        // SAFETY: share lock just acquired.
        while page_opaque(&unsafe { page_ref(*buf) }).hasho_nextblkno != InvalidBlockNumber {
            _hash_readnext(ctx, buf)?;
        }

        ctx.so.hashso_buc_split = false;
    }
    Ok(())
}

/// _hash_first.
pub(crate) fn _hash_first(ctx: &mut HashScanCtx<'_, '_>, dir: ScanDirection) -> PgResult<bool> {
    let rel = ctx.rel;

    if rel.pgstat_enabled.get() {
        *ctx.xs_pgstat_index_scans += 1;
    }
    // C hashsearch.c:301-302 (scan->instrument->nsearches++).
    *ctx.xs_nsearches += 1;

    if ctx.keys.is_empty() {
        return Err(Box::new(
            PgError::error("hash indexes do not support whole-index scans")
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    let cur = &ctx.keys[0];
    debug_assert!(cur.sk_attno == 1);
    debug_assert!(cur.sk_strategy == 1 /* HTEqualStrategyNumber */);

    if cur.sk_flags & SK_ISNULL != 0 {
        return Ok(false);
    }

    let hashkey = if cur.sk_subtype == rel.rd_opcintype[0] || cur.sk_subtype == 0 {
        _hash_datum2hashkey(rel, cur.sk_argument)?
    } else {
        _hash_datum2hashkey_type(rel, cur.sk_argument, cur.sk_subtype)?
    };

    ctx.so.hashso_sk_hash = hashkey;

    let (mut buf, bucket) = _hash_getbucketbuf_from_hashkey(rel, hashkey, HASH_READ)?;
    if let Some(snap) = ctx.snapshot {
        predicate_seams::predicate_lock_page::call(
            rel,
            bm::buffer_get_block_number::call(buf),
            snap,
        )?;
    }
    // SAFETY: share lock held.
    let mut opaque = page_opaque(&unsafe { page_ref(buf) });

    ctx.so.hashso_bucket_buf = buf;

    if H_BUCKET_BEING_POPULATED(opaque.hasho_flag) {
        // A split is in progress: also pin the old bucket, lock ordering
        // old-then-new (see hashsearch.c).
        let old_blkno = _hash_get_oldblock_from_newbucket(rel, bucket)?;

        bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;

        let old_buf = _hash_getbuf(rel, old_blkno, HASH_READ, LH_BUCKET_PAGE)?;
        ctx.so.hashso_split_bucket_buf = old_buf;
        bm::lock_buffer::call(old_buf, bm::BUFFER_LOCK_UNLOCK)?;

        bm::lock_buffer::call(buf, bm::BUFFER_LOCK_SHARE)?;
        // SAFETY: share lock reacquired.
        opaque = page_opaque(&unsafe { page_ref(buf) });
        debug_assert!(opaque.hasho_bucket == bucket);

        if H_BUCKET_BEING_POPULATED(opaque.hasho_flag) {
            ctx.so.hashso_buc_populated = true;
        } else {
            _hash_dropbuf(ctx.so.hashso_split_bucket_buf)?;
            ctx.so.hashso_split_bucket_buf = InvalidBuffer;
        }
    }

    if dir == BackwardScanDirection {
        while opaque.hasho_nextblkno != InvalidBlockNumber
            || (ctx.so.hashso_buc_populated && !ctx.so.hashso_buc_split)
        {
            _hash_readnext(ctx, &mut buf)?;
            // SAFETY: readnext returns buf locked (or the loop condition
            // fails on the stale opaque only when buf went invalid).
            if buf == InvalidBuffer {
                break;
            }
            opaque = page_opaque(&unsafe { page_ref(buf) });
        }
    }

    debug_assert!(ctx.so.currPos.buf == InvalidBuffer);
    ctx.so.currPos.buf = buf;

    if !_hash_readpage(ctx, buf, dir)? {
        return Ok(false);
    }

    // SAFETY: itemIndex set by readpage within the loaded range.
    let curr_item = unsafe { ctx.so.currPos.item(ctx.so.currPos.itemIndex as usize) };
    *ctx.xs_heaptid = curr_item.heapTid;

    Ok(true)
}

/// _hash_readpage: load all qualifying items on the chain page into currPos.
fn _hash_readpage(
    ctx: &mut HashScanCtx<'_, '_>,
    mut buf: Buffer,
    dir: ScanDirection,
) -> PgResult<bool> {
    debug_assert!(buf != InvalidBuffer);
    _hash_checkpage(ctx.rel, buf, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;

    ctx.so.currPos.buf = buf;
    ctx.so.currPos.currPage = bm::buffer_get_block_number::call(buf);

    if dir == ForwardScanDirection {
        let mut prev_blkno = InvalidBlockNumber;
        loop {
            // SAFETY: lock held on buf across the page read.
            let offnum = _hash_binsearch(&unsafe { page_ref(buf) }, ctx.so.hashso_sk_hash);
            let item_index = _hash_load_qualified_items(ctx, buf, offnum, dir);

            if item_index != 0 {
                ctx.so.currPos.firstItem = 0;
                ctx.so.currPos.lastItem = item_index - 1;
                ctx.so.currPos.itemIndex = 0;
                break;
            }

            if ctx.so.numKilled > 0 {
                _hash_kill_items(ctx)?;
            }

            if ctx.so.currPos.buf == ctx.so.hashso_bucket_buf
                || ctx.so.currPos.buf == ctx.so.hashso_split_bucket_buf
            {
                prev_blkno = InvalidBlockNumber;
            } else {
                // SAFETY: lock held on buf.
                prev_blkno = page_opaque(&unsafe { page_ref(buf) }).hasho_prevblkno;
            }

            _hash_readnext(ctx, &mut buf)?;
            if buf != InvalidBuffer {
                ctx.so.currPos.buf = buf;
                ctx.so.currPos.currPage = bm::buffer_get_block_number::call(buf);
            } else {
                ctx.so.currPos.prevPage = prev_blkno;
                ctx.so.currPos.nextPage = InvalidBlockNumber;
                ctx.so.currPos.buf = buf;
                return Ok(false);
            }
        }
    } else {
        let mut next_blkno = InvalidBlockNumber;
        loop {
            // SAFETY: lock held on buf.
            let offnum = _hash_binsearch_last(&unsafe { page_ref(buf) }, ctx.so.hashso_sk_hash);
            let item_index = _hash_load_qualified_items(ctx, buf, offnum, dir);

            if item_index != MaxIndexTuplesPerPage as i32 {
                ctx.so.currPos.firstItem = item_index;
                ctx.so.currPos.lastItem = MaxIndexTuplesPerPage as i32 - 1;
                ctx.so.currPos.itemIndex = MaxIndexTuplesPerPage as i32 - 1;
                break;
            }

            if ctx.so.numKilled > 0 {
                _hash_kill_items(ctx)?;
            }

            if ctx.so.currPos.buf == ctx.so.hashso_bucket_buf
                || ctx.so.currPos.buf == ctx.so.hashso_split_bucket_buf
            {
                // SAFETY: lock held on buf.
                next_blkno = page_opaque(&unsafe { page_ref(buf) }).hasho_nextblkno;
            }

            _hash_readprev(ctx, &mut buf)?;
            if buf != InvalidBuffer {
                ctx.so.currPos.buf = buf;
                ctx.so.currPos.currPage = bm::buffer_get_block_number::call(buf);
            } else {
                ctx.so.currPos.prevPage = InvalidBlockNumber;
                ctx.so.currPos.nextPage = next_blkno;
                ctx.so.currPos.buf = buf;
                return Ok(false);
            }
        }
    }

    if ctx.so.currPos.buf == ctx.so.hashso_bucket_buf
        || ctx.so.currPos.buf == ctx.so.hashso_split_bucket_buf
    {
        // SAFETY: lock still held on the current page.
        let opaque = page_opaque(&unsafe { page_ref(ctx.so.currPos.buf) });
        ctx.so.currPos.prevPage = InvalidBlockNumber;
        ctx.so.currPos.nextPage = opaque.hasho_nextblkno;
        bm::lock_buffer::call(ctx.so.currPos.buf, bm::BUFFER_LOCK_UNLOCK)?;
    } else {
        // SAFETY: lock still held on the current page.
        let opaque = page_opaque(&unsafe { page_ref(ctx.so.currPos.buf) });
        ctx.so.currPos.prevPage = opaque.hasho_prevblkno;
        ctx.so.currPos.nextPage = opaque.hasho_nextblkno;
        _hash_relbuf(ctx.so.currPos.buf)?;
        ctx.so.currPos.buf = InvalidBuffer;
    }

    debug_assert!(ctx.so.currPos.firstItem <= ctx.so.currPos.lastItem);
    Ok(true)
}

/// _hash_load_qualified_items.
fn _hash_load_qualified_items(
    ctx: &mut HashScanCtx<'_, '_>,
    buf: Buffer,
    mut offnum: OffsetNumber,
    dir: ScanDirection,
) -> i32 {
    // SAFETY: lock held on buf for the whole load.
    let page = unsafe { page_ref(buf) };
    let maxoff = page.max_offset_number();

    if dir == ForwardScanDirection {
        let mut item_index = 0i32;
        while offnum <= maxoff {
            debug_assert!(offnum >= 1);
            let id = page.item_id(offnum);
            // SAFETY: bounded offset of the locked page.
            let itup = unsafe { page.item_raw(id) }.0;
            // SAFETY: live on-page tuple.
            let (t_info, hashkey, tid) = unsafe {
                (nbtree::itup::t_info(itup), _hash_get_indextuple_hashkey(itup), nbtree::itup::t_tid(itup))
            };

            // skip split-moved tuples for scans started mid-split, and killed
            // tuples.
            if (ctx.so.hashso_buc_populated
                && !ctx.so.hashso_buc_split
                && t_info & INDEX_MOVED_BY_SPLIT_MASK != 0)
                || (ctx.ignore_killed_tuples && id.is_dead())
            {
                offnum += 1;
                continue;
            }

            if ctx.so.hashso_sk_hash == hashkey && _hash_checkqual() {
                ctx.so.currPos.set_item(
                    item_index as usize,
                    HashScanPosItem { heapTid: tid, indexOffset: offnum },
                );
                item_index += 1;
            } else {
                break;
            }
            offnum += 1;
        }
        debug_assert!(item_index <= MaxIndexTuplesPerPage as i32);
        item_index
    } else {
        let mut item_index = MaxIndexTuplesPerPage as i32;
        while offnum >= 1 {
            debug_assert!(offnum <= maxoff);
            let id = page.item_id(offnum);
            // SAFETY: bounded offset of the locked page.
            let itup = unsafe { page.item_raw(id) }.0;
            // SAFETY: live on-page tuple.
            let (t_info, hashkey, tid) = unsafe {
                (nbtree::itup::t_info(itup), _hash_get_indextuple_hashkey(itup), nbtree::itup::t_tid(itup))
            };

            if (ctx.so.hashso_buc_populated
                && !ctx.so.hashso_buc_split
                && t_info & INDEX_MOVED_BY_SPLIT_MASK != 0)
                || (ctx.ignore_killed_tuples && id.is_dead())
            {
                offnum -= 1;
                continue;
            }

            if ctx.so.hashso_sk_hash == hashkey && _hash_checkqual() {
                item_index -= 1;
                ctx.so.currPos.set_item(
                    item_index as usize,
                    HashScanPosItem { heapTid: tid, indexOffset: offnum },
                );
            } else {
                break;
            }
            offnum -= 1;
        }
        debug_assert!(item_index >= 0);
        item_index
    }
}

/// _hash_kill_items (hashutil.c).
pub(crate) fn _hash_kill_items(ctx: &mut HashScanCtx<'_, '_>) -> PgResult<()> {
    let so = &mut *ctx.so;
    debug_assert!(so.numKilled > 0);
    debug_assert!(!so.killedItems.is_empty());
    debug_assert!(HashScanPosIsValid(&so.currPos));

    let num_killed = so.numKilled as usize;
    // Reset so we don't chase the same items on another page.
    so.numKilled = 0;

    let blkno = so.currPos.currPage;
    let mut have_pin = false;
    let buf;
    if HashScanPosIsPinned(&so.currPos) {
        have_pin = true;
        buf = so.currPos.buf;
        bm::lock_buffer::call(buf, bm::BUFFER_LOCK_SHARE)?;
    } else {
        buf = _hash_getbuf(ctx.rel, blkno, HASH_READ, LH_OVERFLOW_PAGE)?;
    }

    let mut killed_something = false;
    {
        // SAFETY: share lock held; LP_DEAD is a hint bit (C sets it under
        // share lock too).
        let mut page = unsafe { crate::page::page_mut(buf) };
        let maxoff = page.as_ref().max_offset_number();

        for i in 0..num_killed {
            let item_index = so.killedItems[i];
            debug_assert!(
                item_index >= so.currPos.firstItem && item_index <= so.currPos.lastItem
            );
            // SAFETY: killedItems holds indexes previously written by readpage.
            let curr_item = unsafe { so.currPos.item(item_index as usize) };
            let mut offnum = curr_item.indexOffset;

            while offnum <= maxoff {
                let mut id = page.as_ref().item_id(offnum);
                // SAFETY: bounded offset of the locked page.
                let tid = unsafe { nbtree::itup::t_tid(page.as_ref().item_raw(id).0) };
                if ItemPointerEquals(&tid, &curr_item.heapTid) {
                    id.mark_dead();
                    page.set_item_id(offnum, id);
                    killed_something = true;
                    break;
                }
                offnum += 1;
            }
        }

        if killed_something {
            let mut opaque = page_opaque(&page.as_ref());
            opaque.hasho_flag |= LH_PAGE_HAS_DEAD_TUPLES;
            crate::page::write_opaque(&mut page, &opaque);
        }
    }
    if killed_something {
        bm::mark_buffer_dirty_hint::call(buf, true)?;
    }

    if so.hashso_bucket_buf == so.currPos.buf || have_pin {
        bm::lock_buffer::call(so.currPos.buf, bm::BUFFER_LOCK_UNLOCK)?;
    } else {
        _hash_relbuf(buf)?;
    }
    Ok(())
}
