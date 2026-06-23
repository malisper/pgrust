//! Port of `src/backend/access/hash/hashsearch.c` (PostgreSQL 18.3): the hash
//! index scan — `_hash_first`, `_hash_next`, and the private page walkers
//! (`_hash_readpage`, `_hash_readnext`, `_hash_readprev`,
//! `_hash_load_qualified_items`, `_hash_saveitem`).
//!
//! The C `IndexScanDesc scan` + `HashScanOpaqueData *so` are threaded as a
//! single owned [`crate::HashScan`].
//!
//! ## Seam-and-panic paths (unported callees in other crates)
//! * Buffer manager seams (see [`crate::hashpage`]).
//! * predicate (`predicate_lock_page`).
//! * pgstat (`pgstat_count_index_scan`).
//! * interrupts (`check_for_interrupts`).

use ::types_core::primitive::{InvalidBlockNumber, OffsetNumber};
use ::types_error::{PgError, PgResult, ERROR};
use ::hash::hashpage::{
    H_BUCKET_BEING_POPULATED, HASH_READ, HashScanPosInvalidate, HashScanPosItem, INDEX_MOVED_BY_SPLIT_MASK, LH_BUCKET_PAGE, LH_OVERFLOW_PAGE,
    MaxIndexTuplesPerPage,
};
use ::types_scan::scankey::SK_ISNULL;
use ::types_scan::sdir::{ScanDirection, ScanDirectionIsForward, ScanDirectionIsBackward};
use ::types_storage::storage::{Buffer, BufferIsValid, InvalidBuffer};

use bufmgr_seams as bufmgr;
use predicate_seams as predicate;
use postgres_seams as postgres;
use pgstat_seams as pgstat;

use ::page::{
    ItemIdIsDead, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
};

use crate::hashpage::{
    block_number_is_valid, with_page_ref, _hash_dropbuf, _hash_dropscanbuf,
    _hash_getbucketbuf_from_hashkey, _hash_getbuf, _hash_relbuf,
};
use crate::hashutil::{
    BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK, _hash_binsearch, _hash_binsearch_last, _hash_checkpage,
    _hash_checkqual, _hash_datum2hashkey, _hash_datum2hashkey_type, _hash_get_indextuple_hashkey,
    _hash_kill_items, _hash_get_oldblock_from_newbucket,
};
use crate::pagebytes::{hasho_bucket, hasho_flag, hasho_nextblkno, hasho_prevblkno, index_tuple_t_info, index_tuple_tid};
use crate::HashScan;

// ===========================================================================
// _hash_next (hashsearch.c:47)
// ===========================================================================

/// `_hash_next(scan, dir)` — get the next item in a scan.
pub fn _hash_next<'mcx>(scan: &mut HashScan<'mcx>, dir: ScanDirection) -> PgResult<bool> {
    let rel = scan.indexRelation.alias();
    let mut end_of_scan = false;

    if ScanDirectionIsForward(dir) {
        scan.opaque.currPos.itemIndex += 1;
        if scan.opaque.currPos.itemIndex > scan.opaque.currPos.lastItem {
            if scan.opaque.numKilled > 0 {
                _hash_kill_items(scan)?;
            }

            let blkno = scan.opaque.currPos.nextPage;
            if block_number_is_valid(blkno) {
                let mut buf = _hash_getbuf(&rel, blkno, HASH_READ, LH_OVERFLOW_PAGE as i32)?;
                if !_hash_readpage(scan, &mut buf, dir)? {
                    end_of_scan = true;
                }
            } else {
                end_of_scan = true;
            }
        }
    } else {
        scan.opaque.currPos.itemIndex -= 1;
        if scan.opaque.currPos.itemIndex < scan.opaque.currPos.firstItem {
            if scan.opaque.numKilled > 0 {
                _hash_kill_items(scan)?;
            }

            let blkno = scan.opaque.currPos.prevPage;
            if block_number_is_valid(blkno) {
                let mut buf = _hash_getbuf(
                    &rel,
                    blkno,
                    HASH_READ,
                    (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32,
                )?;

                // We always maintain the pin on bucket page; release the extra.
                if buf == scan.opaque.hashso_bucket_buf
                    || buf == scan.opaque.hashso_split_bucket_buf
                {
                    _hash_dropbuf(&rel, buf);
                }

                if !_hash_readpage(scan, &mut buf, dir)? {
                    end_of_scan = true;
                }
            } else {
                end_of_scan = true;
            }
        }
    }

    if end_of_scan {
        _hash_dropscanbuf(&rel, &mut scan.opaque);
        HashScanPosInvalidate(&mut scan.opaque.currPos);
        return Ok(false);
    }

    // itemIndex says what to return
    let idx = scan.opaque.currPos.itemIndex as usize;
    scan.xs_heaptid = scan.opaque.currPos.items[idx].heapTid;

    Ok(true)
}

// ===========================================================================
// _hash_readnext (hashsearch.c:130)
// ===========================================================================

/// `_hash_readnext(scan, &buf)` — advance to next page in a bucket, if any.
/// Returns the new buffer (`InvalidBuffer` if none). Mirrors the C in-out
/// `*bufp` parameter.
fn _hash_readnext<'mcx>(scan: &mut HashScan<'mcx>, bufp: &mut Buffer) -> PgResult<()> {
    let rel = scan.indexRelation.alias();

    let blkno = with_page_ref(*bufp, |p| Ok(hasho_nextblkno(p.as_bytes())))?;

    // Retain pin on primary bucket page till end of scan.
    if *bufp == scan.opaque.hashso_bucket_buf || *bufp == scan.opaque.hashso_split_bucket_buf {
        bufmgr::lock_buffer::call(*bufp, BUFFER_LOCK_UNLOCK)?;
    } else {
        _hash_relbuf(&rel, *bufp);
    }

    *bufp = InvalidBuffer;
    postgres::check_for_interrupts::call()?;

    if block_number_is_valid(blkno) {
        *bufp = _hash_getbuf(&rel, blkno, HASH_READ, LH_OVERFLOW_PAGE as i32)?;
    } else if scan.opaque.hashso_buc_populated && !scan.opaque.hashso_buc_split {
        // end of bucket, scan bucket being split if there was a split.
        *bufp = scan.opaque.hashso_split_bucket_buf;
        debug_assert!(BufferIsValid(*bufp));

        bufmgr::lock_buffer::call(*bufp, BUFFER_LOCK_SHARE)?;
        let blk = bufmgr::buffer_get_block_number::call(*bufp);
        predicate::predicate_lock_page::call(scan.indexRelation.alias(), blk, scan.xs_snapshot.clone())?;

        scan.opaque.hashso_buc_split = true;
    }

    Ok(())
}

// ===========================================================================
// _hash_readprev (hashsearch.c:196)
// ===========================================================================

/// `_hash_readprev(scan, &buf)` — advance to previous page in a bucket, if any.
fn _hash_readprev<'mcx>(scan: &mut HashScan<'mcx>, bufp: &mut Buffer) -> PgResult<()> {
    let rel = scan.indexRelation.alias();

    let blkno = with_page_ref(*bufp, |p| Ok(hasho_prevblkno(p.as_bytes())))?;

    let haveprevblk;
    if *bufp == scan.opaque.hashso_bucket_buf || *bufp == scan.opaque.hashso_split_bucket_buf {
        bufmgr::lock_buffer::call(*bufp, BUFFER_LOCK_UNLOCK)?;
        haveprevblk = false;
    } else {
        _hash_relbuf(&rel, *bufp);
        haveprevblk = true;
    }

    *bufp = InvalidBuffer;
    postgres::check_for_interrupts::call()?;

    if haveprevblk {
        debug_assert!(block_number_is_valid(blkno));
        *bufp = _hash_getbuf(
            &rel,
            blkno,
            HASH_READ,
            (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32,
        )?;

        // release the extra pin if this is a primary bucket page.
        if *bufp == scan.opaque.hashso_bucket_buf || *bufp == scan.opaque.hashso_split_bucket_buf {
            _hash_dropbuf(&rel, *bufp);
        }
    } else if scan.opaque.hashso_buc_populated && scan.opaque.hashso_buc_split {
        // end of bucket, scan bucket being populated.
        *bufp = scan.opaque.hashso_bucket_buf;
        debug_assert!(BufferIsValid(*bufp));

        bufmgr::lock_buffer::call(*bufp, BUFFER_LOCK_SHARE)?;

        // move to the end of bucket chain
        while block_number_is_valid(with_page_ref(*bufp, |p| Ok(hasho_nextblkno(p.as_bytes())))?) {
            _hash_readnext(scan, bufp)?;
        }

        scan.opaque.hashso_buc_split = false;
    }

    Ok(())
}

// ===========================================================================
// _hash_first (hashsearch.c:287)
// ===========================================================================

/// `_hash_first(scan, dir)` — find the first item in a scan.
pub fn _hash_first<'mcx>(scan: &mut HashScan<'mcx>, dir: ScanDirection) -> PgResult<bool> {
    let rel = scan.indexRelation.alias();

    pgstat::pgstat_count_index_scan::call(rel.rd_id, rel.rd_rel.relisshared, rel.pgstat_enabled);
    if scan.instrument {
        scan.nsearches += 1;
    }

    // We do not support hash scans with no index qualification.
    if scan.numberOfKeys < 1 {
        return Err(PgError::new(
            ERROR,
            "hash indexes do not support whole-index scans",
        ));
    }

    // There may be more than one index qual, but we hash only the first.
    let cur = scan.keyData[0].clone();

    debug_assert!(cur.sk_attno == 1);
    debug_assert!(cur.sk_strategy == ::hash::hash::HTEqualStrategyNumber);

    // NULL constant cannot match any items.
    if (cur.sk_flags & SK_ISNULL) != 0 {
        return Ok(false);
    }

    // Compute the hash key (before acquiring locks). The scan key argument may
    // be a by-reference value (e.g. a `uuid`/`macaddr` equality bound), which
    // must cross the fmgr boundary on the by-reference lane; a scratch context
    // backs that marshalling (the hash result is a by-value `int4`, read out
    // before the scratch is dropped).
    let opcintype = relcache_seams::rd_opcintype::call(&rel, 1)?;
    let hashkey_cxt = mcx::MemoryContext::new("_hash_first hashkey");
    let hk_mcx = hashkey_cxt.mcx();
    let hashkey = if cur.sk_subtype == opcintype || cur.sk_subtype == ::types_core::InvalidOid {
        _hash_datum2hashkey(hk_mcx, &rel, &cur.sk_argument)?
    } else {
        _hash_datum2hashkey_type(hk_mcx, &rel, &cur.sk_argument, cur.sk_subtype)?
    };
    drop(hashkey_cxt);

    scan.opaque.hashso_sk_hash = hashkey;

    let mut buf = _hash_getbucketbuf_from_hashkey(&rel, hashkey, HASH_READ, None)?;
    let blk = bufmgr::buffer_get_block_number::call(buf);
    predicate::predicate_lock_page::call(rel.alias(), blk, scan.xs_snapshot.clone())?;

    let (mut pageflag, bucket) =
        with_page_ref(buf, |p| Ok((hasho_flag(p.as_bytes()), hasho_bucket(p.as_bytes()))))?;

    scan.opaque.hashso_bucket_buf = buf;

    // If a bucket split is in progress, deal with the split bucket.
    if H_BUCKET_BEING_POPULATED(pageflag) {
        let old_blkno = _hash_get_oldblock_from_newbucket(&rel, bucket)?;

        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;

        let old_buf = _hash_getbuf(&rel, old_blkno, HASH_READ, LH_BUCKET_PAGE as i32)?;

        scan.opaque.hashso_split_bucket_buf = old_buf;
        bufmgr::lock_buffer::call(old_buf, BUFFER_LOCK_UNLOCK)?;

        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;
        pageflag = with_page_ref(buf, |p| Ok(hasho_flag(p.as_bytes())))?;
        debug_assert!(with_page_ref(buf, |p| Ok(hasho_bucket(p.as_bytes())))? == bucket);

        if H_BUCKET_BEING_POPULATED(pageflag) {
            scan.opaque.hashso_buc_populated = true;
        } else {
            _hash_dropbuf(&rel, scan.opaque.hashso_split_bucket_buf);
            scan.opaque.hashso_split_bucket_buf = InvalidBuffer;
        }
    }

    // If a backwards scan is requested, move to the end of the chain.
    if ScanDirectionIsBackward(dir) {
        loop {
            let next_valid = block_number_is_valid(with_page_ref(buf, |p| Ok(hasho_nextblkno(p.as_bytes())))?);
            let pop = scan.opaque.hashso_buc_populated && !scan.opaque.hashso_buc_split;
            if !(next_valid || pop) {
                break;
            }
            _hash_readnext(scan, &mut buf)?;
        }
    }

    debug_assert!(!BufferIsValid(scan.opaque.currPos.buf));
    scan.opaque.currPos.buf = buf;

    // Find all the tuples satisfying the qualification from a page.
    if !_hash_readpage(scan, &mut buf, dir)? {
        return Ok(false);
    }

    let idx = scan.opaque.currPos.itemIndex as usize;
    scan.xs_heaptid = scan.opaque.currPos.items[idx].heapTid;

    Ok(true)
}

// ===========================================================================
// _hash_readpage (hashsearch.c:447)
// ===========================================================================

/// `_hash_readpage(scan, &buf, dir)` — load matching items from the current page
/// into `so->currPos`. Returns true if any were found.
fn _hash_readpage<'mcx>(
    scan: &mut HashScan<'mcx>,
    bufp: &mut Buffer,
    dir: ScanDirection,
) -> PgResult<bool> {
    let rel = scan.indexRelation.alias();

    let mut buf = *bufp;
    debug_assert!(BufferIsValid(buf));
    _hash_checkpage(&rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32)?;

    scan.opaque.currPos.buf = buf;
    scan.opaque.currPos.currPage = bufmgr::buffer_get_block_number::call(buf);

    if ScanDirectionIsForward(dir) {
        let mut prev_blkno = InvalidBlockNumber;
        let item_index;

        loop {
            let sk_hash = scan.opaque.hashso_sk_hash;
            let offnum = with_page_ref(buf, |p| _hash_binsearch(p, sk_hash))?;
            let ii = _hash_load_qualified_items(scan, buf, offnum, dir)?;

            if ii != 0 {
                item_index = ii;
                break;
            }

            // No matches; move to next page. Deal with killed items first.
            if scan.opaque.numKilled > 0 {
                _hash_kill_items(scan)?;
            }

            // hasho_prevblkno is not a real block number on a primary bucket.
            if scan.opaque.currPos.buf == scan.opaque.hashso_bucket_buf
                || scan.opaque.currPos.buf == scan.opaque.hashso_split_bucket_buf
            {
                prev_blkno = InvalidBlockNumber;
            } else {
                prev_blkno = with_page_ref(buf, |p| Ok(hasho_prevblkno(p.as_bytes())))?;
            }

            _hash_readnext(scan, &mut buf)?;
            if BufferIsValid(buf) {
                scan.opaque.currPos.buf = buf;
                scan.opaque.currPos.currPage = bufmgr::buffer_get_block_number::call(buf);
            } else {
                scan.opaque.currPos.prevPage = prev_blkno;
                scan.opaque.currPos.nextPage = InvalidBlockNumber;
                scan.opaque.currPos.buf = buf;
                *bufp = buf;
                return Ok(false);
            }
        }

        scan.opaque.currPos.firstItem = 0;
        scan.opaque.currPos.lastItem = item_index - 1;
        scan.opaque.currPos.itemIndex = 0;
    } else {
        let mut next_blkno = InvalidBlockNumber;
        let item_index;

        loop {
            let sk_hash = scan.opaque.hashso_sk_hash;
            let offnum = with_page_ref(buf, |p| _hash_binsearch_last(p, sk_hash))?;
            let ii = _hash_load_qualified_items(scan, buf, offnum, dir)?;

            if ii != MaxIndexTuplesPerPage as i32 {
                item_index = ii;
                break;
            }

            if scan.opaque.numKilled > 0 {
                _hash_kill_items(scan)?;
            }

            if scan.opaque.currPos.buf == scan.opaque.hashso_bucket_buf
                || scan.opaque.currPos.buf == scan.opaque.hashso_split_bucket_buf
            {
                next_blkno = with_page_ref(buf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;
            }

            _hash_readprev(scan, &mut buf)?;
            if BufferIsValid(buf) {
                scan.opaque.currPos.buf = buf;
                scan.opaque.currPos.currPage = bufmgr::buffer_get_block_number::call(buf);
            } else {
                scan.opaque.currPos.prevPage = InvalidBlockNumber;
                scan.opaque.currPos.nextPage = next_blkno;
                scan.opaque.currPos.buf = buf;
                *bufp = buf;
                return Ok(false);
            }
        }

        scan.opaque.currPos.firstItem = item_index;
        scan.opaque.currPos.lastItem = MaxIndexTuplesPerPage as i32 - 1;
        scan.opaque.currPos.itemIndex = MaxIndexTuplesPerPage as i32 - 1;
    }

    if scan.opaque.currPos.buf == scan.opaque.hashso_bucket_buf
        || scan.opaque.currPos.buf == scan.opaque.hashso_split_bucket_buf
    {
        scan.opaque.currPos.prevPage = InvalidBlockNumber;
        scan.opaque.currPos.nextPage = with_page_ref(scan.opaque.currPos.buf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;
        bufmgr::lock_buffer::call(scan.opaque.currPos.buf, BUFFER_LOCK_UNLOCK)?;
    } else {
        scan.opaque.currPos.prevPage = with_page_ref(scan.opaque.currPos.buf, |p| Ok(hasho_prevblkno(p.as_bytes())))?;
        scan.opaque.currPos.nextPage = with_page_ref(scan.opaque.currPos.buf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;
        _hash_relbuf(&rel, scan.opaque.currPos.buf);
        scan.opaque.currPos.buf = InvalidBuffer;
    }

    debug_assert!(scan.opaque.currPos.firstItem <= scan.opaque.currPos.lastItem);
    *bufp = buf;
    Ok(true)
}

// ===========================================================================
// _hash_load_qualified_items (hashsearch.c:603)
// ===========================================================================

/// `_hash_load_qualified_items(scan, page, offnum, dir)` — load all qualified
/// items from a page into `so->currPos`. `page` is given as the pinned `Buffer`
/// (the C `Page` argument; we read it back under the buffer).
fn _hash_load_qualified_items<'mcx>(
    scan: &mut HashScan<'mcx>,
    buf: Buffer,
    mut offnum: OffsetNumber,
    dir: ScanDirection,
) -> PgResult<i32> {
    let maxoff = with_page_ref(buf, |p| Ok(PageGetMaxOffsetNumber(p)))?;
    let sk_hash = scan.opaque.hashso_sk_hash;
    let buc_populated = scan.opaque.hashso_buc_populated;
    let buc_split = scan.opaque.hashso_buc_split;
    let ignore_killed = scan.ignore_killed_tuples;

    if ScanDirectionIsForward(dir) {
        let mut item_index = 0i32;

        while offnum <= maxoff {
            debug_assert!(offnum >= 1);
            let (itup, is_dead) = with_page_ref(buf, |p| {
                let iid = PageGetItemId(p, offnum)?;
                Ok((PageGetItem(p, &iid)?.to_vec(), ItemIdIsDead(&iid)))
            })?;

            // skip moved-by-split tuples and dead tuples
            if (buc_populated
                && !buc_split
                && (index_tuple_t_info(&itup) & INDEX_MOVED_BY_SPLIT_MASK) != 0)
                || (ignore_killed && is_dead)
            {
                offnum += 1;
                continue;
            }

            if sk_hash == _hash_get_indextuple_hashkey(&itup) && _hash_checkqual(scan, &itup) {
                _hash_saveitem(scan, item_index, offnum, &itup);
                item_index += 1;
            } else {
                break;
            }

            offnum += 1;
        }

        debug_assert!(item_index <= MaxIndexTuplesPerPage as i32);
        Ok(item_index)
    } else {
        let mut item_index = MaxIndexTuplesPerPage as i32;

        while offnum >= 1 {
            debug_assert!(offnum <= maxoff);
            let (itup, is_dead) = with_page_ref(buf, |p| {
                let iid = PageGetItemId(p, offnum)?;
                Ok((PageGetItem(p, &iid)?.to_vec(), ItemIdIsDead(&iid)))
            })?;

            if (buc_populated
                && !buc_split
                && (index_tuple_t_info(&itup) & INDEX_MOVED_BY_SPLIT_MASK) != 0)
                || (ignore_killed && is_dead)
            {
                // move back; offnum can underflow to 0 which ends the loop.
                if offnum == 0 {
                    break;
                }
                offnum -= 1;
                continue;
            }

            if sk_hash == _hash_get_indextuple_hashkey(&itup) && _hash_checkqual(scan, &itup) {
                item_index -= 1;
                _hash_saveitem(scan, item_index, offnum, &itup);
            } else {
                break;
            }

            if offnum == 0 {
                break;
            }
            offnum -= 1;
        }

        debug_assert!(item_index >= 0);
        Ok(item_index)
    }
}

// ===========================================================================
// _hash_saveitem (hashsearch.c:709)
// ===========================================================================

/// `_hash_saveitem(so, itemIndex, offnum, itup)` — save an index item into
/// `so->currPos.items[itemIndex]`.
fn _hash_saveitem<'mcx>(scan: &mut HashScan<'mcx>, item_index: i32, offnum: OffsetNumber, itup: &[u8]) {
    scan.opaque.currPos.items[item_index as usize] = HashScanPosItem {
        heapTid: index_tuple_tid(itup),
        indexOffset: offnum,
    };
}

