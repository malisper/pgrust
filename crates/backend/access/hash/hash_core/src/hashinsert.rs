//! Port of `src/backend/access/hash/hashinsert.c` (PostgreSQL 18.3): item
//! insertion — `_hash_doinsert`, `_hash_pgaddtup`, `_hash_pgaddmultitup`,
//! `_hash_vacuum_one_page`.
//!
//! ## Seam-and-panic paths (unported callees in other crates)
//! * Buffer manager + WAL insert seams (see [`crate::hashpage`]).
//! * predicate (`check_for_serializable_conflict_in_page`).
//! * heapam (`index_compute_xid_horizon_for_tuples`).
//! * xlog (`xlog_ensure_record_space`).

extern crate alloc;
use alloc::vec::Vec;

use types_core::primitive::OffsetNumber;
use types_error::{PgError, PgResult, ERROR};
use hash::hashpage::{
    H_BUCKET_BEING_SPLIT, H_HAS_DEAD_TUPLES, HASH_METAPAGE, HASH_NOLOCK, HASH_WRITE,
    LH_BUCKET_PAGE, LH_META_PAGE, LH_OVERFLOW_PAGE, LH_PAGE_HAS_DEAD_TUPLES, LH_PAGE_TYPE,
};
use rel::Relation;
use types_storage::storage::Buffer;

use heapam_seams as heapam;
use xloginsert_seams as xloginsert;
use bufmgr_seams as bufmgr;
use predicate_seams as predicate;

use page::{
    ItemIdIsDead, PageAddItemExtended, PageGetFreeSpace, PageGetItemId, PageGetMaxOffsetNumber,
    PageIndexMultiDelete, PageMut, PageRef,
};

use crate::hashpage::{
    block_number_is_valid, with_page_ref, _hash_dropbuf,
    _hash_expandtable, _hash_finish_split, _hash_getbucketbuf_from_hashkey, _hash_getbuf,
    _hash_relbuf, REGBUF_STANDARD, XLOG_HASH_INSERT, XLOG_HASH_VACUUM_ONE_PAGE,
};
use crate::hashovfl::_hash_addovflpage;
use crate::hashutil::{
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, _hash_binsearch, _hash_checkpage,
    _hash_get_indextuple_hashkey,
};
use crate::pagebytes::{
    hasho_bucket, hasho_flag, hasho_nextblkno, index_tuple_size, maxalign, meta_ntuples,
    set_hasho_flag, set_meta_ntuples,
};
use crate::wal::RM_HASH_ID;

// ===========================================================================
// _hash_doinsert (hashinsert.c:37)
// ===========================================================================

/// `_hash_doinsert(rel, itup, heapRel, sorted)` — insert a single index tuple.
/// `itup` is the formed on-disk index-tuple bytes (from hash.c's
/// `index_form_tuple`). `sorted` must only be true when inserts are in hashkey
/// order.
pub fn _hash_doinsert<'mcx>(
    rel: &Relation<'mcx>,
    itup: &[u8],
    heap_rel: &Relation<'mcx>,
    sorted: bool,
) -> PgResult<()> {
    // Hash key for the item (stored in the tuple itself).
    let hashkey = _hash_get_indextuple_hashkey(itup);

    // compute item size too
    let itemsz = maxalign(index_tuple_size(itup));

    loop {
        // restart_insert:
        // Read the metapage (unlocked).
        let metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_NOLOCK, LH_META_PAGE as i32)?;

        // Check whether the item can fit on a hash page at all.
        let max_item = with_page_ref(metabuf, |p| {
            Ok(crate::pagebytes::hash_max_item_size(
                page::PageGetPageSize(p),
            ))
        })?;
        if itemsz > max_item {
            _hash_relbuf(rel, metabuf);
            return Err(PgError::new(ERROR, "index row size exceeds hash maximum"));
        }

        // Lock the primary bucket page for the target bucket.
        let mut usedmetap = hash::hashpage::HashMetaPageData::default();
        let mut buf =
            _hash_getbucketbuf_from_hashkey(rel, hashkey, HASH_WRITE, Some(&mut usedmetap))?;

        predicate::check_for_serializable_conflict_in_page::call(
            rel.rd_id,
            bufmgr::buffer_get_block_number::call(buf),
        )?;

        // remember the primary bucket buffer to release the pin at end.
        let bucket_buf = buf;

        let (mut pageflag, bucket) =
            with_page_ref(buf, |p| Ok((hasho_flag(p.as_bytes()), hasho_bucket(p.as_bytes()))))?;

        // If this bucket is being split, try to finish the split first.
        if H_BUCKET_BEING_SPLIT(pageflag) && bufmgr::is_buffer_cleanup_ok::call(buf)? {
            bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;

            _hash_finish_split(
                rel,
                metabuf,
                buf,
                bucket,
                usedmetap.hashm_maxbucket,
                usedmetap.hashm_highmask,
                usedmetap.hashm_lowmask,
            )?;

            _hash_dropbuf(rel, buf);
            _hash_dropbuf(rel, metabuf);
            continue; // goto restart_insert
        }

        // Do the insertion. Walk the bucket chain looking for free space.
        loop {
            let free = with_page_ref(buf, |p| Ok(PageGetFreeSpace(p)))?;
            if free >= itemsz {
                break;
            }

            // Check for DEAD tuples on this page first.
            if H_HAS_DEAD_TUPLES(pageflag) && bufmgr::is_buffer_cleanup_ok::call(buf)? {
                _hash_vacuum_one_page(rel, heap_rel, metabuf, buf)?;
                let free2 = with_page_ref(buf, |p| Ok(PageGetFreeSpace(p)))?;
                if free2 >= itemsz {
                    break; // OK, now we have enough space
                }
            }

            // no space on this page; check for an overflow page
            let nextblkno = with_page_ref(buf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;

            if block_number_is_valid(nextblkno) {
                if buf != bucket_buf {
                    _hash_relbuf(rel, buf);
                } else {
                    bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                }
                buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE as i32)?;
            } else {
                // end of chain; allocate a new overflow page.
                bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                buf = _hash_addovflpage(rel, metabuf, buf, buf == bucket_buf)?;
            }

            pageflag = with_page_ref(buf, |p| Ok(hasho_flag(p.as_bytes())))?;
            debug_assert!((pageflag & LH_PAGE_TYPE) == LH_OVERFLOW_PAGE);
            debug_assert!(with_page_ref(buf, |p| Ok(hasho_bucket(p.as_bytes())))? == bucket);
        }

        // Write-lock the metapage to bump the tuple count.
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;

        // START_CRIT_SECTION — no ereport(ERROR) until logged.
        let itup_off = _hash_pgaddtup(rel, buf, itemsz, itup, sorted)?;
        bufmgr::mark_buffer_dirty::call(buf);

        // metapage operations
        let do_expand = {
            let mut do_expand = false;
            bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
                let n = meta_ntuples(page) + 1.0;
                set_meta_ntuples(page, n);
                let ffactor = crate::pagebytes::meta_ffactor(page) as f64;
                let maxbucket = crate::pagebytes::meta_maxbucket(page) as f64;
                do_expand = n > ffactor * (maxbucket + 1.0);
                Ok(())
            })?;
            do_expand
        };

        bufmgr::mark_buffer_dirty::call(metabuf);

        // XLOG stuff
        if relation_needs_wal(rel)? {
            xloginsert::xlog_begin_insert::call()?;
            let xlrec = itup_off.to_ne_bytes(); // xl_hash_insert { OffsetNumber offnum; }
            xloginsert::xlog_register_data::call(&xlrec)?;
            xloginsert::xlog_register_buffer::call(1, metabuf, REGBUF_STANDARD)?;
            xloginsert::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
            xloginsert::xlog_register_buf_data::call(0, &itup[..index_tuple_size(itup)])?;
            let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_INSERT)?;
            bufmgr::page_set_lsn::call(buf, recptr)?;
            bufmgr::page_set_lsn::call(metabuf, recptr)?;
        }
        // END_CRIT_SECTION

        // drop lock on metapage, but keep pin
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;

        // Release the modified page and the pin on the primary page.
        _hash_relbuf(rel, buf);
        if buf != bucket_buf {
            _hash_dropbuf(rel, bucket_buf);
        }

        // Attempt to split if needed.
        if do_expand {
            _hash_expandtable(rel, metabuf)?;
        }

        // Finally drop our pin on the metapage.
        _hash_dropbuf(rel, metabuf);
        return Ok(());
    }
}

// ===========================================================================
// _hash_pgaddtup (hashinsert.c:273)
// ===========================================================================

/// `_hash_pgaddtup(rel, buf, itemsize, itup, appendtup)` — add a tuple to a
/// page, preserving hashkey ordering. Returns the offset of the new item.
pub fn _hash_pgaddtup<'mcx>(
    rel: &Relation<'mcx>,
    buf: Buffer,
    itemsize: usize,
    itup: &[u8],
    appendtup: bool,
) -> PgResult<OffsetNumber> {
    _hash_checkpage(rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32)?;

    let mut placed = 0u16;
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let itup_off = if appendtup {
            let pref = PageRef::new(page)?;
            PageGetMaxOffsetNumber(&pref) + 1
        } else {
            let hashkey = _hash_get_indextuple_hashkey(itup);
            let pref = PageRef::new(page)?;
            _hash_binsearch(&pref, hashkey)?
        };

        let mut pmut = PageMut::new(page)?;
        let off = PageAddItemExtended(&mut pmut, &itup[..itemsize], itup_off, 0)?;
        if off == 0 {
            return Err(PgError::new(ERROR, "failed to add index item"));
        }
        placed = off;
        Ok(())
    })?;
    Ok(placed)
}

// ===========================================================================
// _hash_pgaddmultitup (hashinsert.c:330)
// ===========================================================================

/// `_hash_pgaddmultitup(rel, buf, itups, itup_offsets, nitups)` — add a tuple
/// vector to a page, preserving hashkey ordering. Returns the placement offsets.
pub fn _hash_pgaddmultitup<'mcx>(
    rel: &Relation<'mcx>,
    buf: Buffer,
    itups: &[Vec<u8>],
) -> PgResult<Vec<OffsetNumber>> {
    _hash_checkpage(rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32)?;

    let mut offsets = Vec::with_capacity(itups.len());
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        offsets.clear();
        for itup in itups {
            let itemsize = maxalign(index_tuple_size(itup));
            let hashkey = _hash_get_indextuple_hashkey(itup);
            let itup_off = {
                let pref = PageRef::new(page)?;
                _hash_binsearch(&pref, hashkey)?
            };
            offsets.push(itup_off);

            let mut pmut = PageMut::new(page)?;
            let off = PageAddItemExtended(&mut pmut, &itup[..itemsize], itup_off, 0)?;
            if off == 0 {
                return Err(PgError::new(ERROR, "failed to add index item"));
            }
        }
        Ok(())
    })?;
    Ok(offsets)
}

// ===========================================================================
// _hash_vacuum_one_page (hashinsert.c:369)
// ===========================================================================

/// `_hash_vacuum_one_page(rel, hrel, metabuf, buf)` — vacuum just one index
/// page (remove LP_DEAD items). Caller holds a cleanup lock on `buf`.
fn _hash_vacuum_one_page<'mcx>(
    rel: &Relation<'mcx>,
    hrel: &Relation<'mcx>,
    metabuf: Buffer,
    buf: Buffer,
) -> PgResult<()> {
    // Scan each tuple to find LP_DEAD items.
    let deletable: Vec<OffsetNumber> = with_page_ref(buf, |p| {
        let maxoff = PageGetMaxOffsetNumber(p);
        let mut v = Vec::new();
        let mut offnum: OffsetNumber = 1; // FirstOffsetNumber
        while offnum <= maxoff {
            let item_id = PageGetItemId(p, offnum)?;
            if ItemIdIsDead(&item_id) {
                v.push(offnum);
            }
            offnum += 1;
        }
        Ok(v)
    })?;

    let ndeletable = deletable.len();
    if ndeletable > 0 {
        let snapshot_conflict_horizon =
            heapam::index_compute_xid_horizon_for_tuples::call(rel, hrel, buf, &deletable)?;

        // Write-lock the meta page to decrement the tuple count.
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;

        // START_CRIT_SECTION
        bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
            let mut pmut = PageMut::new(page)?;
            PageIndexMultiDelete(&mut pmut, &deletable)?;
            // Mark the page as not containing LP_DEAD items (hint).
            let flag = hasho_flag(page) & !LH_PAGE_HAS_DEAD_TUPLES;
            set_hasho_flag(page, flag);
            Ok(())
        })?;

        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            let n = meta_ntuples(page) - ndeletable as f64;
            set_meta_ntuples(page, n);
            Ok(())
        })?;

        bufmgr::mark_buffer_dirty::call(buf);
        bufmgr::mark_buffer_dirty::call(metabuf);

        // XLOG stuff
        if relation_needs_wal(rel)? {
            // xl_hash_vacuum_one_page { TransactionId snapshotConflictHorizon;
            //   uint16 ntuples; bool isCatalogRel; }
            let is_catalog = relation_is_accessible_in_logical_decoding(hrel)?;
            let mut xlrec = [0u8; 8]; // SizeOfHashVacuumOnePage (padded)
            xlrec[0..4].copy_from_slice(&snapshot_conflict_horizon.to_ne_bytes());
            xlrec[4..6].copy_from_slice(&(ndeletable as u16).to_ne_bytes());
            xlrec[6] = is_catalog as u8;

            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_buffer::call(0, buf, REGBUF_STANDARD)?;
            xloginsert::xlog_register_data::call(&xlrec)?;
            // the target-offsets array (needed on the standby).
            let mut offs = Vec::with_capacity(ndeletable * 2);
            for &o in &deletable {
                offs.extend_from_slice(&o.to_ne_bytes());
            }
            xloginsert::xlog_register_data::call(&offs)?;
            xloginsert::xlog_register_buffer::call(1, metabuf, REGBUF_STANDARD)?;

            let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_VACUUM_ONE_PAGE)?;
            bufmgr::page_set_lsn::call(buf, recptr)?;
            bufmgr::page_set_lsn::call(metabuf, recptr)?;
        }
        // END_CRIT_SECTION

        // Release write lock on meta page.
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
    }
    Ok(())
}

// ===========================================================================
// helpers
// ===========================================================================

fn relation_needs_wal<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool> {
    Ok(relcache_seams::relation_needs_wal::call(rel))
}

/// `RelationIsAccessibleInLogicalDecoding(hrel)` — used for the
/// `xl_hash_vacuum_one_page.isCatalogRel` flag (relcache-owned predicate).
fn relation_is_accessible_in_logical_decoding<'mcx>(hrel: &Relation<'mcx>) -> PgResult<bool> {
    relcache_seams::relation_is_accessible_in_logical_decoding::call(hrel)
}

