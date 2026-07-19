//! hashinsert.c.

use ::bufmgr_seams as bm;
use ::types_core::{Buffer, InvalidBlockNumber, OffsetNumber, BLCKSZ};
use ::types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_hash::*;
use ::types_rel::Relation;
use ::types_storage::bufpage::SizeOfPageHeaderData;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD};

use crate::ovfl::_hash_addovflpage;
use crate::page::{
    page_get_free_space_for_multiple_tuples, page_mut, page_opaque, page_ref, with_meta,
    with_meta_mut, _hash_dropbuf, _hash_expandtable, _hash_finish_split, _hash_getbuf,
    _hash_getbucketbuf_from_hashkey, _hash_relbuf, with_cached_metap,
};
use crate::util::{_hash_binsearch, _hash_checkpage, _hash_get_indextuple_hashkey};
use crate::{relation_needs_wal, RM_HASH};

const fn maxalign(sz: usize) -> usize {
    (sz + 7) & !7
}

// HashMaxItemSize (pd_pagesize_version is always BLCKSZ here).
const HASH_MAX_ITEM_SIZE: usize =
    (BLCKSZ - SizeOfPageHeaderData - 4 - core::mem::size_of::<HashPageOpaqueData>()) & !7;

/// _hash_doinsert. `itup` is the formed index tuple image.
pub fn _hash_doinsert(
    rel: &Relation<'_>,
    itup: &[u8],
    heap_rel: &Relation<'_>,
    sorted: bool,
) -> PgResult<()> {
    // SAFETY: itup is a live, owned index-tuple image.
    let hashkey = unsafe { _hash_get_indextuple_hashkey(itup.as_ptr()) };
    let itemsz = maxalign(itup.len());

    'restart_insert: loop {
        let metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_NOLOCK, LH_META_PAGE)?;

        if itemsz > HASH_MAX_ITEM_SIZE {
            return Err(Box::new(
                PgError::error(format!(
                    "index row size {itemsz} exceeds hash maximum {HASH_MAX_ITEM_SIZE}"
                ))
                .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .with_hint("Values larger than a buffer page cannot be indexed."),
            ));
        }

        let (mut buf, _) = _hash_getbucketbuf_from_hashkey(rel, hashkey, HASH_WRITE)?;

        predicate_seams::check_for_serializable_conflict_in::call(
            rel,
            None,
            bm::buffer_get_block_number::call(buf),
        )?;

        let bucket_buf = buf;

        // SAFETY: write lock held on buf.
        let mut pageopaque = page_opaque(&unsafe { page_ref(buf) });
        let bucket = pageopaque.hasho_bucket;

        if H_BUCKET_BEING_SPLIT(pageopaque.hasho_flag) && bufmgr::IsBufferCleanupOK(buf) {
            bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;
            let (maxbucket, highmask, lowmask) = with_cached_metap(rel, |m| {
                (m.hashm_maxbucket, m.hashm_highmask, m.hashm_lowmask)
            });
            _hash_finish_split(rel, metabuf, buf, bucket, maxbucket, highmask, lowmask)?;
            _hash_dropbuf(buf)?;
            _hash_dropbuf(metabuf)?;
            continue 'restart_insert;
        }

        loop {
            // SAFETY: write lock held on buf.
            let free = unsafe { page_ref(buf) }.free_space();
            if free >= itemsz {
                break;
            }

            if H_HAS_DEAD_TUPLES(pageopaque.hasho_flag) && bufmgr::IsBufferCleanupOK(buf) {
                _hash_vacuum_one_page(rel, heap_rel, metabuf, buf)?;
                // SAFETY: write lock still held.
                if unsafe { page_ref(buf) }.free_space() >= itemsz {
                    break;
                }
            }

            let nextblkno = pageopaque.hasho_nextblkno;
            if nextblkno != InvalidBlockNumber {
                if buf != bucket_buf {
                    _hash_relbuf(buf)?;
                } else {
                    bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;
                }
                buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE)?;
            } else {
                bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;
                buf = _hash_addovflpage(rel, metabuf, buf, buf == bucket_buf)?;
                // SAFETY: addovflpage returns the page write-locked.
                debug_assert!(unsafe { page_ref(buf) }.free_space() >= itemsz);
            }
            // SAFETY: write lock held on the new buf.
            pageopaque = page_opaque(&unsafe { page_ref(buf) });
            debug_assert!(pageopaque.hasho_flag & LH_PAGE_TYPE == LH_OVERFLOW_PAGE);
            debug_assert!(pageopaque.hasho_bucket == bucket);
        }

        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;

        let itup_off = _hash_pgaddtup(rel, buf, itup, sorted)?;
        bm::mark_buffer_dirty::call(buf)?;

        let do_expand = with_meta_mut(metabuf, |metap| {
            metap.hashm_ntuples += 1.0;
            // Stays in sync with _hash_expandtable.
            metap.hashm_ntuples > metap.hashm_ffactor as f64 * (metap.hashm_maxbucket as f64 + 1.0)
        });
        bm::mark_buffer_dirty::call(metabuf)?;

        if relation_needs_wal(rel) {
            let xlrec = crate::wal::xl_hash_insert(itup_off);
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RM_HASH,
                XLOG_HASH_INSERT,
                0,
                &[&xlrec],
                &[
                    XLogRegBuf { block_id: 0, buffer: buf, flags: REGBUF_STANDARD, bufdata: &[itup] },
                    XLogRegBuf { block_id: 1, buffer: metabuf, flags: REGBUF_STANDARD, bufdata: &[] },
                ],
            )?;
            // SAFETY: pins + exclusive locks held.
            unsafe { page_mut(buf) }.set_lsn(recptr);
            unsafe { page_mut(metabuf) }.set_lsn(recptr);
        }

        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;

        _hash_relbuf(buf)?;
        if buf != bucket_buf {
            _hash_dropbuf(bucket_buf)?;
        }

        if do_expand {
            _hash_expandtable(rel, metabuf)?;
        }

        _hash_dropbuf(metabuf)?;
        return Ok(());
    }
}

/// _hash_pgaddtup.
pub(crate) fn _hash_pgaddtup(
    rel: &Relation<'_>,
    buf: Buffer,
    itup: &[u8],
    appendtup: bool,
) -> PgResult<OffsetNumber> {
    _hash_checkpage(rel, buf, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;
    // SAFETY: pin + write lock held by caller.
    let mut page = unsafe { page_mut(buf) };

    let itup_off = if appendtup {
        let off = page.as_ref().max_offset_number() + 1;
        #[cfg(debug_assertions)]
        if off > 1 {
            // SAFETY: last item of the locked page.
            unsafe {
                let r = page.as_ref();
                let last = r.item_raw(r.item_id(off - 1)).0;
                debug_assert!(
                    _hash_get_indextuple_hashkey(last) <= _hash_get_indextuple_hashkey(itup.as_ptr())
                );
            }
        }
        off
    } else {
        // SAFETY: itup is a live image.
        let hashkey = unsafe { _hash_get_indextuple_hashkey(itup.as_ptr()) };
        _hash_binsearch(&page.as_ref(), hashkey)
    };

    if page.add_item(itup, itup_off, 0).is_none() {
        panic!("failed to add index item to \"{}\"", rel.name());
    }
    Ok(itup_off)
}

/// _hash_pgaddmultitup: `itups` are (start, len) into `storage`.
pub(crate) fn _hash_pgaddmultitup(
    rel: &Relation<'_>,
    buf: Buffer,
    storage: &[u8],
    itups: &[(usize, usize)],
    itup_offsets: &mut [OffsetNumber],
) -> PgResult<()> {
    _hash_checkpage(rel, buf, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;
    // SAFETY: pin + write lock held by caller.
    let mut page = unsafe { page_mut(buf) };

    for (i, &(start, len)) in itups.iter().enumerate() {
        let item = &storage[start..start + len];
        // SAFETY: item is a live tuple image in storage.
        let hashkey = unsafe { _hash_get_indextuple_hashkey(item.as_ptr()) };
        let itup_off = _hash_binsearch(&page.as_ref(), hashkey);
        itup_offsets[i] = itup_off;
        if page.add_item(item, itup_off, 0).is_none() {
            panic!("failed to add index item to \"{}\"", rel.name());
        }
    }
    Ok(())
}

/// _hash_vacuum_one_page: remove LP_DEAD items so the insert can proceed.
/// The xid-horizon callee is a loud unported (genam) — reaching this arm with
/// dead items panics there rather than silently diverging.
fn _hash_vacuum_one_page(
    rel: &Relation<'_>,
    hrel: &Relation<'_>,
    metabuf: Buffer,
    buf: Buffer,
) -> PgResult<()> {
    let mut deletable = [0 as OffsetNumber; MaxIndexTuplesPerPage];
    let mut ndeletable = 0usize;

    {
        // SAFETY: cleanup lock held by caller.
        let page = unsafe { page_ref(buf) };
        let maxoff = page.max_offset_number();
        for offnum in 1..=maxoff {
            if page.item_id(offnum).is_dead() {
                deletable[ndeletable] = offnum;
                ndeletable += 1;
            }
        }
    }

    if ndeletable > 0 {
        // unported: index_compute_xid_horizon_for_tuples (needs heapam
        // heap_index_delete_tuples). Clean 0A000 in place of the old panic:
        // nothing has been mutated yet (the page edit, meta update and WAL
        // record all happen below), so failing the INSERT here is
        // unwind-safe. A wrong horizon (e.g. InvalidTransactionId) would
        // silently break hot-standby conflict detection for C replayers, so
        // the deletion body below stays gated until the callee lands.
        let _ = hrel;
        let snapshot_conflict_horizon: ::types_core::TransactionId = unported_xid_horizon()?;

        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;

        {
            // SAFETY: cleanup lock held.
            let mut page = unsafe { page_mut(buf) };
            page.index_multi_delete(&deletable[..ndeletable]);
            let mut opaque = page_opaque(&page.as_ref());
            opaque.hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
            crate::page::write_opaque(&mut page, &opaque);
        }
        with_meta_mut(metabuf, |m| m.hashm_ntuples -= ndeletable as f64);

        bm::mark_buffer_dirty::call(buf)?;
        bm::mark_buffer_dirty::call(metabuf)?;

        if relation_needs_wal(rel) {
            let is_catalog = false; // RelationIsAccessibleInLogicalDecoding: wal_level < logical
            let xlrec = crate::wal::xl_hash_vacuum_one_page(
                snapshot_conflict_horizon,
                ndeletable as u16,
                is_catalog,
            );
            let mut offbytes: Vec<u8> = Vec::with_capacity(ndeletable * 2);
            for &off in &deletable[..ndeletable] {
                offbytes.extend_from_slice(&off.to_ne_bytes());
            }
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RM_HASH,
                XLOG_HASH_VACUUM_ONE_PAGE,
                0,
                &[&xlrec, &offbytes],
                &[
                    XLogRegBuf { block_id: 0, buffer: buf, flags: REGBUF_STANDARD, bufdata: &[] },
                    XLogRegBuf { block_id: 1, buffer: metabuf, flags: REGBUF_STANDARD, bufdata: &[] },
                ],
            )?;
            // SAFETY: pins + exclusive locks held.
            unsafe { page_mut(buf) }.set_lsn(recptr);
            unsafe { page_mut(metabuf) }.set_lsn(recptr);
        }

        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;
    }
    Ok(())
}

// unported: shared 0A000 for the missing xid-horizon callee (see
// _hash_vacuum_one_page); mirrors genam::index_compute_xid_horizon_for_tuples.
#[cold]
#[inline(never)]
fn unported_xid_horizon() -> PgResult<::types_core::TransactionId> {
    Err(Box::new(
        PgError::error(
            "reuse of dead index entries is not supported (index_compute_xid_horizon_for_tuples unported)",
        )
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    ))
}
