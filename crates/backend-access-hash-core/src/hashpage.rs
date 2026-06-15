//! Port of `src/backend/access/hash/hashpage.c` (PostgreSQL 18.3): hash page
//! management — buffer acquisition wrappers, page init, `_hash_init`,
//! `_hash_expandtable` + `_hash_splitbucket` + `_hash_finish_split`,
//! `_hash_getcachedmetap`, `_hash_getbucketbuf_from_hashkey`.
//!
//! ## Seam-and-panic paths (unported callees in other crates)
//! * Buffer manager (`backend-storage-buffer-bufmgr-seams`): `ReadBuffer`,
//!   `ReadBufferExtended` (RBM_ZERO_AND_LOCK / strategy), `ExtendBufferedRel`,
//!   `LockBuffer`, `UnlockReleaseBuffer`, `ReleaseBuffer`, `MarkBufferDirty`,
//!   `ConditionalLockBufferForCleanup`, `IsBufferCleanupOK`, `with_buffer_page`,
//!   `BufferGetBlockNumber`, `PageSetLSN`, `log_newpage`, `smgr_extend_page`.
//! * WAL insert (`backend-access-transam-xloginsert-seams`).
//! * relcache (`backend-utils-cache-relcache-seams`): `index_getprocid`,
//!   `relation_needs_wal`, `relation_get_number_of_blocks_in_fork` (bufmgr),
//!   `rd_amcache` hash-meta cache.
//! * predicate (`backend-storage-lmgr-predicate-seams`): `PredicateLockPageSplit`.
//! * dynahash (`backend-utils-hash-dynahash-seams`): the TID hash table used by
//!   `_hash_finish_split`.
//! * interrupts (`backend-tcop-postgres-seams::check_for_interrupts`).

extern crate alloc;
use alloc::vec::Vec;

use types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, RegProcedure, BLCKSZ,
};
use types_error::{PgError, PgResult, ERROR};
use types_hash::hashpage::{
    Bucket, HashMetaPageData, H_BUCKET_BEING_SPLIT, H_NEEDS_SPLIT_CLEANUP, HASH_MAX_BITMAPS,
    HASH_METAPAGE, HASH_READ, HASH_WRITE, HASH_NOLOCK, INDEX_MOVED_BY_SPLIT_MASK,
    InvalidBucket, LH_BUCKET_BEING_POPULATED, LH_BUCKET_BEING_SPLIT,
    LH_BUCKET_NEEDS_SPLIT_CLEANUP, LH_BUCKET_PAGE, LH_META_PAGE, LH_OVERFLOW_PAGE,
    LH_UNUSED_PAGE,
};
use types_hash::hash::HASHSTANDARD_PROC;
use types_rel::Relation;
use types_storage::storage::{Buffer, BufferIsValid, InvalidBuffer};
use types_tuple::heaptuple::ItemPointerData;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_lmgr_predicate_seams as predicate;
use backend_tcop_postgres_seams as postgres;
use backend_utils_cache_relcache_seams as relcache;
use backend_utils_hash_dynahash_seams as dynahash;
use backend_access_hash_entry_seams as hash_entry;

use backend_access_transam_xloginsert_seams as xloginsert;

use backend_storage_page::{
    PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageGetFreeSpaceForMultipleTuples,
    PageRef, ItemIdIsDead,
};

use types_hash::hsearch::{HASHACTION, HASHCTL, HASH_BLOBS, HASH_CONTEXT, HASH_ELEM, HTAB};

use crate::hashutil::{
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, _hash_checkpage, _hash_get_newblock_from_oldbucket,
    _hash_get_totalbuckets, _hash_hashkey2bucket, _hash_get_indextuple_hashkey, _hash_spareindex,
};
use crate::hashovfl::_hash_addovflpage;
use crate::hashinsert::_hash_pgaddmultitup;
use crate::pagebytes::{
    _hash_init_metabuffer_bytes, _hash_initbitmapbuffer_bytes, _hash_initbuf_bytes,
    _hash_pageinit_bytes, hasho_bucket, hasho_flag, hasho_nextblkno, hasho_prevblkno,
    hash_get_target_page_usage, maxalign_sizeof_index_tuple_data, read_hash_meta,
    set_hasho_bucket, set_hasho_flag, set_hasho_nextblkno, set_hasho_page_id, set_hasho_prevblkno,
    index_tuple_size, index_tuple_t_info, index_tuple_tid, meta_highmask,
    meta_lowmask, meta_ovflpoint, meta_nmaps,
    set_meta_maxbucket, set_meta_highmask, set_meta_lowmask, set_meta_ovflpoint, set_meta_nmaps,
    page_meta_spare, set_page_meta_spare, set_page_meta_mapp, SIZEOF_ITEM_ID_DATA,
};
use crate::wal::RM_HASH_ID;

// XLOG opcodes (hash_xlog.h). The framework masks off the low nibble.
pub(crate) const XLOG_HASH_INIT_META_PAGE: u8 = 0x00;
pub(crate) const XLOG_HASH_INIT_BITMAP_PAGE: u8 = 0x10;
pub(crate) const XLOG_HASH_INSERT: u8 = 0x20;
pub(crate) const XLOG_HASH_ADD_OVFL_PAGE: u8 = 0x30;
pub(crate) const XLOG_HASH_SPLIT_ALLOCATE_PAGE: u8 = 0x40;
pub(crate) const XLOG_HASH_SPLIT_PAGE: u8 = 0x50;
pub(crate) const XLOG_HASH_SPLIT_COMPLETE: u8 = 0x60;
pub(crate) const XLOG_HASH_MOVE_PAGE_CONTENTS: u8 = 0x70;
pub(crate) const XLOG_HASH_SQUEEZE_PAGE: u8 = 0x80;
pub(crate) const XLOG_HASH_DELETE: u8 = 0x90;
pub(crate) const XLOG_HASH_SPLIT_CLEANUP: u8 = 0xA0;
pub(crate) const XLOG_HASH_UPDATE_META_PAGE: u8 = 0xB0;
pub(crate) const XLOG_HASH_VACUUM_ONE_PAGE: u8 = 0xC0;

/// `XLH_SPLIT_META_UPDATE_MASKS` (hash_xlog.h).
const XLH_SPLIT_META_UPDATE_MASKS: u8 = 1 << 0;
/// `XLH_SPLIT_META_UPDATE_SPLITPOINT` (hash_xlog.h).
const XLH_SPLIT_META_UPDATE_SPLITPOINT: u8 = 1 << 1;

// REGBUF flags (xloginsert.h).
pub(crate) const REGBUF_STANDARD: u8 = 0x04;
pub(crate) const REGBUF_WILL_INIT: u8 = 0x08;
pub(crate) const REGBUF_FORCE_IMAGE: u8 = 0x01;
pub(crate) const REGBUF_NO_IMAGE: u8 = 0x02;
pub(crate) const REGBUF_NO_CHANGE: u8 = 0x10;

/// `P_NEW` (bufmgr.h): `InvalidBlockNumber`. The hash AM forbids it.
const P_NEW: BlockNumber = InvalidBlockNumber;

// ===========================================================================
// metapage decode helpers (used to read HashMetaPageData out of a metabuffer).
// ===========================================================================

/// Run `f` over the `HashMetaPageData` decoded from a pinned metabuffer page.
pub fn with_metap<R>(metabuf: Buffer, f: impl FnOnce(&HashMetaPageData) -> R) -> PgResult<R> {
    let mut out: Option<R> = None;
    let mut f = Some(f);
    bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
        let metap = read_hash_meta(page);
        out = Some((f.take().unwrap())(&metap));
        Ok(())
    })?;
    Ok(out.expect("with_metap closure ran"))
}

/// `BUCKET_TO_BLKNO(metap, B)` (hash.h):
/// `(BlockNumber) ((B) + ((B) ? metap->hashm_spares[_hash_spareindex((B)+1)-1] : 0)) + 1`.
pub fn bucket_to_blkno(metap: &HashMetaPageData, b: Bucket) -> BlockNumber {
    let spare = if b != 0 {
        metap.hashm_spares[(_hash_spareindex(b + 1) - 1) as usize]
    } else {
        0
    };
    (b + spare) + 1
}

/// Read `(hashm_maxbucket, hashm_ntuples)` from the on-disk metapage of a
/// pinned/locked metabuffer (`HashPageGetMeta(BufferGetPage(metabuf))`).
pub fn metap_maxbucket_ntuples(metabuf: Buffer) -> PgResult<(u32, f64)> {
    let mut out = (0u32, 0.0f64);
    bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
        out = (
            crate::pagebytes::meta_maxbucket(page),
            crate::pagebytes::meta_ntuples(page),
        );
        Ok(())
    })?;
    Ok(out)
}

/// `metap->hashm_ntuples = value` on the on-disk metapage of a pinned/locked
/// (exclusive) metabuffer. The caller marks the buffer dirty / WAL-logs.
pub fn set_metap_ntuples(metabuf: Buffer, value: f64) -> PgResult<()> {
    bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
        crate::pagebytes::set_meta_ntuples(page, value);
        Ok(())
    })
}

// ===========================================================================
// _hash_getbuf (hashpage.c:69)
// ===========================================================================

/// `_hash_getbuf(rel, blkno, access, flags)` — get a buffer for read or write.
pub fn _hash_getbuf<'mcx>(
    rel: &Relation<'mcx>,
    blkno: BlockNumber,
    access: i32,
    flags: i32,
) -> PgResult<Buffer> {
    if blkno == P_NEW {
        return Err(PgError::new(ERROR, "hash AM does not use P_NEW"));
    }

    let buf = bufmgr::read_buffer::call(rel, blkno)?;

    if access != HASH_NOLOCK {
        bufmgr::lock_buffer::call(buf, access)?;
    }

    _hash_checkpage(rel, buf, flags)?;

    Ok(buf)
}

// ===========================================================================
// _hash_getbuf_with_condlock_cleanup (hashpage.c:95)
// ===========================================================================

/// `_hash_getbuf_with_condlock_cleanup(rel, blkno, flags)` — try to get a buffer
/// for cleanup; `InvalidBuffer` if the cleanup lock can't be acquired.
pub fn _hash_getbuf_with_condlock_cleanup<'mcx>(
    rel: &Relation<'mcx>,
    blkno: BlockNumber,
    flags: i32,
) -> PgResult<Buffer> {
    if blkno == P_NEW {
        return Err(PgError::new(ERROR, "hash AM does not use P_NEW"));
    }

    let buf = bufmgr::read_buffer::call(rel, blkno)?;

    if !bufmgr::conditional_lock_buffer_for_cleanup::call(buf)? {
        bufmgr::release_buffer::call(buf);
        return Ok(InvalidBuffer);
    }

    _hash_checkpage(rel, buf, flags)?;

    Ok(buf)
}

// ===========================================================================
// _hash_getinitbuf (hashpage.c:134)
// ===========================================================================

/// `_hash_getinitbuf(rel, blkno)` — get and zero-init a buffer by block number,
/// write-locked.
pub fn _hash_getinitbuf<'mcx>(rel: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    if blkno == P_NEW {
        return Err(PgError::new(ERROR, "hash AM does not use P_NEW"));
    }

    let buf = bufmgr::read_buffer_zero_and_lock::call(rel, ForkNumber::MAIN_FORKNUM, blkno)?;

    // initialize the page
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        _hash_pageinit_bytes(page, BLCKSZ)
    })?;

    Ok(buf)
}

// ===========================================================================
// _hash_initbuf (hashpage.c:156) — page-byte body in pagebytes.rs.
// ===========================================================================

/// `_hash_initbuf(buf, max_bucket, num_bucket, flag, initpage)`.
pub fn _hash_initbuf(
    buf: Buffer,
    max_bucket: u32,
    num_bucket: u32,
    flag: u32,
    initpage: bool,
) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        _hash_initbuf_bytes(page, max_bucket, num_bucket, flag, initpage)
    })
}

// ===========================================================================
// _hash_getnewbuf (hashpage.c:197)
// ===========================================================================

/// `_hash_getnewbuf(rel, blkno, forkNum)` — get a new page at the end of the
/// index (extending the relation when `blkno == nblocks`).
pub fn _hash_getnewbuf<'mcx>(
    rel: &Relation<'mcx>,
    blkno: BlockNumber,
    fork_num: ForkNumber,
) -> PgResult<Buffer> {
    let nblocks = bufmgr::relation_get_number_of_blocks_in_fork::call(rel.rd_id, fork_num)?;

    if blkno == P_NEW {
        return Err(PgError::new(ERROR, "hash AM does not use P_NEW"));
    }
    if blkno > nblocks {
        return Err(PgError::new(ERROR, "access to noncontiguous page in hash index"));
    }

    // smgr insists we explicitly extend the relation
    let buf = if blkno == nblocks {
        let b = bufmgr::extend_buffered_rel::call(rel, fork_num)?;
        if bufmgr::buffer_get_block_number::call(b) != blkno {
            return Err(PgError::new(ERROR, "unexpected hash relation size"));
        }
        b
    } else {
        bufmgr::read_buffer_zero_and_lock::call(rel, fork_num, blkno)?
    };

    // initialize the page
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        _hash_pageinit_bytes(page, BLCKSZ)
    })?;

    Ok(buf)
}

// ===========================================================================
// _hash_getbuf_with_strategy (hashpage.c:238)
// ===========================================================================

/// `_hash_getbuf_with_strategy(rel, blkno, access, flags, bstrategy)`.
pub fn _hash_getbuf_with_strategy<'mcx>(
    rel: &Relation<'mcx>,
    blkno: BlockNumber,
    access: i32,
    flags: i32,
    bstrategy: types_storage::buf::BufferAccessStrategy,
) -> PgResult<Buffer> {
    if blkno == P_NEW {
        return Err(PgError::new(ERROR, "hash AM does not use P_NEW"));
    }

    let buf = bufmgr::read_buffer_with_strategy::call(rel, blkno, bstrategy)?;

    if access != HASH_NOLOCK {
        bufmgr::lock_buffer::call(buf, access)?;
    }

    _hash_checkpage(rel, buf, flags)?;

    Ok(buf)
}

// ===========================================================================
// _hash_relbuf / _hash_dropbuf / _hash_dropscanbuf (hashpage.c:266..311)
// ===========================================================================

/// `_hash_relbuf(rel, buf)` — release a locked buffer (lock + pin dropped).
pub fn _hash_relbuf<'mcx>(_rel: &Relation<'mcx>, buf: Buffer) {
    bufmgr::unlock_release_buffer::call(buf);
}

/// `_hash_dropbuf(rel, buf)` — release an unlocked buffer (pin only).
pub fn _hash_dropbuf<'mcx>(_rel: &Relation<'mcx>, buf: Buffer) {
    bufmgr::release_buffer::call(buf);
}

/// `_hash_dropscanbuf(rel, so)` — release buffers used in a scan.
pub fn _hash_dropscanbuf<'mcx>(
    rel: &Relation<'mcx>,
    so: &mut types_hash::hashpage::HashScanOpaqueData,
) {
    // release pin we hold on primary bucket page
    if BufferIsValid(so.hashso_bucket_buf) && so.hashso_bucket_buf != so.currPos.buf {
        _hash_dropbuf(rel, so.hashso_bucket_buf);
    }
    so.hashso_bucket_buf = InvalidBuffer;

    // release pin we hold on primary bucket page of bucket being split
    if BufferIsValid(so.hashso_split_bucket_buf) && so.hashso_split_bucket_buf != so.currPos.buf {
        _hash_dropbuf(rel, so.hashso_split_bucket_buf);
    }
    so.hashso_split_bucket_buf = InvalidBuffer;

    // release any pin we still hold
    if BufferIsValid(so.currPos.buf) {
        _hash_dropbuf(rel, so.currPos.buf);
    }
    so.currPos.buf = InvalidBuffer;

    // reset split scan
    so.hashso_buc_populated = false;
    so.hashso_buc_split = false;
}

// ===========================================================================
// _hash_init (hashpage.c:326)
// ===========================================================================

/// `_hash_init(rel, num_tuples, forkNum)` — initialize the metadata page, the
/// initial buckets and the initial bitmap page. Returns the chosen bucket count.
pub fn _hash_init<'mcx>(
    rel: &Relation<'mcx>,
    num_tuples: f64,
    fork_num: ForkNumber,
) -> PgResult<u32> {
    // safety check
    if bufmgr::relation_get_number_of_blocks_in_fork::call(rel.rd_id, fork_num)? != 0 {
        return Err(PgError::new(ERROR, "cannot initialize non-empty hash index"));
    }

    // WAL log creation of pages if persistent, or this is the init fork.
    let use_wal =
        relcache::relation_needs_wal::call(rel) || fork_num == ForkNumber::INIT_FORKNUM;

    // Determine the target fill factor (tuples per bucket).
    let data_width = 4i32; // sizeof(uint32)
    let item_width = maxalign_sizeof_index_tuple_data() as i32
        + crate::pagebytes::maxalign_data_width(data_width)
        + SIZEOF_ITEM_ID_DATA as i32; // line pointer
    let mut ffactor = hash_get_target_page_usage(types_hash::hashpage::HASH_DEFAULT_FILLFACTOR) / item_width;
    if ffactor < 10 {
        ffactor = 10;
    }

    let procid: RegProcedure = relcache::index_getprocid::call(rel, 1, HASHSTANDARD_PROC)?;

    // metapage
    let metabuf = _hash_getnewbuf(rel, HASH_METAPAGE, fork_num)?;
    bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
        _hash_init_metabuffer_bytes(page, num_tuples, procid, ffactor as u16, false)
    })?;
    bufmgr::mark_buffer_dirty::call(metabuf);

    // read the relevant metapage fields
    let metap = with_metap(metabuf, |m| m.clone())?;

    // XLOG stuff
    if use_wal {
        xloginsert::xlog_begin_insert::call()?;
        let mut xlrec = [0u8; 16]; // SizeOfHashInitMetaPage
        xlrec[0..8].copy_from_slice(&num_tuples.to_ne_bytes());
        xlrec[8..12].copy_from_slice(&metap.hashm_procid.to_ne_bytes());
        xlrec[12..14].copy_from_slice(&metap.hashm_ffactor.to_ne_bytes());
        xloginsert::xlog_register_data::call(&xlrec)?;
        xloginsert::xlog_register_buffer::call(0, metabuf, REGBUF_WILL_INIT | REGBUF_STANDARD)?;
        let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_INIT_META_PAGE)?;
        bufmgr::page_set_lsn::call(metabuf, recptr)?;
    }

    let num_buckets = metap.hashm_maxbucket + 1;

    // Release buffer lock on metapage while we initialize buckets.
    bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;

    // Initialize and WAL log the first N buckets.
    for i in 0..num_buckets {
        postgres::check_for_interrupts::call()?;

        let blkno = bucket_to_blkno(&metap, i);
        let buf = _hash_getnewbuf(rel, blkno, fork_num)?;
        _hash_initbuf(buf, metap.hashm_maxbucket, i, LH_BUCKET_PAGE as u32, false)?;
        bufmgr::mark_buffer_dirty::call(buf);

        if use_wal {
            let mut page_copy = alloc::vec![0u8; BLCKSZ];
            bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
                page_copy.copy_from_slice(&page[..BLCKSZ]);
                Ok(())
            })?;
            bufmgr::log_newpage::call(rel.rd_locator, fork_num, blkno, &page_copy, true)?;
        }
        _hash_relbuf(rel, buf);
    }

    // Reacquire buffer lock on metapage.
    bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;

    // Initialize bitmap page.
    let bitmapbuf = _hash_getnewbuf(rel, num_buckets + 1, fork_num)?;
    bufmgr::with_buffer_page::call(bitmapbuf, &mut |page: &mut [u8]| {
        _hash_initbitmapbuffer_bytes(page, metap.hashm_bmsize, false)
    })?;
    bufmgr::mark_buffer_dirty::call(bitmapbuf);

    // add the new bitmap page to the metapage's list of bitmaps (metapage
    // already has a write lock).
    let nmaps_now = with_metap(metabuf, |m| m.hashm_nmaps)?;
    if nmaps_now as usize >= HASH_MAX_BITMAPS {
        return Err(PgError::new(ERROR, "out of overflow pages in hash index"));
    }
    bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
        let nmaps = meta_nmaps(page);
        set_page_meta_mapp(page, nmaps as usize, num_buckets + 1);
        set_meta_nmaps(page, nmaps + 1);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(metabuf);

    // XLOG stuff
    if use_wal {
        xloginsert::xlog_begin_insert::call()?;
        let mut xlrec = [0u8; 2]; // SizeOfHashInitBitmapPage
        xlrec[0..2].copy_from_slice(&metap.hashm_bmsize.to_ne_bytes());
        xloginsert::xlog_register_data::call(&xlrec)?;
        xloginsert::xlog_register_buffer::call(0, bitmapbuf, REGBUF_WILL_INIT)?;
        xloginsert::xlog_register_buffer::call(1, metabuf, REGBUF_STANDARD)?;
        let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_INIT_BITMAP_PAGE)?;
        bufmgr::page_set_lsn::call(bitmapbuf, recptr)?;
        bufmgr::page_set_lsn::call(metabuf, recptr)?;
    }

    // all done
    _hash_relbuf(rel, bitmapbuf);
    _hash_relbuf(rel, metabuf);

    Ok(num_buckets)
}

// ===========================================================================
// _hash_init_metabuffer (hashpage.c:498) — page-byte body in pagebytes.rs.
// ===========================================================================

/// `_hash_init_metabuffer(buf, num_tuples, procid, ffactor, initpage)`.
pub fn _hash_init_metabuffer(
    buf: Buffer,
    num_tuples: f64,
    procid: RegProcedure,
    ffactor: u16,
    initpage: bool,
) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        _hash_init_metabuffer_bytes(page, num_tuples, procid, ffactor, initpage)
    })
}

// ===========================================================================
// _hash_pageinit (hashpage.c:596)
// ===========================================================================

/// `_hash_pageinit(buf)` — initialize a new hash index page.
pub fn _hash_pageinit(buf: Buffer) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        _hash_pageinit_bytes(page, BLCKSZ)
    })
}

// ===========================================================================
// _hash_expandtable (hashpage.c:614)
// ===========================================================================

/// `_hash_expandtable(rel, metabuf)` — expand the hash table by creating one new
/// bucket. Silently does nothing if cleanup locks can't be acquired or no split
/// is needed.
pub fn _hash_expandtable<'mcx>(rel: &Relation<'mcx>, metabuf: Buffer) -> PgResult<()> {
    loop {
        // restart_expand:
        // Write-lock the meta page.
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;
        _hash_checkpage(rel, metabuf, LH_META_PAGE as i32)?;

        let metap = with_metap(metabuf, |m| m.clone())?;

        // Check to see if split is still needed.
        if metap.hashm_ntuples
            <= (metap.hashm_ffactor as f64) * (metap.hashm_maxbucket as f64 + 1.0)
        {
            // fail
            bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
            return Ok(());
        }

        // Can't split anymore if maxbucket reached its maximum.
        if metap.hashm_maxbucket >= 0x7FFFFFFE {
            bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
            return Ok(());
        }

        let new_bucket = metap.hashm_maxbucket + 1;
        let old_bucket = new_bucket & metap.hashm_lowmask;
        let start_oblkno = bucket_to_blkno(&metap, old_bucket);

        let buf_oblkno =
            _hash_getbuf_with_condlock_cleanup(rel, start_oblkno, LH_BUCKET_PAGE as i32)?;
        if !BufferIsValid(buf_oblkno) {
            // fail
            bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
            return Ok(());
        }

        let oflag = bufmgr_hasho_flag(buf_oblkno)?;

        // Finish a pending split from old bucket first.
        if H_BUCKET_BEING_SPLIT(oflag) {
            let maxbucket = metap.hashm_maxbucket;
            let highmask = metap.hashm_highmask;
            let lowmask = metap.hashm_lowmask;

            bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
            bufmgr::lock_buffer::call(buf_oblkno, BUFFER_LOCK_UNLOCK)?;

            _hash_finish_split(rel, metabuf, buf_oblkno, old_bucket, maxbucket, highmask, lowmask)?;

            _hash_dropbuf(rel, buf_oblkno);
            continue; // goto restart_expand
        }

        // Clean tuples remaining from a previous split.
        if H_NEEDS_SPLIT_CLEANUP(oflag) {
            let maxbucket = metap.hashm_maxbucket;
            let highmask = metap.hashm_highmask;
            let lowmask = metap.hashm_lowmask;

            bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;

            hash_entry::hashbucketcleanup_split_cleanup::call(
                rel, old_bucket, buf_oblkno, start_oblkno, maxbucket, highmask, lowmask,
            )?;

            _hash_dropbuf(rel, buf_oblkno);
            continue; // goto restart_expand
        }

        let start_nblkno = bucket_to_blkno(&metap, new_bucket);

        // If the split point is increasing, allocate a new batch of bucket pages.
        let spare_ndx = _hash_spareindex(new_bucket + 1);
        if spare_ndx > metap.hashm_ovflpoint {
            debug_assert!(spare_ndx == metap.hashm_ovflpoint + 1);

            let buckets_to_add = _hash_get_totalbuckets(spare_ndx) - new_bucket;
            if !_hash_alloc_buckets(rel, start_nblkno, buckets_to_add)? {
                // can't split due to BlockNumber overflow
                _hash_relbuf(rel, buf_oblkno);
                bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
                return Ok(());
            }
        }

        // Physically allocate the new bucket's primary page.
        let buf_nblkno = _hash_getnewbuf(rel, start_nblkno, ForkNumber::MAIN_FORKNUM)?;
        if !bufmgr::is_buffer_cleanup_ok::call(buf_nblkno)? {
            _hash_relbuf(rel, buf_oblkno);
            _hash_relbuf(rel, buf_nblkno);
            bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;
            return Ok(());
        }

        // START_CRIT_SECTION — update metapage bucket mapping.
        let mut metap_update_masks = false;
        let mut metap_update_splitpoint = false;
        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            set_meta_maxbucket(page, new_bucket);

            if new_bucket > meta_highmask(page) {
                // Starting a new doubling
                set_meta_lowmask(page, meta_highmask(page));
                let lowmask = meta_lowmask(page);
                set_meta_highmask(page, new_bucket | lowmask);
                metap_update_masks = true;
            }

            if spare_ndx > meta_ovflpoint(page) {
                let ovflpoint = meta_ovflpoint(page);
                let v = page_meta_spare(page, ovflpoint as usize);
                set_page_meta_spare(page, spare_ndx as usize, v);
                set_meta_ovflpoint(page, spare_ndx);
                metap_update_splitpoint = true;
            }
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(metabuf);

        // Re-read mapping info now (saves re-accessing meta in splitbucket).
        let metap2 = with_metap(metabuf, |m| m.clone())?;
        let maxbucket = metap2.hashm_maxbucket;
        let highmask = metap2.hashm_highmask;
        let lowmask = metap2.hashm_lowmask;

        // Mark the old bucket "being split", and update hasho_prevblkno.
        bufmgr::with_buffer_page::call(buf_oblkno, &mut |page: &mut [u8]| {
            let flag = hasho_flag(page);
            set_hasho_flag(page, flag | LH_BUCKET_BEING_SPLIT);
            set_hasho_prevblkno(page, maxbucket);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(buf_oblkno);

        // Initialize the new bucket's primary page (being populated).
        bufmgr::with_buffer_page::call(buf_nblkno, &mut |page: &mut [u8]| {
            set_hasho_prevblkno(page, maxbucket);
            set_hasho_nextblkno(page, InvalidBlockNumber);
            set_hasho_bucket(page, new_bucket);
            set_hasho_flag(page, LH_BUCKET_PAGE | LH_BUCKET_BEING_POPULATED);
            set_hasho_page_id(page, types_hash::hashpage::HASHO_PAGE_ID);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(buf_nblkno);

        // XLOG stuff
        if relcache::relation_needs_wal::call(rel) {
            let old_bucket_flag = bufmgr_hasho_flag(buf_oblkno)?;
            let new_bucket_flag = bufmgr_hasho_flag(buf_nblkno)?;
            let mut flags: u8 = 0;

            xloginsert::xlog_begin_insert::call()?;
            xloginsert::xlog_register_buffer::call(0, buf_oblkno, REGBUF_STANDARD)?;
            xloginsert::xlog_register_buffer::call(1, buf_nblkno, REGBUF_WILL_INIT)?;
            xloginsert::xlog_register_buffer::call(2, metabuf, REGBUF_STANDARD)?;

            if metap_update_masks {
                flags |= XLH_SPLIT_META_UPDATE_MASKS;
                xloginsert::xlog_register_buf_data::call(2, &metap2.hashm_lowmask.to_ne_bytes())?;
                xloginsert::xlog_register_buf_data::call(2, &metap2.hashm_highmask.to_ne_bytes())?;
            }
            if metap_update_splitpoint {
                flags |= XLH_SPLIT_META_UPDATE_SPLITPOINT;
                xloginsert::xlog_register_buf_data::call(2, &metap2.hashm_ovflpoint.to_ne_bytes())?;
                let spare = metap2.hashm_spares[metap2.hashm_ovflpoint as usize];
                xloginsert::xlog_register_buf_data::call(2, &spare.to_ne_bytes())?;
            }

            // xl_hash_split_allocate_page: new_bucket u32, old_flag u16,
            // new_flag u16, flags u8 (padded to 12).
            let mut xlrec = [0u8; 12]; // SizeOfHashSplitAllocPage
            xlrec[0..4].copy_from_slice(&maxbucket.to_ne_bytes());
            xlrec[4..6].copy_from_slice(&old_bucket_flag.to_ne_bytes());
            xlrec[6..8].copy_from_slice(&new_bucket_flag.to_ne_bytes());
            xlrec[8] = flags;
            xloginsert::xlog_register_data::call(&xlrec)?;

            let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_SPLIT_ALLOCATE_PAGE)?;
            bufmgr::page_set_lsn::call(buf_oblkno, recptr)?;
            bufmgr::page_set_lsn::call(buf_nblkno, recptr)?;
            bufmgr::page_set_lsn::call(metabuf, recptr)?;
        }
        // END_CRIT_SECTION

        // drop lock, but keep pin
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;

        // Relocate records to the new bucket.
        _hash_splitbucket(
            rel, metabuf, old_bucket, new_bucket, buf_oblkno, buf_nblkno, None, maxbucket, highmask,
            lowmask,
        )?;

        _hash_dropbuf(rel, buf_oblkno);
        _hash_dropbuf(rel, buf_nblkno);
        return Ok(());
    }
}

/// Read `hasho_flag` off a pinned buffer.
fn bufmgr_hasho_flag(buf: Buffer) -> PgResult<u16> {
    let mut flag = 0u16;
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        flag = hasho_flag(page);
        Ok(())
    })?;
    Ok(flag)
}

/// Read `hasho_nextblkno` off a pinned buffer.
fn bufmgr_hasho_nextblkno(buf: Buffer) -> PgResult<BlockNumber> {
    let mut v = InvalidBlockNumber;
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        v = hasho_nextblkno(page);
        Ok(())
    })?;
    Ok(v)
}

// ===========================================================================
// _hash_alloc_buckets (hashpage.c:991)
// ===========================================================================

/// `_hash_alloc_buckets(rel, firstblock, nblocks)` — allocate a new
/// splitpoint's worth of bucket pages by extending the logical EOF with a
/// single zero page at the end. Returns false on BlockNumber overflow.
fn _hash_alloc_buckets<'mcx>(
    rel: &Relation<'mcx>,
    firstblock: BlockNumber,
    nblocks: u32,
) -> PgResult<bool> {
    let lastblock = firstblock.wrapping_add(nblocks).wrapping_sub(1);

    // Check for overflow in block number calculation.
    if lastblock < firstblock || lastblock == InvalidBlockNumber {
        return Ok(false);
    }

    // Initialize the zero page in-memory and write it past EOF.
    let mut zerobuf = alloc::vec![0u8; BLCKSZ];
    _hash_pageinit_bytes(&mut zerobuf, BLCKSZ)?;
    set_hasho_prevblkno(&mut zerobuf, InvalidBlockNumber);
    set_hasho_nextblkno(&mut zerobuf, InvalidBlockNumber);
    set_hasho_bucket(&mut zerobuf, InvalidBucket);
    set_hasho_flag(&mut zerobuf, LH_UNUSED_PAGE);
    set_hasho_page_id(&mut zerobuf, types_hash::hashpage::HASHO_PAGE_ID);

    if relcache::relation_needs_wal::call(rel) {
        bufmgr::log_newpage::call(rel.rd_locator, ForkNumber::MAIN_FORKNUM, lastblock, &zerobuf, true)?;
    }

    // PageSetChecksumInplace + smgrextend.
    bufmgr::smgr_extend_page::call(
        rel.rd_locator,
        ForkNumber::MAIN_FORKNUM,
        lastblock,
        &mut zerobuf,
        false,
    )?;

    Ok(true)
}

// ===========================================================================
// _hash_splitbucket (hashpage.c:1072)
// ===========================================================================

/// `_hash_splitbucket(...)` — partition tuples between old and new buckets.
/// `htab == None` moves all tuples belonging to the new bucket; a non-None
/// `htab` skips TIDs already present (finishing an interrupted split).
#[allow(clippy::too_many_arguments)]
fn _hash_splitbucket<'mcx>(
    rel: &Relation<'mcx>,
    metabuf: Buffer,
    obucket: Bucket,
    nbucket: Bucket,
    obuf_in: Buffer,
    nbuf_in: Buffer,
    htab: Option<*mut HTAB>,
    maxbucket: u32,
    highmask: u32,
    lowmask: u32,
) -> PgResult<()> {
    let bucket_obuf = obuf_in;
    let bucket_nbuf = nbuf_in;
    let mut obuf = obuf_in;
    let mut nbuf = nbuf_in;

    // Copy predicate locks from old bucket to new bucket.
    let oblk = bufmgr::buffer_get_block_number::call(bucket_obuf);
    let nblk = bufmgr::buffer_get_block_number::call(bucket_nbuf);
    predicate::predicate_lock_page_split::call(rel.rd_id, oblk, nblk)?;

    // Accumulated tuples destined for the new page (owned copies).
    let mut itups: Vec<Vec<u8>> = Vec::new();
    let mut all_tups_size: usize = 0;

    // Outer loop iterates once per page in old bucket.
    loop {
        // Read each tuple in the old page, decide its bucket.
        let omaxoffnum = with_page_ref(obuf, |p| Ok(PageGetMaxOffsetNumber(p)))?;
        let mut ooffnum: u16 = 1; // FirstOffsetNumber
        while ooffnum <= omaxoffnum {
            // skip dead tuples
            let is_dead = with_page_ref(obuf, |p| {
                let iid = PageGetItemId(p, ooffnum)?;
                Ok(ItemIdIsDead(&iid))
            })?;
            if is_dead {
                ooffnum += 1;
                continue;
            }

            let itup_bytes = with_page_ref(obuf, |p| {
                let iid = PageGetItemId(p, ooffnum)?;
                Ok(PageGetItem(p, &iid)?.to_vec())
            })?;

            // Probe htab for the TID.
            let mut found = false;
            if let Some(htab) = htab {
                let tid = index_tuple_tid(&itup_bytes);
                let key_ptr = (&tid) as *const ItemPointerData as *const u8;
                let (_entry, f) =
                    dynahash::hash_search::call(htab, key_ptr, HASHACTION::HASH_FIND)?;
                found = f;
            }
            if found {
                ooffnum += 1;
                continue;
            }

            let bucket = _hash_hashkey2bucket(
                _hash_get_indextuple_hashkey(&itup_bytes),
                maxbucket,
                highmask,
                lowmask,
            );

            if bucket == nbucket {
                // make a copy and mark moved-by-split
                let mut new_itup = itup_bytes.clone();
                let t_info = index_tuple_t_info(&new_itup) | INDEX_MOVED_BY_SPLIT_MASK;
                new_itup[6..8].copy_from_slice(&t_info.to_ne_bytes());

                let itemsz = crate::pagebytes::maxalign(index_tuple_size(&new_itup));

                let free = with_page_ref(nbuf, |p| {
                    Ok(PageGetFreeSpaceForMultipleTuples(p, (itups.len() + 1) as i32))
                })?;
                if free < all_tups_size + itemsz {
                    // START_CRIT_SECTION
                    _hash_pgaddmultitup(rel, nbuf, &itups)?;
                    bufmgr::mark_buffer_dirty::call(nbuf);
                    log_split_page(rel, nbuf)?;
                    // END_CRIT_SECTION

                    bufmgr::lock_buffer::call(nbuf, BUFFER_LOCK_UNLOCK)?;

                    itups.clear();
                    all_tups_size = 0;

                    // chain to a new overflow page
                    nbuf = _hash_addovflpage(rel, metabuf, nbuf, nbuf == bucket_nbuf)?;
                }

                itups.push(new_itup);
                all_tups_size += itemsz;
            } else {
                debug_assert!(bucket == obucket);
            }
            ooffnum += 1;
        }

        let oblkno = with_page_ref(obuf, |p| {
            Ok(hasho_nextblkno(p.as_bytes()))
        })?;

        // retain the pin on the old primary bucket
        if obuf == bucket_obuf {
            bufmgr::lock_buffer::call(obuf, BUFFER_LOCK_UNLOCK)?;
        } else {
            _hash_relbuf(rel, obuf);
        }

        if !block_number_is_valid(oblkno) {
            // START_CRIT_SECTION
            _hash_pgaddmultitup(rel, nbuf, &itups)?;
            bufmgr::mark_buffer_dirty::call(nbuf);
            log_split_page(rel, nbuf)?;
            // END_CRIT_SECTION

            if nbuf == bucket_nbuf {
                bufmgr::lock_buffer::call(nbuf, BUFFER_LOCK_UNLOCK)?;
            } else {
                _hash_relbuf(rel, nbuf);
            }
            itups.clear();
            break;
        }

        // advance to next old page
        obuf = _hash_getbuf(rel, oblkno, HASH_READ, LH_OVERFLOW_PAGE as i32)?;
    }

    // Mark the old and new buckets to indicate split is finished.
    bufmgr::lock_buffer::call(bucket_obuf, BUFFER_LOCK_EXCLUSIVE)?;
    bufmgr::lock_buffer::call(bucket_nbuf, BUFFER_LOCK_EXCLUSIVE)?;

    // START_CRIT_SECTION
    bufmgr::with_buffer_page::call(bucket_obuf, &mut |page: &mut [u8]| {
        let flag = hasho_flag(page);
        let flag = (flag & !LH_BUCKET_BEING_SPLIT) | LH_BUCKET_NEEDS_SPLIT_CLEANUP;
        set_hasho_flag(page, flag);
        Ok(())
    })?;
    bufmgr::with_buffer_page::call(bucket_nbuf, &mut |page: &mut [u8]| {
        let flag = hasho_flag(page) & !LH_BUCKET_BEING_POPULATED;
        set_hasho_flag(page, flag);
        Ok(())
    })?;

    bufmgr::mark_buffer_dirty::call(bucket_obuf);
    bufmgr::mark_buffer_dirty::call(bucket_nbuf);

    if relcache::relation_needs_wal::call(rel) {
        let old_bucket_flag = bufmgr_hasho_flag(bucket_obuf)?;
        let new_bucket_flag = bufmgr_hasho_flag(bucket_nbuf)?;

        xloginsert::xlog_begin_insert::call()?;
        let mut xlrec = [0u8; 4]; // SizeOfHashSplitComplete
        xlrec[0..2].copy_from_slice(&old_bucket_flag.to_ne_bytes());
        xlrec[2..4].copy_from_slice(&new_bucket_flag.to_ne_bytes());
        xloginsert::xlog_register_data::call(&xlrec)?;
        xloginsert::xlog_register_buffer::call(0, bucket_obuf, REGBUF_STANDARD)?;
        xloginsert::xlog_register_buffer::call(1, bucket_nbuf, REGBUF_STANDARD)?;
        let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_SPLIT_COMPLETE)?;
        bufmgr::page_set_lsn::call(bucket_obuf, recptr)?;
        bufmgr::page_set_lsn::call(bucket_nbuf, recptr)?;
    }
    // END_CRIT_SECTION

    // Clean up the old bucket if possible.
    if bufmgr::is_buffer_cleanup_ok::call(bucket_obuf)? {
        bufmgr::lock_buffer::call(bucket_nbuf, BUFFER_LOCK_UNLOCK)?;
        let obkno = bufmgr::buffer_get_block_number::call(bucket_obuf);
        hash_entry::hashbucketcleanup_split_cleanup::call(
            rel, obucket, bucket_obuf, obkno, maxbucket, highmask, lowmask,
        )?;
    } else {
        bufmgr::lock_buffer::call(bucket_nbuf, BUFFER_LOCK_UNLOCK)?;
        bufmgr::lock_buffer::call(bucket_obuf, BUFFER_LOCK_UNLOCK)?;
    }

    Ok(())
}

// ===========================================================================
// _hash_finish_split (hashpage.c:1356)
// ===========================================================================

/// `_hash_finish_split(rel, metabuf, obuf, obucket, maxbucket, highmask,
/// lowmask)` — finish a previously interrupted split operation.
pub fn _hash_finish_split<'mcx>(
    rel: &Relation<'mcx>,
    metabuf: Buffer,
    obuf: Buffer,
    obucket: Bucket,
    maxbucket: u32,
    highmask: u32,
    lowmask: u32,
) -> PgResult<()> {
    // Initialize hash table used to track TIDs.
    let hash_ctl = HASHCTL {
        keysize: ::core::mem::size_of::<ItemPointerData>(),
        entrysize: ::core::mem::size_of::<ItemPointerData>(),
        ..Default::default()
    };
    let tidhtab = dynahash::hash_create::call(
        "bucket ctids",
        256,
        &hash_ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    )?;

    let bucket_nblkno = _hash_get_newblock_from_oldbucket(rel, obucket)?;
    let mut nblkno = bucket_nblkno;
    let mut bucket_nbuf = InvalidBuffer;

    // Scan the new bucket and build hash table of TIDs.
    loop {
        let nbuf = _hash_getbuf(
            rel,
            nblkno,
            HASH_READ,
            (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32,
        )?;

        if nblkno == bucket_nblkno {
            bucket_nbuf = nbuf;
        }

        let (nmaxoffnum, tids): (u16, Vec<ItemPointerData>) = with_page_ref(nbuf, |p| {
            let max = PageGetMaxOffsetNumber(p);
            let mut v = Vec::new();
            let mut off = 1u16;
            while off <= max {
                let iid = PageGetItemId(p, off)?;
                let itup = PageGetItem(p, &iid)?;
                v.push(index_tuple_tid(itup));
                off += 1;
            }
            Ok((max, v))
        })?;
        let _ = nmaxoffnum;

        for tid in tids {
            let key_ptr = (&tid) as *const ItemPointerData as *const u8;
            let (_entry, found) =
                dynahash::hash_search::call(tidhtab, key_ptr, HASHACTION::HASH_ENTER)?;
            debug_assert!(!found);
        }

        let next = bufmgr_hasho_nextblkno(nbuf)?;

        if nbuf == bucket_nbuf {
            bufmgr::lock_buffer::call(nbuf, BUFFER_LOCK_UNLOCK)?;
        } else {
            _hash_relbuf(rel, nbuf);
        }

        if !block_number_is_valid(next) {
            break;
        }
        nblkno = next;
    }

    // Conditionally get cleanup locks on old and new buckets.
    if !bufmgr::conditional_lock_buffer_for_cleanup::call(obuf)? {
        dynahash::hash_destroy::call(tidhtab)?;
        return Ok(());
    }
    if !bufmgr::conditional_lock_buffer_for_cleanup::call(bucket_nbuf)? {
        bufmgr::lock_buffer::call(obuf, BUFFER_LOCK_UNLOCK)?;
        dynahash::hash_destroy::call(tidhtab)?;
        return Ok(());
    }

    let nbucket = with_page_ref(bucket_nbuf, |p| Ok(hasho_bucket(p.as_bytes())))?;

    _hash_splitbucket(
        rel,
        metabuf,
        obucket,
        nbucket,
        obuf,
        bucket_nbuf,
        Some(tidhtab),
        maxbucket,
        highmask,
        lowmask,
    )?;

    _hash_dropbuf(rel, bucket_nbuf);
    dynahash::hash_destroy::call(tidhtab)?;
    Ok(())
}

// ===========================================================================
// log_split_page (hashpage.c:1473)
// ===========================================================================

/// `log_split_page(rel, buf)` — log the split operation (the whole new page).
fn log_split_page<'mcx>(rel: &Relation<'mcx>, buf: Buffer) -> PgResult<()> {
    if relcache::relation_needs_wal::call(rel) {
        xloginsert::xlog_begin_insert::call()?;
        xloginsert::xlog_register_buffer::call(0, buf, REGBUF_FORCE_IMAGE | REGBUF_STANDARD)?;
        let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_SPLIT_PAGE)?;
        bufmgr::page_set_lsn::call(buf, recptr)?;
    }
    Ok(())
}

// ===========================================================================
// _hash_getcachedmetap (hashpage.c:1501)
// ===========================================================================

/// `_hash_getcachedmetap(rel, metabuf, force_refresh)` — return cached metapage
/// data. `metabuf` is updated in place (the C `Buffer *metabuf`). Returns the
/// cached `HashMetaPageData`.
pub fn _hash_getcachedmetap<'mcx>(
    rel: &Relation<'mcx>,
    metabuf: &mut Buffer,
    force_refresh: bool,
) -> PgResult<HashMetaPageData> {
    let cached = relcache::rd_amcache_hashmeta::call(rel.rd_id)?;

    if force_refresh || cached.is_none() {
        // Read the metapage.
        if BufferIsValid(*metabuf) {
            bufmgr::lock_buffer::call(*metabuf, crate::hashutil::BUFFER_LOCK_SHARE)?;
        } else {
            *metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE as i32)?;
        }

        let metap = with_metap(*metabuf, |m| m.clone())?;

        // Populate the cache.
        relcache::set_rd_amcache_hashmeta::call(rel.rd_id, metap.clone())?;

        // Release metapage lock, but keep the pin.
        bufmgr::lock_buffer::call(*metabuf, BUFFER_LOCK_UNLOCK)?;

        return Ok(metap);
    }

    Ok(cached.unwrap())
}

// ===========================================================================
// _hash_getbucketbuf_from_hashkey (hashpage.c:1559)
// ===========================================================================

/// `_hash_getbucketbuf_from_hashkey(rel, hashkey, access, cachedmetap)` — get
/// the target bucket's buffer for `hashkey`. On return, `cachedmetap` (if
/// `Some`) is filled with the metapage contents used.
pub fn _hash_getbucketbuf_from_hashkey<'mcx>(
    rel: &Relation<'mcx>,
    hashkey: u32,
    access: i32,
    cachedmetap: Option<&mut HashMetaPageData>,
) -> PgResult<Buffer> {
    debug_assert!(access == HASH_READ || access == HASH_WRITE);

    let mut metabuf = InvalidBuffer;
    let mut metap = _hash_getcachedmetap(rel, &mut metabuf, false)?;

    let buf;
    loop {
        let bucket = _hash_hashkey2bucket(
            hashkey,
            metap.hashm_maxbucket,
            metap.hashm_highmask,
            metap.hashm_lowmask,
        );
        let blkno = bucket_to_blkno(&metap, bucket);

        let b = _hash_getbuf(rel, blkno, access, LH_BUCKET_PAGE as i32)?;
        let (opaque_bucket, opaque_prevblkno) = with_page_ref(b, |p| {
            Ok((hasho_bucket(p.as_bytes()), hasho_prevblkno(p.as_bytes())))
        })?;
        debug_assert!(opaque_bucket == bucket);
        debug_assert!(opaque_prevblkno != InvalidBlockNumber);

        // If this bucket hasn't been split, we're done.
        if opaque_prevblkno <= metap.hashm_maxbucket {
            buf = b;
            break;
        }

        // Drop lock, refresh cached metapage, retry.
        _hash_relbuf(rel, b);
        metap = _hash_getcachedmetap(rel, &mut metabuf, true)?;
    }

    if BufferIsValid(metabuf) {
        _hash_dropbuf(rel, metabuf);
    }

    if let Some(out) = cachedmetap {
        *out = metap;
    }

    Ok(buf)
}

// ===========================================================================
// small helpers
// ===========================================================================

/// `BlockNumberIsValid(blkno)`.
pub(crate) fn block_number_is_valid(blkno: BlockNumber) -> bool {
    blkno != InvalidBlockNumber
}

/// Run `f` over a read-only `PageRef` of a pinned buffer's page bytes.
pub(crate) fn with_page_ref<R>(buf: Buffer, f: impl FnOnce(&PageRef<'_>) -> PgResult<R>) -> PgResult<R> {
    let mut out: Option<R> = None;
    let mut f = Some(f);
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let pref = PageRef::new(page)?;
        out = Some((f.take().unwrap())(&pref)?);
        Ok(())
    })?;
    Ok(out.expect("with_page_ref closure ran"))
}

