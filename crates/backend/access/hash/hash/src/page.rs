//! hashpage.c: metapage/bucket page management, expand/split, rd_amcache.

use ::bufmgr_seams as bm;
use ::mcx::PgHashMap;
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, OffsetNumber, RegProcedure, BLCKSZ,
};
use ::types_error::{PgError, PgResult, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_hash::*;
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::ItemPointerData;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_FORCE_IMAGE, REGBUF_STANDARD, REGBUF_WILL_INIT};

use crate::insert::_hash_pgaddmultitup;
use crate::ovfl::{_hash_addovflpage, _hash_initbitmapbuffer};
use crate::util::{_hash_checkpage, _hash_get_indextuple_hashkey, _hash_hashkey2bucket};
use crate::{hashbucketcleanup, relation_needs_wal, RM_HASH};

pub(crate) const P_NEW: BlockNumber = InvalidBlockNumber;
const SIZEOF_OPAQUE: usize = core::mem::size_of::<HashPageOpaqueData>();

/// # Safety
/// `buf` is pinned; readers of the returned view hold at least a share lock.
pub(crate) unsafe fn page_ref<'p>(buf: Buffer) -> PageRef<'p> {
    unsafe { PageRef::from_raw(bm::buffer_get_page::call(buf)) }
}

/// # Safety
/// `buf` is pinned and exclusively locked for the borrow.
pub(crate) unsafe fn page_mut<'p>(buf: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bm::buffer_get_page::call(buf)) }
}

pub(crate) fn page_opaque(page: &PageRef<'_>) -> HashPageOpaqueData {
    let off = page.pd_special() as usize;
    debug_assert!(off == BLCKSZ - SIZEOF_OPAQUE);
    // SAFETY: in-bounds 4-aligned special area of a checked hash page.
    unsafe { page.as_ptr().add(off).cast::<HashPageOpaqueData>().read() }
}

pub(crate) fn write_opaque(page: &mut PageMut<'_>, opaque: &HashPageOpaqueData) {
    let off = page.as_ref().pd_special() as usize;
    debug_assert!(off == BLCKSZ - SIZEOF_OPAQUE);
    // SAFETY: in-bounds 4-aligned special area; exclusive page access.
    unsafe {
        page.as_ref().as_ptr().cast_mut().add(off).cast::<HashPageOpaqueData>().write(*opaque)
    }
}

/// HashPageGetMeta: metapage contents at MAXALIGN(SizeOfPageHeaderData).
/// # Safety
/// `buf` is a pinned hash metapage; caller synchronizes via the buffer lock.
pub(crate) unsafe fn meta_ptr(buf: Buffer) -> *mut HashMetaPageData {
    unsafe { bm::buffer_get_page::call(buf).as_ptr().add(SizeOfPageHeaderData).cast() }
}

/// Scoped read of the (locked or lock-free-stable) metapage.
pub(crate) fn with_meta<R>(metabuf: Buffer, f: impl FnOnce(&HashMetaPageData) -> R) -> R {
    // SAFETY: pin held by caller per the C call contract.
    unsafe { f(&*meta_ptr(metabuf)) }
}

/// Scoped mutation of the exclusively locked metapage.
pub(crate) fn with_meta_mut<R>(metabuf: Buffer, f: impl FnOnce(&mut HashMetaPageData) -> R) -> R {
    // SAFETY: pin + exclusive lock held by caller per the C call contract.
    unsafe { f(&mut *meta_ptr(metabuf)) }
}

/// _hash_getbuf.
pub(crate) fn _hash_getbuf(
    rel: &Relation<'_>,
    blkno: BlockNumber,
    access: i32,
    flags: u16,
) -> PgResult<Buffer> {
    if blkno == P_NEW {
        panic!("hash AM does not use P_NEW");
    }
    let buf = bm::read_buffer::call(rel, blkno)?;
    if access != HASH_NOLOCK {
        bm::lock_buffer::call(buf, access)?;
    }
    _hash_checkpage(rel, buf, flags)?;
    Ok(buf)
}

/// _hash_getbuf_with_condlock_cleanup.
pub(crate) fn _hash_getbuf_with_condlock_cleanup(
    rel: &Relation<'_>,
    blkno: BlockNumber,
    flags: u16,
) -> PgResult<Option<Buffer>> {
    if blkno == P_NEW {
        panic!("hash AM does not use P_NEW");
    }
    let buf = bm::read_buffer::call(rel, blkno)?;
    if !bm::conditional_lock_buffer_for_cleanup::call(buf)? {
        bm::release_buffer::call(buf)?;
        return Ok(None);
    }
    _hash_checkpage(rel, buf, flags)?;
    Ok(Some(buf))
}

/// _hash_getinitbuf: pre-EOF page to be filled from scratch.
pub(crate) fn _hash_getinitbuf(rel: &Relation<'_>, blkno: BlockNumber) -> PgResult<Buffer> {
    if blkno == P_NEW {
        panic!("hash AM does not use P_NEW");
    }
    let buf = bm::read_buffer_extended::call(
        rel,
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::ZeroAndLock,
        None,
    )?;
    // SAFETY: RBM_ZERO_AND_LOCK returns the buffer exclusively locked.
    _hash_pageinit(&mut unsafe { page_mut(buf) });
    Ok(buf)
}

/// _hash_initbuf.
pub(crate) fn _hash_initbuf(
    buf: Buffer,
    max_bucket: u32,
    num_bucket: u32,
    flag: u16,
    initpage: bool,
) {
    // SAFETY: caller holds pin + exclusive lock.
    let mut page = unsafe { page_mut(buf) };
    if initpage {
        _hash_pageinit(&mut page);
    }
    write_opaque(
        &mut page,
        &HashPageOpaqueData {
            // Primed with hashm_maxbucket for cached-metapage validation; see
            // _hash_getbucketbuf_from_hashkey.
            hasho_prevblkno: max_bucket,
            hasho_nextblkno: InvalidBlockNumber,
            hasho_bucket: num_bucket,
            hasho_flag: flag,
            hasho_page_id: HASHO_PAGE_ID,
        },
    );
}

/// _hash_getnewbuf: extend the index by one page (or reuse a crashed extend).
pub(crate) fn _hash_getnewbuf(
    rel: &Relation<'_>,
    blkno: BlockNumber,
    fork_num: ForkNumber,
) -> PgResult<Buffer> {
    let nblocks = bm::relation_get_number_of_blocks_in_fork::call(rel, fork_num)?;
    if blkno == P_NEW {
        panic!("hash AM does not use P_NEW");
    }
    if blkno > nblocks {
        panic!("access to noncontiguous page in hash index \"{}\"", rel.name());
    }
    let buf = if blkno == nblocks {
        let (buf, extended_by) = bm::extend_buffered_rel_by::call(
            rel,
            fork_num,
            None,
            bm::EB_LOCK_FIRST | bm::EB_SKIP_EXTENSION_LOCK,
            1,
        )?;
        debug_assert!(extended_by == 1);
        if bm::buffer_get_block_number::call(buf) != blkno {
            panic!(
                "unexpected hash relation size: {}, should be {blkno}",
                bm::buffer_get_block_number::call(buf)
            );
        }
        buf
    } else {
        bm::read_buffer_extended::call(rel, fork_num, blkno, ReadBufferMode::ZeroAndLock, None)?
    };
    // SAFETY: both arms return the buffer exclusively locked.
    _hash_pageinit(&mut unsafe { page_mut(buf) });
    Ok(buf)
}

/// _hash_getbuf_with_strategy.
pub(crate) fn _hash_getbuf_with_strategy(
    rel: &Relation<'_>,
    blkno: BlockNumber,
    access: i32,
    flags: u16,
    bstrategy: ::types_storage::buf::BufferAccessStrategy,
) -> PgResult<Buffer> {
    if blkno == P_NEW {
        panic!("hash AM does not use P_NEW");
    }
    let buf = bm::read_buffer_extended::call(
        rel,
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::Normal,
        bstrategy,
    )?;
    if access != HASH_NOLOCK {
        bm::lock_buffer::call(buf, access)?;
    }
    _hash_checkpage(rel, buf, flags)?;
    Ok(buf)
}

/// _hash_relbuf: drop lock and pin.
pub(crate) fn _hash_relbuf(buf: Buffer) -> PgResult<()> {
    bm::lock_buffer::call(buf, bm::BUFFER_LOCK_UNLOCK)?;
    bm::release_buffer::call(buf)
}

/// _hash_dropbuf: drop pin only.
pub(crate) fn _hash_dropbuf(buf: Buffer) -> PgResult<()> {
    bm::release_buffer::call(buf)
}

/// _hash_dropscanbuf.
pub(crate) fn _hash_dropscanbuf(so: &mut HashScanOpaqueData<'_>) -> PgResult<()> {
    if so.hashso_bucket_buf != ::types_core::InvalidBuffer
        && so.hashso_bucket_buf != so.currPos.buf
    {
        _hash_dropbuf(so.hashso_bucket_buf)?;
    }
    so.hashso_bucket_buf = ::types_core::InvalidBuffer;

    if so.hashso_split_bucket_buf != ::types_core::InvalidBuffer
        && so.hashso_split_bucket_buf != so.currPos.buf
    {
        _hash_dropbuf(so.hashso_split_bucket_buf)?;
    }
    so.hashso_split_bucket_buf = ::types_core::InvalidBuffer;

    if so.currPos.buf != ::types_core::InvalidBuffer {
        _hash_dropbuf(so.currPos.buf)?;
    }
    so.currPos.buf = ::types_core::InvalidBuffer;

    so.hashso_buc_populated = false;
    so.hashso_buc_split = false;
    Ok(())
}

/// _hash_init: metapage + initial buckets + first bitmap page.
pub fn _hash_init(
    rel: &Relation<'_>,
    num_tuples: f64,
    fork_num: ForkNumber,
) -> PgResult<u32> {
    if bm::relation_get_number_of_blocks_in_fork::call(rel, fork_num)? != 0 {
        panic!("cannot initialize non-empty hash index \"{}\"", rel.name());
    }

    let use_wal = relation_needs_wal(rel) || fork_num == ForkNumber::INIT_FORKNUM;

    let data_width = core::mem::size_of::<u32>();
    let item_width = maxalign(8) + maxalign(data_width) + 4;
    let mut ffactor = hash_get_target_page_usage(rel) / item_width as i32;
    if ffactor < 10 {
        ffactor = 10;
    }

    let procid: RegProcedure = crate::util::hash_procinfo(rel)?.fn_oid;

    let metabuf = _hash_getnewbuf(rel, HASH_METAPAGE, fork_num)?;
    _hash_init_metabuffer(metabuf, num_tuples, procid, ffactor as u16, false);
    bm::mark_buffer_dirty::call(metabuf)?;

    if use_wal {
        let (m_procid, m_ffactor) =
            with_meta(metabuf, |m| (m.hashm_procid, m.hashm_ffactor));
        let xlrec = crate::wal::xl_hash_init_meta_page(num_tuples, m_procid, m_ffactor);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_HASH,
            XLOG_HASH_INIT_META_PAGE,
            0,
            &[&xlrec],
            &[XLogRegBuf {
                block_id: 0,
                buffer: metabuf,
                flags: REGBUF_WILL_INIT | REGBUF_STANDARD,
                bufdata: &[],
            }],
        )?;
        // SAFETY: pin + exclusive lock held.
        unsafe { page_mut(metabuf) }.set_lsn(recptr);
    }

    let num_buckets = with_meta(metabuf, |m| m.hashm_maxbucket + 1);

    bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;

    for i in 0..num_buckets {
        crate::check_for_interrupts()?;
        let blkno = with_meta(metabuf, |m| m.bucket_to_blkno(i));
        let buf = _hash_getnewbuf(rel, blkno, fork_num)?;
        let maxbucket = with_meta(metabuf, |m| m.hashm_maxbucket);
        _hash_initbuf(buf, maxbucket, i, LH_BUCKET_PAGE, false);
        bm::mark_buffer_dirty::call(buf)?;

        if use_wal {
            // log_newpage(&rel->rd_locator, ...): the page is in `buf`, so the
            // buffer form is the same record.
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                0, /* RM_XLOG_ID */
                0xB0, /* XLOG_FPI */
                0,
                &[],
                &[XLogRegBuf {
                    block_id: 0,
                    buffer: buf,
                    flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
                    bufdata: &[],
                }],
            )?;
            // SAFETY: pin + exclusive lock held.
            unsafe { page_mut(buf) }.set_lsn(recptr);
        }
        _hash_relbuf(buf)?;
    }

    bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;

    let bitmapbuf = _hash_getnewbuf(rel, num_buckets + 1, fork_num)?;
    let bmsize = with_meta(metabuf, |m| m.hashm_bmsize);
    _hash_initbitmapbuffer(bitmapbuf, bmsize, false);
    bm::mark_buffer_dirty::call(bitmapbuf)?;

    with_meta_mut(metabuf, |m| -> PgResult<()> {
        if m.hashm_nmaps as usize >= HASH_MAX_BITMAPS {
            return Err(out_of_overflow_pages(rel));
        }
        m.hashm_mapp[m.hashm_nmaps as usize] = num_buckets + 1;
        m.hashm_nmaps += 1;
        Ok(())
    })?;
    bm::mark_buffer_dirty::call(metabuf)?;

    if use_wal {
        let xlrec = crate::wal::xl_hash_init_bitmap_page(bmsize);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_HASH,
            XLOG_HASH_INIT_BITMAP_PAGE,
            0,
            &[&xlrec],
            &[
                XLogRegBuf { block_id: 0, buffer: bitmapbuf, flags: REGBUF_WILL_INIT, bufdata: &[] },
                XLogRegBuf { block_id: 1, buffer: metabuf, flags: REGBUF_STANDARD, bufdata: &[] },
            ],
        )?;
        // SAFETY: pins + exclusive locks held on both.
        unsafe { page_mut(bitmapbuf) }.set_lsn(recptr);
        unsafe { page_mut(metabuf) }.set_lsn(recptr);
    }

    _hash_relbuf(bitmapbuf)?;
    _hash_relbuf(metabuf)?;

    Ok(num_buckets)
}

#[track_caller]
#[cold]
#[inline(never)]
fn out_of_overflow_pages(rel: &Relation<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!("out of overflow pages in hash index \"{}\"", rel.name()))
            .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

const fn maxalign(sz: usize) -> usize {
    (sz + 7) & !7
}

// HashGetFillFactor
fn hash_get_fill_factor(rel: &Relation<'_>) -> i32 {
    rel.get_fillfactor(HASH_DEFAULT_FILLFACTOR)
}

fn hash_get_target_page_usage(rel: &Relation<'_>) -> i32 {
    (BLCKSZ as i32) * hash_get_fill_factor(rel) / 100
}

/// _hash_init_metabuffer.
pub(crate) fn _hash_init_metabuffer(
    buf: Buffer,
    num_tuples: f64,
    procid: RegProcedure,
    ffactor: u16,
    initpage: bool,
) {
    let dnumbuckets = num_tuples / ffactor as f64;
    let num_buckets = if dnumbuckets <= 2.0 {
        2
    } else if dnumbuckets >= 0x40000000u32 as f64 {
        0x40000000
    } else {
        _hash_get_totalbuckets(_hash_spareindex(dnumbuckets as u32))
    };

    let spare_index = _hash_spareindex(num_buckets);
    debug_assert!((spare_index as usize) < HASH_MAX_SPLITPOINTS);

    // SAFETY: caller holds pin + exclusive lock (index build / redo).
    let mut page = unsafe { page_mut(buf) };
    if initpage {
        _hash_pageinit(&mut page);
    }
    write_opaque(
        &mut page,
        &HashPageOpaqueData {
            hasho_prevblkno: InvalidBlockNumber,
            hasho_nextblkno: InvalidBlockNumber,
            hasho_bucket: InvalidBucket,
            hasho_flag: LH_META_PAGE,
            hasho_page_id: HASHO_PAGE_ID,
        },
    );

    let bsize = hash_get_max_bitmap_size() as u16;
    let lshift = 31 - (bsize as u32).leading_zeros();
    debug_assert!(lshift > 0);

    with_meta_mut(buf, |metap| {
        metap.hashm_magic = HASH_MAGIC;
        metap.hashm_version = HASH_VERSION;
        metap.hashm_ntuples = 0.0;
        metap.hashm_nmaps = 0;
        metap.hashm_ffactor = ffactor;
        metap.hashm_bsize = bsize;
        metap.hashm_bmsize = 1 << lshift;
        metap.hashm_bmshift = (lshift + BYTE_TO_BIT) as u16;
        metap.hashm_procid = procid;
        metap.hashm_maxbucket = num_buckets - 1;
        metap.hashm_highmask = (num_buckets + 1).next_power_of_two() - 1;
        metap.hashm_lowmask = metap.hashm_highmask >> 1;
        metap.hashm_spares = [0; HASH_MAX_SPLITPOINTS];
        metap.hashm_mapp = [0; HASH_MAX_BITMAPS];
        metap.hashm_spares[spare_index as usize] = 1;
        metap.hashm_ovflpoint = spare_index;
        metap.hashm_firstfree = 0;
    });

    // pd_lower past the metadata keeps it out of the xlog page-hole.
    page.set_pd_lower(
        (SizeOfPageHeaderData + core::mem::size_of::<HashMetaPageData>()) as u16,
    );
}

// HashGetMaxBitmapSize.
pub(crate) fn hash_get_max_bitmap_size() -> usize {
    BLCKSZ - (maxalign(SizeOfPageHeaderData) + maxalign(SIZEOF_OPAQUE))
}

/// _hash_pageinit.
pub(crate) fn _hash_pageinit(page: &mut PageMut<'_>) {
    page.init(SIZEOF_OPAQUE);
}

/// _hash_expandtable: create (and populate) one new bucket.
pub(crate) fn _hash_expandtable(rel: &Relation<'_>, metabuf: Buffer) -> PgResult<()> {
    'restart_expand: loop {
        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_EXCLUSIVE)?;
        _hash_checkpage(rel, metabuf, LH_META_PAGE)?;

        let (ntuples, ffactor, maxbucket_now) =
            with_meta(metabuf, |m| (m.hashm_ntuples, m.hashm_ffactor, m.hashm_maxbucket));

        // Re-check under the lock; stays in sync with _hash_doinsert.
        if ntuples <= ffactor as f64 * (maxbucket_now as f64 + 1.0) {
            return fail_unlock(metabuf);
        }
        if maxbucket_now >= 0x7FFFFFFE {
            return fail_unlock(metabuf);
        }

        let new_bucket = maxbucket_now + 1;
        let old_bucket = new_bucket & with_meta(metabuf, |m| m.hashm_lowmask);
        let start_oblkno = with_meta(metabuf, |m| m.bucket_to_blkno(old_bucket));

        let Some(buf_oblkno) =
            _hash_getbuf_with_condlock_cleanup(rel, start_oblkno, LH_BUCKET_PAGE)?
        else {
            return fail_unlock(metabuf);
        };

        // SAFETY: cleanup lock held on buf_oblkno.
        let oopaque = page_opaque(&unsafe { page_ref(buf_oblkno) });

        if H_BUCKET_BEING_SPLIT(oopaque.hasho_flag) {
            let (maxbucket, highmask, lowmask) = with_meta(metabuf, |m| {
                (m.hashm_maxbucket, m.hashm_highmask, m.hashm_lowmask)
            });
            bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;
            bm::lock_buffer::call(buf_oblkno, bm::BUFFER_LOCK_UNLOCK)?;
            _hash_finish_split(rel, metabuf, buf_oblkno, old_bucket, maxbucket, highmask, lowmask)?;
            _hash_dropbuf(buf_oblkno)?;
            continue 'restart_expand;
        }

        if H_NEEDS_SPLIT_CLEANUP(oopaque.hasho_flag) {
            let (maxbucket, highmask, lowmask) = with_meta(metabuf, |m| {
                (m.hashm_maxbucket, m.hashm_highmask, m.hashm_lowmask)
            });
            bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;
            hashbucketcleanup(
                rel, old_bucket, buf_oblkno, start_oblkno, None, maxbucket, highmask, lowmask,
                None, None, true, None,
            )?;
            _hash_dropbuf(buf_oblkno)?;
            continue 'restart_expand;
        }

        let start_nblkno = with_meta(metabuf, |m| m.bucket_to_blkno(new_bucket));

        let spare_ndx = _hash_spareindex(new_bucket + 1);
        if spare_ndx > with_meta(metabuf, |m| m.hashm_ovflpoint) {
            debug_assert!(spare_ndx == with_meta(metabuf, |m| m.hashm_ovflpoint) + 1);
            let buckets_to_add = _hash_get_totalbuckets(spare_ndx) - new_bucket;
            if !_hash_alloc_buckets(rel, start_nblkno, buckets_to_add)? {
                _hash_relbuf(buf_oblkno)?;
                return fail_unlock(metabuf);
            }
        }

        let buf_nblkno = _hash_getnewbuf(rel, start_nblkno, ForkNumber::MAIN_FORKNUM)?;
        if !bufmgr::IsBufferCleanupOK(buf_nblkno) {
            _hash_relbuf(buf_oblkno)?;
            _hash_relbuf(buf_nblkno)?;
            return fail_unlock(metabuf);
        }

        let mut metap_update_masks = false;
        let mut metap_update_splitpoint = false;
        with_meta_mut(metabuf, |metap| {
            metap.hashm_maxbucket = new_bucket;
            if new_bucket > metap.hashm_highmask {
                metap.hashm_lowmask = metap.hashm_highmask;
                metap.hashm_highmask = new_bucket | metap.hashm_lowmask;
                metap_update_masks = true;
            }
            if spare_ndx > metap.hashm_ovflpoint {
                metap.hashm_spares[spare_ndx as usize] =
                    metap.hashm_spares[metap.hashm_ovflpoint as usize];
                metap.hashm_ovflpoint = spare_ndx;
                metap_update_splitpoint = true;
            }
        });
        bm::mark_buffer_dirty::call(metabuf)?;

        let (maxbucket, highmask, lowmask) =
            with_meta(metabuf, |m| (m.hashm_maxbucket, m.hashm_highmask, m.hashm_lowmask));

        // Mark old bucket split-in-progress; prevblkno tracks bucket count as
        // of this split.
        let old_flag;
        {
            // SAFETY: cleanup lock held.
            let mut opage = unsafe { page_mut(buf_oblkno) };
            let mut oop = page_opaque(&opage.as_ref());
            oop.hasho_flag |= LH_BUCKET_BEING_SPLIT;
            oop.hasho_prevblkno = maxbucket;
            old_flag = oop.hasho_flag;
            write_opaque(&mut opage, &oop);
        }
        bm::mark_buffer_dirty::call(buf_oblkno)?;

        let new_flag = LH_BUCKET_PAGE | LH_BUCKET_BEING_POPULATED;
        {
            // SAFETY: cleanup lock held on the fresh page.
            let mut npage = unsafe { page_mut(buf_nblkno) };
            write_opaque(
                &mut npage,
                &HashPageOpaqueData {
                    hasho_prevblkno: maxbucket,
                    hasho_nextblkno: InvalidBlockNumber,
                    hasho_bucket: new_bucket,
                    hasho_flag: new_flag,
                    hasho_page_id: HASHO_PAGE_ID,
                },
            );
        }
        bm::mark_buffer_dirty::call(buf_nblkno)?;

        if relation_needs_wal(rel) {
            let mut flags = 0u8;
            let mut meta_data: Vec<u8> = Vec::with_capacity(16);
            if metap_update_masks {
                flags |= XLH_SPLIT_META_UPDATE_MASKS;
                with_meta(metabuf, |m| {
                    meta_data.extend_from_slice(&m.hashm_lowmask.to_ne_bytes());
                    meta_data.extend_from_slice(&m.hashm_highmask.to_ne_bytes());
                });
            }
            if metap_update_splitpoint {
                flags |= XLH_SPLIT_META_UPDATE_SPLITPOINT;
                with_meta(metabuf, |m| {
                    meta_data.extend_from_slice(&m.hashm_ovflpoint.to_ne_bytes());
                    meta_data
                        .extend_from_slice(&m.hashm_spares[m.hashm_ovflpoint as usize].to_ne_bytes());
                });
            }
            let xlrec =
                crate::wal::xl_hash_split_allocate_page(maxbucket, old_flag, new_flag, flags);
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RM_HASH,
                XLOG_HASH_SPLIT_ALLOCATE_PAGE,
                0,
                &[&xlrec],
                &[
                    XLogRegBuf { block_id: 0, buffer: buf_oblkno, flags: REGBUF_STANDARD, bufdata: &[] },
                    XLogRegBuf { block_id: 1, buffer: buf_nblkno, flags: REGBUF_WILL_INIT, bufdata: &[] },
                    XLogRegBuf { block_id: 2, buffer: metabuf, flags: REGBUF_STANDARD, bufdata: &[&meta_data] },
                ],
            )?;
            // SAFETY: pins + exclusive locks held on all three.
            unsafe { page_mut(buf_oblkno) }.set_lsn(recptr);
            unsafe { page_mut(buf_nblkno) }.set_lsn(recptr);
            unsafe { page_mut(metabuf) }.set_lsn(recptr);
        }

        bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)?;

        _hash_splitbucket(
            rel, metabuf, old_bucket, new_bucket, buf_oblkno, buf_nblkno, None, maxbucket,
            highmask, lowmask,
        )?;

        _hash_dropbuf(buf_oblkno)?;
        _hash_dropbuf(buf_nblkno)?;
        return Ok(());
    }
}

fn fail_unlock(metabuf: Buffer) -> PgResult<()> {
    bm::lock_buffer::call(metabuf, bm::BUFFER_LOCK_UNLOCK)
}

/// _hash_alloc_buckets: extend the logical EOF to the end of the splitpoint.
fn _hash_alloc_buckets(rel: &Relation<'_>, firstblock: BlockNumber, nblocks: u32) -> PgResult<bool> {
    let lastblock = firstblock.wrapping_add(nblocks).wrapping_sub(1);
    if lastblock < firstblock || lastblock == InvalidBlockNumber {
        return Ok(false);
    }

    // hashpage.c: PGIOAlignedBlock zerobuf — I/O-aligned (4096) so the
    // smgrextend below can hand it to an O_DIRECT fd (debug_io_direct=data).
    #[repr(align(4096))]
    struct ZeroBuf([u8; BLCKSZ]);
    let mut zerobuf = ZeroBuf([0u8; BLCKSZ]);
    // SAFETY: owned, aligned BLCKSZ scratch.
    let mut page =
        unsafe { PageMut::from_raw(core::ptr::NonNull::new_unchecked(zerobuf.0.as_mut_ptr())) };
    _hash_pageinit(&mut page);
    write_opaque(
        &mut page,
        &HashPageOpaqueData {
            hasho_prevblkno: InvalidBlockNumber,
            hasho_nextblkno: InvalidBlockNumber,
            hasho_bucket: InvalidBucket,
            hasho_flag: LH_UNUSED_PAGE,
            hasho_page_id: HASHO_PAGE_ID,
        },
    );

    if relation_needs_wal(rel) {
        xloginsert::log_newpage(
            &rel.rd_locator.get(),
            ForkNumber::MAIN_FORKNUM,
            lastblock,
            &mut zerobuf.0,
            true,
        )?;
    }

    bufmgr::PageSetChecksumInplace(&mut zerobuf.0, lastblock);
    smgr::RelationGetSmgr(rel)?;
    smgr::smgrextend(
        bm::relation_smgr_locator::call(rel),
        ForkNumber::MAIN_FORKNUM,
        lastblock,
        &zerobuf.0,
        false,
    )?;

    Ok(true)
}

/// _hash_splitbucket. `tidhtab` carries _hash_finish_split's already-moved
/// TIDs.
fn _hash_splitbucket(
    rel: &Relation<'_>,
    metabuf: Buffer,
    obucket: Bucket,
    nbucket: Bucket,
    obuf: Buffer,
    nbuf: Buffer,
    tidhtab: Option<&PgHashMap<'_, ItemPointerData, ()>>,
    maxbucket: u32,
    highmask: u32,
    lowmask: u32,
) -> PgResult<()> {
    let bucket_obuf = obuf;
    let bucket_nbuf = nbuf;
    let mut obuf = obuf;
    let mut nbuf = nbuf;

    predicate_seams::predicate_lock_page_split::call(
        rel,
        bm::buffer_get_block_number::call(bucket_obuf),
        bm::buffer_get_block_number::call(bucket_nbuf),
    )?;

    // itups carry (start,len) into copy storage: C pallocs CopyIndexTuple per
    // moved tuple; one retained buffer replaces the per-tuple allocs.
    let mut tup_storage: Vec<u8> = Vec::with_capacity(BLCKSZ);
    let mut itups: Vec<(usize, usize)> = Vec::with_capacity(MaxIndexTuplesPerPage);
    let mut itup_offsets = [0 as OffsetNumber; MaxIndexTuplesPerPage];
    let mut all_tups_size = 0usize;

    loop {
        let (omaxoffnum, onextblkno) = {
            // SAFETY: obuf pinned + locked (cleanup on entry; read on chain).
            let opage = unsafe { page_ref(obuf) };
            (opage.max_offset_number(), page_opaque(&opage).hasho_nextblkno)
        };

        for ooffnum in 1..=omaxoffnum {
            // SAFETY: lock held on obuf; offsets bounded by omaxoffnum.
            let (is_dead, itup_ptr, itup_len) = unsafe {
                let opage = page_ref(obuf);
                let id = opage.item_id(ooffnum);
                let (p, l) = opage.item_raw(id);
                (id.is_dead(), p, l as usize)
            };
            if is_dead {
                continue;
            }

            if let Some(htab) = tidhtab {
                // SAFETY: t_tid header read of the live on-page tuple.
                let tid = unsafe { nbtree::itup::t_tid(itup_ptr) };
                if htab.get(&tid).is_some() {
                    continue;
                }
            }

            // SAFETY: live on-page tuple under the lock.
            let hashkey = unsafe { _hash_get_indextuple_hashkey(itup_ptr) };
            let bucket = _hash_hashkey2bucket(hashkey, maxbucket, highmask, lowmask);

            if bucket == nbucket {
                let itemsz = maxalign(itup_len);

                let nfree = {
                    // SAFETY: nbuf pinned + write-locked.
                    let npage = unsafe { page_ref(nbuf) };
                    page_get_free_space_for_multiple_tuples(&npage, itups.len() + 1)
                };
                if nfree < all_tups_size + itemsz {
                    flush_split_batch(rel, nbuf, &tup_storage, &itups, &mut itup_offsets)?;
                    bm::lock_buffer::call(nbuf, bm::BUFFER_LOCK_UNLOCK)?;
                    itups.clear();
                    tup_storage.clear();
                    all_tups_size = 0;
                    nbuf = _hash_addovflpage(rel, metabuf, nbuf, nbuf == bucket_nbuf)?;
                }

                // CopyIndexTuple + INDEX_MOVED_BY_SPLIT_MASK.
                let start = tup_storage.len();
                // SAFETY: itup_len bytes live under the obuf lock.
                tup_storage
                    .extend_from_slice(unsafe { core::slice::from_raw_parts(itup_ptr, itup_len) });
                let info_off = start + 6;
                let t_info = u16::from_ne_bytes([tup_storage[info_off], tup_storage[info_off + 1]])
                    | INDEX_MOVED_BY_SPLIT_MASK;
                tup_storage[info_off..info_off + 2].copy_from_slice(&t_info.to_ne_bytes());
                itups.push((start, itup_len));
                all_tups_size += itemsz;
            } else {
                debug_assert!(bucket == obucket);
            }
        }

        if obuf == bucket_obuf {
            bm::lock_buffer::call(obuf, bm::BUFFER_LOCK_UNLOCK)?;
        } else {
            _hash_relbuf(obuf)?;
        }

        if onextblkno == InvalidBlockNumber {
            flush_split_batch(rel, nbuf, &tup_storage, &itups, &mut itup_offsets)?;
            if nbuf == bucket_nbuf {
                bm::lock_buffer::call(nbuf, bm::BUFFER_LOCK_UNLOCK)?;
            } else {
                _hash_relbuf(nbuf)?;
            }
            break;
        }

        obuf = _hash_getbuf(rel, onextblkno, HASH_READ, LH_OVERFLOW_PAGE)?;
    }

    bm::lock_buffer::call(bucket_obuf, bm::BUFFER_LOCK_EXCLUSIVE)?;
    bm::lock_buffer::call(bucket_nbuf, bm::BUFFER_LOCK_EXCLUSIVE)?;

    let (old_flag, new_flag);
    {
        // SAFETY: exclusive locks reacquired above.
        let mut opage = unsafe { page_mut(bucket_obuf) };
        let mut oop = page_opaque(&opage.as_ref());
        oop.hasho_flag &= !LH_BUCKET_BEING_SPLIT;
        oop.hasho_flag |= LH_BUCKET_NEEDS_SPLIT_CLEANUP;
        old_flag = oop.hasho_flag;
        write_opaque(&mut opage, &oop);

        let mut npage = unsafe { page_mut(bucket_nbuf) };
        let mut nop = page_opaque(&npage.as_ref());
        nop.hasho_flag &= !LH_BUCKET_BEING_POPULATED;
        new_flag = nop.hasho_flag;
        write_opaque(&mut npage, &nop);
    }

    bm::mark_buffer_dirty::call(bucket_obuf)?;
    bm::mark_buffer_dirty::call(bucket_nbuf)?;

    if relation_needs_wal(rel) {
        let xlrec = crate::wal::xl_hash_split_complete(old_flag, new_flag);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_HASH,
            XLOG_HASH_SPLIT_COMPLETE,
            0,
            &[&xlrec],
            &[
                XLogRegBuf { block_id: 0, buffer: bucket_obuf, flags: REGBUF_STANDARD, bufdata: &[] },
                XLogRegBuf { block_id: 1, buffer: bucket_nbuf, flags: REGBUF_STANDARD, bufdata: &[] },
            ],
        )?;
        // SAFETY: pins + exclusive locks held.
        unsafe { page_mut(bucket_obuf) }.set_lsn(recptr);
        unsafe { page_mut(bucket_nbuf) }.set_lsn(recptr);
    }

    if bufmgr::IsBufferCleanupOK(bucket_obuf) {
        bm::lock_buffer::call(bucket_nbuf, bm::BUFFER_LOCK_UNLOCK)?;
        hashbucketcleanup(
            rel,
            obucket,
            bucket_obuf,
            bm::buffer_get_block_number::call(bucket_obuf),
            None,
            maxbucket,
            highmask,
            lowmask,
            None,
            None,
            true,
            None,
        )?;
    } else {
        bm::lock_buffer::call(bucket_nbuf, bm::BUFFER_LOCK_UNLOCK)?;
        bm::lock_buffer::call(bucket_obuf, bm::BUFFER_LOCK_UNLOCK)?;
    }
    Ok(())
}

// The accumulated-batch flush both split exits share: add tuples, dirty, log.
fn flush_split_batch(
    rel: &Relation<'_>,
    nbuf: Buffer,
    tup_storage: &[u8],
    itups: &[(usize, usize)],
    itup_offsets: &mut [OffsetNumber],
) -> PgResult<()> {
    _hash_pgaddmultitup(rel, nbuf, tup_storage, itups, itup_offsets)?;
    bm::mark_buffer_dirty::call(nbuf)?;
    log_split_page(rel, nbuf)?;
    Ok(())
}

/// PageGetFreeSpaceForMultipleTuples.
pub(crate) fn page_get_free_space_for_multiple_tuples(page: &PageRef<'_>, ntups: usize) -> usize {
    let space = page.pd_upper() as isize - page.pd_lower() as isize;
    let need = (ntups * core::mem::size_of::<::types_storage::bufpage::ItemIdData>()) as isize;
    if space < need {
        return 0;
    }
    (space - need) as usize
}

/// _hash_finish_split.
pub(crate) fn _hash_finish_split(
    rel: &Relation<'_>,
    metabuf: Buffer,
    obuf: Buffer,
    obucket: Bucket,
    maxbucket: u32,
    highmask: u32,
    lowmask: u32,
) -> PgResult<()> {
    let cx = ::mcx::MemoryContext::new("hash finish split");
    let mcx = cx.mcx();
    let mut tidhtab: PgHashMap<'_, ItemPointerData, ()> = PgHashMap::with_capacity_in(256, mcx);

    let bucket_nblkno = crate::util::_hash_get_newblock_from_oldbucket(rel, obucket)?;
    let mut nblkno = bucket_nblkno;
    let mut bucket_nbuf = ::types_core::InvalidBuffer;

    loop {
        let nbuf = _hash_getbuf(rel, nblkno, HASH_READ, LH_BUCKET_PAGE | LH_OVERFLOW_PAGE)?;
        if nblkno == bucket_nblkno {
            bucket_nbuf = nbuf;
        }

        let next;
        {
            // SAFETY: share lock held on nbuf.
            let npage = unsafe { page_ref(nbuf) };
            let nmaxoffnum = npage.max_offset_number();
            for noffnum in 1..=nmaxoffnum {
                // SAFETY: bounded offset on the locked page.
                let tid = unsafe {
                    let id = npage.item_id(noffnum);
                    nbtree::itup::t_tid(npage.item_raw(id).0)
                };
                tidhtab.insert(tid, ());
            }
            next = page_opaque(&npage).hasho_nextblkno;
        }

        if nbuf == bucket_nbuf {
            bm::lock_buffer::call(nbuf, bm::BUFFER_LOCK_UNLOCK)?;
        } else {
            _hash_relbuf(nbuf)?;
        }

        if next == InvalidBlockNumber {
            break;
        }
        nblkno = next;
    }

    if !bm::conditional_lock_buffer_for_cleanup::call(obuf)? {
        _hash_dropbuf(bucket_nbuf)?;
        return Ok(());
    }
    if !bm::conditional_lock_buffer_for_cleanup::call(bucket_nbuf)? {
        bm::lock_buffer::call(obuf, bm::BUFFER_LOCK_UNLOCK)?;
        _hash_dropbuf(bucket_nbuf)?;
        return Ok(());
    }

    // SAFETY: cleanup lock held on bucket_nbuf.
    let nbucket = page_opaque(&unsafe { page_ref(bucket_nbuf) }).hasho_bucket;

    _hash_splitbucket(
        rel,
        metabuf,
        obucket,
        nbucket,
        obuf,
        bucket_nbuf,
        Some(&tidhtab),
        maxbucket,
        highmask,
        lowmask,
    )?;

    _hash_dropbuf(bucket_nbuf)?;
    Ok(())
}

/// log_split_page: FPI of the filled new-bucket page.
fn log_split_page(rel: &Relation<'_>, buf: Buffer) -> PgResult<()> {
    if relation_needs_wal(rel) {
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RM_HASH,
            XLOG_HASH_SPLIT_PAGE,
            0,
            &[],
            &[XLogRegBuf {
                block_id: 0,
                buffer: buf,
                flags: REGBUF_FORCE_IMAGE | REGBUF_STANDARD,
                bufdata: &[],
            }],
        )?;
        // SAFETY: pin + exclusive lock held by caller.
        unsafe { page_mut(buf) }.set_lsn(recptr);
    }
    Ok(())
}

/// _hash_getcachedmetap: rd_amcache_hash fill/refresh; returns with `*metabuf`
/// pinned (unlocked) if it had to read the metapage.
pub(crate) fn _hash_getcachedmetap(
    rel: &Relation<'_>,
    metabuf: &mut Buffer,
    force_refresh: bool,
) -> PgResult<()> {
    if force_refresh || rel.rd_amcache_hash.borrow().is_none() {
        if *metabuf != ::types_core::InvalidBuffer {
            bm::lock_buffer::call(*metabuf, bm::BUFFER_LOCK_SHARE)?;
        } else {
            *metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE)?;
        }
        {
            let mut cache = rel.rd_amcache_hash.borrow_mut();
            let snap = with_meta(*metabuf, |m| m.clone());
            match cache.as_mut() {
                Some(b) => **b = snap,
                None => *cache = Some(Box::new(snap)),
            }
        }
        bm::lock_buffer::call(*metabuf, bm::BUFFER_LOCK_UNLOCK)?;
    }
    Ok(())
}

/// Read fields off the cached metapage (must be primed).
pub(crate) fn with_cached_metap<R>(rel: &Relation<'_>, f: impl FnOnce(&HashMetaPageData) -> R) -> R {
    let cache = rel.rd_amcache_hash.borrow();
    f(cache.as_ref().expect("hash rd_amcache primed"))
}

/// _hash_getbucketbuf_from_hashkey; on return the cached metapage the mapping
/// came from is current (callers read it via with_cached_metap).
pub(crate) fn _hash_getbucketbuf_from_hashkey(
    rel: &Relation<'_>,
    hashkey: u32,
    access: i32,
) -> PgResult<(Buffer, Bucket)> {
    debug_assert!(access == HASH_READ || access == HASH_WRITE);

    let mut metabuf = ::types_core::InvalidBuffer;
    _hash_getcachedmetap(rel, &mut metabuf, false)?;

    let (buf, bucket) = loop {
        let (bucket, blkno) = with_cached_metap(rel, |m| {
            let bucket =
                _hash_hashkey2bucket(hashkey, m.hashm_maxbucket, m.hashm_highmask, m.hashm_lowmask);
            (bucket, m.bucket_to_blkno(bucket))
        });

        let buf = _hash_getbuf(rel, blkno, access, LH_BUCKET_PAGE)?;
        // SAFETY: lock acquired by _hash_getbuf.
        let opaque = page_opaque(&unsafe { page_ref(buf) });
        debug_assert!(opaque.hasho_bucket == bucket);
        debug_assert!(opaque.hasho_prevblkno != InvalidBlockNumber);

        if opaque.hasho_prevblkno <= with_cached_metap(rel, |m| m.hashm_maxbucket) {
            break (buf, bucket);
        }

        _hash_relbuf(buf)?;
        _hash_getcachedmetap(rel, &mut metabuf, true)?;
    };

    if metabuf != ::types_core::InvalidBuffer {
        _hash_dropbuf(metabuf)?;
    }

    Ok((buf, bucket))
}
