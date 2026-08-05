//! brin_pageops.c + brin_revmap.c: BRIN page and range-map operations.
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::bufmgr_seams::{
    buffer_get_block_number, buffer_get_page, extend_buffered_rel_by, lock_buffer,
    mark_buffer_dirty, mark_buffer_dirty_hint, read_buffer, release_buffer, BUFFER_LOCK_EXCLUSIVE,
    BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK, EB_LOCK_FIRST, EB_SKIP_EXTENSION_LOCK,
};
use ::mcx::PgVec;
use ::types_brin::*;
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, InvalidBuffer, OffsetNumber, RmgrIds,
    BLCKSZ,
};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED, ERRCODE_PROGRAM_LIMIT_EXCEEDED};
use ::types_rel::{ExclusiveLock, Relation, RelationData};
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::{
    ItemPointerData, ItemPointerEquals, ItemPointerGetBlockNumber, ItemPointerIsValid,
};
use ::types_tuple::InvalidOffsetNumber;
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD, REGBUF_WILL_INIT};

pub const BrinMaxItemSize: usize =
    (BLCKSZ - (((SizeOfPageHeaderData + 4) + 7) & !7) - BrinSpecialSpaceSize) & !7;

#[inline]
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

// SAFETY: buf pinned; callers follow C's lock discipline for reads/writes.
unsafe fn page_ref<'a>(buf: Buffer) -> PageRef<'a> {
    PageRef::from_raw(buffer_get_page::call(buf))
}

// SAFETY: buf pinned + exclusively locked by caller.
unsafe fn page_mut<'a>(buf: Buffer) -> PageMut<'a> {
    PageMut::from_raw(buffer_get_page::call(buf))
}

pub fn relation_needs_wal(rel: &RelationData<'_>) -> bool {
    rel.is_permanent()
        && (transam_xlog_seams::xlog_standby_info_active::call()
            || (rel.rd_createSubid.get() == types_core::InvalidSubTransactionId
                && rel.rd_firstRelfilelocatorSubid.get() == types_core::InvalidSubTransactionId))
}

#[track_caller]
#[cold]
#[inline(never)]
fn row_too_big(itemsz: usize, maxsz: usize, rel: &RelationData<'_>) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "index row size {itemsz} exceeds maximum {maxsz} for index \"{}\"",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn corrupted_revmap() -> Box<PgError> {
    Box::new(
        PgError::error("corrupted BRIN index: inconsistent range map".to_string())
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED),
    )
}

pub fn brin_page_init(page: &mut PageMut<'_>, ty: u16) {
    page.init(BrinSpecialSpaceSize);
    BrinSetPageType(page, ty);
}

pub fn brin_metapage_init(page: &mut PageMut<'_>, pagesPerRange: BlockNumber, version: u16) {
    brin_page_init(page, BRIN_PAGETYPE_META);
    brin_meta_write(
        page,
        &BrinMetaPageData {
            brinMagic: BRIN_META_MAGIC,
            brinVersion: version as u32,
            pagesPerRange,
            lastRevmapPage: 0,
        },
    );
    // pd_lower past the metadata: xlog page-hole compression must keep it.
    page.set_pd_lower((SizeOfPageHeaderData + SizeOfBrinMetaPageData) as u16);
}

fn br_page_get_freespace(page: &PageRef<'_>) -> usize {
    if !BRIN_IS_REGULAR_PAGE(page) || (BrinPageFlags(page) & BRIN_EVACUATE_PAGE) != 0 {
        0
    } else {
        page.free_space()
    }
}

pub fn brin_can_do_samepage_update(buf: Buffer, origsz: usize, newsz: usize) -> bool {
    // SAFETY: pinned by caller.
    newsz <= origsz || unsafe { page_ref(buf) }.exact_free_space() >= newsz - origsz
}

fn discard_newbuf(
    idxrel: &Relation<'_>,
    newbuf: Buffer,
    newblk: BlockNumber,
    extended: bool,
) -> PgResult<()> {
    if newbuf == InvalidBuffer {
        return Ok(());
    }
    if extended {
        brin_initialize_empty_new_buffer(idxrel, newbuf)?;
    }
    lock_buffer::call(newbuf, BUFFER_LOCK_UNLOCK)?;
    release_buffer::call(newbuf)?;
    if extended {
        freespace::FreeSpaceMapVacuumRange(idxrel, newblk, newblk + 1)?;
    }
    Ok(())
}

/// brin_doupdate. oldbuf enters and leaves unlocked (pin retained by caller);
/// false = caller retries.
pub fn brin_doupdate(
    idxrel: &Relation<'_>,
    pagesPerRange: BlockNumber,
    revmap: &BrinRevmap,
    heapBlk: BlockNumber,
    oldbuf: Buffer,
    oldoff: OffsetNumber,
    origtup: &[u8],
    newtup: &[u8],
    samepage: bool,
) -> PgResult<bool> {
    let newsz = newtup.len();
    debug_assert!(newsz == maxalign(newsz));

    if newsz > BrinMaxItemSize {
        return Err(row_too_big(newsz, BrinMaxItemSize, idxrel));
    }

    brinRevmapExtend(idxrel, revmap, heapBlk)?;

    let mut newbuf: Buffer;
    let mut newblk: BlockNumber = InvalidBlockNumber;
    let extended: bool;
    if !samepage {
        let (b, ext) = brin_getinsertbuffer(idxrel, oldbuf, newsz)?;
        extended = ext;
        if b == InvalidBuffer {
            debug_assert!(!extended);
            return Ok(false);
        }
        if b == oldbuf {
            debug_assert!(!extended);
            newbuf = InvalidBuffer;
        } else {
            newbuf = b;
            newblk = buffer_get_block_number::call(newbuf);
        }
    } else {
        lock_buffer::call(oldbuf, BUFFER_LOCK_EXCLUSIVE)?;
        newbuf = InvalidBuffer;
        extended = false;
    }

    // SAFETY: oldbuf pinned + exclusively locked from here on.
    let oldpage = unsafe { page_ref(oldbuf) };

    if !BRIN_IS_REGULAR_PAGE(&oldpage)
        || oldoff > oldpage.max_offset_number()
        || !oldpage.item_id(oldoff).is_normal()
    {
        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        discard_newbuf(idxrel, newbuf, newblk, extended)?;
        return Ok(false);
    }

    let oldlp = oldpage.item_id(oldoff);
    let (oldp, oldsz) = oldpage.item_raw(oldlp);
    // SAFETY: item bytes within the locked page.
    let oldtup = unsafe { core::slice::from_raw_parts(oldp, oldsz as usize) };

    if oldtup != origtup {
        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        discard_newbuf(idxrel, newbuf, newblk, extended)?;
        return Ok(false);
    }

    let origsz = origtup.len();
    if (BrinPageFlags(&oldpage) & BRIN_EVACUATE_PAGE) == 0
        && brin_can_do_samepage_update(oldbuf, origsz, newsz)
    {
        // SAFETY: exclusive lock held.
        let mut oldpage = unsafe { page_mut(oldbuf) };
        if !oldpage.index_tuple_overwrite(oldoff, newtup) {
            panic!("failed to replace BRIN tuple");
        }
        mark_buffer_dirty::call(oldbuf)?;

        if relation_needs_wal(idxrel) {
            let xlrec = xl_brin_samepage_update(oldoff);
            let bufdata: [&[u8]; 1] = [newtup];
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RmgrIds::RM_BRIN_ID as u8,
                XLOG_BRIN_SAMEPAGE_UPDATE,
                0,
                &[&xlrec],
                &[XLogRegBuf {
                    block_id: 0,
                    buffer: oldbuf,
                    flags: REGBUF_STANDARD,
                    bufdata: &bufdata,
                }],
            )?;
            oldpage.set_lsn(recptr);
        }

        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        discard_newbuf(idxrel, newbuf, newblk, extended)?;
        Ok(true)
    } else if newbuf == InvalidBuffer {
        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        Ok(false)
    } else {
        let revmapbuf = brinLockRevmapPageForUpdate(idxrel, revmap, heapBlk)?;

        // SAFETY: newbuf pinned + exclusively locked by brin_getinsertbuffer.
        let mut newpage = unsafe { page_mut(newbuf) };
        if extended {
            brin_page_init(&mut newpage, BRIN_PAGETYPE_REGULAR);
        }
        // SAFETY: exclusive lock held on oldbuf.
        let mut oldpage = unsafe { page_mut(oldbuf) };
        oldpage.index_tuple_delete_no_compact(oldoff);
        let Some(newoff) = newpage.add_item(newtup, InvalidOffsetNumber, 0) else {
            panic!("failed to add BRIN tuple to new page");
        };
        mark_buffer_dirty::call(oldbuf)?;
        mark_buffer_dirty::call(newbuf)?;

        let freesp = if extended {
            br_page_get_freespace(&newpage.as_ref())
        } else {
            0
        };

        let newtid = ItemPointerData::new(newblk, newoff);
        brinSetHeapBlockItemptr(revmapbuf, pagesPerRange, heapBlk, newtid);
        mark_buffer_dirty::call(revmapbuf)?;

        if relation_needs_wal(idxrel) {
            let info = XLOG_BRIN_UPDATE | if extended { XLOG_BRIN_INIT_PAGE } else { 0 };
            let xlrec = xl_brin_update(oldoff, heapBlk, pagesPerRange, newoff);
            let bufdata: [&[u8]; 1] = [newtup];
            let newflags = REGBUF_STANDARD | if extended { REGBUF_WILL_INIT } else { 0 };
            let recptr = ::xloginsert_seams::xlog_insert_record::call(
                RmgrIds::RM_BRIN_ID as u8,
                info,
                0,
                &[&xlrec],
                &[
                    XLogRegBuf { block_id: 0, buffer: newbuf, flags: newflags, bufdata: &bufdata },
                    XLogRegBuf { block_id: 1, buffer: revmapbuf, flags: 0, bufdata: &[] },
                    XLogRegBuf {
                        block_id: 2,
                        buffer: oldbuf,
                        flags: REGBUF_STANDARD,
                        bufdata: &[],
                    },
                ],
            )?;
            oldpage.set_lsn(recptr);
            newpage.set_lsn(recptr);
            // SAFETY: revmapbuf exclusively locked.
            unsafe { page_mut(revmapbuf) }.set_lsn(recptr);
        }

        lock_buffer::call(revmapbuf, BUFFER_LOCK_UNLOCK)?;
        lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        lock_buffer::call(newbuf, BUFFER_LOCK_UNLOCK)?;
        release_buffer::call(newbuf)?;

        if extended {
            freespace::RecordPageWithFreeSpace(idxrel, newblk, freesp)?;
            freespace::FreeSpaceMapVacuumRange(idxrel, newblk, newblk + 1)?;
        }
        Ok(true)
    }
}

/// brin_doinsert. `*buffer` (if valid) is pinned and unlocked on entry and on
/// exit; the caller keeps it across calls for locality.
pub fn brin_doinsert(
    idxrel: &Relation<'_>,
    pagesPerRange: BlockNumber,
    revmap: &BrinRevmap,
    buffer: &mut Buffer,
    heapBlk: BlockNumber,
    tup: &[u8],
) -> PgResult<OffsetNumber> {
    let itemsz = tup.len();
    debug_assert!(itemsz == maxalign(itemsz));

    if itemsz > BrinMaxItemSize {
        return Err(row_too_big(itemsz, BrinMaxItemSize, idxrel));
    }

    brinRevmapExtend(idxrel, revmap, heapBlk)?;

    if *buffer != InvalidBuffer {
        lock_buffer::call(*buffer, BUFFER_LOCK_EXCLUSIVE)?;
        // SAFETY: pinned + locked.
        if br_page_get_freespace(unsafe { &page_ref(*buffer) }) < itemsz {
            lock_buffer::call(*buffer, BUFFER_LOCK_UNLOCK)?;
            release_buffer::call(*buffer)?;
            *buffer = InvalidBuffer;
        }
    }

    let mut extended = false;
    if *buffer == InvalidBuffer {
        loop {
            let (b, ext) = brin_getinsertbuffer(idxrel, InvalidBuffer, itemsz)?;
            if b != InvalidBuffer {
                *buffer = b;
                extended = ext;
                break;
            }
        }
    }

    let revmapbuf = brinLockRevmapPageForUpdate(idxrel, revmap, heapBlk)?;

    let blk = buffer_get_block_number::call(*buffer);

    // SAFETY: pinned + exclusively locked.
    let mut page = unsafe { page_mut(*buffer) };
    if extended {
        brin_page_init(&mut page, BRIN_PAGETYPE_REGULAR);
    }
    let Some(off) = page.add_item(tup, InvalidOffsetNumber, 0) else {
        panic!("failed to add BRIN tuple to new page");
    };
    mark_buffer_dirty::call(*buffer)?;

    let freesp = if extended {
        br_page_get_freespace(&page.as_ref())
    } else {
        0
    };

    let tid = ItemPointerData::new(blk, off);
    brinSetHeapBlockItemptr(revmapbuf, pagesPerRange, heapBlk, tid);
    mark_buffer_dirty::call(revmapbuf)?;

    if relation_needs_wal(idxrel) {
        let info = XLOG_BRIN_INSERT | if extended { XLOG_BRIN_INIT_PAGE } else { 0 };
        let xlrec = xl_brin_insert(heapBlk, pagesPerRange, off);
        let bufdata: [&[u8]; 1] = [tup];
        let flags = REGBUF_STANDARD | if extended { REGBUF_WILL_INIT } else { 0 };
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RmgrIds::RM_BRIN_ID as u8,
            info,
            0,
            &[&xlrec],
            &[
                XLogRegBuf { block_id: 0, buffer: *buffer, flags, bufdata: &bufdata },
                XLogRegBuf { block_id: 1, buffer: revmapbuf, flags: 0, bufdata: &[] },
            ],
        )?;
        page.set_lsn(recptr);
        // SAFETY: revmapbuf exclusively locked.
        unsafe { page_mut(revmapbuf) }.set_lsn(recptr);
    }

    lock_buffer::call(*buffer, BUFFER_LOCK_UNLOCK)?;
    lock_buffer::call(revmapbuf, BUFFER_LOCK_UNLOCK)?;

    if extended {
        freespace::RecordPageWithFreeSpace(idxrel, blk, freesp)?;
        freespace::FreeSpaceMapVacuumRange(idxrel, blk, blk + 1)?;
    }

    Ok(off)
}

pub fn brin_start_evacuating_page(_idxRel: &Relation<'_>, buf: Buffer) -> PgResult<bool> {
    // SAFETY: pinned + exclusively locked by caller.
    let page = unsafe { page_ref(buf) };
    if page.is_new() {
        return Ok(false);
    }
    let maxoff = page.max_offset_number();
    for off in 1..=maxoff {
        if page.item_id(off).is_used() {
            // BRIN_EVACUATE_PAGE is not WAL-logged, except accidentally.
            let mut pm = unsafe { page_mut(buf) };
            let flags = BrinPageFlags(&page);
            BrinSetPageFlags(&mut pm, flags | BRIN_EVACUATE_PAGE);
            mark_buffer_dirty_hint::call(buf, true)?;
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn brin_evacuate_page(
    idxRel: &Relation<'_>,
    pagesPerRange: BlockNumber,
    revmap: &BrinRevmap,
    buf: Buffer,
) -> PgResult<()> {
    let scratch = ::mcx::MemoryContext::new_bump("brin evacuate");
    let mut btup: PgVec<'_, u8> = PgVec::new_in(scratch.mcx());

    // SAFETY: pinned; the lock dance below follows C.
    let page = unsafe { page_ref(buf) };
    debug_assert!(BrinPageFlags(&page) & BRIN_EVACUATE_PAGE != 0);

    let maxoff = page.max_offset_number();
    let mut off = 1;
    while off <= maxoff {
        let lp = page.item_id(off);
        if lp.is_used() {
            let (p, sz) = page.item_raw(lp);
            // SAFETY: item bytes within the locked page, copied before the
            let src = unsafe { core::slice::from_raw_parts(p, sz as usize) };
            btup.clear();
            ::mcx::vec_append_bytes(&mut btup, src)?;

            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;

            let blkno = brin_tuple_blkno(&btup);
            if !brin_doupdate(
                idxRel,
                pagesPerRange,
                revmap,
                blkno,
                buf,
                off,
                &btup,
                &btup,
                false,
            )? {
                off -= 1; // retry
            }

            lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;

            if !BRIN_IS_REGULAR_PAGE(&page) {
                break;
            }
        }
        off += 1;
    }

    lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
    release_buffer::call(buf)?;
    Ok(())
}

// brin_initialize_empty_new_buffer: init as an empty regular page, WAL-log,
// record in FSM. Caller holds pin + exclusive lock.
fn brin_initialize_empty_new_buffer(idxrel: &Relation<'_>, buffer: Buffer) -> PgResult<()> {
    // SAFETY: pinned + exclusively locked by caller.
    let mut page = unsafe { page_mut(buffer) };
    brin_page_init(&mut page, BRIN_PAGETYPE_REGULAR);
    mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(idxrel) {
        ::xloginsert_seams::log_newpage_buffer::call(buffer, true)?;
    }

    // FSM update not WAL-logged; VACUUM repairs after a crash.
    freespace::RecordPageWithFreeSpace(
        idxrel,
        buffer_get_block_number::call(buffer),
        br_page_get_freespace(&page.as_ref()),
    )
}

// RelationGetTargetBlock/RelationSetTargetBlock: the cache is C's
// rd_smgr->smgr_targblock, so RelationTruncate's smgr reset invalidates it.
fn relation_get_target_block(rel: &RelationData<'_>) -> BlockNumber {
    let locator = rel.rd_locator.get();
    if locator.relNumber == 0 {
        return InvalidBlockNumber;
    }
    smgr::smgrgettargblock(::types_storage::RelFileLocatorBackend {
        locator,
        backend: rel.rd_backend,
    })
}

fn relation_set_target_block(rel: &RelationData<'_>, blk: BlockNumber) {
    let locator = rel.rd_locator.get();
    if locator.relNumber == 0 {
        return;
    }
    let key = ::types_storage::RelFileLocatorBackend { locator, backend: rel.rd_backend };
    if smgr::smgropen(key.locator, key.backend).is_ok() {
        smgr::smgrsettargblock(key, blk);
    }
}

// brin_getinsertbuffer: (InvalidBuffer, false) = caller restarts (old page
// became a revmap page). On success the returned buffer AND oldbuf (if
// valid) are exclusively locked.
fn brin_getinsertbuffer(
    irel: &Relation<'_>,
    oldbuf: Buffer,
    itemsz: usize,
) -> PgResult<(Buffer, bool)> {
    debug_assert!(itemsz <= BrinMaxItemSize);

    let oldblk = if oldbuf != InvalidBuffer {
        buffer_get_block_number::call(oldbuf)
    } else {
        InvalidBlockNumber
    };

    let mut newblk = relation_get_target_block(irel);
    if newblk == InvalidBlockNumber {
        newblk = freespace::GetPageWithFreeSpace(irel, itemsz)?;
    }

    loop {
        let mut extended = false;
        let mut extension_lock_held = false;
        let buf: Buffer;

        if newblk == InvalidBlockNumber {
            if !relation_is_local(irel) {
                lmgr::LockRelationForExtension(irel, ExclusiveLock)?;
                extension_lock_held = true;
            }
            let (b, _n) = extend_buffered_rel_by::call(
                irel,
                ForkNumber::MAIN_FORKNUM,
                None,
                EB_SKIP_EXTENSION_LOCK,
                1,
            )?;
            buf = b;
            newblk = buffer_get_block_number::call(buf);
            extended = true;
        } else if newblk == oldblk {
            buf = oldbuf;
        } else {
            buf = read_buffer::call(irel, newblk)?;
        }

        if oldbuf != InvalidBuffer && oldblk < newblk {
            lock_buffer::call(oldbuf, BUFFER_LOCK_EXCLUSIVE)?;
            // SAFETY: pinned + locked.
            if !BRIN_IS_REGULAR_PAGE(unsafe { &page_ref(oldbuf) }) {
                lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;

                if extended {
                    lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
                    brin_initialize_empty_new_buffer(irel, buf)?;
                    lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                }
                if extension_lock_held {
                    lmgr::UnlockRelationForExtension(irel, ExclusiveLock)?;
                }
                release_buffer::call(buf)?;
                if extended {
                    freespace::FreeSpaceMapVacuumRange(irel, newblk, newblk + 1)?;
                }
                return Ok((InvalidBuffer, false));
            }
        }

        lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;

        if extension_lock_held {
            lmgr::UnlockRelationForExtension(irel, ExclusiveLock)?;
        }

        // SAFETY: pinned + exclusively locked.
        let page = unsafe { page_ref(buf) };
        let freesp = if extended {
            BrinMaxItemSize
        } else {
            br_page_get_freespace(&page)
        };
        if freesp >= itemsz {
            relation_set_target_block(irel, newblk);

            if oldbuf != InvalidBuffer && oldblk > newblk {
                lock_buffer::call(oldbuf, BUFFER_LOCK_EXCLUSIVE)?;
                // SAFETY: pinned + locked.
                debug_assert!(BRIN_IS_REGULAR_PAGE(unsafe { &page_ref(oldbuf) }));
            }
            return Ok((buf, extended));
        }

        if extended {
            brin_initialize_empty_new_buffer(irel, buf)?;
            let err = row_too_big(itemsz, freesp, irel);
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            release_buffer::call(buf)?;
            return Err(err);
        }

        if newblk != oldblk {
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            release_buffer::call(buf)?;
        }
        if oldbuf != InvalidBuffer && oldblk <= newblk {
            lock_buffer::call(oldbuf, BUFFER_LOCK_UNLOCK)?;
        }

        newblk = freespace::RecordAndGetPageWithFreeSpace(irel, newblk, freesp, itemsz)?;
    }
}

fn relation_is_local(rel: &RelationData<'_>) -> bool {
    rel.rd_islocaltemp || rel.rd_createSubid.get() != types_core::InvalidSubTransactionId
}

/// brin_page_cleanup. Caller holds only a pin; the momentary extension lock
/// orders us after any concurrent extender's initialization.
pub fn brin_page_cleanup(idxrel: &Relation<'_>, buf: Buffer) -> PgResult<()> {
    // SAFETY: pinned; unlocked PageIsNew probe, as C.
    if unsafe { page_ref(buf) }.is_new() {
        lmgr::LockRelationForExtension(idxrel, ::types_rel::ShareLock)?;
        lmgr::UnlockRelationForExtension(idxrel, ::types_rel::ShareLock)?;

        lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
        // SAFETY: pinned + exclusively locked.
        if unsafe { page_ref(buf) }.is_new() {
            brin_initialize_empty_new_buffer(idxrel, buf)?;
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            return Ok(());
        }
        lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
    }

    // SAFETY: pinned; special-space flags are stable for an initialized page.
    let page = unsafe { page_ref(buf) };
    if BRIN_IS_META_PAGE(&page) || BRIN_IS_REVMAP_PAGE(&page) {
        return Ok(());
    }

    freespace::RecordPageWithFreeSpace(
        idxrel,
        buffer_get_block_number::call(buf),
        br_page_get_freespace(&page),
    )
}


pub fn brinRevmapInitialize(idxrel: &Relation<'_>) -> PgResult<(BrinRevmap, BlockNumber)> {
    let meta = read_buffer::call(idxrel, BRIN_METAPAGE_BLKNO)?;
    lock_buffer::call(meta, BUFFER_LOCK_SHARE)?;
    // SAFETY: pinned + share locked.
    let metadata = brin_meta_read(unsafe { &page_ref(meta) });

    let revmap = BrinRevmap {
        rm_pagesPerRange: metadata.pagesPerRange,
        rm_lastRevmapPage: core::cell::Cell::new(metadata.lastRevmapPage),
        rm_metaBuf: meta,
        rm_currBuf: core::cell::Cell::new(InvalidBuffer),
    };
    lock_buffer::call(meta, BUFFER_LOCK_UNLOCK)?;
    Ok((revmap, metadata.pagesPerRange))
}

pub fn brinRevmapTerminate(revmap: BrinRevmap) -> PgResult<()> {
    release_buffer::call(revmap.rm_metaBuf)?;
    if revmap.rm_currBuf.get() != InvalidBuffer {
        release_buffer::call(revmap.rm_currBuf.get())?;
    }
    Ok(())
}

pub fn brinRevmapExtend(
    idxrel: &Relation<'_>,
    revmap: &BrinRevmap,
    heapBlk: BlockNumber,
) -> PgResult<()> {
    let mapBlk = revmap_extend_and_get_blkno(idxrel, revmap, heapBlk)?;
    debug_assert!(
        mapBlk != InvalidBlockNumber
            && mapBlk != BRIN_METAPAGE_BLKNO
            && mapBlk <= revmap.rm_lastRevmapPage.get()
    );
    let _ = mapBlk;
    Ok(())
}

/// brinLockRevmapPageForUpdate: the locked revmap buffer; its pin stays with
/// the revmap struct (caller only unlocks).
pub fn brinLockRevmapPageForUpdate(
    idxrel: &Relation<'_>,
    revmap: &BrinRevmap,
    heapBlk: BlockNumber,
) -> PgResult<Buffer> {
    let rmBuf = revmap_get_buffer(idxrel, revmap, heapBlk)?;
    lock_buffer::call(rmBuf, BUFFER_LOCK_EXCLUSIVE)?;
    Ok(rmBuf)
}

/// brinSetHeapBlockItemptr; caller holds the lock and updates the page LSN.
/// Used in normal operation AND redo.
pub fn brinSetHeapBlockItemptr(
    buf: Buffer,
    pagesPerRange: BlockNumber,
    heapBlk: BlockNumber,
    tid: ItemPointerData,
) {
    // SAFETY: correct revmap page pinned + locked by caller.
    let mut page = unsafe { page_mut(buf) };
    let index = HEAPBLK_TO_REVMAP_INDEX(pagesPerRange, heapBlk);
    if ItemPointerIsValid(&tid) {
        revmap_set_tid(&mut page, index, tid);
    } else {
        revmap_set_tid(&mut page, index, ItemPointerData::invalid());
    }
}

/// brinGetTupleForHeapBlock. On Some, the tuple bytes were copied into `out`
/// (C returns a pointer into the buffer that every caller immediately
/// copies; the copy-out keeps the lock scope honest) and `*buf` stays pinned;
/// with `keep_lock` it also stays locked in `mode`. Returns the item's offset.
/// On None, `*buf` may remain pinned (caller releases) but is unlocked.
pub fn brinGetTupleForHeapBlock(
    idxrel: &Relation<'_>,
    revmap: &BrinRevmap,
    heapBlk: BlockNumber,
    buf: &mut Buffer,
    out: &mut PgVec<'_, u8>,
    mode: i32,
    keep_lock: bool,
) -> PgResult<Option<OffsetNumber>> {
    let heapBlk = (heapBlk / revmap.rm_pagesPerRange) * revmap.rm_pagesPerRange;

    let mapBlk = revmap_get_blkno(revmap, heapBlk);
    if mapBlk == InvalidBlockNumber {
        return Ok(None);
    }

    let mut previptr = ItemPointerData::invalid();
    loop {
        if revmap.rm_currBuf.get() == InvalidBuffer
            || buffer_get_block_number::call(revmap.rm_currBuf.get()) != mapBlk
        {
            if revmap.rm_currBuf.get() != InvalidBuffer {
                release_buffer::call(revmap.rm_currBuf.get())?;
            }
            debug_assert!(mapBlk != InvalidBlockNumber);
            revmap.rm_currBuf.set(read_buffer::call(idxrel, mapBlk)?);
        }

        lock_buffer::call(revmap.rm_currBuf.get(), BUFFER_LOCK_SHARE)?;

        // SAFETY: pinned + share locked.
        let iptr = revmap_get_tid(
            unsafe { &page_ref(revmap.rm_currBuf.get()) },
            HEAPBLK_TO_REVMAP_INDEX(revmap.rm_pagesPerRange, heapBlk),
        );

        if !ItemPointerIsValid(&iptr) {
            lock_buffer::call(revmap.rm_currBuf.get(), BUFFER_LOCK_UNLOCK)?;
            return Ok(None);
        }

        if ItemPointerIsValid(&previptr) && ItemPointerEquals(&previptr, &iptr) {
            return Err(corrupted_revmap());
        }
        previptr = iptr;

        let blk = ItemPointerGetBlockNumber(&iptr);
        let off = iptr.ip_posid;

        lock_buffer::call(revmap.rm_currBuf.get(), BUFFER_LOCK_UNLOCK)?;

        if *buf == InvalidBuffer || buffer_get_block_number::call(*buf) != blk {
            if *buf != InvalidBuffer {
                release_buffer::call(*buf)?;
            }
            *buf = read_buffer::call(idxrel, blk)?;
        }
        lock_buffer::call(*buf, mode)?;
        // SAFETY: pinned + locked.
        let page = unsafe { page_ref(*buf) };

        if BRIN_IS_REGULAR_PAGE(&page) {
            if off > page.max_offset_number() {
                lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;
                return Ok(None);
            }
            let lp = page.item_id(off);
            if lp.is_used() {
                let (p, sz) = page.item_raw(lp);
                // SAFETY: item bytes within the locked page; copied out below.
                let item = unsafe { core::slice::from_raw_parts(p, sz as usize) };
                if brin_tuple_blkno(item) == heapBlk {
                    out.clear();
                    ::mcx::vec_append_bytes(out, item)?;
                    if !keep_lock {
                        lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;
                    }
                    return Ok(Some(off));
                }
            }
        }

        lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;
    }
}

pub fn brinRevmapDesummarizeRange(idxrel: &Relation<'_>, heapBlk: BlockNumber) -> PgResult<bool> {
    let (revmap, _ppr) = brinRevmapInitialize(idxrel)?;

    let revmapBlk = revmap_get_blkno(&revmap, heapBlk);
    if revmapBlk == InvalidBlockNumber {
        brinRevmapTerminate(revmap)?;
        return Ok(true);
    }

    let revmapBuf = brinLockRevmapPageForUpdate(idxrel, &revmap, heapBlk)?;
    let revmapOffset = HEAPBLK_TO_REVMAP_INDEX(revmap.rm_pagesPerRange, heapBlk);

    // SAFETY: pinned + exclusively locked.
    let iptr = revmap_get_tid(unsafe { &page_ref(revmapBuf) }, revmapOffset);

    if !ItemPointerIsValid(&iptr) {
        lock_buffer::call(revmapBuf, BUFFER_LOCK_UNLOCK)?;
        brinRevmapTerminate(revmap)?;
        return Ok(true);
    }

    let regBuf = read_buffer::call(idxrel, ItemPointerGetBlockNumber(&iptr))?;
    lock_buffer::call(regBuf, BUFFER_LOCK_EXCLUSIVE)?;
    // SAFETY: pinned + exclusively locked.
    let regPg = unsafe { page_ref(regBuf) };

    if !BRIN_IS_REGULAR_PAGE(&regPg) {
        lock_buffer::call(revmapBuf, BUFFER_LOCK_UNLOCK)?;
        lock_buffer::call(regBuf, BUFFER_LOCK_UNLOCK)?;
        release_buffer::call(regBuf)?;
        brinRevmapTerminate(revmap)?;
        return Ok(false);
    }

    let regOffset = iptr.ip_posid;
    if regOffset > regPg.max_offset_number() {
        return Err(corrupted_revmap());
    }
    if !regPg.item_id(regOffset).is_used() {
        return Err(corrupted_revmap());
    }

    // Leftover placeholder tuples from a crashed/aborted summarization are
    // removed silently (ShareUpdateExclusiveLock excludes a live one).
    brinSetHeapBlockItemptr(
        revmapBuf,
        revmap.rm_pagesPerRange,
        heapBlk,
        ItemPointerData::invalid(),
    );
    // SAFETY: exclusive lock held.
    unsafe { page_mut(regBuf) }.index_tuple_delete_no_compact(regOffset);

    mark_buffer_dirty::call(regBuf)?;
    mark_buffer_dirty::call(revmapBuf)?;

    if relation_needs_wal(idxrel) {
        let xlrec = xl_brin_desummarize(revmap.rm_pagesPerRange, heapBlk, regOffset);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RmgrIds::RM_BRIN_ID as u8,
            XLOG_BRIN_DESUMMARIZE,
            0,
            &[&xlrec],
            &[
                XLogRegBuf { block_id: 0, buffer: revmapBuf, flags: 0, bufdata: &[] },
                XLogRegBuf { block_id: 1, buffer: regBuf, flags: REGBUF_STANDARD, bufdata: &[] },
            ],
        )?;
        // SAFETY: both exclusively locked.
        unsafe { page_mut(revmapBuf) }.set_lsn(recptr);
        unsafe { page_mut(regBuf) }.set_lsn(recptr);
    }

    lock_buffer::call(regBuf, BUFFER_LOCK_UNLOCK)?;
    release_buffer::call(regBuf)?;
    lock_buffer::call(revmapBuf, BUFFER_LOCK_UNLOCK)?;
    brinRevmapTerminate(revmap)?;

    Ok(true)
}

// revmap_get_blkno: physical revmap block for heapBlk, or InvalidBlockNumber
// when not yet allocated.
fn revmap_get_blkno(revmap: &BrinRevmap, heapBlk: BlockNumber) -> BlockNumber {
    let targetblk = HEAPBLK_TO_REVMAP_BLK(revmap.rm_pagesPerRange, heapBlk) + 1;
    if targetblk <= revmap.rm_lastRevmapPage.get() {
        targetblk
    } else {
        InvalidBlockNumber
    }
}

// revmap_get_buffer; the pin is cached in rm_currBuf (terminate releases it).
fn revmap_get_buffer(
    idxrel: &Relation<'_>,
    revmap: &BrinRevmap,
    heapBlk: BlockNumber,
) -> PgResult<Buffer> {
    let mapBlk = revmap_get_blkno(revmap, heapBlk);
    if mapBlk == InvalidBlockNumber {
        panic!("revmap does not cover heap block {heapBlk}");
    }
    debug_assert!(mapBlk != BRIN_METAPAGE_BLKNO && mapBlk <= revmap.rm_lastRevmapPage.get());

    if revmap.rm_currBuf.get() == InvalidBuffer
        || mapBlk != buffer_get_block_number::call(revmap.rm_currBuf.get())
    {
        if revmap.rm_currBuf.get() != InvalidBuffer {
            release_buffer::call(revmap.rm_currBuf.get())?;
        }
        revmap.rm_currBuf.set(read_buffer::call(idxrel, mapBlk)?);
    }
    Ok(revmap.rm_currBuf.get())
}

fn revmap_extend_and_get_blkno(
    idxrel: &Relation<'_>,
    revmap: &BrinRevmap,
    heapBlk: BlockNumber,
) -> PgResult<BlockNumber> {
    let targetblk = HEAPBLK_TO_REVMAP_BLK(revmap.rm_pagesPerRange, heapBlk) + 1;
    while targetblk > revmap.rm_lastRevmapPage.get() {
        revmap_physical_extend(idxrel, revmap)?;
    }
    Ok(targetblk)
}

// revmap_physical_extend: try to extend the revmap by one page; caller loops
// until the cached rm_lastRevmapPage catches up.
fn revmap_physical_extend(irel: &Relation<'_>, revmap: &BrinRevmap) -> PgResult<()> {
    lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_EXCLUSIVE)?;
    // SAFETY: pinned + exclusively locked.
    let mut metadata = brin_meta_read(unsafe { &page_ref(revmap.rm_metaBuf) });

    if metadata.lastRevmapPage != revmap.rm_lastRevmapPage.get() {
        revmap.rm_lastRevmapPage.set(metadata.lastRevmapPage);
        lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
        return Ok(());
    }
    let mapBlk = metadata.lastRevmapPage + 1;

    let nblocks = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
        irel,
        ForkNumber::MAIN_FORKNUM,
    )?;
    let buf: Buffer;
    if mapBlk < nblocks {
        buf = read_buffer::call(irel, mapBlk)?;
        lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
    } else {
        let (b, _n) =
            extend_buffered_rel_by::call(irel, ForkNumber::MAIN_FORKNUM, None, EB_LOCK_FIRST, 1)?;
        buf = b;
        if buffer_get_block_number::call(buf) != mapBlk {
            // Rare: concurrent extension after we read the length; caller
            // starts over (we'll evacuate that page from its user).
            lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
            lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
            release_buffer::call(buf)?;
            return Ok(());
        }
    }

    // SAFETY: pinned + exclusively locked.
    let page = unsafe { page_ref(buf) };
    if !page.is_new() && !BRIN_IS_REGULAR_PAGE(&page) {
        return Err(Box::new(
            PgError::error(format!(
                "unexpected page type 0x{:04X} in BRIN index \"{}\" block {}",
                BrinPageType(&page),
                irel.name(),
                buffer_get_block_number::call(buf)
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED),
        ));
    }

    if brin_start_evacuating_page(irel, buf)? {
        lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
        brin_evacuate_page(irel, revmap.rm_pagesPerRange, revmap, buf)?;
        return Ok(());
    }

    // SAFETY: exclusive locks held on both pages.
    let mut pm = unsafe { page_mut(buf) };
    brin_page_init(&mut pm, BRIN_PAGETYPE_REVMAP);
    mark_buffer_dirty::call(buf)?;

    metadata.lastRevmapPage = mapBlk;
    let mut metapage = unsafe { page_mut(revmap.rm_metaBuf) };
    brin_meta_write(&mut metapage, &metadata);
    // pd_lower past the metadata (pre-v11 pg_upgrade rescue; see C).
    metapage.set_pd_lower((SizeOfPageHeaderData + SizeOfBrinMetaPageData) as u16);
    mark_buffer_dirty::call(revmap.rm_metaBuf)?;

    if relation_needs_wal(irel) {
        let xlrec = xl_brin_revmap_extend(mapBlk);
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            RmgrIds::RM_BRIN_ID as u8,
            XLOG_BRIN_REVMAP_EXTEND,
            0,
            &[&xlrec],
            &[
                XLogRegBuf {
                    block_id: 0,
                    buffer: revmap.rm_metaBuf,
                    flags: REGBUF_STANDARD,
                    bufdata: &[],
                },
                XLogRegBuf { block_id: 1, buffer: buf, flags: REGBUF_WILL_INIT, bufdata: &[] },
            ],
        )?;
        metapage.set_lsn(recptr);
        pm.set_lsn(recptr);
    }

    lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
    lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
    release_buffer::call(buf)?;
    Ok(())
}
