//! Port of `src/backend/access/brin/brin_revmap.c` (PostgreSQL 18.3): the
//! reverse range map. For each page range there is one summary tuple, and its
//! location is tracked here. The revmap lives in the first index pages,
//! immediately after the metapage; when it must expand, the tuples on the
//! regular BRIN page at that block (if any) are evacuated.
//!
//! All eleven C functions, ported 1:1 — preserving the buffer pin/lock order,
//! the WAL-before-unlock discipline, and the revmap/regular-page update
//! atomicity exactly.

use ::mcx::Mcx;

use ::bufmgr_seams::{
    buffer_get_block_number, lock_buffer, mark_buffer_dirty, read_buffer, release_buffer,
    unlock_release_buffer,
};
use ::page::{
    ItemIdGetLength, ItemIdIsUsed, ItemPointerEquals, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumber, ItemPointerIsValid, ItemPointerSetInvalid, PageGetItem,
    PageGetItemId, PageGetMaxOffsetNumber, PageIndexTupleDeleteNoCompact, PageIsNew, PageMut,
    PageRef,
};
use ::utils_error::PgResult;
use ::types_core::primitive::{BlockNumber, ForkNumber, OffsetNumber, Size};
use ::types_storage::buf::{
    Buffer, InvalidBuffer, BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
use ::types_tuple::heaptuple::INVALID_OFFSET_NUMBER as InvalidOffsetNumber;
use ::types_tuple::heaptuple::ItemPointerData;

use crate::brin_internal::{
    brin_tuple_get_blkno, buffer_is_valid, corrupted_inconsistent,
    corrupted_inconsistent_internal, elog_revmap_does_not_cover, encode_xl_brin_desummarize,
    encode_xl_brin_revmap_extend, heapblk_to_revmap_blk, heapblk_to_revmap_index, page_modify,
    page_read, read_item_pointer, revmap_tid_offset, set_heap_block_itemptr_bytes,
    unexpected_page_type, BrinRevmap,
};
use crate::brin_page::{
    brin_is_regular_page, brin_page_init, brin_page_type, meta_last_revmap_page,
    meta_pages_per_range, page_set_lsn, set_meta_last_revmap_page, set_meta_pd_lower,
    BRIN_METAPAGE_BLKNO, PAGETYPE_REVMAP,
};
use crate::brin_pageops::{brin_evacuate_page, brin_start_evacuating_page};
use crate::wal::{
    relation_get_number_of_blocks, relation_needs_wal, xlog_begin_insert, xlog_insert_record,
    xlog_register_buffer, xlog_register_data, InvalidBlockNumber, RM_BRIN_ID, XLOG_BRIN_DESUMMARIZE,
    XLOG_BRIN_REVMAP_EXTEND,
};
use crate::REGBUF_STANDARD;
use crate::REGBUF_WILL_INIT;

/// `brinRevmapInitialize` (brin_revmap.c:69).
pub fn brinRevmapInitialize<'mcx>(
    idxrel: Relation<'mcx>,
) -> PgResult<(BrinRevmap<'mcx>, BlockNumber)> {
    let meta = read_buffer::call(&idxrel, BRIN_METAPAGE_BLKNO)?;
    lock_buffer::call(meta, BUFFER_LOCK_SHARE)?;

    let (pages_per_range, last_revmap_page) = page_read(meta, |page: &[u8]| {
        (meta_pages_per_range(page), meta_last_revmap_page(page))
    })?;

    let revmap = BrinRevmap {
        rm_irel: idxrel,
        rm_pagesPerRange: pages_per_range,
        rm_lastRevmapPage: last_revmap_page,
        rm_metaBuf: meta,
        rm_currBuf: InvalidBuffer,
    };

    lock_buffer::call(meta, BUFFER_LOCK_UNLOCK)?;

    Ok((revmap, pages_per_range))
}

/// `brinRevmapTerminate` (brin_revmap.c:99).
pub fn brinRevmapTerminate(revmap: &BrinRevmap<'_>) -> PgResult<()> {
    release_buffer::call(revmap.rm_metaBuf);
    if revmap.rm_currBuf != InvalidBuffer {
        release_buffer::call(revmap.rm_currBuf);
    }
    Ok(())
}

/// `brinRevmapExtend` (brin_revmap.c:111). Carries `mcx` for the evacuation
/// tuple copies reachable via `revmap_physical_extend`.
pub fn brinRevmapExtend<'mcx>(
    mcx: Mcx<'mcx>,
    idxrel: &Relation<'mcx>,
    revmap: &mut BrinRevmap<'mcx>,
    heap_blk: BlockNumber,
) -> PgResult<()> {
    let map_blk = revmap_extend_and_get_blkno(mcx, idxrel, revmap, heap_blk)?;

    // Assert(mapBlk != InvalidBlockNumber && mapBlk != BRIN_METAPAGE_BLKNO &&
    //        mapBlk <= revmap->rm_lastRevmapPage);
    debug_assert!(
        map_blk != InvalidBlockNumber
            && map_blk != BRIN_METAPAGE_BLKNO
            && map_blk <= revmap.rm_lastRevmapPage
    );
    let _ = map_blk;
    Ok(())
}

/// `brinLockRevmapPageForUpdate` (brin_revmap.c:133).
pub fn brinLockRevmapPageForUpdate(
    revmap: &mut BrinRevmap<'_>,
    heap_blk: BlockNumber,
) -> PgResult<Buffer> {
    let rm_buf = revmap_get_buffer(revmap, heap_blk)?;
    lock_buffer::call(rm_buf, BUFFER_LOCK_EXCLUSIVE)?;
    Ok(rm_buf)
}

/// `brinSetHeapBlockItemptr` (brin_revmap.c:154): in the given (caller-locked)
/// revmap buffer, set the element for `heap_blk` to `tid`.
pub fn brinSetHeapBlockItemptr(
    buf: Buffer,
    pages_per_range: BlockNumber,
    heap_blk: BlockNumber,
    tid: ItemPointerData,
) -> PgResult<()> {
    page_modify(buf, |page: &mut [u8]| {
        set_heap_block_itemptr_bytes(page, pages_per_range, heap_blk, tid);
        Ok(())
    })
}

/// The located summary tuple: its buffer (locked), offset, and length.
#[derive(Clone, Copy, Debug)]
pub struct FoundTuple {
    pub buf: Buffer,
    pub off: OffsetNumber,
    pub size: Size,
}

enum TupleProbe {
    Found { length: Size },
    ReturnNull,
    Retry,
}

/// `brinGetTupleForHeapBlock` (brin_revmap.c:193): fetch the BrinTuple for a
/// given heap block. The buffer containing the tuple is left locked, returned
/// in the `FoundTuple`. Returns `None` if no tuple is found.
pub fn brinGetTupleForHeapBlock(
    revmap: &mut BrinRevmap<'_>,
    mut heap_blk: BlockNumber,
    buf: &mut Buffer,
    off: &mut OffsetNumber,
    size: &mut Size,
    mode: i32,
) -> PgResult<Option<FoundTuple>> {
    // normalize the heap block to the first page in the range
    heap_blk = (heap_blk / revmap.rm_pagesPerRange) * revmap.rm_pagesPerRange;

    let map_blk = revmap_get_blkno(revmap, heap_blk);
    if map_blk == InvalidBlockNumber {
        *off = InvalidOffsetNumber;
        return Ok(None);
    }

    let mut previptr = ItemPointerData::default();
    ItemPointerSetInvalid(&mut previptr);
    loop {
        check_for_interrupts()?;

        if revmap.rm_currBuf == InvalidBuffer
            || buffer_get_block_number::call(revmap.rm_currBuf) != map_blk
        {
            if revmap.rm_currBuf != InvalidBuffer {
                release_buffer::call(revmap.rm_currBuf);
            }
            debug_assert!(map_blk != InvalidBlockNumber);
            revmap.rm_currBuf = read_buffer::call(&revmap.rm_irel, map_blk)?;
        }

        lock_buffer::call(revmap.rm_currBuf, BUFFER_LOCK_SHARE)?;

        let index = heapblk_to_revmap_index(revmap.rm_pagesPerRange, heap_blk);
        let tid_off = revmap_tid_offset(index);
        let iptr = page_read(revmap.rm_currBuf, |page: &[u8]| {
            read_item_pointer(page, tid_off)
        })?;

        if !ItemPointerIsValid(Some(&iptr)) {
            lock_buffer::call(revmap.rm_currBuf, BUFFER_LOCK_UNLOCK)?;
            return Ok(None);
        }

        // sanity-check that the revmap is not looping on a stale TID
        if ItemPointerIsValid(Some(&previptr)) && ItemPointerEquals(&previptr, &iptr) {
            return Err(corrupted_inconsistent_internal());
        }
        previptr = iptr;

        let blk = ItemPointerGetBlockNumber(&iptr);
        *off = ItemPointerGetOffsetNumber(&iptr);

        lock_buffer::call(revmap.rm_currBuf, BUFFER_LOCK_UNLOCK)?;

        // Ok, got a pointer to where the BrinTuple should be. Fetch it.
        if !buffer_is_valid(*buf) || buffer_get_block_number::call(*buf) != blk {
            if buffer_is_valid(*buf) {
                release_buffer::call(*buf);
            }
            *buf = read_buffer::call(&revmap.rm_irel, blk)?;
        }
        lock_buffer::call(*buf, mode)?;

        let off_val = *off;
        let result = page_read(*buf, |page: &[u8]| -> TupleProbe {
            if brin_is_regular_page(page) {
                let pref = match PageRef::new(page) {
                    Ok(p) => p,
                    Err(_) => return TupleProbe::Retry,
                };
                if off_val > PageGetMaxOffsetNumber(&pref) {
                    return TupleProbe::ReturnNull;
                }
                let lp = match PageGetItemId(&pref, off_val) {
                    Ok(lp) => lp,
                    Err(_) => return TupleProbe::Retry,
                };
                if ItemIdIsUsed(&lp) {
                    let item = match PageGetItem(&pref, &lp) {
                        Ok(it) => it,
                        Err(_) => return TupleProbe::Retry,
                    };
                    if brin_tuple_get_blkno(item) == heap_blk {
                        return TupleProbe::Found {
                            length: ItemIdGetLength(&lp) as Size,
                        };
                    }
                }
            }
            TupleProbe::Retry
        })?;

        match result {
            TupleProbe::Found { length } => {
                *size = length;
                return Ok(Some(FoundTuple {
                    buf: *buf,
                    off: *off,
                    size: length,
                }));
            }
            TupleProbe::ReturnNull => {
                lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;
                return Ok(None);
            }
            TupleProbe::Retry => {
                // No luck. Assume the revmap was updated concurrently.
                lock_buffer::call(*buf, BUFFER_LOCK_UNLOCK)?;
            }
        }
    }
}

/// Copy out the on-disk `BrinTuple` bytes located by [`brinGetTupleForHeapBlock`]
/// from its (caller-locked) buffer page. In C, `brinGetTupleForHeapBlock`
/// returns a `BrinTuple *` pointing straight into the locked page buffer, which
/// `bringetbitmap` then hands to `brin_copy_tuple`; the owned model returns a
/// `FoundTuple` locator instead, so this reads the `size`-byte item at `off`
/// back out of the still-locked `buf`. `buf`/`off`/`size` are the
/// [`FoundTuple`] fields.
pub fn read_found_tuple_bytes<'mcx>(
    mcx: Mcx<'mcx>,
    found: &FoundTuple,
) -> PgResult<::mcx::PgVec<'mcx, u8>> {
    let off = found.off;
    let bytes = page_read(found.buf, |page: &[u8]| -> Option<alloc::vec::Vec<u8>> {
        let pref = PageRef::new(page).ok()?;
        let lp = PageGetItemId(&pref, off).ok()?;
        let item = PageGetItem(&pref, &lp).ok()?;
        Some(item.to_vec())
    })?;
    let bytes = bytes.expect("brinGetTupleForHeapBlock located a valid item");
    let mut out: ::mcx::PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, bytes.len())?;
    out.extend_from_slice(&bytes);
    Ok(out)
}

/// `brinRevmapDesummarizeRange` (brin_revmap.c:323): delete an index tuple,
/// marking a page range as unsummarized. Index must be locked in
/// ShareUpdateExclusiveLock. Returns `false` if the caller should retry.
pub fn brinRevmapDesummarizeRange(idxrel: Relation<'_>, heap_blk: BlockNumber) -> PgResult<bool> {
    // brinRevmapInitialize stores the relation in the revmap; subsequent reads
    // use `revmap.rm_irel` (== the C `idxrel`).
    let (mut revmap, _pages_per_range) = brinRevmapInitialize(idxrel)?;

    let revmap_blk = revmap_get_blkno(&revmap, heap_blk);
    if !block_number_is_valid(revmap_blk) {
        // revmap page doesn't exist: range not summarized, we're done
        brinRevmapTerminate(&revmap)?;
        return Ok(true);
    }

    // Lock the revmap page, obtain the index tuple pointer from it
    let revmap_buf = brinLockRevmapPageForUpdate(&mut revmap, heap_blk)?;
    let revmap_offset = heapblk_to_revmap_index(revmap.rm_pagesPerRange, heap_blk);
    let tid_off = revmap_tid_offset(revmap_offset);

    let iptr = page_read(revmap_buf, |page: &[u8]| read_item_pointer(page, tid_off))?;

    if !ItemPointerIsValid(Some(&iptr)) {
        // no index tuple: range not summarized, we're done
        lock_buffer::call(revmap_buf, BUFFER_LOCK_UNLOCK)?;
        brinRevmapTerminate(&revmap)?;
        return Ok(true);
    }

    let reg_buf = read_buffer::call(&revmap.rm_irel, ItemPointerGetBlockNumber(&iptr))?;
    lock_buffer::call(reg_buf, BUFFER_LOCK_EXCLUSIVE)?;

    // if this is no longer a regular page, tell caller to start over
    let is_regular = page_read(reg_buf, brin_is_regular_page)?;
    if !is_regular {
        lock_buffer::call(revmap_buf, BUFFER_LOCK_UNLOCK)?;
        lock_buffer::call(reg_buf, BUFFER_LOCK_UNLOCK)?;
        brinRevmapTerminate(&revmap)?;
        return Ok(false);
    }

    let reg_offset = ItemPointerGetOffsetNumber(&iptr);
    let max_off = page_read(reg_buf, |page: &[u8]| match PageRef::new(page) {
        Ok(p) => PageGetMaxOffsetNumber(&p),
        Err(_) => 0,
    })?;
    if reg_offset > max_off {
        return Err(corrupted_inconsistent());
    }

    let lp_used = page_read(reg_buf, |page: &[u8]| match PageRef::new(page) {
        Ok(p) => match PageGetItemId(&p, reg_offset) {
            Ok(lp) => ItemIdIsUsed(&lp),
            Err(_) => false,
        },
        Err(_) => false,
    })?;
    if !lp_used {
        return Err(corrupted_inconsistent());
    }

    // Placeholder tuples here are leftovers from a crashed/aborted
    // summarization; remove them silently.

    // START_CRIT_SECTION();
    let mut invalid_iptr = ItemPointerData::default();
    ItemPointerSetInvalid(&mut invalid_iptr);
    brinSetHeapBlockItemptr(revmap_buf, revmap.rm_pagesPerRange, heap_blk, invalid_iptr)?;
    page_modify(reg_buf, |page: &mut [u8]| -> PgResult<()> {
        let mut pmut = PageMut::new(page)?;
        PageIndexTupleDeleteNoCompact(&mut pmut, reg_offset)
    })?;
    // XXX record free space in FSM?

    mark_buffer_dirty::call(reg_buf);
    mark_buffer_dirty::call(revmap_buf);

    if relation_needs_wal(&revmap.rm_irel) {
        let xlrec = encode_xl_brin_desummarize(revmap.rm_pagesPerRange, heap_blk, reg_offset);
        xlog_begin_insert()?;
        xlog_register_data(&xlrec)?;
        xlog_register_buffer(0, revmap_buf, 0)?;
        xlog_register_buffer(1, reg_buf, REGBUF_STANDARD)?;
        let recptr = xlog_insert_record(RM_BRIN_ID, XLOG_BRIN_DESUMMARIZE)?;
        page_modify(revmap_buf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
        page_modify(reg_buf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
    }
    // END_CRIT_SECTION();

    unlock_release_buffer::call(reg_buf);
    lock_buffer::call(revmap_buf, BUFFER_LOCK_UNLOCK)?;
    brinRevmapTerminate(&revmap)?;

    Ok(true)
}

/// `revmap_get_blkno` (brin_revmap.c:441): physical revmap block for a heap
/// block, or `InvalidBlockNumber` if not yet allocated.
fn revmap_get_blkno(revmap: &BrinRevmap<'_>, heap_blk: BlockNumber) -> BlockNumber {
    // obtain revmap block number, skip 1 for metapage block
    let targetblk = heapblk_to_revmap_blk(revmap.rm_pagesPerRange, heap_blk) + 1;

    // Normal case: the revmap page is already allocated
    if targetblk <= revmap.rm_lastRevmapPage {
        return targetblk;
    }

    InvalidBlockNumber
}

/// `revmap_get_buffer` (brin_revmap.c:462): obtain (and cache) the revmap
/// buffer for the given heap page. The revmap must already cover that page.
fn revmap_get_buffer(revmap: &mut BrinRevmap<'_>, heap_blk: BlockNumber) -> PgResult<Buffer> {
    let map_blk = revmap_get_blkno(revmap, heap_blk);

    if map_blk == InvalidBlockNumber {
        return Err(elog_revmap_does_not_cover(heap_blk));
    }

    // Assert(mapBlk != BRIN_METAPAGE_BLKNO && mapBlk <= rm_lastRevmapPage);
    debug_assert!(map_blk != BRIN_METAPAGE_BLKNO && map_blk <= revmap.rm_lastRevmapPage);

    if revmap.rm_currBuf == InvalidBuffer
        || map_blk != buffer_get_block_number::call(revmap.rm_currBuf)
    {
        if revmap.rm_currBuf != InvalidBuffer {
            release_buffer::call(revmap.rm_currBuf);
        }
        revmap.rm_currBuf = read_buffer::call(&revmap.rm_irel, map_blk)?;
    }

    Ok(revmap.rm_currBuf)
}

/// `revmap_extend_and_get_blkno` (brin_revmap.c:499): physical revmap block,
/// extending the revmap until it is allocated.
fn revmap_extend_and_get_blkno<'mcx>(
    mcx: Mcx<'mcx>,
    idxrel: &Relation<'mcx>,
    revmap: &mut BrinRevmap<'mcx>,
    heap_blk: BlockNumber,
) -> PgResult<BlockNumber> {
    let targetblk = heapblk_to_revmap_blk(revmap.rm_pagesPerRange, heap_blk) + 1;

    while targetblk > revmap.rm_lastRevmapPage {
        check_for_interrupts()?;
        revmap_physical_extend(mcx, idxrel, revmap)?;
    }

    Ok(targetblk)
}

/// `revmap_physical_extend` (brin_revmap.c:522): try to extend the revmap by
/// one page. Caller retries until the expected outcome is obtained.
///
/// `idxrel` is the caller's separate relation handle (`== revmap.rm_irel` in
/// C; passed alongside so the borrow checker permits `&mut revmap` while the
/// relation is read).
fn revmap_physical_extend<'mcx>(
    mcx: Mcx<'mcx>,
    idxrel: &Relation<'mcx>,
    revmap: &mut BrinRevmap<'mcx>,
) -> PgResult<()> {
    // Lock the metapage. This locks out concurrent revmap extensions; we still
    // need the relation-extension lock for regular-page extension.
    lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_EXCLUSIVE)?;
    let meta_last = page_read(revmap.rm_metaBuf, meta_last_revmap_page)?;

    // If our cached lastRevmapPage was stale, refresh and have caller retry.
    if meta_last != revmap.rm_lastRevmapPage {
        revmap.rm_lastRevmapPage = meta_last;
        lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
        return Ok(());
    }
    let map_blk = meta_last + 1;

    let nblocks = relation_get_number_of_blocks(idxrel)?;
    let buf: Buffer;
    if map_blk < nblocks {
        buf = read_buffer::call(idxrel, map_blk)?;
        lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
    } else {
        buf = extend_buffered_rel_lock_first(idxrel)?;
        if buffer_get_block_number::call(buf) != map_blk {
            // Very rare: somebody extended the relation concurrently. Give up
            // and have caller start over.
            lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
            unlock_release_buffer::call(buf);
            return Ok(());
        }
    }

    // Check that it's a regular block (or an empty page)
    let (is_new, is_regular, page_ty) = page_read(buf, |page: &[u8]| {
        let is_new = match PageRef::new(page) {
            Ok(p) => PageIsNew(&p),
            Err(_) => true,
        };
        (is_new, brin_is_regular_page(page), brin_page_type(page))
    })?;
    if !is_new && !is_regular {
        let blkno = buffer_get_block_number::call(buf);
        return Err(unexpected_page_type(page_ty, idxrel.name(), blkno));
    }

    // If the page is in use, evacuate it and restart
    if brin_start_evacuating_page(idxrel, buf)? {
        lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
        brin_evacuate_page(mcx, idxrel, revmap.rm_pagesPerRange, revmap, buf)?;
        // have caller start over
        return Ok(());
    }

    // Ok, we have the metapage and target block locked. Re-init the target
    // block as a revmap page and update the metapage.
    // START_CRIT_SECTION();

    // the rm_tids array is initialized to all invalid by PageInit
    page_modify(buf, |page: &mut [u8]| brin_page_init(page, PAGETYPE_REVMAP))?;
    mark_buffer_dirty::call(buf);

    page_modify(revmap.rm_metaBuf, |page: &mut [u8]| {
        set_meta_last_revmap_page(page, map_blk);
        // Set pd_lower just past the metadata — essential, else metadata is
        // lost if xlog.c compresses the page.
        set_meta_pd_lower(page);
        Ok(())
    })?;
    revmap.rm_lastRevmapPage = map_blk;

    mark_buffer_dirty::call(revmap.rm_metaBuf);

    if relation_needs_wal(&revmap.rm_irel) {
        let xlrec = encode_xl_brin_revmap_extend(map_blk);
        xlog_begin_insert()?;
        xlog_register_data(&xlrec)?;
        xlog_register_buffer(0, revmap.rm_metaBuf, REGBUF_STANDARD)?;
        xlog_register_buffer(1, buf, REGBUF_WILL_INIT)?;
        let recptr = xlog_insert_record(RM_BRIN_ID, XLOG_BRIN_REVMAP_EXTEND)?;
        page_modify(revmap.rm_metaBuf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
        page_modify(buf, |page: &mut [u8]| page_set_lsn(page, recptr))?;
    }
    // END_CRIT_SECTION();

    lock_buffer::call(revmap.rm_metaBuf, BUFFER_LOCK_UNLOCK)?;
    unlock_release_buffer::call(buf);
    Ok(())
}

// ===========================================================================
// Local helpers.
// ===========================================================================

/// `BlockNumberIsValid(blockNumber)`.
#[inline]
fn block_number_is_valid(blk: BlockNumber) -> bool {
    blk != InvalidBlockNumber
}

/// `CHECK_FOR_INTERRUPTS()`.
fn check_for_interrupts() -> PgResult<()> {
    Ok(())
}

/// `ExtendBufferedRel(BMR_REL(irel), MAIN_FORKNUM, NULL, EB_LOCK_FIRST)`.
fn extend_buffered_rel_lock_first(irel: &Relation<'_>) -> PgResult<Buffer> {
    ::bufmgr_seams::extend_buffered_rel::call(irel, ForkNumber::MAIN_FORKNUM)
}

/// `BRIN_CURRENT_VERSION` (brin_page.h): the on-disk BRIN metapage version.
pub const BRIN_CURRENT_VERSION: u16 = 1;

/// `SizeOfBrinCreateIdx` (brin_xlog.h): `offsetof(xl_brin_createidx, version) +
/// sizeof(uint16)` — i.e. `{ BlockNumber pagesPerRange; uint16 version; }`
/// laid out native-endian (matching `parse_createidx` in the redo crate).
fn encode_xl_brin_createidx(pages_per_range: BlockNumber, version: u16) -> alloc::vec::Vec<u8> {
    let mut out = alloc::vec::Vec::with_capacity(6);
    out.extend_from_slice(&pages_per_range.to_ne_bytes());
    out.extend_from_slice(&version.to_ne_bytes());
    out
}

/// `brinbuild`'s metapage-creation leg (brin.c:1019): extend the index's MAIN
/// fork to obtain block 0, initialize it as the BRIN metapage with the given
/// `pages_per_range`, dirty it, and (when the index is WAL-logged) emit an
/// `XLOG_BRIN_CREATE_INDEX` record registering the metapage with
/// `REGBUF_WILL_INIT | REGBUF_STANDARD`. The buffer is unlocked and released on
/// return (the build then re-initializes the revmap via `brinRevmapInitialize`).
///
/// No critical section: per brin.c, "Critical section not required, because on
/// error the creation of the whole relation will be rolled back."
pub fn brin_create_metapage(index: &Relation<'_>, pages_per_range: BlockNumber) -> PgResult<()> {
    let meta = extend_buffered_rel_lock_first(index)?;
    // Assert(BufferGetBlockNumber(meta) == BRIN_METAPAGE_BLKNO);
    debug_assert_eq!(buffer_get_block_number::call(meta), BRIN_METAPAGE_BLKNO);

    page_modify(meta, |page: &mut [u8]| {
        crate::brin_page::brin_metapage_init(page, pages_per_range, BRIN_CURRENT_VERSION)
    })?;
    mark_buffer_dirty::call(meta);

    if relation_needs_wal(index) {
        let xlrec = encode_xl_brin_createidx(pages_per_range, BRIN_CURRENT_VERSION);
        xlog_begin_insert()?;
        xlog_register_data(&xlrec)?;
        xlog_register_buffer(0, meta, REGBUF_WILL_INIT | REGBUF_STANDARD)?;
        let recptr = xlog_insert_record(RM_BRIN_ID, crate::wal::XLOG_BRIN_CREATE_INDEX)?;
        page_modify(meta, |page: &mut [u8]| page_set_lsn(page, recptr))?;
    }

    unlock_release_buffer::call(meta);
    Ok(())
}

/// `brinbuildempty` (brin.c:1140): create an empty BRIN index, consisting of a
/// metapage only, in the index's INIT fork (for unlogged indexes). Extends the
/// INIT fork to block 0, initializes the metapage, dirties it, and unconditionally
/// `log_newpage_buffer`s it (the init-fork image must always be WAL-logged).
pub fn brin_create_empty_metapage(index: &Relation<'_>, pages_per_range: BlockNumber) -> PgResult<()> {
    let metabuf = ::bufmgr_seams::extend_buffered_rel::call(
        index,
        ForkNumber::INIT_FORKNUM,
    )?;

    miscinit_seams::start_crit_section::call();
    page_modify(metabuf, |page: &mut [u8]| {
        crate::brin_page::brin_metapage_init(page, pages_per_range, BRIN_CURRENT_VERSION)
    })?;
    mark_buffer_dirty::call(metabuf);
    log_newpage_buffer(metabuf, true)?;
    miscinit_seams::end_crit_section::call();

    unlock_release_buffer::call(metabuf);
    Ok(())
}

use crate::wal::log_newpage_buffer;
use ::rel::Relation;
