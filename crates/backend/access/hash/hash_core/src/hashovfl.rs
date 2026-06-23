//! Port of `src/backend/access/hash/hashovfl.c` (PostgreSQL 18.3): overflow
//! page management — `_hash_addovflpage`, `_hash_freeovflpage`,
//! `_hash_squeezebucket`, `_hash_initbitmapbuffer`, plus the bit/block-number
//! conversions (`bitno_to_blkno`, `_hash_ovflblkno_to_bitno`,
//! `_hash_firstfreebit`).
//!
//! ## Seam-and-panic paths (unported callees in other crates)
//! * Buffer manager + WAL insert seams (see [`crate::hashpage`]).
//! * xlog (`xlog_ensure_record_space`).

extern crate alloc;
use alloc::vec::Vec;

use types_core::primitive::{BlockNumber, InvalidBlockNumber};
use types_error::{PgError, PgResult, ERROR};
use hash::hashpage::{
    Bucket, HashMetaPageData, HASH_MAX_BITMAPS, HASH_METAPAGE, HASH_READ, HASH_WRITE,
    InvalidBucket, LH_BITMAP_PAGE, LH_BUCKET_PAGE, LH_META_PAGE, LH_OVERFLOW_PAGE, LH_UNUSED_PAGE,
    ALL_SET,
};
use rel::Relation;
use types_storage::storage::{Buffer, BufferIsValid, InvalidBuffer};

use transam_xlog_seams as xlog;
use xloginsert_seams as xloginsert;
use bufmgr_seams as bufmgr;

use page::{
    ItemIdIsDead, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetFreeSpaceForMultipleTuples, PageIndexMultiDelete, PageMut,
};

use crate::hashinsert::_hash_pgaddmultitup;
use crate::hashpage::{
    block_number_is_valid, with_metap, with_page_ref, _hash_getbuf,
    _hash_getbuf_with_strategy, _hash_getinitbuf, _hash_getnewbuf, _hash_relbuf,
    REGBUF_NO_CHANGE, REGBUF_NO_IMAGE, REGBUF_STANDARD, XLOG_HASH_ADD_OVFL_PAGE,
    XLOG_HASH_MOVE_PAGE_CONTENTS, XLOG_HASH_SQUEEZE_PAGE,
};
use crate::hashutil::{
    BUFFER_LOCK_EXCLUSIVE, BUFFER_LOCK_UNLOCK, _hash_checkpage, _hash_get_totalbuckets,
};
use crate::pagebytes::{
    _hash_initbitmapbuffer_bytes, _hash_pageinit_bytes, bitmap_clrbit, bitmap_isset, bitmap_setbit,
    bitmap_word, bmpg_mask, bmpg_shift, bmpgsz_bit, hasho_bucket, hasho_nextblkno, hasho_prevblkno,
    index_tuple_size, maxalign, set_hasho_bucket, set_hasho_flag, set_hasho_nextblkno,
    set_hasho_page_id, set_hasho_prevblkno, set_meta_firstfree, set_meta_nmaps, set_page_meta_mapp,
    set_page_meta_spare, BITS_PER_MAP_U32,
};
use crate::wal::RM_HASH_ID;

/// `HASH_XLOG_FREE_OVFL_BUFS` (hash_xlog.h): the max # of bufs freeovflpage logs.
const HASH_XLOG_FREE_OVFL_BUFS: i32 = 6;

// ===========================================================================
// bitno_to_blkno (hashovfl.c:34)
// ===========================================================================

/// `bitno_to_blkno(metap, ovflbitnum)` — convert overflow page bit number to
/// block number within the index.
fn bitno_to_blkno(metap: &HashMetaPageData, ovflbitnum: u32) -> BlockNumber {
    let splitnum = metap.hashm_ovflpoint;
    // Convert zero-based bitnumber to 1-based page number.
    let ovflbitnum = ovflbitnum + 1;

    // Determine the split number for this page (must be >= 1).
    let mut i: u32 = 1;
    while i < splitnum && ovflbitnum > metap.hashm_spares[i as usize] {
        i += 1;
    }

    // Convert to absolute page number.
    _hash_get_totalbuckets(i) + ovflbitnum
}

// ===========================================================================
// _hash_ovflblkno_to_bitno (hashovfl.c:61)
// ===========================================================================

/// `_hash_ovflblkno_to_bitno(metap, ovflblkno)` — convert overflow page block
/// number to bit number for the free-page bitmap.
pub fn _hash_ovflblkno_to_bitno(metap: &HashMetaPageData, ovflblkno: BlockNumber) -> PgResult<u32> {
    let splitnum = metap.hashm_ovflpoint;

    let mut i: u32 = 1;
    while i <= splitnum {
        if ovflblkno <= _hash_get_totalbuckets(i) {
            break; // oops
        }
        let bitnum = ovflblkno - _hash_get_totalbuckets(i);

        if bitnum > metap.hashm_spares[(i - 1) as usize] && bitnum <= metap.hashm_spares[i as usize]
        {
            return Ok(bitnum - 1); // -1: 1-based -> 0-based
        }
        i += 1;
    }

    Err(PgError::new(
        ERROR,
        &alloc::format!("invalid overflow block number {ovflblkno}"),
    ))
}

// ===========================================================================
// _hash_addovflpage (hashovfl.c:111)
// ===========================================================================

/// `_hash_addovflpage(rel, metabuf, buf, retain_pin)` — add an overflow page to
/// the bucket whose last page is `buf`. Returns the new (empty, write-locked,
/// pinned) overflow buffer.
pub fn _hash_addovflpage<'mcx>(
    rel: &Relation<'mcx>,
    metabuf: Buffer,
    mut buf: Buffer,
    mut retain_pin: bool,
) -> PgResult<Buffer> {
    let mut mapbuf = InvalidBuffer;
    let mut newmapbuf = InvalidBuffer;
    let ovflbuf;

    // Write-lock the tail page.
    bufmgr::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
    _hash_checkpage(rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32)?;

    // loop to find current tail page
    loop {
        let nextblkno = with_page_ref(buf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;
        if !block_number_is_valid(nextblkno) {
            break;
        }
        if retain_pin {
            bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
        } else {
            _hash_relbuf(rel, buf);
        }
        retain_pin = false;
        buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE as i32)?;
    }

    // Get exclusive lock on the meta page.
    bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;
    _hash_checkpage(rel, metabuf, LH_META_PAGE as i32)?;
    let metap = with_metap(metabuf, |m| m.clone())?;

    // start search at hashm_firstfree
    let orig_firstfree = metap.hashm_firstfree;
    let first_page = orig_firstfree >> bmpg_shift(&metap);
    let mut bit = orig_firstfree & bmpg_mask(&metap);
    let mut i = first_page;
    let mut j = bit / BITS_PER_MAP_U32;
    bit &= !(BITS_PER_MAP_U32 - 1);

    let mut page_found = false;
    let mut bitmap_page_bit: u32 = 0;
    let mut blkno: BlockNumber = InvalidBlockNumber;
    let mut splitnum = metap.hashm_ovflpoint;
    let mut last_bit: u32 = 0;
    let mut found = false;

    // outer loop iterates once per bitmap page
    loop {
        splitnum = with_metap(metabuf, |m| m.hashm_ovflpoint)?;
        let (max_ovflpg, bmshift, bmsize, nmaps) = with_metap(metabuf, |m| {
            (m.hashm_spares[splitnum as usize] - 1, bmpg_shift(m), bmpgsz_bit(m), m.hashm_nmaps)
        })?;
        let last_page = max_ovflpg >> bmshift;
        last_bit = max_ovflpg & ((1u32 << bmshift) - 1);

        if i > last_page {
            break;
        }

        debug_assert!(i < nmaps);
        let mapblkno = with_metap(metabuf, |m| m.hashm_mapp[i as usize])?;

        let last_inpage = if i == last_page { last_bit } else { bmsize - 1 };

        // Release exclusive lock on metapage while reading bitmap page.
        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;

        mapbuf = _hash_getbuf(rel, mapblkno, HASH_WRITE, LH_BITMAP_PAGE as i32)?;

        // scan the words of this bitmap page
        let mut scanned_bit = bit;
        let mut local_found = false;
        let mut jj = j;
        while scanned_bit <= last_inpage {
            let word = with_page_ref(mapbuf, |p| Ok(bitmap_word(p.as_bytes(), jj as usize)))?;
            if word != ALL_SET {
                page_found = true;
                local_found = true;

                // Reacquire exclusive lock on meta page.
                bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;

                // convert bit to bit number within page
                scanned_bit += _hash_firstfreebit(word)?;
                bitmap_page_bit = scanned_bit;

                // convert bit to absolute bit number
                let bmshift2 = with_metap(metabuf, |m| bmpg_shift(m))?;
                scanned_bit += i << bmshift2;
                blkno = with_metap(metabuf, |m| bitno_to_blkno(m, scanned_bit))?;

                bit = scanned_bit;
                break;
            }
            jj += 1;
            scanned_bit += BITS_PER_MAP_U32;
        }

        if local_found {
            found = true;
            // Fetch and init the recycled page.
            ovflbuf = _hash_getinitbuf(rel, blkno)?;
            return _hash_addovflpage_finish(
                rel, metabuf, buf, retain_pin, ovflbuf, mapbuf, newmapbuf, page_found,
                bitmap_page_bit, orig_firstfree, bit,
            );
        }

        // No free space here, advance to next map page.
        _hash_relbuf(rel, mapbuf);
        mapbuf = InvalidBuffer;
        i += 1;
        j = 0;
        bit = 0;

        bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;
    }
    let _ = (first_page, found, j);

    // No free pages --- extend the relation. Add a new bitmap page too?
    let bmsize = with_metap(metabuf, |m| m.hashm_bmsize)?;
    let bmpgsz = with_metap(metabuf, |m| bmpgsz_bit(m))?;
    if last_bit == bmpgsz - 1 {
        bit = with_metap(metabuf, |m| m.hashm_spares[splitnum as usize])?;

        let nmaps = with_metap(metabuf, |m| m.hashm_nmaps)?;
        if nmaps as usize >= HASH_MAX_BITMAPS {
            return Err(PgError::new(ERROR, "out of overflow pages in hash index"));
        }

        let newmapblk = with_metap(metabuf, |m| bitno_to_blkno(m, bit))?;
        newmapbuf = _hash_getnewbuf(rel, newmapblk, types_core::primitive::ForkNumber::MAIN_FORKNUM)?;
    }

    // Calculate address of the new overflow page.
    bit = if BufferIsValid(newmapbuf) {
        with_metap(metabuf, |m| m.hashm_spares[splitnum as usize])? + 1
    } else {
        with_metap(metabuf, |m| m.hashm_spares[splitnum as usize])?
    };
    blkno = with_metap(metabuf, |m| bitno_to_blkno(m, bit))?;

    ovflbuf = _hash_getnewbuf(rel, blkno, types_core::primitive::ForkNumber::MAIN_FORKNUM)?;

    let _ = bmsize;
    _hash_addovflpage_finish(
        rel, metabuf, buf, retain_pin, ovflbuf, mapbuf, newmapbuf, page_found, bitmap_page_bit,
        orig_firstfree, bit,
    )
}

/// The `found:` tail of `_hash_addovflpage` — performs the crit-section update,
/// the WAL record, and the buffer releases. Shared between the recycle and the
/// extend paths.
#[allow(clippy::too_many_arguments)]
fn _hash_addovflpage_finish<'mcx>(
    rel: &Relation<'mcx>,
    metabuf: Buffer,
    buf: Buffer,
    retain_pin: bool,
    ovflbuf: Buffer,
    mapbuf: Buffer,
    newmapbuf: Buffer,
    page_found: bool,
    bitmap_page_bit: u32,
    orig_firstfree: u32,
    bit: u32,
) -> PgResult<Buffer> {
    // START_CRIT_SECTION

    if page_found {
        debug_assert!(BufferIsValid(mapbuf));
        // mark page "in use" in the bitmap
        bufmgr::with_buffer_page::call(mapbuf, &mut |page: &mut [u8]| {
            bitmap_setbit(page, bitmap_page_bit);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(mapbuf);
    } else {
        // update the count to indicate new overflow page is added
        let splitnum = with_metap(metabuf, |m| m.hashm_ovflpoint)?;
        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            let v = crate::pagebytes::page_meta_spare(page, splitnum as usize) + 1;
            set_page_meta_spare(page, splitnum as usize, v);
            Ok(())
        })?;

        if BufferIsValid(newmapbuf) {
            let bmsize = with_metap(metabuf, |m| m.hashm_bmsize)?;
            bufmgr::with_buffer_page::call(newmapbuf, &mut |page: &mut [u8]| {
                _hash_initbitmapbuffer_bytes(page, bmsize, false)
            })?;
            bufmgr::mark_buffer_dirty::call(newmapbuf);

            let newmapblk = bufmgr::buffer_get_block_number::call(newmapbuf);
            bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
                let nmaps = crate::pagebytes::meta_nmaps(page);
                set_page_meta_mapp(page, nmaps as usize, newmapblk);
                set_meta_nmaps(page, nmaps + 1);
                let v = crate::pagebytes::page_meta_spare(page, splitnum as usize) + 1;
                set_page_meta_spare(page, splitnum as usize, v);
                Ok(())
            })?;
        }

        bufmgr::mark_buffer_dirty::call(metabuf);
    }

    // Adjust hashm_firstfree to avoid redundant searches.
    let cur_firstfree = with_metap(metabuf, |m| m.hashm_firstfree)?;
    if cur_firstfree == orig_firstfree {
        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            set_meta_firstfree(page, bit + 1);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(metabuf);
    }

    // initialize new overflow page
    let prevblk = bufmgr::buffer_get_block_number::call(buf);
    let bucket = with_page_ref(buf, |p| Ok(hasho_bucket(p.as_bytes())))?;
    bufmgr::with_buffer_page::call(ovflbuf, &mut |page: &mut [u8]| {
        set_hasho_prevblkno(page, prevblk);
        set_hasho_nextblkno(page, InvalidBlockNumber);
        set_hasho_bucket(page, bucket);
        set_hasho_flag(page, LH_OVERFLOW_PAGE);
        set_hasho_page_id(page, hash::hashpage::HASHO_PAGE_ID);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(ovflbuf);

    // logically chain overflow page to previous page
    let ovflblk = bufmgr::buffer_get_block_number::call(ovflbuf);
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        set_hasho_nextblkno(page, ovflblk);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(buf);

    // XLOG stuff
    if relation_needs_wal(rel)? {
        let bmsize = with_metap(metabuf, |m| m.hashm_bmsize)?;
        let firstfree = with_metap(metabuf, |m| m.hashm_firstfree)?;

        xloginsert::xlog_begin_insert::call()?;
        // xl_hash_add_ovfl_page { uint16 bmsize; bool bmpage_found; }
        let mut xlrec = [0u8; 4];
        xlrec[0..2].copy_from_slice(&bmsize.to_ne_bytes());
        xlrec[2] = page_found as u8;
        xloginsert::xlog_register_data::call(&xlrec)?;

        xloginsert::xlog_register_buffer::call(0, ovflbuf, crate::hashpage::REGBUF_WILL_INIT)?;
        xloginsert::xlog_register_buf_data::call(0, &bucket.to_ne_bytes())?;

        xloginsert::xlog_register_buffer::call(1, buf, REGBUF_STANDARD)?;

        if BufferIsValid(mapbuf) {
            xloginsert::xlog_register_buffer::call(2, mapbuf, REGBUF_STANDARD)?;
            xloginsert::xlog_register_buf_data::call(2, &bitmap_page_bit.to_ne_bytes())?;
        }

        if BufferIsValid(newmapbuf) {
            xloginsert::xlog_register_buffer::call(3, newmapbuf, crate::hashpage::REGBUF_WILL_INIT)?;
        }

        xloginsert::xlog_register_buffer::call(4, metabuf, REGBUF_STANDARD)?;
        xloginsert::xlog_register_buf_data::call(4, &firstfree.to_ne_bytes())?;

        let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_ADD_OVFL_PAGE)?;

        bufmgr::page_set_lsn::call(ovflbuf, recptr)?;
        bufmgr::page_set_lsn::call(buf, recptr)?;
        if BufferIsValid(mapbuf) {
            bufmgr::page_set_lsn::call(mapbuf, recptr)?;
        }
        if BufferIsValid(newmapbuf) {
            bufmgr::page_set_lsn::call(newmapbuf, recptr)?;
        }
        bufmgr::page_set_lsn::call(metabuf, recptr)?;
    }
    // END_CRIT_SECTION

    if retain_pin {
        bufmgr::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
    } else {
        _hash_relbuf(rel, buf);
    }

    if BufferIsValid(mapbuf) {
        _hash_relbuf(rel, mapbuf);
    }

    bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;

    if BufferIsValid(newmapbuf) {
        _hash_relbuf(rel, newmapbuf);
    }

    Ok(ovflbuf)
}

// ===========================================================================
// _hash_firstfreebit (hashovfl.c:447)
// ===========================================================================

/// `_hash_firstfreebit(map)` — number of the first bit not set in `map`.
fn _hash_firstfreebit(map: u32) -> PgResult<u32> {
    let mut mask: u32 = 0x1;
    for i in 0..BITS_PER_MAP_U32 {
        if mask & map == 0 {
            return Ok(i);
        }
        mask <<= 1;
    }
    Err(PgError::new(ERROR, "firstfreebit found no free bit"))
}

// ===========================================================================
// _hash_freeovflpage (hashovfl.c:489)
// ===========================================================================

/// `_hash_freeovflpage(rel, bucketbuf, ovflbuf, wbuf, itups, ..., bstrategy)` —
/// remove the overflow page from its bucket chain, mark it free, and add the
/// accumulated `itups` to `wbuf`. Returns the following page's block number (or
/// `InvalidBlockNumber`).
#[allow(clippy::too_many_arguments)]
pub fn _hash_freeovflpage<'mcx>(
    rel: &Relation<'mcx>,
    bucketbuf: Buffer,
    ovflbuf: Buffer,
    wbuf: Buffer,
    itups: &[Vec<u8>],
    tups_size: &[usize],
    nitups: u16,
    bstrategy: &types_storage::buf::BufferAccessStrategy,
) -> PgResult<BlockNumber> {
    // Get information from the doomed page.
    _hash_checkpage(rel, ovflbuf, LH_OVERFLOW_PAGE as i32)?;
    let ovflblkno = bufmgr::buffer_get_block_number::call(ovflbuf);
    let (nextblkno, prevblkno) =
        with_page_ref(ovflbuf, |p| Ok((hasho_nextblkno(p.as_bytes()), hasho_prevblkno(p.as_bytes()))))?;
    let writeblkno = bufmgr::buffer_get_block_number::call(wbuf);

    // Fix up the bucket chain (doubly-linked).
    let mut prevbuf = InvalidBuffer;
    if block_number_is_valid(prevblkno) {
        prevbuf = if prevblkno == writeblkno {
            wbuf
        } else {
            _hash_getbuf_with_strategy(
                rel,
                prevblkno,
                HASH_WRITE,
                (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as i32,
                bstrategy,
            )?
        };
    }
    let mut nextbuf = InvalidBuffer;
    if block_number_is_valid(nextblkno) {
        nextbuf =
            _hash_getbuf_with_strategy(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE as i32, bstrategy)?;
    }

    // Read the metapage to find which bitmap page to use.
    let metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE as i32)?;
    let metap = with_metap(metabuf, |m| m.clone())?;

    // Identify which bit to set.
    let ovflbitno = _hash_ovflblkno_to_bitno(&metap, ovflblkno)?;

    let bitmappage = ovflbitno >> bmpg_shift(&metap);
    let bitmapbit = ovflbitno & bmpg_mask(&metap);

    if bitmappage >= metap.hashm_nmaps {
        return Err(PgError::new(ERROR, "invalid overflow bit number"));
    }
    let mapblk = metap.hashm_mapp[bitmappage as usize];

    // Release metapage lock while we access the bitmap page.
    bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_UNLOCK)?;

    let mapbuf = _hash_getbuf(rel, mapblk, HASH_WRITE, LH_BITMAP_PAGE as i32)?;
    debug_assert!(with_page_ref(mapbuf, |p| Ok(bitmap_isset(p.as_bytes(), bitmapbit)))?);

    // Get write-lock on metapage to update firstfree.
    bufmgr::lock_buffer::call(metabuf, BUFFER_LOCK_EXCLUSIVE)?;

    if relation_needs_wal(rel)? {
        xlog::xlog_ensure_record_space::call(HASH_XLOG_FREE_OVFL_BUFS, 4 + nitups as i32)?;
    }

    // START_CRIT_SECTION

    // Insert tuples on the "write" page (capturing the placement offsets for
    // the WAL record's `itup_offsets` array).
    let mut itup_offsets: Vec<u16> = Vec::new();
    if nitups > 0 {
        itup_offsets = _hash_pgaddmultitup(rel, wbuf, itups)?;
        bufmgr::mark_buffer_dirty::call(wbuf);
    }

    // Reinitialize the freed overflow page.
    bufmgr::with_buffer_page::call(ovflbuf, &mut |page: &mut [u8]| {
        _hash_pageinit_bytes(page, page.len())?;
        set_hasho_prevblkno(page, InvalidBlockNumber);
        set_hasho_nextblkno(page, InvalidBlockNumber);
        set_hasho_bucket(page, InvalidBucket);
        set_hasho_flag(page, LH_UNUSED_PAGE);
        set_hasho_page_id(page, hash::hashpage::HASHO_PAGE_ID);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(ovflbuf);

    if BufferIsValid(prevbuf) {
        bufmgr::with_buffer_page::call(prevbuf, &mut |page: &mut [u8]| {
            set_hasho_nextblkno(page, nextblkno);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(prevbuf);
    }
    if BufferIsValid(nextbuf) {
        bufmgr::with_buffer_page::call(nextbuf, &mut |page: &mut [u8]| {
            set_hasho_prevblkno(page, prevblkno);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(nextbuf);
    }

    // Clear the bitmap bit to indicate this overflow page is free.
    bufmgr::with_buffer_page::call(mapbuf, &mut |page: &mut [u8]| {
        bitmap_clrbit(page, bitmapbit);
        Ok(())
    })?;
    bufmgr::mark_buffer_dirty::call(mapbuf);

    // if this is now the first free page, update hashm_firstfree.
    let mut update_metap = false;
    if ovflbitno < metap.hashm_firstfree {
        bufmgr::with_buffer_page::call(metabuf, &mut |page: &mut [u8]| {
            set_meta_firstfree(page, ovflbitno);
            Ok(())
        })?;
        update_metap = true;
        bufmgr::mark_buffer_dirty::call(metabuf);
    }

    // XLOG stuff
    if relation_needs_wal(rel)? {
        let is_prim_bucket_same_wrt = wbuf == bucketbuf;
        let is_prev_bucket_same_wrt = wbuf == prevbuf;
        let mut mod_wbuf = false;

        xloginsert::xlog_begin_insert::call()?;
        // xl_hash_squeeze_page { BlockNumber prevblkno; BlockNumber nextblkno;
        //   uint16 ntups; bool is_prim_bucket_same_wrt; bool is_prev_bucket_same_wrt; }
        let mut xlrec = [0u8; 12]; // SizeOfHashSqueezePage
        xlrec[0..4].copy_from_slice(&prevblkno.to_ne_bytes());
        xlrec[4..8].copy_from_slice(&nextblkno.to_ne_bytes());
        xlrec[8..10].copy_from_slice(&nitups.to_ne_bytes());
        xlrec[10] = is_prim_bucket_same_wrt as u8;
        xlrec[11] = is_prev_bucket_same_wrt as u8;
        xloginsert::xlog_register_data::call(&xlrec)?;

        if !is_prim_bucket_same_wrt {
            let flags = REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE;
            xloginsert::xlog_register_buffer::call(0, bucketbuf, flags)?;
        }

        if nitups > 0 {
            xloginsert::xlog_register_buffer::call(1, wbuf, REGBUF_STANDARD)?;
            mod_wbuf = true;
            // itup_offsets array (the placement offsets captured above).
            let mut offs = Vec::with_capacity(nitups as usize * 2);
            for o in &itup_offsets {
                offs.extend_from_slice(&o.to_ne_bytes());
            }
            xloginsert::xlog_register_buf_data::call(1, &offs)?;
            for i in 0..nitups as usize {
                xloginsert::xlog_register_buf_data::call(1, &itups[i][..tups_size[i]])?;
            }
        } else if is_prim_bucket_same_wrt || is_prev_bucket_same_wrt {
            let mut wbuf_flags = REGBUF_STANDARD;
            if !is_prev_bucket_same_wrt {
                wbuf_flags |= REGBUF_NO_CHANGE;
            } else {
                mod_wbuf = true;
            }
            xloginsert::xlog_register_buffer::call(1, wbuf, wbuf_flags)?;
        }

        xloginsert::xlog_register_buffer::call(2, ovflbuf, REGBUF_STANDARD)?;

        if BufferIsValid(prevbuf) && !is_prev_bucket_same_wrt {
            xloginsert::xlog_register_buffer::call(3, prevbuf, REGBUF_STANDARD)?;
        }
        if BufferIsValid(nextbuf) {
            xloginsert::xlog_register_buffer::call(4, nextbuf, REGBUF_STANDARD)?;
        }

        xloginsert::xlog_register_buffer::call(5, mapbuf, REGBUF_STANDARD)?;
        xloginsert::xlog_register_buf_data::call(5, &bitmapbit.to_ne_bytes())?;

        if update_metap {
            let firstfree = with_metap(metabuf, |m| m.hashm_firstfree)?;
            xloginsert::xlog_register_buffer::call(6, metabuf, REGBUF_STANDARD)?;
            xloginsert::xlog_register_buf_data::call(6, &firstfree.to_ne_bytes())?;
        }

        let recptr = xloginsert::xlog_insert_record::call(RM_HASH_ID, XLOG_HASH_SQUEEZE_PAGE)?;

        if mod_wbuf {
            bufmgr::page_set_lsn::call(wbuf, recptr)?;
        }
        bufmgr::page_set_lsn::call(ovflbuf, recptr)?;
        if BufferIsValid(prevbuf) && !is_prev_bucket_same_wrt {
            bufmgr::page_set_lsn::call(prevbuf, recptr)?;
        }
        if BufferIsValid(nextbuf) {
            bufmgr::page_set_lsn::call(nextbuf, recptr)?;
        }
        bufmgr::page_set_lsn::call(mapbuf, recptr)?;
        if update_metap {
            bufmgr::page_set_lsn::call(metabuf, recptr)?;
        }
    }
    // END_CRIT_SECTION

    // release previous bucket if it is not same as write bucket
    if BufferIsValid(prevbuf) && prevblkno != writeblkno {
        _hash_relbuf(rel, prevbuf);
    }
    if BufferIsValid(ovflbuf) {
        _hash_relbuf(rel, ovflbuf);
    }
    if BufferIsValid(nextbuf) {
        _hash_relbuf(rel, nextbuf);
    }
    _hash_relbuf(rel, mapbuf);
    _hash_relbuf(rel, metabuf);

    Ok(nextblkno)
}

// ===========================================================================
// _hash_initbitmapbuffer (hashovfl.c:777) — page-byte body in pagebytes.rs.
// ===========================================================================

/// `_hash_initbitmapbuffer(buf, bmsize, initpage)`.
pub fn _hash_initbitmapbuffer(buf: Buffer, bmsize: u16, initpage: bool) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        _hash_initbitmapbuffer_bytes(page, bmsize, initpage)
    })
}

// ===========================================================================
// _hash_squeezebucket (hashovfl.c:841)
// ===========================================================================

/// `_hash_squeezebucket(rel, bucket, bucket_blkno, bucket_buf, bstrategy)` —
/// squeeze tuples onto earlier pages in the bucket chain to free overflow pages.
pub fn _hash_squeezebucket<'mcx>(
    rel: &Relation<'mcx>,
    bucket: Bucket,
    bucket_blkno: BlockNumber,
    bucket_buf: Buffer,
    bstrategy: &types_storage::buf::BufferAccessStrategy,
) -> PgResult<()> {
    // start squeezing into the primary bucket page.
    let mut wblkno = bucket_blkno;
    let mut wbuf = bucket_buf;

    let wnext = with_page_ref(wbuf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;
    // if there aren't any overflow pages, there's nothing to squeeze.
    if !block_number_is_valid(wnext) {
        bufmgr::lock_buffer::call(wbuf, BUFFER_LOCK_UNLOCK)?;
        return Ok(());
    }

    // Find the last page in the bucket chain.
    let mut rbuf = InvalidBuffer;
    let mut rblkno;
    let mut rnext = wnext;
    loop {
        rblkno = rnext;
        if rbuf != InvalidBuffer {
            _hash_relbuf(rel, rbuf);
        }
        rbuf = _hash_getbuf_with_strategy(rel, rblkno, HASH_WRITE, LH_OVERFLOW_PAGE as i32, bstrategy)?;
        rnext = with_page_ref(rbuf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;
        debug_assert!(with_page_ref(rbuf, |p| Ok(hasho_bucket(p.as_bytes())))? == bucket);
        if !block_number_is_valid(rnext) {
            break;
        }
    }

    // squeeze the tuples
    'outer: loop {
        let mut deletable: Vec<u16> = Vec::new();
        let mut itups: Vec<Vec<u8>> = Vec::new();
        let mut tups_size: Vec<usize> = Vec::new();
        let mut all_tups_size: usize = 0;
        let mut retain_pin = false;

        // readpage:
        'readpage: loop {
            let maxroffnum = with_page_ref(rbuf, |p| Ok(PageGetMaxOffsetNumber(p)))?;
            let mut roffnum: u16 = 1; // FirstOffsetNumber
            while roffnum <= maxroffnum {
                // skip dead tuples
                let is_dead = with_page_ref(rbuf, |p| {
                    let iid = PageGetItemId(p, roffnum)?;
                    Ok(ItemIdIsDead(&iid))
                })?;
                if is_dead {
                    roffnum += 1;
                    continue;
                }

                let itup = with_page_ref(rbuf, |p| {
                    let iid = PageGetItemId(p, roffnum)?;
                    Ok(PageGetItem(p, &iid)?.to_vec())
                })?;
                let itemsz = maxalign(index_tuple_size(&itup));

                // Walk up the chain looking for a page big enough.
                loop {
                    let free = with_page_ref(wbuf, |p| {
                        Ok(PageGetFreeSpaceForMultipleTuples(p, (itups.len() + 1) as i32))
                    })?;
                    if free >= all_tups_size + itemsz {
                        break;
                    }

                    let mut next_wbuf = InvalidBuffer;
                    let mut tups_moved = false;

                    if wblkno == bucket_blkno {
                        retain_pin = true;
                    }

                    wblkno = with_page_ref(wbuf, |p| Ok(hasho_nextblkno(p.as_bytes())))?;
                    debug_assert!(block_number_is_valid(wblkno));

                    if wblkno != rblkno {
                        next_wbuf = _hash_getbuf_with_strategy(
                            rel, wblkno, HASH_WRITE, LH_OVERFLOW_PAGE as i32, bstrategy,
                        )?;
                    }

                    if !itups.is_empty() {
                        debug_assert!(itups.len() == deletable.len());

                        if relation_needs_wal(rel)? {
                            xlog::xlog_ensure_record_space::call(0, 3 + itups.len() as i32)?;
                        }

                        // START_CRIT_SECTION
                        let placed = _hash_pgaddmultitup(rel, wbuf, &itups)?;
                        bufmgr::mark_buffer_dirty::call(wbuf);

                        // Delete tuples we moved off read page.
                        bufmgr::with_buffer_page::call(rbuf, &mut |page: &mut [u8]| {
                            let mut pmut = PageMut::new(page)?;
                            PageIndexMultiDelete(&mut pmut, &deletable)?;
                            Ok(())
                        })?;
                        bufmgr::mark_buffer_dirty::call(rbuf);

                        // XLOG stuff
                        if relation_needs_wal(rel)? {
                            let is_prim_bucket_same_wrt = wbuf == bucket_buf;

                            xloginsert::xlog_begin_insert::call()?;
                            // xl_hash_move_page_contents { uint16 ntups;
                            //   bool is_prim_bucket_same_wrt; }
                            let mut xlrec = [0u8; 4]; // SizeOfHashMovePageContents
                            xlrec[0..2].copy_from_slice(&(itups.len() as u16).to_ne_bytes());
                            xlrec[2] = is_prim_bucket_same_wrt as u8;
                            xloginsert::xlog_register_data::call(&xlrec)?;

                            if !is_prim_bucket_same_wrt {
                                let flags = REGBUF_STANDARD | REGBUF_NO_IMAGE | REGBUF_NO_CHANGE;
                                xloginsert::xlog_register_buffer::call(0, bucket_buf, flags)?;
                            }

                            xloginsert::xlog_register_buffer::call(1, wbuf, REGBUF_STANDARD)?;
                            let mut offs = Vec::with_capacity(placed.len() * 2);
                            for o in &placed {
                                offs.extend_from_slice(&o.to_ne_bytes());
                            }
                            xloginsert::xlog_register_buf_data::call(1, &offs)?;
                            for i in 0..itups.len() {
                                xloginsert::xlog_register_buf_data::call(1, &itups[i][..tups_size[i]])?;
                            }

                            xloginsert::xlog_register_buffer::call(2, rbuf, REGBUF_STANDARD)?;
                            let mut dels = Vec::with_capacity(deletable.len() * 2);
                            for d in &deletable {
                                dels.extend_from_slice(&d.to_ne_bytes());
                            }
                            xloginsert::xlog_register_buf_data::call(2, &dels)?;

                            let recptr = xloginsert::xlog_insert_record::call(
                                RM_HASH_ID,
                                XLOG_HASH_MOVE_PAGE_CONTENTS,
                            )?;
                            bufmgr::page_set_lsn::call(wbuf, recptr)?;
                            bufmgr::page_set_lsn::call(rbuf, recptr)?;
                        }
                        // END_CRIT_SECTION
                        tups_moved = true;
                    }

                    // release lock on previous page after acquiring next
                    if retain_pin {
                        bufmgr::lock_buffer::call(wbuf, BUFFER_LOCK_UNLOCK)?;
                    } else {
                        _hash_relbuf(rel, wbuf);
                    }

                    // nothing more to do if we reached the read page
                    if rblkno == wblkno {
                        _hash_relbuf(rel, rbuf);
                        return Ok(());
                    }

                    wbuf = next_wbuf;
                    debug_assert!(with_page_ref(wbuf, |p| Ok(hasho_bucket(p.as_bytes())))? == bucket);
                    retain_pin = false;

                    itups.clear();
                    tups_size.clear();
                    all_tups_size = 0;
                    deletable.clear();

                    if tups_moved {
                        continue 'readpage;
                    }
                }

                // remember tuple for deletion from "read" page
                deletable.push(roffnum);

                // copy of index tuple (freed as part of overflow page)
                itups.push(itup);
                tups_size.push(itemsz);
                all_tups_size += itemsz;

                roffnum += 1;
            }
            break 'readpage;
        }

        // No live tuples on the read page — free it and advance backward.
        let rprev = with_page_ref(rbuf, |p| Ok(hasho_prevblkno(p.as_bytes())))?;
        debug_assert!(block_number_is_valid(rprev));

        // free this overflow page (releases rbuf)
        _hash_freeovflpage(
            rel, bucket_buf, rbuf, wbuf, &itups, &tups_size, itups.len() as u16, bstrategy,
        )?;

        // are we freeing the page adjacent to wbuf?
        if rprev == wblkno {
            if wblkno == bucket_blkno {
                bufmgr::lock_buffer::call(wbuf, BUFFER_LOCK_UNLOCK)?;
            } else {
                _hash_relbuf(rel, wbuf);
            }
            return Ok(());
        }

        rblkno = rprev;
        rbuf = _hash_getbuf_with_strategy(rel, rblkno, HASH_WRITE, LH_OVERFLOW_PAGE as i32, bstrategy)?;
        debug_assert!(with_page_ref(rbuf, |p| Ok(hasho_bucket(p.as_bytes())))? == bucket);
        let _ = &mut wbuf;
        continue 'outer;
    }
}

// ===========================================================================
// helpers
// ===========================================================================

fn relation_needs_wal<'mcx>(rel: &Relation<'mcx>) -> PgResult<bool> {
    Ok(relcache_seams::relation_needs_wal::call(rel))
}

