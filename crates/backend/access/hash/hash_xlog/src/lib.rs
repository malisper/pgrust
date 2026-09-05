//! hash_xlog.c — hash rmgr redo + hash_mask. Every arm the write side emits
//! is live (INIT_META/INIT_BITMAP/INSERT/ADD_OVFL/SPLIT_*/MOVE/SQUEEZE/
//! DELETE/SPLIT_CLEANUP/VACUUM_ONE_PAGE); UPDATE_META_PAGE is written only by
//! the unported VACUUM lane but replays anyway (a C-written WAL stream may
//! carry it).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{BlockNumber, Buffer, InvalidBlockNumber, InvalidBuffer, OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult, ERRCODE_DATA_CORRUPTED};
use types_hash::*;
use types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use types_storage::ReadBufferMode;
use xlogreader_seams::XLogReaderState;
use xlogutils::{
    XLogFlushBufferForRedoIfInit, XLogInitBufferForRedo, XLogReadBufferForRedo,
    XLogReadBufferForRedoExtended, BLK_NEEDS_REDO, BLK_RESTORED,
};

const XLR_INFO_MASK: u8 = 0x0F;
const SIZEOF_OPAQUE: usize = core::mem::size_of::<HashPageOpaqueData>();

// HashGetMaxBitmapSize: bytes available for the bitmap bit-array on a page,
// i.e. the whole page minus the aligned header and special area. Every bitmap
// page's bit array is confined to this region, so it bounds both the redo
// bitmap memset (init_bitmapbuffer) and the SETBIT/CLRBIT word writes.
const MAX_BITMAP_SIZE: usize =
    BLCKSZ - (maxalign(SizeOfPageHeaderData) + maxalign(SIZEOF_OPAQUE));

fn main_data<'a>(record: &'a XLogReaderState) -> &'a [u8] {
    let rec = record.record.as_ref().expect("hash redo with no decoded record");
    // SAFETY: points into the reader's decode buffer, valid for the redo
    // callback's duration.
    unsafe { rec.main_data_bytes() }
}

fn block_data<'a>(record: &'a XLogReaderState, block_id: u8) -> &'a [u8] {
    // SAFETY: same decode-buffer lifetime as main_data.
    unsafe { record.block(block_id).data_bytes() }
}

#[track_caller]
#[cold]
fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(types_error::PANIC, msg))
}

/// sizeof(IndexTupleData): 6-byte ItemPointerData t_tid + 2-byte t_info. Every
/// hash index tuple is at least this large, and the size word lives in the low
/// 13 bits of t_info.
const SIZE_OF_INDEX_TUPLE_DATA: usize = 8;

// Minimum main-data length each opcode's fixed struct occupies, derived from the
// byte offsets the decode below reads (mirrors the C SizeOfHash* macros). A
// replayed record whose main data is shorter is corruption, not a slice panic.
const SIZE_OF_HASH_INIT_META_PAGE: usize = 14; // num_tuples f64@0, procid u32@8, ffactor u16@12
const SIZE_OF_HASH_INIT_BITMAP_PAGE: usize = 2; // bmsize u16@0
const SIZE_OF_HASH_INSERT: usize = 2; // offnum u16@0
const SIZE_OF_HASH_ADD_OVFL_PAGE: usize = 3; // bmsize u16@0, bmpage_found u8@2
const SIZE_OF_HASH_SPLIT_ALLOC_PAGE: usize = 9; // new_bucket u32@0, flags u16@4,u16@6, u8@8
const SIZE_OF_HASH_SPLIT_COMPLETE: usize = 4; // old_flag u16@0, new_flag u16@2
const SIZE_OF_HASH_MOVE_PAGE_CONTENTS: usize = 3; // ntups u16@0, is_prim u8@2
const SIZE_OF_HASH_SQUEEZE_PAGE: usize = 12; // prev u32@0, next u32@4, ntups u16@8, u8@10, u8@11
const SIZE_OF_HASH_DELETE: usize = 2; // clear_dead u8@0, is_primary u8@1
const SIZE_OF_HASH_UPDATE_META_PAGE: usize = 8; // ntuples f64@0
const SIZE_OF_HASH_VACUUM_ONE_PAGE: usize = 8; // horizon u32@0, ntuples u16@4, isCatalog u8@6, offsets@8

#[cold]
#[inline(never)]
fn corrupt_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_DATA_CORRUPTED))
}

#[inline]
fn require(cond: bool, msg: impl FnOnce() -> String) -> PgResult<()> {
    if cond {
        Ok(())
    } else {
        Err(corrupt_err(msg()))
    }
}

/// Confirm a redo payload slice holds at least `need` bytes before fixed-offset
/// decoding reads it. C reads its xl_hash_* fields through a struct-pointer cast
/// that trusts the record length; here the equivalent `b[off..off+N]` slices
/// would panic the startup redo thread on a truncated record, so surface
/// ERRCODE_DATA_CORRUPTED instead.
#[inline]
fn require_len(b: &[u8], need: usize, what: &str) -> PgResult<()> {
    require(b.len() >= need, || {
        format!("hash redo: {what} main data too short: {} bytes, need {need}", b.len())
    })
}

/// IndexTupleSize() over a tuple-stream slice, validated against what remains.
///
/// C walks the (offsets, tuples) block data casting each element to an
/// IndexTuple and reading its size word out of the decode buffer; a truncated or
/// size-inflated stream would read (and then slice) past the buffer. Confirm the
/// header is present and the MAXALIGN'd declared size lies within the remaining
/// payload before returning, so the caller can slice `data[off..off+sz]` safely.
fn checked_item_size(data: &[u8], off: usize) -> PgResult<usize> {
    require(off + SIZE_OF_INDEX_TUPLE_DATA <= data.len(), || {
        format!(
            "hash redo: tuple stream too short for item header at offset {off} in {} bytes",
            data.len()
        )
    })?;
    // t_info size bits: low 13 bits of the u16 at offset 6 of the tuple. The
    // raw declared size must itself cover the header (a sub-header size is
    // corruption, even though MAXALIGN would otherwise round it up to 8).
    let raw = (u16::from_ne_bytes([data[off + 6], data[off + 7]]) & 0x1FFF) as usize;
    let sz = maxalign(raw);
    require(raw >= SIZE_OF_INDEX_TUPLE_DATA && off + sz <= data.len(), || {
        format!(
            "hash redo: invalid item size {raw} at offset {off} in {} bytes of tuple stream",
            data.len()
        )
    })?;
    Ok(sz)
}

/// Range-check a splitpoint index (`hashm_ovflpoint`, or a WAL-sourced
/// `ovflpoint`) before it indexes the fixed `hashm_spares[HASH_MAX_SPLITPOINTS]`
/// array. C indexes the array unchecked; here an out-of-range value is a
/// deterministic array-bounds panic in redo, so reject it as corruption.
fn checked_splitpoint(ovflpoint: u32) -> PgResult<usize> {
    let idx = ovflpoint as usize;
    require(idx < HASH_MAX_SPLITPOINTS, || {
        format!("hash redo: ovflpoint {ovflpoint} out of range (max {HASH_MAX_SPLITPOINTS})")
    })?;
    Ok(idx)
}

/// Range-check a bitmap-map slot (`hashm_nmaps`) before it indexes the fixed
/// `hashm_mapp[HASH_MAX_BITMAPS]` array. As with the spares array, C trusts the
/// meta-page value and a hostile full-page image could drive it out of range.
fn checked_nmaps(nmaps: u32) -> PgResult<usize> {
    let idx = nmaps as usize;
    require(idx < HASH_MAX_BITMAPS, || {
        format!("hash redo: nmaps {nmaps} out of range (max {HASH_MAX_BITMAPS})")
    })?;
    Ok(idx)
}

/// Range-check a WAL-supplied bitmap bit index before it drives a raw SETBIT/
/// CLRBIT word write on a bitmap page. C's SETBIT/CLRBIT macros index the bit
/// array (`(A)[N/BITS_PER_MAP]`) unchecked, trusting the WAL producer; a hostile
/// bit up to 2^32-1 would compute a word address far past the 8KB buffer. The
/// word must lie wholly within the page's bitmap region (`MAX_BITMAP_SIZE`
/// bytes starting at the page contents). Returns the in-bounds word index.
fn checked_bitmap_bit(bit: u32) -> PgResult<usize> {
    let word = (bit / BITS_PER_MAP) as usize;
    // 4-byte u32 word at byte offset `word * 4` within the bitmap region.
    require(word * 4 + 4 <= MAX_BITMAP_SIZE, || {
        format!(
            "hash redo: bitmap bit {bit} out of range (max bitmap {MAX_BITMAP_SIZE} bytes)"
        )
    })?;
    Ok(word)
}

/// Range-check a WAL-supplied bitmap size before it drives the redo bitmap
/// memset. C's `_hash_initbitmapbuffer` does `memset(freep, 0xFF, bmsize)` with
/// the WAL-sourced `bmsize` (a u16, up to 65535) unchecked; a value larger than
/// the page's bitmap region would memset past the 8KB buffer into adjacent
/// shared buffers. A legitimate `bmsize` is nonzero and no larger than
/// `MAX_BITMAP_SIZE`. Returns the validated size in bytes.
fn checked_bmsize(bmsize: u16) -> PgResult<usize> {
    let n = bmsize as usize;
    require(n != 0 && n <= MAX_BITMAP_SIZE, || {
        format!("hash redo: bmsize {bmsize} out of range (max {MAX_BITMAP_SIZE} bytes)")
    })?;
    Ok(n)
}

/// Confirm a `u32` read at `off` lies within a block-data slice. C reads these
/// (bit index, firstfree, num_bucket, mask/ovflpoint words) via a pointer cast
/// that trusts the registered block-data length.
#[inline]
fn require_u32(b: &[u8], off: usize, what: &str) -> PgResult<()> {
    require(b.len() >= off + 4, || {
        format!("hash redo: {what} block data too short: {} bytes, need {}", b.len(), off + 4)
    })
}

// SAFETY contract shared by the redo arms: the buffer is pinned and
// exclusively locked (XLogReadBufferForRedo protocol) until the unlock below.
unsafe fn page_mut<'p>(buffer: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

// Read-only twin of page_mut for redo arms that only inspect the page; awaits those arms.
#[allow(dead_code)]
unsafe fn page_ref<'p>(buffer: Buffer) -> PageRef<'p> {
    unsafe { PageRef::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    bufmgr_seams::release_buffer::call(buffer)
}

fn page_opaque(page: &PageRef<'_>) -> HashPageOpaqueData {
    let off = page.pd_special() as usize;
    debug_assert!(off == BLCKSZ - SIZEOF_OPAQUE);
    // SAFETY: in-bounds 4-aligned special area of a hash page.
    unsafe { page.as_ptr().add(off).cast::<HashPageOpaqueData>().read() }
}

fn write_opaque(page: &mut PageMut<'_>, opaque: &HashPageOpaqueData) {
    let off = page.as_ref().pd_special() as usize;
    debug_assert!(off == BLCKSZ - SIZEOF_OPAQUE);
    // SAFETY: in-bounds 4-aligned special area; exclusive page access.
    unsafe {
        page.as_ref().as_ptr().cast_mut().add(off).cast::<HashPageOpaqueData>().write(*opaque)
    }
}

fn hash_pageinit(page: &mut PageMut<'_>) {
    page.init(SIZEOF_OPAQUE);
}

// HashPageGetMeta over a redo-locked buffer.
// SAFETY: caller follows the module's pin+lock contract.
unsafe fn meta_ptr(buffer: Buffer) -> *mut HashMetaPageData {
    unsafe {
        bufmgr_seams::buffer_get_page::call(buffer).as_ptr().add(SizeOfPageHeaderData).cast()
    }
}

const fn maxalign(sz: usize) -> usize {
    (sz + 7) & !7
}

fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes(b[off..off + 4].try_into().unwrap())
}

fn u16_at(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes(b[off..off + 2].try_into().unwrap())
}

// _hash_init_metabuffer's redo twin, writing through the buffer.
fn init_metabuffer(buffer: Buffer, num_tuples: f64, procid: u32, ffactor: u16) {
    // SAFETY: redo pin+lock contract.
    let mut pm = unsafe { page_mut(buffer) };
    hash_pageinit(&mut pm);
    write_opaque(
        &mut pm,
        &HashPageOpaqueData {
            hasho_prevblkno: InvalidBlockNumber,
            hasho_nextblkno: InvalidBlockNumber,
            hasho_bucket: InvalidBucket,
            hasho_flag: LH_META_PAGE,
            hasho_page_id: HASHO_PAGE_ID,
        },
    );

    let dnumbuckets = num_tuples / ffactor as f64;
    let num_buckets = if dnumbuckets <= 2.0 {
        2u32
    } else if dnumbuckets >= 0x40000000u32 as f64 {
        0x40000000
    } else {
        _hash_get_totalbuckets(_hash_spareindex(dnumbuckets as u32))
    };
    let spare_index = _hash_spareindex(num_buckets);

    let bsize = (BLCKSZ - (maxalign(SizeOfPageHeaderData) + maxalign(SIZEOF_OPAQUE))) as u16;
    let lshift = 31 - (bsize as u32).leading_zeros();

    // SAFETY: redo pin+lock contract.
    unsafe {
        let m = meta_ptr(buffer);
        (*m).hashm_magic = HASH_MAGIC;
        (*m).hashm_version = HASH_VERSION;
        (*m).hashm_ntuples = 0.0;
        (*m).hashm_nmaps = 0;
        (*m).hashm_ffactor = ffactor;
        (*m).hashm_bsize = bsize;
        (*m).hashm_bmsize = 1 << lshift;
        (*m).hashm_bmshift = (lshift + BYTE_TO_BIT) as u16;
        (*m).hashm_procid = procid;
        (*m).hashm_maxbucket = num_buckets - 1;
        (*m).hashm_highmask = (num_buckets + 1).next_power_of_two() - 1;
        (*m).hashm_lowmask = (*m).hashm_highmask >> 1;
        (*m).hashm_spares = [0; HASH_MAX_SPLITPOINTS];
        (*m).hashm_mapp = [0; HASH_MAX_BITMAPS];
        (*m).hashm_spares[spare_index as usize] = 1;
        (*m).hashm_ovflpoint = spare_index;
        (*m).hashm_firstfree = 0;
    }
    pm.set_pd_lower((SizeOfPageHeaderData + core::mem::size_of::<HashMetaPageData>()) as u16);
}

fn init_bitmapbuffer(buffer: Buffer, bmsize: u16) -> PgResult<()> {
    // Bound the WAL-supplied bmsize before the memset so a hostile value cannot
    // write 0xFF past the bitmap region into adjacent shared buffers.
    let bmsize_bytes = checked_bmsize(bmsize)?;
    // SAFETY: redo pin+lock contract.
    let mut pm = unsafe { page_mut(buffer) };
    hash_pageinit(&mut pm);
    write_opaque(
        &mut pm,
        &HashPageOpaqueData {
            hasho_prevblkno: InvalidBlockNumber,
            hasho_nextblkno: InvalidBlockNumber,
            hasho_bucket: InvalidBucket,
            hasho_flag: LH_BITMAP_PAGE,
            hasho_page_id: HASHO_PAGE_ID,
        },
    );
    // SAFETY: bitmap region in-page (bmsize_bytes <= MAX_BITMAP_SIZE).
    unsafe {
        core::ptr::write_bytes(
            pm.as_ref().as_ptr().cast_mut().add(SizeOfPageHeaderData),
            0xFF,
            bmsize_bytes,
        );
    }
    pm.set_pd_lower((SizeOfPageHeaderData + bmsize_bytes) as u16);
    Ok(())
}

fn hash_xlog_init_meta_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    require_len(xlrec, SIZE_OF_HASH_INIT_META_PAGE, "INIT_META_PAGE")?;
    let num_tuples = f64::from_ne_bytes(xlrec[0..8].try_into().unwrap());
    let procid = u32_at(xlrec, 8);
    let ffactor = u16_at(xlrec, 12);

    let metabuf = XLogInitBufferForRedo(record, 0)?;
    init_metabuffer(metabuf, num_tuples, procid, ffactor);
    // SAFETY: redo pin+lock contract.
    unsafe { page_mut(metabuf) }.set_lsn(lsn);
    bufmgr_seams::mark_buffer_dirty::call(metabuf)?;

    XLogFlushBufferForRedoIfInit(record, 0, metabuf)?;
    unlock_release(metabuf)
}

fn hash_xlog_init_bitmap_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    require_len(xlrec, SIZE_OF_HASH_INIT_BITMAP_PAGE, "INIT_BITMAP_PAGE")?;
    let bmsize = u16_at(xlrec, 0);

    let bitmapbuf = XLogInitBufferForRedo(record, 0)?;
    init_bitmapbuffer(bitmapbuf, bmsize)?;
    // SAFETY: redo pin+lock contract.
    unsafe { page_mut(bitmapbuf) }.set_lsn(lsn);
    bufmgr_seams::mark_buffer_dirty::call(bitmapbuf)?;
    XLogFlushBufferForRedoIfInit(record, 0, bitmapbuf)?;
    unlock_release(bitmapbuf)?;

    let (action, metabuf) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let nmaps = unsafe { (*meta_ptr(metabuf)).hashm_nmaps };
        let map_slot = checked_nmaps(nmaps)?;
        unsafe {
            let m = meta_ptr(metabuf);
            let num_buckets = (*m).hashm_maxbucket + 1;
            (*m).hashm_mapp[map_slot] = num_buckets + 1;
            (*m).hashm_nmaps += 1;
            page_mut(metabuf).set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
        XLogFlushBufferForRedoIfInit(record, 1, metabuf)?;
    }
    if metabuf != InvalidBuffer {
        unlock_release(metabuf)?;
    }
    Ok(())
}

fn hash_xlog_insert(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    require_len(xlrec, SIZE_OF_HASH_INSERT, "INSERT")?;
    let offnum = u16_at(xlrec, 0);

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let datapos = block_data(record, 0);
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        if pm.add_item(datapos, offnum, 0).is_none() {
            return Err(panic_err("hash_xlog_insert: failed to add item".into()));
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    let (action, metabuf) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        unsafe {
            (*meta_ptr(metabuf)).hashm_ntuples += 1.0;
            page_mut(metabuf).set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
    }
    if metabuf != InvalidBuffer {
        unlock_release(metabuf)?;
    }
    Ok(())
}

fn hash_xlog_add_ovfl_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    require_len(xlrec, SIZE_OF_HASH_ADD_OVFL_PAGE, "ADD_OVFL_PAGE")?;
    let bmsize = u16_at(xlrec, 0);
    let bmpage_found = xlrec[2] != 0;

    let (_, _, rightblk, _) =
        record.block_tag_extended(0).expect("hash_xlog_add_ovfl_page: no block 0");
    let (_, _, leftblk, _) =
        record.block_tag_extended(1).expect("hash_xlog_add_ovfl_page: no block 1");

    let ovflbuf = XLogInitBufferForRedo(record, 0)?;
    let data = block_data(record, 0);
    require_u32(data, 0, "ADD_OVFL_PAGE num_bucket")?;
    let num_bucket = u32_at(data, 0);

    {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(ovflbuf) };
        hash_pageinit(&mut pm);
        write_opaque(
            &mut pm,
            &HashPageOpaqueData {
                hasho_prevblkno: leftblk,
                hasho_nextblkno: InvalidBlockNumber,
                hasho_bucket: num_bucket,
                hasho_flag: LH_OVERFLOW_PAGE,
                hasho_page_id: HASHO_PAGE_ID,
            },
        );
        pm.set_lsn(lsn);
    }
    bufmgr_seams::mark_buffer_dirty::call(ovflbuf)?;

    let (action, leftbuf) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(leftbuf) };
        let mut opaque = page_opaque(&pm.as_ref());
        opaque.hasho_nextblkno = rightblk;
        write_opaque(&mut pm, &opaque);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(leftbuf)?;
    }
    if leftbuf != InvalidBuffer {
        unlock_release(leftbuf)?;
    }
    unlock_release(ovflbuf)?;

    let mut new_bmpage = false;
    let mut newmapblk = InvalidBlockNumber;

    if record.has_block_ref(2) {
        let (action, mapbuffer) = XLogReadBufferForRedo(record, 2)?;
        if action == BLK_NEEDS_REDO {
            let data = block_data(record, 2);
            require_u32(data, 0, "ADD_OVFL_PAGE bitmap bit")?;
            let bit = u32_at(data, 0);
            // Bound the WAL-supplied bit index before the raw SETBIT word write.
            let word = checked_bitmap_bit(bit)?;
            // SAFETY: redo pin+lock contract; bitmap word in-page (checked).
            unsafe {
                let p = bufmgr_seams::buffer_get_page::call(mapbuffer)
                    .as_ptr()
                    .add(SizeOfPageHeaderData)
                    .cast::<u32>()
                    .add(word);
                p.write(p.read() | (1u32 << (bit % BITS_PER_MAP)));
                page_mut(mapbuffer).set_lsn(lsn);
            }
            bufmgr_seams::mark_buffer_dirty::call(mapbuffer)?;
        }
        if mapbuffer != InvalidBuffer {
            unlock_release(mapbuffer)?;
        }
    }

    if record.has_block_ref(3) {
        let newmapbuf = XLogInitBufferForRedo(record, 3)?;
        init_bitmapbuffer(newmapbuf, bmsize)?;
        new_bmpage = true;
        newmapblk = bufmgr_seams::buffer_get_block_number::call(newmapbuf);
        bufmgr_seams::mark_buffer_dirty::call(newmapbuf)?;
        // SAFETY: redo pin+lock contract.
        unsafe { page_mut(newmapbuf) }.set_lsn(lsn);
        unlock_release(newmapbuf)?;
    }

    let (action, metabuf) = XLogReadBufferForRedo(record, 4)?;
    if action == BLK_NEEDS_REDO {
        let data = block_data(record, 4);
        require_u32(data, 0, "ADD_OVFL_PAGE firstfree")?;
        let firstfree = u32_at(data, 0);
        // Validate the meta-page-sourced array indices before touching the
        // fixed hashm_spares/hashm_mapp arrays (C indexes them unchecked).
        // SAFETY: redo pin+lock contract.
        let (ovflpoint, nmaps) =
            unsafe { ((*meta_ptr(metabuf)).hashm_ovflpoint, (*meta_ptr(metabuf)).hashm_nmaps) };
        let spare_slot = if !bmpage_found { Some(checked_splitpoint(ovflpoint)?) } else { None };
        let map_slot = if !bmpage_found && new_bmpage { Some(checked_nmaps(nmaps)?) } else { None };
        // SAFETY: redo pin+lock contract.
        unsafe {
            let m = meta_ptr(metabuf);
            (*m).hashm_firstfree = firstfree;
            if !bmpage_found {
                let spare = spare_slot.expect("spare_slot set when !bmpage_found");
                (*m).hashm_spares[spare] += 1;
                if new_bmpage {
                    debug_assert!(newmapblk != InvalidBlockNumber);
                    (*m).hashm_mapp[map_slot.expect("map_slot set when new_bmpage")] = newmapblk;
                    (*m).hashm_nmaps += 1;
                    (*m).hashm_spares[spare] += 1;
                }
            }
            page_mut(metabuf).set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
    }
    if metabuf != InvalidBuffer {
        unlock_release(metabuf)?;
    }
    Ok(())
}

fn hash_xlog_split_allocate_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    require_len(xlrec, SIZE_OF_HASH_SPLIT_ALLOC_PAGE, "SPLIT_ALLOCATE_PAGE")?;
    let new_bucket = u32_at(xlrec, 0);
    let old_bucket_flag = u16_at(xlrec, 4);
    let new_bucket_flag = u16_at(xlrec, 6);
    let flags = xlrec[8];

    let (action, oldbuf) =
        XLogReadBufferForRedoExtended(record, 0, ReadBufferMode::Normal, true)?;
    // The special space is not included in the image: update either way.
    if action == BLK_NEEDS_REDO || action == BLK_RESTORED {
        // SAFETY: redo pin+cleanup-lock contract.
        let mut pm = unsafe { page_mut(oldbuf) };
        let mut opaque = page_opaque(&pm.as_ref());
        opaque.hasho_flag = old_bucket_flag;
        opaque.hasho_prevblkno = new_bucket;
        write_opaque(&mut pm, &opaque);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(oldbuf)?;
    }

    let (_, newbuf) =
        XLogReadBufferForRedoExtended(record, 1, ReadBufferMode::ZeroAndCleanupLock, true)?;
    {
        // SAFETY: redo pin+cleanup-lock contract.
        let mut pm = unsafe { page_mut(newbuf) };
        hash_pageinit(&mut pm);
        write_opaque(
            &mut pm,
            &HashPageOpaqueData {
                hasho_prevblkno: new_bucket,
                hasho_nextblkno: InvalidBlockNumber,
                hasho_bucket: new_bucket,
                hasho_flag: new_bucket_flag,
                hasho_page_id: HASHO_PAGE_ID,
            },
        );
        pm.set_lsn(lsn);
    }
    bufmgr_seams::mark_buffer_dirty::call(newbuf)?;

    if oldbuf != InvalidBuffer {
        unlock_release(oldbuf)?;
    }
    if newbuf != InvalidBuffer {
        unlock_release(newbuf)?;
    }

    let (action, metabuf) = XLogReadBufferForRedo(record, 2)?;
    if action == BLK_NEEDS_REDO {
        let data = block_data(record, 2);
        let mut off = 0usize;
        // Validate the block-data words and the WAL-sourced splitpoint index
        // before decoding, so a truncated payload or out-of-range ovflpoint
        // fails replay instead of panicking. C reads these unchecked.
        let masks = if flags & XLH_SPLIT_META_UPDATE_MASKS != 0 {
            require_u32(data, off, "SPLIT_ALLOCATE_PAGE lowmask")?;
            require_u32(data, off + 4, "SPLIT_ALLOCATE_PAGE highmask")?;
            let masks = (u32_at(data, off), u32_at(data, off + 4));
            off += 8;
            Some(masks)
        } else {
            None
        };
        let splitpoint = if flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0 {
            require_u32(data, off, "SPLIT_ALLOCATE_PAGE ovflpoint")?;
            require_u32(data, off + 4, "SPLIT_ALLOCATE_PAGE ovflpages")?;
            let ovflpoint = u32_at(data, off);
            let ovflpages = u32_at(data, off + 4);
            Some((checked_splitpoint(ovflpoint)?, ovflpoint, ovflpages))
        } else {
            None
        };
        // SAFETY: redo pin+lock contract.
        unsafe {
            let m = meta_ptr(metabuf);
            (*m).hashm_maxbucket = new_bucket;
            if let Some((lowmask, highmask)) = masks {
                (*m).hashm_lowmask = lowmask;
                (*m).hashm_highmask = highmask;
            }
            if let Some((slot, ovflpoint, ovflpages)) = splitpoint {
                (*m).hashm_spares[slot] = ovflpages;
                (*m).hashm_ovflpoint = ovflpoint;
            }
            page_mut(metabuf).set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
    }
    if metabuf != InvalidBuffer {
        unlock_release(metabuf)?;
    }
    Ok(())
}

fn hash_xlog_split_page(record: &mut XLogReaderState) -> PgResult<()> {
    let (action, buf) = XLogReadBufferForRedo(record, 0)?;
    if action != BLK_RESTORED {
        return Err(panic_err("Hash split record did not contain a full-page image".into()));
    }
    unlock_release(buf)
}

fn hash_xlog_split_complete(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    require_len(xlrec, SIZE_OF_HASH_SPLIT_COMPLETE, "SPLIT_COMPLETE")?;
    let old_bucket_flag = u16_at(xlrec, 0);
    let new_bucket_flag = u16_at(xlrec, 2);

    for (block_id, flag) in [(0u8, old_bucket_flag), (1u8, new_bucket_flag)] {
        let (action, buf) = XLogReadBufferForRedo(record, block_id)?;
        // The bucket flag is not included in the image: update either way.
        if action == BLK_NEEDS_REDO || action == BLK_RESTORED {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(buf) };
            let mut opaque = page_opaque(&pm.as_ref());
            opaque.hasho_flag = flag;
            write_opaque(&mut pm, &opaque);
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(buf)?;
        }
        if buf != InvalidBuffer {
            unlock_release(buf)?;
        }
    }
    Ok(())
}

// The (offsets array, tuples stream) add-back both MOVE and SQUEEZE replay.
fn replay_add_tuples(buffer: Buffer, ntups: u16, data: &[u8]) -> PgResult<()> {
    let mut off = core::mem::size_of::<OffsetNumber>() * ntups as usize;
    // The offsets array (ntups u16s) must fit before the tuple stream that
    // follows; C trusts `ntups` and walks `data + sizeof(OffsetNumber)*ntups`.
    require(data.len() >= off, || {
        format!(
            "hash replay: block data too short for {ntups} offsets: {} bytes",
            data.len()
        )
    })?;
    let towrite = &data[..off];
    let mut ninserted = 0usize;
    // SAFETY: redo pin+lock contract.
    let mut pm = unsafe { page_mut(buffer) };
    while off < data.len() {
        // More tuples in the stream than the record's offsets array can address
        // would drive the `towrite` index out of bounds: reject as corruption.
        require(ninserted < ntups as usize, || {
            format!("hash replay: tuple stream has more than {ntups} tuples")
        })?;
        let itemsz = checked_item_size(data, off)?;
        let item = &data[off..off + itemsz];
        let target = u16_at(towrite, ninserted * 2);
        if pm.add_item(item, target, 0).is_none() {
            return Err(panic_err(format!(
                "hash replay: failed to add item to hash index page, size {itemsz} bytes"
            )));
        }
        off += itemsz;
        ninserted += 1;
    }
    require(ninserted == ntups as usize, || {
        format!("hash replay: tuple stream held {ninserted} tuples, expected {ntups}")
    })?;
    Ok(())
}

fn hash_xlog_move_page_contents(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = main_data(record);
    require_len(xldata, SIZE_OF_HASH_MOVE_PAGE_CONTENTS, "MOVE_PAGE_CONTENTS")?;
    let ntups = u16_at(xldata, 0);
    let is_prim_bucket_same_wrt = xldata[2] != 0;

    let mut bucketbuf = InvalidBuffer;
    let action;
    let writebuf;
    if is_prim_bucket_same_wrt {
        let (a, w) = XLogReadBufferForRedoExtended(record, 1, ReadBufferMode::Normal, true)?;
        action = a;
        writebuf = w;
    } else {
        let (_, b) = XLogReadBufferForRedoExtended(record, 0, ReadBufferMode::Normal, true)?;
        bucketbuf = b;
        let (a, w) = XLogReadBufferForRedo(record, 1)?;
        action = a;
        writebuf = w;
    }

    if action == BLK_NEEDS_REDO {
        let data = block_data(record, 1);
        if ntups > 0 {
            replay_add_tuples(writebuf, ntups, data)?;
        }
        // SAFETY: redo pin+lock contract.
        unsafe { page_mut(writebuf) }.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(writebuf)?;
    }

    let (action, deletebuf) = XLogReadBufferForRedo(record, 2)?;
    if action == BLK_NEEDS_REDO {
        let ptr = block_data(record, 2);
        if !ptr.is_empty() {
            let mut unused: Vec<OffsetNumber> = Vec::with_capacity(ptr.len() / 2);
            for ch in ptr.chunks_exact(2) {
                unused.push(u16::from_ne_bytes([ch[0], ch[1]]));
            }
            // SAFETY: redo pin+lock contract.
            unsafe { page_mut(deletebuf) }.index_multi_delete(&unused);
        }
        // SAFETY: redo pin+lock contract.
        unsafe { page_mut(deletebuf) }.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(deletebuf)?;
    }

    if deletebuf != InvalidBuffer {
        unlock_release(deletebuf)?;
    }
    if writebuf != InvalidBuffer {
        unlock_release(writebuf)?;
    }
    if bucketbuf != InvalidBuffer {
        unlock_release(bucketbuf)?;
    }
    Ok(())
}

fn hash_xlog_squeeze_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = main_data(record);
    require_len(xldata, SIZE_OF_HASH_SQUEEZE_PAGE, "SQUEEZE_PAGE")?;
    let prevblkno: BlockNumber = u32_at(xldata, 0);
    let nextblkno: BlockNumber = u32_at(xldata, 4);
    let ntups = u16_at(xldata, 8);
    let is_prim_bucket_same_wrt = xldata[10] != 0;
    let is_prev_bucket_same_wrt = xldata[11] != 0;

    let mut bucketbuf = InvalidBuffer;
    let mut writebuf = InvalidBuffer;
    let action;
    if is_prim_bucket_same_wrt {
        let (a, w) = XLogReadBufferForRedoExtended(record, 1, ReadBufferMode::Normal, true)?;
        action = a;
        writebuf = w;
    } else {
        let (_, b) = XLogReadBufferForRedoExtended(record, 0, ReadBufferMode::Normal, true)?;
        bucketbuf = b;
        if ntups > 0 || is_prev_bucket_same_wrt {
            let (a, w) = XLogReadBufferForRedo(record, 1)?;
            action = a;
            writebuf = w;
        } else {
            action = xlogutils::BLK_NOTFOUND;
        }
    }

    if action == BLK_NEEDS_REDO {
        let mut mod_wbuf = false;
        if ntups > 0 {
            replay_add_tuples(writebuf, ntups, block_data(record, 1))?;
            mod_wbuf = true;
        } else {
            debug_assert!(is_prim_bucket_same_wrt || is_prev_bucket_same_wrt);
        }

        if is_prev_bucket_same_wrt {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(writebuf) };
            let mut opaque = page_opaque(&pm.as_ref());
            opaque.hasho_nextblkno = nextblkno;
            write_opaque(&mut pm, &opaque);
            mod_wbuf = true;
        }

        if mod_wbuf {
            // SAFETY: redo pin+lock contract.
            unsafe { page_mut(writebuf) }.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(writebuf)?;
        }
    }

    let (action, ovflbuf) = XLogReadBufferForRedo(record, 2)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(ovflbuf) };
        hash_pageinit(&mut pm);
        write_opaque(
            &mut pm,
            &HashPageOpaqueData {
                hasho_prevblkno: InvalidBlockNumber,
                hasho_nextblkno: InvalidBlockNumber,
                hasho_bucket: InvalidBucket,
                hasho_flag: LH_UNUSED_PAGE,
                hasho_page_id: HASHO_PAGE_ID,
            },
        );
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(ovflbuf)?;
    }
    if ovflbuf != InvalidBuffer {
        unlock_release(ovflbuf)?;
    }

    if !is_prev_bucket_same_wrt {
        let (action, prevbuf) = XLogReadBufferForRedo(record, 3)?;
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(prevbuf) };
            let mut opaque = page_opaque(&pm.as_ref());
            opaque.hasho_nextblkno = nextblkno;
            write_opaque(&mut pm, &opaque);
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(prevbuf)?;
        }
        if prevbuf != InvalidBuffer {
            unlock_release(prevbuf)?;
        }
    }

    if record.has_block_ref(4) {
        let (action, nextbuf) = XLogReadBufferForRedo(record, 4)?;
        if action == BLK_NEEDS_REDO {
            // SAFETY: redo pin+lock contract.
            let mut pm = unsafe { page_mut(nextbuf) };
            let mut opaque = page_opaque(&pm.as_ref());
            opaque.hasho_prevblkno = prevblkno;
            write_opaque(&mut pm, &opaque);
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(nextbuf)?;
        }
        if nextbuf != InvalidBuffer {
            unlock_release(nextbuf)?;
        }
    }

    if writebuf != InvalidBuffer {
        unlock_release(writebuf)?;
    }
    if bucketbuf != InvalidBuffer {
        unlock_release(bucketbuf)?;
    }

    let (action, mapbuf) = XLogReadBufferForRedo(record, 5)?;
    if action == BLK_NEEDS_REDO {
        let data = block_data(record, 5);
        require_u32(data, 0, "SQUEEZE_PAGE bitmap bit")?;
        let bit = u32_at(data, 0);
        // Bound the WAL-supplied bit index before the raw CLRBIT word write.
        let word = checked_bitmap_bit(bit)?;
        // SAFETY: redo pin+lock contract; bitmap word in-page (checked).
        unsafe {
            let p = bufmgr_seams::buffer_get_page::call(mapbuf)
                .as_ptr()
                .add(SizeOfPageHeaderData)
                .cast::<u32>()
                .add(word);
            p.write(p.read() & !(1u32 << (bit % BITS_PER_MAP)));
            page_mut(mapbuf).set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(mapbuf)?;
    }
    if mapbuf != InvalidBuffer {
        unlock_release(mapbuf)?;
    }

    if record.has_block_ref(6) {
        let (action, metabuf) = XLogReadBufferForRedo(record, 6)?;
        if action == BLK_NEEDS_REDO {
            let data = block_data(record, 6);
            require_u32(data, 0, "SQUEEZE_PAGE firstfree")?;
            // SAFETY: redo pin+lock contract.
            unsafe {
                (*meta_ptr(metabuf)).hashm_firstfree = u32_at(data, 0);
                page_mut(metabuf).set_lsn(lsn);
            }
            bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
        }
        if metabuf != InvalidBuffer {
            unlock_release(metabuf)?;
        }
    }
    Ok(())
}

fn hash_xlog_delete(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = main_data(record);
    require_len(xldata, SIZE_OF_HASH_DELETE, "DELETE")?;
    let clear_dead_marking = xldata[0] != 0;
    let is_primary_bucket_page = xldata[1] != 0;

    let mut bucketbuf = InvalidBuffer;
    let action;
    let deletebuf;
    if is_primary_bucket_page {
        let (a, d) = XLogReadBufferForRedoExtended(record, 1, ReadBufferMode::Normal, true)?;
        action = a;
        deletebuf = d;
    } else {
        let (_, b) = XLogReadBufferForRedoExtended(record, 0, ReadBufferMode::Normal, true)?;
        bucketbuf = b;
        let (a, d) = XLogReadBufferForRedo(record, 1)?;
        action = a;
        deletebuf = d;
    }

    if action == BLK_NEEDS_REDO {
        let ptr = block_data(record, 1);
        if !ptr.is_empty() {
            let mut unused: Vec<OffsetNumber> = Vec::with_capacity(ptr.len() / 2);
            for ch in ptr.chunks_exact(2) {
                unused.push(u16::from_ne_bytes([ch[0], ch[1]]));
            }
            // SAFETY: redo pin+lock contract.
            unsafe { page_mut(deletebuf) }.index_multi_delete(&unused);
        }

        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(deletebuf) };
        if clear_dead_marking {
            let mut opaque = page_opaque(&pm.as_ref());
            opaque.hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
            write_opaque(&mut pm, &opaque);
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(deletebuf)?;
    }
    if deletebuf != InvalidBuffer {
        unlock_release(deletebuf)?;
    }
    if bucketbuf != InvalidBuffer {
        unlock_release(bucketbuf)?;
    }
    Ok(())
}

fn hash_xlog_split_cleanup(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        let mut opaque = page_opaque(&pm.as_ref());
        opaque.hasho_flag &= !LH_BUCKET_NEEDS_SPLIT_CLEANUP;
        write_opaque(&mut pm, &opaque);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn hash_xlog_update_meta_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let md = main_data(record);
    require_len(md, SIZE_OF_HASH_UPDATE_META_PAGE, "UPDATE_META_PAGE")?;
    let ntuples = f64::from_ne_bytes(md[0..8].try_into().unwrap());
    let (action, metabuf) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        unsafe {
            (*meta_ptr(metabuf)).hashm_ntuples = ntuples;
            page_mut(metabuf).set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
    }
    if metabuf != InvalidBuffer {
        unlock_release(metabuf)?;
    }
    Ok(())
}

fn hash_xlog_vacuum_one_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = main_data(record);
    require_len(xldata, SIZE_OF_HASH_VACUUM_ONE_PAGE, "VACUUM_ONE_PAGE")?;
    let ntuples = u16_at(xldata, 4);

    if xlogutils::InHotStandby() {
        let horizon = u32::from_ne_bytes(xldata[0..4].try_into().unwrap());
        let is_catalog_rel = xldata[6] != 0;
        let (rlocator, _, _, _) = record
            .block_tag_extended(0)
            .expect("hash_xlog_vacuum_one_page: no block 0");
        standby::ResolveRecoveryConflictWithSnapshot(horizon, is_catalog_rel, rlocator)?;
    }

    let (action, buffer) = XLogReadBufferForRedoExtended(record, 0, ReadBufferMode::Normal, true)?;
    if action == BLK_NEEDS_REDO {
        require_len(xldata, 8 + ntuples as usize * 2, "VACUUM_ONE_PAGE offsets")?;
        let to_delete = &xldata[8..8 + ntuples as usize * 2];
        let mut unused: Vec<OffsetNumber> = Vec::with_capacity(ntuples as usize);
        for ch in to_delete.chunks_exact(2) {
            unused.push(u16::from_ne_bytes([ch[0], ch[1]]));
        }
        // SAFETY: redo pin+cleanup-lock contract.
        let mut pm = unsafe { page_mut(buffer) };
        pm.index_multi_delete(&unused);
        let mut opaque = page_opaque(&pm.as_ref());
        opaque.hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
        write_opaque(&mut pm, &opaque);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    let (action, metabuf) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        unsafe {
            (*meta_ptr(metabuf)).hashm_ntuples -= ntuples as f64;
            page_mut(metabuf).set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
    }
    if metabuf != InvalidBuffer {
        unlock_release(metabuf)?;
    }
    Ok(())
}

pub fn hash_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record.record.as_ref().expect("hash_redo with no decoded record").xl_info
        & !XLR_INFO_MASK;
    match info {
        XLOG_HASH_INIT_META_PAGE => hash_xlog_init_meta_page(record),
        XLOG_HASH_INIT_BITMAP_PAGE => hash_xlog_init_bitmap_page(record),
        XLOG_HASH_INSERT => hash_xlog_insert(record),
        XLOG_HASH_ADD_OVFL_PAGE => hash_xlog_add_ovfl_page(record),
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => hash_xlog_split_allocate_page(record),
        XLOG_HASH_SPLIT_PAGE => hash_xlog_split_page(record),
        XLOG_HASH_SPLIT_COMPLETE => hash_xlog_split_complete(record),
        XLOG_HASH_MOVE_PAGE_CONTENTS => hash_xlog_move_page_contents(record),
        XLOG_HASH_SQUEEZE_PAGE => hash_xlog_squeeze_page(record),
        XLOG_HASH_DELETE => hash_xlog_delete(record),
        XLOG_HASH_SPLIT_CLEANUP => hash_xlog_split_cleanup(record),
        XLOG_HASH_UPDATE_META_PAGE => hash_xlog_update_meta_page(record),
        XLOG_HASH_VACUUM_ONE_PAGE => hash_xlog_vacuum_one_page(record),
        other => Err(panic_err(format!("hash_redo: unknown op code {other}"))),
    }
}

pub fn hash_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum(pagedata);
    bufmask::mask_page_hint_bits(pagedata);
    bufmask::mask_unused_space(pagedata)?;

    let ptr = core::ptr::NonNull::new(pagedata.as_mut_ptr()).unwrap();
    // SAFETY: pagedata is a full BLCKSZ page image, exclusively borrowed here.
    let pm = unsafe { PageMut::from_raw(ptr) };
    let mut opaque = page_opaque(&pm.as_ref());
    let pagetype = opaque.hasho_flag & LH_PAGE_TYPE;
    drop(pm);

    if pagetype == LH_UNUSED_PAGE {
        bufmask::mask_page_content(pagedata);
    } else if pagetype == LH_BUCKET_PAGE || pagetype == LH_OVERFLOW_PAGE {
        bufmask::mask_lp_flags(pagedata);
    }

    opaque.hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;
    let ptr = core::ptr::NonNull::new(pagedata.as_mut_ptr()).unwrap();
    // SAFETY: as above.
    let mut pm = unsafe { PageMut::from_raw(ptr) };
    write_opaque(&mut pm, &opaque);
    Ok(())
}

pub fn init_seams() {}

#[cfg(test)]
mod redo_bounds_tests {
    use super::*;

    // A record whose main data is shorter than the opcode's fixed struct must
    // surface as ERRCODE_DATA_CORRUPTED, never a slice panic in the startup
    // redo thread. Cover the full family of fixed-struct sizes.
    #[test]
    fn short_main_data_is_data_corruption() {
        for need in [
            SIZE_OF_HASH_INIT_META_PAGE,
            SIZE_OF_HASH_INIT_BITMAP_PAGE,
            SIZE_OF_HASH_INSERT,
            SIZE_OF_HASH_ADD_OVFL_PAGE,
            SIZE_OF_HASH_SPLIT_ALLOC_PAGE,
            SIZE_OF_HASH_SPLIT_COMPLETE,
            SIZE_OF_HASH_MOVE_PAGE_CONTENTS,
            SIZE_OF_HASH_SQUEEZE_PAGE,
            SIZE_OF_HASH_DELETE,
            SIZE_OF_HASH_UPDATE_META_PAGE,
            SIZE_OF_HASH_VACUUM_ONE_PAGE,
        ] {
            for len in 0..need {
                let err = require_len(&vec![0u8; len], need, "test").err().unwrap();
                assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
            }
            // Exactly the required length is accepted.
            require_len(&vec![0u8; need], need, "test").unwrap();
        }
    }

    // A truncated block-data u32 (bit index / firstfree / mask word) is rejected
    // rather than driving an out-of-bounds read.
    #[test]
    fn short_block_u32_is_data_corruption() {
        for len in 0..4 {
            let err = require_u32(&vec![0u8; len], 0, "test").err().unwrap();
            assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        require_u32(&[0u8; 4], 0, "test").unwrap();
        // Offset past the end is caught too.
        let err = require_u32(&[0u8; 4], 4, "test").err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    // A tuple stream truncated before a full item header, or whose declared
    // item size runs past the payload (or is below the header minimum), is
    // rejected instead of panicking the redo walk.
    #[test]
    fn item_size_bounds_are_enforced() {
        // Truncated header.
        for len in 0..SIZE_OF_INDEX_TUPLE_DATA {
            let err = checked_item_size(&vec![0u8; len], 0).err().unwrap();
            assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        }
        // Header present but declared size (0x1FFF) far exceeds the payload.
        let mut inflated = vec![0u8; 16];
        inflated[6] = 0xFF;
        inflated[7] = 0x1F;
        let err = checked_item_size(&inflated, 0).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        // Declared size below the 8-byte header minimum.
        let mut small = vec![0u8; 16];
        small[6] = 4;
        let err = checked_item_size(&small, 0).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        // A well-formed, MAXALIGN'd tuple size that fits is accepted.
        let mut ok = vec![0u8; 24];
        ok[6] = 16;
        assert_eq!(checked_item_size(&ok, 0).unwrap(), 16);
    }

    // Meta/WAL-sourced array indices are range-checked against the fixed-array
    // capacities before indexing hashm_spares / hashm_mapp.
    #[test]
    fn out_of_range_meta_indices_are_rejected() {
        checked_splitpoint(0).unwrap();
        checked_splitpoint(HASH_MAX_SPLITPOINTS as u32 - 1).unwrap();
        let err = checked_splitpoint(HASH_MAX_SPLITPOINTS as u32).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        let err = checked_splitpoint(u32::MAX).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        checked_nmaps(0).unwrap();
        checked_nmaps(HASH_MAX_BITMAPS as u32 - 1).unwrap();
        let err = checked_nmaps(HASH_MAX_BITMAPS as u32).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        let err = checked_nmaps(u32::MAX).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }

    // A WAL-supplied bitmap bit index and bitmap size must be confined to the
    // page's bitmap region before driving a raw SETBIT/CLRBIT word write or the
    // 0xFF memset; an out-of-range value is corruption, not an OOB write.
    #[test]
    fn bitmap_bit_and_size_bounds_are_enforced() {
        // The highest bit whose 4-byte word still fits in the bitmap region.
        let last_word = MAX_BITMAP_SIZE / 4 - 1;
        let last_bit = (last_word as u32) * BITS_PER_MAP + (BITS_PER_MAP - 1);
        assert_eq!(checked_bitmap_bit(0).unwrap(), 0);
        assert_eq!(checked_bitmap_bit(last_bit).unwrap(), last_word);
        // First bit whose word lands past the region, and the extreme value.
        let first_oob_bit = (last_word as u32 + 1) * BITS_PER_MAP;
        let err = checked_bitmap_bit(first_oob_bit).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        let err = checked_bitmap_bit(u32::MAX).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);

        // bmsize: zero and anything past the region are rejected; the legitimate
        // 4096 and the region maximum are accepted.
        let err = checked_bmsize(0).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
        assert_eq!(checked_bmsize(4096).unwrap(), 4096);
        assert_eq!(checked_bmsize(MAX_BITMAP_SIZE as u16).unwrap(), MAX_BITMAP_SIZE);
        let err = checked_bmsize(u16::MAX).err().unwrap();
        assert_eq!(err.sqlstate(), ERRCODE_DATA_CORRUPTED);
    }
}
