//! hash_xlog.c — hash rmgr redo + hash_mask. Every arm the write side emits
//! is live (INIT_META/INIT_BITMAP/INSERT/ADD_OVFL/SPLIT_*/MOVE/SQUEEZE/
//! DELETE/SPLIT_CLEANUP/VACUUM_ONE_PAGE); UPDATE_META_PAGE is written only by
//! the unported VACUUM lane but replays anyway (a C-written WAL stream may
//! carry it).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{BlockNumber, Buffer, InvalidBlockNumber, InvalidBuffer, OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult};
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

// SAFETY contract shared by the redo arms: the buffer is pinned and
// exclusively locked (XLogReadBufferForRedo protocol) until the unlock below.
unsafe fn page_mut<'p>(buffer: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

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

fn init_bitmapbuffer(buffer: Buffer, bmsize: u16) {
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
    // SAFETY: bitmap region in-page.
    unsafe {
        core::ptr::write_bytes(
            pm.as_ref().as_ptr().cast_mut().add(SizeOfPageHeaderData),
            0xFF,
            bmsize as usize,
        );
    }
    pm.set_pd_lower((SizeOfPageHeaderData + bmsize as usize) as u16);
}

fn hash_xlog_init_meta_page(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
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
    let bmsize = u16_at(xlrec, 0);

    let bitmapbuf = XLogInitBufferForRedo(record, 0)?;
    init_bitmapbuffer(bitmapbuf, bmsize);
    // SAFETY: redo pin+lock contract.
    unsafe { page_mut(bitmapbuf) }.set_lsn(lsn);
    bufmgr_seams::mark_buffer_dirty::call(bitmapbuf)?;
    XLogFlushBufferForRedoIfInit(record, 0, bitmapbuf)?;
    unlock_release(bitmapbuf)?;

    let (action, metabuf) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: redo pin+lock contract.
        unsafe {
            let m = meta_ptr(metabuf);
            let num_buckets = (*m).hashm_maxbucket + 1;
            (*m).hashm_mapp[(*m).hashm_nmaps as usize] = num_buckets + 1;
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
    let offnum = u16_at(main_data(record), 0);

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
    let bmsize = u16_at(xlrec, 0);
    let bmpage_found = xlrec[2] != 0;

    let (_, _, rightblk, _) =
        record.block_tag_extended(0).expect("hash_xlog_add_ovfl_page: no block 0");
    let (_, _, leftblk, _) =
        record.block_tag_extended(1).expect("hash_xlog_add_ovfl_page: no block 1");

    let ovflbuf = XLogInitBufferForRedo(record, 0)?;
    let data = block_data(record, 0);
    debug_assert!(data.len() == 4);
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
            let bit = u32_at(data, 0);
            // SAFETY: redo pin+lock contract; bitmap word in-page.
            unsafe {
                let p = bufmgr_seams::buffer_get_page::call(mapbuffer)
                    .as_ptr()
                    .add(SizeOfPageHeaderData)
                    .cast::<u32>()
                    .add((bit / BITS_PER_MAP) as usize);
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
        init_bitmapbuffer(newmapbuf, bmsize);
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
        let firstfree = u32_at(data, 0);
        // SAFETY: redo pin+lock contract.
        unsafe {
            let m = meta_ptr(metabuf);
            (*m).hashm_firstfree = firstfree;
            if !bmpage_found {
                (*m).hashm_spares[(*m).hashm_ovflpoint as usize] += 1;
                if new_bmpage {
                    debug_assert!(newmapblk != InvalidBlockNumber);
                    (*m).hashm_mapp[(*m).hashm_nmaps as usize] = newmapblk;
                    (*m).hashm_nmaps += 1;
                    (*m).hashm_spares[(*m).hashm_ovflpoint as usize] += 1;
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
        // SAFETY: redo pin+lock contract.
        unsafe {
            let m = meta_ptr(metabuf);
            (*m).hashm_maxbucket = new_bucket;
            if flags & XLH_SPLIT_META_UPDATE_MASKS != 0 {
                (*m).hashm_lowmask = u32_at(data, off);
                (*m).hashm_highmask = u32_at(data, off + 4);
                off += 8;
            }
            if flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0 {
                let ovflpoint = u32_at(data, off);
                let ovflpages = u32_at(data, off + 4);
                (*m).hashm_spares[ovflpoint as usize] = ovflpages;
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
    let towrite = &data[..off];
    let mut ninserted = 0usize;
    // SAFETY: redo pin+lock contract.
    let mut pm = unsafe { page_mut(buffer) };
    while off < data.len() {
        let itemsz = maxalign(
            (u16::from_ne_bytes([data[off + 6], data[off + 7]]) & 0x1FFF) as usize,
        );
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
    debug_assert!(ninserted == ntups as usize);
    Ok(())
}

fn hash_xlog_move_page_contents(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = main_data(record);
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
        let bit = u32_at(data, 0);
        // SAFETY: redo pin+lock contract; bitmap word in-page.
        unsafe {
            let p = bufmgr_seams::buffer_get_page::call(mapbuf)
                .as_ptr()
                .add(SizeOfPageHeaderData)
                .cast::<u32>()
                .add((bit / BITS_PER_MAP) as usize);
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
    let ntuples = f64::from_ne_bytes(main_data(record)[0..8].try_into().unwrap());
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
    bufmask::mask_unused_space(pagedata);

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
