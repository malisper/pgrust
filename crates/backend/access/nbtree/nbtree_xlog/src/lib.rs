//! nbtxlog.c — btree rmgr redo. Live arms cover exactly what the write side
//! (nbtree insert lane) emits: INSERT_LEAF/UPPER/META/POST, SPLIT_L/R,
//! NEWROOT. Every other op is a loud panic naming its C function and unit.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{Buffer, InvalidBuffer, OffsetNumber, BLCKSZ};
use types_error::{PgError, PgResult};
use types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTP_INCOMPLETE_SPLIT, BTP_LEAF, BTP_META, BTP_ROOT,
    BTREE_MAGIC, BTREE_METAPAGE, BTREE_NOVAC_VERSION, P_FIRSTDATAKEY, P_HIKEY,
    P_INCOMPLETE_SPLIT, P_NONE, XLOG_BTREE_DEDUP, XLOG_BTREE_DELETE, XLOG_BTREE_INSERT_LEAF,
    XLOG_BTREE_INSERT_META, XLOG_BTREE_INSERT_POST, XLOG_BTREE_INSERT_UPPER,
    XLOG_BTREE_MARK_PAGE_HALFDEAD, XLOG_BTREE_META_CLEANUP, XLOG_BTREE_NEWROOT,
    XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_SPLIT_L, XLOG_BTREE_SPLIT_R, XLOG_BTREE_UNLINK_PAGE,
    XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo, BLK_NEEDS_REDO};

const XLR_INFO_MASK: u8 = 0x0F;
const INDEX_SIZE_MASK: u16 = 0x1FFF;
const SizeOfBtreeOpaque: usize = core::mem::size_of::<BTPageOpaqueData>();
const MaxIndexTuplesPerPage: usize = (BLCKSZ - SizeOfPageHeaderData) / (16 + 4);

fn main_data<'a>(record: &'a XLogReaderState) -> &'a [u8] {
    let rec = record.record.as_ref().expect("btree redo with no decoded record");
    // SAFETY: points into the reader's decode buffer, valid for the redo
    // callback's duration.
    unsafe { rec.main_data_bytes() }
}

fn block_data<'a>(record: &'a XLogReaderState, block_id: u8) -> &'a [u8] {
    // SAFETY: same decode-buffer lifetime as main_data.
    unsafe { record.block(block_id).data_bytes() }
}

#[cold]
fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(types_error::PANIC, msg))
}

#[cold]
fn error_err(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg))
}

// SAFETY contract shared by the redo arms: the buffer is pinned and
// exclusively locked (XLogReadBufferForRedo protocol), so the PageMut is the
// sole writer of the image until the unlock below.
unsafe fn page_mut<'p>(buffer: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    bufmgr_seams::release_buffer::call(buffer)
}

fn page_opaque(page: &PageRef<'_>) -> BTPageOpaqueData {
    let off = page.pd_special() as usize;
    debug_assert!(off == BLCKSZ - SizeOfBtreeOpaque);
    // SAFETY: in-bounds 4-aligned special area of a btree page.
    unsafe { page.as_ptr().add(off).cast::<BTPageOpaqueData>().read() }
}

fn write_opaque(page: &mut PageMut<'_>, opaque: &BTPageOpaqueData) {
    let off = page.as_ref().pd_special() as usize;
    debug_assert!(off == BLCKSZ - SizeOfBtreeOpaque);
    // SAFETY: in-bounds 4-aligned special area; exclusive page access.
    unsafe {
        page.as_ref().as_ptr().cast_mut().add(off).cast::<BTPageOpaqueData>().write(*opaque)
    }
}

fn bt_pageinit(page: &mut PageMut<'_>) {
    page.init(SizeOfBtreeOpaque);
}

const fn maxalign(sz: usize) -> usize {
    (sz + 7) & !7
}

fn itup_size_at(stream: &[u8], off: usize) -> usize {
    (u16::from_ne_bytes([stream[off + 6], stream[off + 7]]) & INDEX_SIZE_MASK) as usize
}

// _bt_restore_page: the stream is page-memory order (highest offset number
// first); boundaries are found forward, items re-added in reverse.
fn bt_restore_page(page: &mut PageMut<'_>, from: &[u8]) -> PgResult<()> {
    let mut bounds = [(0u16, 0u16); MaxIndexTuplesPerPage];
    let mut nitems = 0usize;
    let mut off = 0usize;
    while off < from.len() {
        let itemsz = maxalign(itup_size_at(from, off));
        bounds[nitems] = (off as u16, itemsz as u16);
        nitems += 1;
        off += itemsz;
    }

    for i in (0..nitems).rev() {
        let (off, itemsz) = (bounds[i].0 as usize, bounds[i].1 as usize);
        if page
            .add_item(&from[off..off + itemsz], (nitems - i) as OffsetNumber, 0)
            .is_none()
        {
            return Err(panic_err("_bt_restore_page: cannot add item to page".into()));
        }
    }
    Ok(())
}

fn bt_restore_meta(record: &mut XLogReaderState, block_id: u8) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let metabuf = XLogInitBufferForRedo(record, block_id)?;
    let xlrec = block_data(record, block_id);

    debug_assert!(xlrec.len() == 28);
    debug_assert!(bufmgr_seams::buffer_get_block_number::call(metabuf) == BTREE_METAPAGE);

    // SAFETY: pin + exclusive lock per the redo protocol (module contract).
    let mut pm = unsafe { page_mut(metabuf) };
    bt_pageinit(&mut pm);

    let u32_at = |o: usize| u32::from_ne_bytes(xlrec[o..o + 4].try_into().unwrap());
    let md = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: u32_at(0),
        btm_root: u32_at(4),
        btm_level: u32_at(8),
        btm_fastroot: u32_at(12),
        btm_fastlevel: u32_at(16),
        btm_last_cleanup_num_delpages: u32_at(20),
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: xlrec[24] != 0,
    };
    debug_assert!(md.btm_version >= BTREE_NOVAC_VERSION);
    let img = md.page_image();
    // SAFETY: metapage contents at +24, 48B in-bounds; exclusive.
    unsafe {
        core::ptr::copy_nonoverlapping(
            img.as_ptr(),
            pm.as_ref().as_ptr().cast_mut().add(SizeOfPageHeaderData),
            img.len(),
        )
    };

    write_opaque(
        &mut pm,
        &BTPageOpaqueData {
            btpo_prev: 0,
            btpo_next: 0,
            btpo_level: 0,
            btpo_flags: BTP_META,
            btpo_cycleid: 0,
        },
    );

    // pd_lower past the metadata keeps it out of the xlog page-hole.
    pm.set_pd_lower((SizeOfPageHeaderData + core::mem::size_of::<BTMetaPageData>()) as u16);

    pm.set_lsn(lsn);
    bufmgr_seams::mark_buffer_dirty::call(metabuf)?;
    unlock_release(metabuf)
}

fn bt_clear_incomplete_split(record: &mut XLogReaderState, block_id: u8) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let (action, buf) = XLogReadBufferForRedo(record, block_id)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buf) };
        let mut opaque = page_opaque(&pm.as_ref());
        debug_assert!(P_INCOMPLETE_SPLIT(&opaque));
        opaque.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
        write_opaque(&mut pm, &opaque);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buf)?;
    }
    if buf != InvalidBuffer {
        unlock_release(buf)?;
    }
    Ok(())
}

// _bt_swap_posting (nbtdedup.c), redo-side transcription over raw images
// (the write-side twin lives in the nbtree crate; this crate cannot depend on
// it). `nposting` starts as a copy of oposting; TIDs are 6-byte raw moves.
fn bt_swap_posting(newitem: &mut [u8], nposting: &mut [u8], postingoff: usize) -> PgResult<()> {
    const IPD_SIZE: usize = 6;
    let u16_at = |b: &[u8], o: usize| u16::from_ne_bytes([b[o], b[o + 1]]);
    let nhtids = (u16_at(nposting, 4) & types_nbtree::BT_OFFSET_MASK) as usize;

    if !(postingoff > 0 && postingoff < nhtids) {
        return Err(error_err(format!(
            "posting list tuple with {nhtids} items cannot be split at offset {postingoff}"
        )));
    }

    // posting offset = ip_blkid of the alt TID (bi_hi << 16 | bi_lo).
    let postoff =
        ((u16_at(nposting, 0) as u32) << 16 | u16_at(nposting, 2) as u32) as usize;
    let replacepos = postoff + postingoff * IPD_SIZE;
    let nmovebytes = (nhtids - postingoff - 1) * IPD_SIZE;

    let omax_pos = postoff + (nhtids - 1) * IPD_SIZE;
    let omax: [u8; IPD_SIZE] = nposting[omax_pos..omax_pos + IPD_SIZE].try_into().unwrap();
    let newtid: [u8; IPD_SIZE] = newitem[0..IPD_SIZE].try_into().unwrap();

    nposting.copy_within(replacepos..replacepos + nmovebytes, replacepos + IPD_SIZE);
    nposting[replacepos..replacepos + IPD_SIZE].copy_from_slice(&newtid);
    newitem[0..IPD_SIZE].copy_from_slice(&omax);
    Ok(())
}

fn btree_xlog_insert(
    isleaf: bool,
    ismeta: bool,
    posting: bool,
    record: &mut XLogReaderState,
) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let offnum = u16::from_ne_bytes(xlrec[0..2].try_into().unwrap());

    if !isleaf {
        bt_clear_incomplete_split(record, 1)?;
    }
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let datapos = block_data(record, 0);
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        if !posting {
            if pm.add_item(datapos, offnum, 0).is_none() {
                return Err(panic_err("failed to add new item".into()));
            }
        } else {
            // block data = uint16 postingoff + orignewitem; repeat the
            // primary's _bt_swap_posting against oposting at offnum - 1.
            debug_assert!(isleaf);
            let postingoff = u16::from_ne_bytes(datapos[0..2].try_into().unwrap());
            let orignewitem = &datapos[2..];
            debug_assert!(postingoff > 0);

            let itemid = pm.as_ref().item_id(offnum - 1);
            let opos_off = itemid.lp_off() as usize;
            let oposting_size =
                (u16_le_native(pm.as_ref(), opos_off + 6) & INDEX_SIZE_MASK) as usize;

            #[repr(C, align(8))]
            struct ItupImage([u8; BLCKSZ]);
            let mut newitem = ItupImage([0u8; BLCKSZ]);
            newitem.0[..orignewitem.len()].copy_from_slice(orignewitem);
            let mut nposting = ItupImage([0u8; BLCKSZ]);
            // SAFETY: in-bounds page item read under the redo lock.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    pm.as_ref().as_ptr().add(opos_off),
                    nposting.0.as_mut_ptr(),
                    oposting_size,
                );
            }
            bt_swap_posting(
                &mut newitem.0[..orignewitem.len()],
                &mut nposting.0[..oposting_size],
                postingoff as usize,
            )?;

            // SAFETY: same-size in-place overwrite of oposting; exclusive.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    nposting.0.as_ptr(),
                    pm.as_ref().as_ptr().cast_mut().add(opos_off),
                    maxalign(oposting_size),
                );
            }
            if pm.add_item(&newitem.0[..orignewitem.len()], offnum, 0).is_none() {
                return Err(panic_err("failed to add posting split new item".into()));
            }
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    if ismeta {
        bt_restore_meta(record, 2)?;
    }
    Ok(())
}

fn u16_le_native(page: PageRef<'_>, off: usize) -> u16 {
    // SAFETY: in-bounds header read of a live page item.
    unsafe { page.as_ptr().add(off).cast::<u16>().read_unaligned() }
}

fn btree_xlog_split(newitemonleft: bool, record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let level = u32::from_ne_bytes(xlrec[0..4].try_into().unwrap());
    let firstrightoff = u16::from_ne_bytes(xlrec[4..6].try_into().unwrap());
    let newitemoff = u16::from_ne_bytes(xlrec[6..8].try_into().unwrap());
    let postingoff = u16::from_ne_bytes(xlrec[8..10].try_into().unwrap());
    let isleaf = level == 0;

    if postingoff != 0 {
        panic!("btree_xlog_split arm not ported: posting-list split (_bt_swap_posting) — land backend-access-nbt-dedup");
    }

    let (_, _, origpagenumber, _) =
        record.block_tag_extended(0).expect("btree_xlog_split: no block 0");
    let (_, _, rightpagenumber, _) =
        record.block_tag_extended(1).expect("btree_xlog_split: no block 1");
    let spagenumber = record.block_tag_extended(2).map(|t| t.2).unwrap_or(P_NONE);

    if !isleaf {
        bt_clear_incomplete_split(record, 3)?;
    }

    let rbuf = XLogInitBufferForRedo(record, 1)?;
    {
        let rdata = block_data(record, 1);
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut rpm = unsafe { page_mut(rbuf) };
        bt_pageinit(&mut rpm);
        write_opaque(
            &mut rpm,
            &BTPageOpaqueData {
                btpo_prev: origpagenumber,
                btpo_next: spagenumber,
                btpo_level: level,
                btpo_flags: if isleaf { BTP_LEAF } else { 0 },
                btpo_cycleid: 0,
            },
        );
        bt_restore_page(&mut rpm, rdata)?;
        rpm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(rbuf)?;
    }

    let (action, buf) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let mut datapos = block_data(record, 0);

        let mut newitem: &[u8] = &[];
        if newitemonleft {
            let newitemsz = maxalign(itup_size_at(datapos, 0));
            newitem = &datapos[..newitemsz];
            datapos = &datapos[newitemsz..];
        }

        let left_hikeysz = maxalign(itup_size_at(datapos, 0));
        let left_hikey = &datapos[..left_hikeysz];
        datapos = &datapos[left_hikeysz..];
        debug_assert!(datapos.is_empty());

        let raw = bufmgr_seams::buffer_get_page::call(buf);
        // SAFETY: pinned + exclusively locked; reads only until the restore
        // memcpy below ends this borrow.
        let origpage = unsafe { PageRef::from_raw(raw) };
        let oopaque = page_opaque(&origpage);

        // PageGetTempPageCopySpecial + item-order rebuild, as _bt_split does.
        #[repr(align(8))]
        struct TempPage([u8; BLCKSZ]);
        let mut temp = TempPage([0u8; BLCKSZ]);
        // SAFETY: owned, aligned BLCKSZ scratch.
        let mut leftpage = unsafe {
            PageMut::from_raw(core::ptr::NonNull::new(temp.0.as_mut_ptr()).unwrap())
        };
        bt_pageinit(&mut leftpage);
        write_opaque(&mut leftpage, &oopaque);

        let mut leftoff = P_HIKEY;
        if leftpage.add_item(left_hikey, P_HIKEY, 0).is_none() {
            return Err(error_err("failed to add high key to left page after split".into()));
        }
        leftoff += 1;

        let mut off = P_FIRSTDATAKEY(&oopaque);
        while off < firstrightoff {
            if newitemonleft && off == newitemoff {
                if leftpage.add_item(newitem, leftoff, 0).is_none() {
                    return Err(error_err(
                        "failed to add new item to left page after split".into(),
                    ));
                }
                leftoff += 1;
            }

            let itemid = origpage.item_id(off);
            let (ptr, len) = origpage.item_raw(itemid);
            // SAFETY: in-page tuple image under the pin + lock.
            let item = unsafe { core::slice::from_raw_parts(ptr, len as usize) };
            if leftpage.add_item(item, leftoff, 0).is_none() {
                return Err(error_err("failed to add old item to left page after split".into()));
            }
            leftoff += 1;
            off += 1;
        }

        if newitemonleft && off == newitemoff {
            if leftpage.add_item(newitem, leftoff, 0).is_none() {
                return Err(error_err("failed to add new item to left page after split".into()));
            }
        }

        // PageRestoreTempPage.
        // SAFETY: whole-page overwrite under the exclusive lock; the read
        // borrow above is dead.
        unsafe { core::ptr::copy_nonoverlapping(temp.0.as_ptr(), raw.as_ptr(), BLCKSZ) };
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut opm = unsafe { page_mut(buf) };
        let mut o = oopaque;
        o.btpo_flags = BTP_INCOMPLETE_SPLIT;
        if isleaf {
            o.btpo_flags |= BTP_LEAF;
        }
        o.btpo_next = rightpagenumber;
        o.btpo_cycleid = 0;
        write_opaque(&mut opm, &o);

        opm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buf)?;
    }

    if spagenumber != P_NONE {
        let (saction, sbuf) = XLogReadBufferForRedo(record, 2)?;
        if saction == BLK_NEEDS_REDO {
            // SAFETY: pin + exclusive lock per the redo protocol.
            let mut spm = unsafe { page_mut(sbuf) };
            let mut spageop = page_opaque(&spm.as_ref());
            spageop.btpo_prev = rightpagenumber;
            write_opaque(&mut spm, &spageop);
            spm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(sbuf)?;
        }
        if sbuf != InvalidBuffer {
            unlock_release(sbuf)?;
        }
    }

    unlock_release(rbuf)?;
    if buf != InvalidBuffer {
        unlock_release(buf)?;
    }
    Ok(())
}

fn btree_xlog_newroot(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let level = u32::from_ne_bytes(xlrec[4..8].try_into().unwrap());

    let buffer = XLogInitBufferForRedo(record, 0)?;
    {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        bt_pageinit(&mut pm);
        let mut flags = BTP_ROOT;
        if level == 0 {
            flags |= BTP_LEAF;
        }
        write_opaque(
            &mut pm,
            &BTPageOpaqueData {
                btpo_prev: P_NONE,
                btpo_next: P_NONE,
                btpo_level: level,
                btpo_flags: flags,
                btpo_cycleid: 0,
            },
        );

        if level > 0 {
            bt_restore_page(&mut pm, block_data(record, 0))?;
            bt_clear_incomplete_split(record, 1)?;
        }

        pm.set_lsn(lsn);
    }
    bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    unlock_release(buffer)?;

    bt_restore_meta(record, 2)
}

pub fn btree_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record.record.as_ref().expect("btree_redo with no decoded record").xl_info
        & !XLR_INFO_MASK;
    match info {
        XLOG_BTREE_INSERT_LEAF => btree_xlog_insert(true, false, false, record),
        XLOG_BTREE_INSERT_UPPER => btree_xlog_insert(false, false, false, record),
        XLOG_BTREE_INSERT_META => btree_xlog_insert(false, true, false, record),
        XLOG_BTREE_SPLIT_L => btree_xlog_split(true, record),
        XLOG_BTREE_SPLIT_R => btree_xlog_split(false, record),
        XLOG_BTREE_NEWROOT => btree_xlog_newroot(record),
        XLOG_BTREE_INSERT_POST => btree_xlog_insert(true, false, true, record),
        XLOG_BTREE_DEDUP => {
            panic!("btree_redo arm not ported: btree_xlog_dedup — land backend-access-nbt-dedup")
        }
        XLOG_BTREE_VACUUM => {
            panic!("btree_redo arm not ported: btree_xlog_vacuum — land backend-access-nbtree-vacuum")
        }
        XLOG_BTREE_DELETE => {
            panic!("btree_redo arm not ported: btree_xlog_delete — land backend-access-nbtree-vacuum")
        }
        XLOG_BTREE_MARK_PAGE_HALFDEAD => {
            panic!("btree_redo arm not ported: btree_xlog_mark_page_halfdead — land backend-access-nbtree-vacuum")
        }
        XLOG_BTREE_UNLINK_PAGE | XLOG_BTREE_UNLINK_PAGE_META => {
            panic!("btree_redo arm not ported: btree_xlog_unlink_page — land backend-access-nbtree-vacuum")
        }
        XLOG_BTREE_REUSE_PAGE => {
            panic!("btree_redo arm not ported: btree_xlog_reuse_page — land backend-access-nbtree-vacuum")
        }
        XLOG_BTREE_META_CLEANUP => {
            panic!("btree_redo arm not ported: _bt_restore_meta cleanup — land backend-access-nbtree-vacuum")
        }
        other => Err(panic_err(format!("btree_redo: unknown op code {other}"))),
    }
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
