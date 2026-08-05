//! nbtxlog.c — btree rmgr redo. Live arms cover what the write side (nbtree
//! insert + dedup + vacuum lanes) emits: INSERT_LEAF/UPPER/META/POST,
//! SPLIT_L/R, NEWROOT, DEDUP, VACUUM, DELETE, MARK_PAGE_HALFDEAD,
//! UNLINK_PAGE(_META), REUSE_PAGE, META_CLEANUP. Hot-standby conflict points
//! are loud panics naming their C function and owning unit.

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

/// DST fault-sweep RED hook (sim-cfg only, zero native surface): when armed,
/// XLOG_BTREE_VACUUM redo keeps the page fully valid but SKIPS the item
/// deletions — a deliberately weakened vacuum-content redo (stale index
/// entries silently survive replay; structure stays walkable). The crash
/// sweep's LP-reuse red leg arms this and must CATCH it SILENTLY through the
/// index key-vs-heap-value / coverage properties alone (the V-O1 silent-only
/// leg), never through a replay failure.
#[cfg(pgrust_sim)]
pub mod sim_red {
    use core::sync::atomic::{AtomicBool, Ordering::Relaxed};
    pub static KEEP_VACUUMED_ITEMS: AtomicBool = AtomicBool::new(false);
    pub fn armed() -> bool {
        KEEP_VACUUMED_ITEMS.load(Relaxed)
    }
}

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

#[track_caller]
#[cold]
fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(types_error::PANIC, msg))
}

#[track_caller]
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

        let raw = bufmgr_seams::buffer_get_page::call(buf);
        // SAFETY: pinned + exclusively locked; reads only until the restore
        // memcpy below ends this borrow.
        let origpage = unsafe { PageRef::from_raw(raw) };
        let oopaque = page_opaque(&origpage);

        // posting-split coincidence: reconstruct newitem + nposting by
        // re-running the primary's _bt_swap_posting against oposting at
        // newitemoff - 1 (as btree_xlog_split does).
        #[repr(C, align(8))]
        struct ItupImage([u8; BLCKSZ]);
        // (newitem, nposting) scratch exists only for posting-split redo; a
        // plain split must not pay the 16KB zero-fill.
        let mut swap_imgs: Option<(ItupImage, ItupImage)> = None;
        let mut nposting_sz = 0usize;
        let replacepostingoff: u16 = if postingoff != 0 { newitemoff - 1 } else { 0 };

        let mut newitem: &[u8] = &[];
        if newitemonleft || postingoff != 0 {
            let newitemsz = maxalign(itup_size_at(datapos, 0));
            newitem = &datapos[..newitemsz];
            datapos = &datapos[newitemsz..];

            if postingoff != 0 {
                let itemid = origpage.item_id(replacepostingoff);
                let opos_off = itemid.lp_off() as usize;
                nposting_sz =
                    (u16_le_native(origpage, opos_off + 6) & INDEX_SIZE_MASK) as usize;
                let (ni, np) =
                    swap_imgs.insert((ItupImage([0u8; BLCKSZ]), ItupImage([0u8; BLCKSZ])));
                ni.0[..newitemsz].copy_from_slice(newitem);
                // SAFETY: in-bounds page item read under the redo lock.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        origpage.as_ptr().add(opos_off),
                        np.0.as_mut_ptr(),
                        nposting_sz,
                    );
                }
                bt_swap_posting(
                    &mut ni.0[..newitemsz],
                    &mut np.0[..nposting_sz],
                    postingoff as usize,
                )?;
                newitem = &swap_imgs.as_ref().unwrap().0 .0[..newitemsz];
            }
        }

        let left_hikeysz = maxalign(itup_size_at(datapos, 0));
        let left_hikey = &datapos[..left_hikeysz];
        datapos = &datapos[left_hikeysz..];
        debug_assert!(datapos.is_empty());

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
            if postingoff != 0 && off == replacepostingoff {
                debug_assert!(newitemonleft || firstrightoff == newitemoff);
                let nposting = &swap_imgs.as_ref().unwrap().1;
                let np_sz = maxalign(itup_size_at(&nposting.0, 0));
                debug_assert!(np_sz == maxalign(nposting_sz));
                if leftpage.add_item(&nposting.0[..np_sz], leftoff, 0).is_none() {
                    return Err(error_err(
                        "failed to add new posting list item to left page after split".into(),
                    ));
                }
                leftoff += 1;
                off += 1;
                continue; // don't insert oposting
            }

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

fn btree_xlog_dedup(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let nintervals = u16::from_ne_bytes(main_data(record)[0..2].try_into().unwrap()) as usize;

    let (action, buf) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let intervals = block_data(record, 0);
        let interval_at = |i: usize| {
            (
                u16::from_ne_bytes(intervals[i * 4..i * 4 + 2].try_into().unwrap()),
                u16::from_ne_bytes(intervals[i * 4 + 2..i * 4 + 4].try_into().unwrap()),
            )
        };

        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let pm = unsafe { page_mut(buf) };
        let page = pm.as_ref();
        let opaque = page_opaque(&page);

        // conservatively larger maxpostingsize than the primary
        let mut state = types_nbtree::dedup::BTDedupState::new(types_nbtree::BTMaxItemSize);

        let minoff = P_FIRSTDATAKEY(&opaque);
        let maxoff = page.max_offset_number();

        // PageGetTempPageCopySpecial
        #[repr(align(8))]
        struct TempPage([u8; BLCKSZ]);
        let mut temp = TempPage([0u8; BLCKSZ]);
        // SAFETY: owned, aligned BLCKSZ scratch.
        let mut newpage = unsafe {
            PageMut::from_raw(core::ptr::NonNull::new(temp.0.as_mut_ptr()).unwrap())
        };
        bt_pageinit(&mut newpage);
        write_opaque(&mut newpage, &opaque);

        if !types_nbtree::P_RIGHTMOST(&opaque) {
            let hitemid = page.item_id(P_HIKEY);
            let (ptr, len) = page.item_raw(hitemid);
            // SAFETY: in-page tuple image under the pin + lock.
            let hitem = unsafe { core::slice::from_raw_parts(ptr, len as usize) };
            if newpage.add_item(hitem, P_HIKEY, 0).is_none() {
                return Err(error_err("deduplication failed to add highkey".into()));
            }
        }

        for offnum in minoff..=maxoff {
            let itemid = page.item_id(offnum);
            let (itup, _) = page.item_raw(itemid);

            // SAFETY: itup is a live on-page tuple; the temp page is separate
            // storage so base pointers stay valid across finish_pending.
            unsafe {
                if offnum == minoff {
                    state.start_pending(itup, offnum);
                } else if state.nintervals < nintervals
                    && state.baseoff == interval_at(state.nintervals).0
                    && state.nitems < interval_at(state.nintervals).1 as usize
                {
                    if !state.save_htid(itup) {
                        return Err(error_err(
                            "deduplication failed to add heap tid to pending posting list"
                                .into(),
                        ));
                    }
                } else {
                    if state.finish_pending(&mut newpage).is_err() {
                        return Err(error_err(
                            "deduplication failed to add tuple to page".into(),
                        ));
                    }
                    state.start_pending(itup, offnum);
                }
            }
        }
        // SAFETY: as above.
        if unsafe { state.finish_pending(&mut newpage) }.is_err() {
            return Err(error_err("deduplication failed to add tuple to page".into()));
        }
        debug_assert!(state.nintervals == nintervals);
        debug_assert!(state.intervals_bytes() == intervals);

        if types_nbtree::P_HAS_GARBAGE(&opaque) {
            let mut nopaque = page_opaque(&newpage.as_ref());
            nopaque.btpo_flags &= !types_nbtree::BTP_HAS_GARBAGE;
            write_opaque(&mut newpage, &nopaque);
        }

        // PageRestoreTempPage
        // SAFETY: whole-page overwrite under the exclusive redo lock.
        unsafe {
            core::ptr::copy_nonoverlapping(
                temp.0.as_ptr(),
                page.as_ptr().cast_mut(),
                BLCKSZ,
            )
        };
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buf) };
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buf)?;
    }

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

fn tup_tinfo(b: &[u8]) -> u16 {
    u16::from_ne_bytes([b[6], b[7]])
}

fn tup_posid(b: &[u8]) -> u16 {
    u16::from_ne_bytes([b[4], b[5]])
}

fn tup_is_posting(b: &[u8]) -> bool {
    (tup_tinfo(b) & types_nbtree::INDEX_ALT_TID_MASK) != 0
        && (tup_posid(b) & types_nbtree::BT_IS_POSTING) != 0
}

fn tup_nposting(b: &[u8]) -> usize {
    debug_assert!(tup_is_posting(b));
    (tup_posid(b) & types_nbtree::BT_OFFSET_MASK) as usize
}

fn tup_posting_offset(b: &[u8]) -> usize {
    debug_assert!(tup_is_posting(b));
    ((u16::from_ne_bytes([b[0], b[1]]) as usize) << 16)
        | u16::from_ne_bytes([b[2], b[3]]) as usize
}

// _bt_update_posting over raw tuple bytes: write the replacement image
// (original minus deletetids posting entries) into `out`, returning its size.
fn xlog_update_posting(orig: &[u8], deletetids: &[u8], out: &mut [u8]) -> usize {
    let ndeleted = deletetids.len() / 2;
    let norig = tup_nposting(orig);
    let nhtids = norig - ndeleted;
    debug_assert!(nhtids > 0 && nhtids < norig);

    let keysize = tup_posting_offset(orig);
    let newsize = if nhtids > 1 {
        maxalign(keysize + nhtids * 6)
    } else {
        keysize
    };

    out[..newsize].fill(0);
    out[..keysize].copy_from_slice(&orig[..keysize]);
    let info = (tup_tinfo(out) & !INDEX_SIZE_MASK) | newsize as u16;

    let htids_off = if nhtids > 1 {
        out[6..8].copy_from_slice(
            &(info | types_nbtree::INDEX_ALT_TID_MASK).to_ne_bytes(),
        );
        let posid = nhtids as u16 | types_nbtree::BT_IS_POSTING;
        out[4..6].copy_from_slice(&posid.to_ne_bytes());
        out[0..2].copy_from_slice(&((keysize >> 16) as u16).to_ne_bytes());
        out[2..4].copy_from_slice(&((keysize & 0xffff) as u16).to_ne_bytes());
        keysize
    } else {
        out[6..8].copy_from_slice(&(info & !types_nbtree::INDEX_ALT_TID_MASK).to_ne_bytes());
        0
    };

    let posting_base = tup_posting_offset(orig);
    let mut ui = 0usize;
    let mut d = 0usize;
    for i in 0..norig {
        if d < ndeleted
            && u16::from_ne_bytes([deletetids[d * 2], deletetids[d * 2 + 1]]) as usize == i
        {
            d += 1;
            continue;
        }
        let src = posting_base + i * 6;
        let dst = htids_off + ui * 6;
        let tid: [u8; 6] = orig[src..src + 6].try_into().unwrap();
        out[dst..dst + 6].copy_from_slice(&tid);
        ui += 1;
    }
    debug_assert!(ui == nhtids && d == ndeleted);
    newsize
}

// btree_xlog_updates: apply the xl_btree_update stream to the page.
fn btree_xlog_updates(
    pm: &mut PageMut<'_>,
    updatedoffsets: &[u8],
    mut updates: &[u8],
    nupdated: usize,
) -> PgResult<()> {
    let mut scratch = [0u8; BLCKSZ];
    for i in 0..nupdated {
        let offnum =
            u16::from_ne_bytes([updatedoffsets[i * 2], updatedoffsets[i * 2 + 1]]);
        let ndeletedtids = u16::from_ne_bytes([updates[0], updates[1]]) as usize;
        let deletetids = &updates[2..2 + ndeletedtids * 2];

        let orig = {
            let page = pm.as_ref();
            let id = page.item_id(offnum);
            let (ptr, len) = page.item_raw(id);
            // SAFETY: in-page tuple bytes under the redo exclusive lock; the
            // borrow ends before index_tuple_overwrite mutates the page.
            unsafe { core::slice::from_raw_parts(ptr, len as usize) }
        };
        let newsize = xlog_update_posting(orig, deletetids, &mut scratch);
        let img_len = maxalign(newsize);
        let img = &scratch[..img_len];
        if !pm.index_tuple_overwrite(offnum, img) {
            return Err(panic_err("failed to update partially dead item".into()));
        }

        updates = &updates[2 + ndeletedtids * 2..];
    }
    Ok(())
}

fn btree_xlog_vacuum(record: &mut XLogReaderState) -> PgResult<()> {
    btree_xlog_vacuum_or_delete(record, true)
}

fn btree_xlog_delete(record: &mut XLogReaderState) -> PgResult<()> {
    if xlogutils::InHotStandby() {
        let xlrec = main_data(record);
        let horizon = u32::from_ne_bytes(xlrec[0..4].try_into().unwrap());
        let is_catalog_rel = xlrec[8] != 0;
        let (rlocator, _, _, _) =
            record.block_tag_extended(0).expect("btree_xlog_delete: no block 0");
        standby::ResolveRecoveryConflictWithSnapshot(horizon, is_catalog_rel, rlocator)?;
    }
    btree_xlog_vacuum_or_delete(record, false)
}

fn btree_xlog_vacuum_or_delete(record: &mut XLogReaderState, is_vacuum: bool) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let (ndeleted, nupdated) = if is_vacuum {
        (
            u16::from_ne_bytes(xlrec[0..2].try_into().unwrap()) as usize,
            u16::from_ne_bytes(xlrec[2..4].try_into().unwrap()) as usize,
        )
    } else {
        (
            u16::from_ne_bytes(xlrec[4..6].try_into().unwrap()) as usize,
            u16::from_ne_bytes(xlrec[6..8].try_into().unwrap()) as usize,
        )
    };

    // VACUUM takes a cleanup lock here, as btvacuumpage (nbtree/README).
    let (action, buffer) = xlogutils::XLogReadBufferForRedoExtended(
        record,
        0,
        types_storage::ReadBufferMode::Normal,
        is_vacuum,
    )?;
    if action == BLK_NEEDS_REDO {
        let ptr = block_data(record, 0);
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };

        if nupdated > 0 {
            let updatedoffsets = &ptr[ndeleted * 2..ndeleted * 2 + nupdated * 2];
            let updates = &ptr[ndeleted * 2 + nupdated * 2..];
            btree_xlog_updates(&mut pm, updatedoffsets, updates, nupdated)?;
        }

        // DST RED (sim-cfg only): the deliberately weakened vacuum redo —
        // stale entries kept, everything else applied. See sim_red.
        #[cfg(pgrust_sim)]
        let ndeleted = if is_vacuum && crate::sim_red::armed() { 0 } else { ndeleted };

        if ndeleted > 0 {
            let mut offsets = [0 as OffsetNumber; MaxIndexTuplesPerPage];
            for (i, off) in offsets[..ndeleted].iter_mut().enumerate() {
                *off = u16::from_ne_bytes([ptr[i * 2], ptr[i * 2 + 1]]);
            }
            pm.index_multi_delete(&offsets[..ndeleted]);
        }

        let mut opaque = page_opaque(&pm.as_ref());
        if is_vacuum {
            opaque.btpo_cycleid = 0;
        }
        opaque.btpo_flags &= !types_nbtree::BTP_HAS_GARBAGE;
        write_opaque(&mut pm, &opaque);

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn btree_xlog_mark_page_halfdead(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let poffset = u16::from_ne_bytes(xlrec[0..2].try_into().unwrap());
    let leftblk = u32::from_ne_bytes(xlrec[8..12].try_into().unwrap());
    let rightblk = u32::from_ne_bytes(xlrec[12..16].try_into().unwrap());
    let topparent = u32::from_ne_bytes(xlrec[16..20].try_into().unwrap());

    let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };

        let nextoffset = poffset + 1;
        let rightsib = {
            let page = pm.as_ref();
            let id = page.item_id(nextoffset);
            let (ptr, _) = page.item_raw(id);
            // SAFETY: pivot tuple's downlink block number (t_tid bytes 0..4).
            unsafe {
                ((ptr.cast::<u16>().read() as u32) << 16)
                    | ptr.add(2).cast::<u16>().read() as u32
            }
        };
        {
            let page = pm.as_ref();
            let id = page.item_id(poffset);
            let (ptr, _) = page.item_raw(id);
            // SAFETY: same-page pivot under the exclusive redo lock; in-place
            // 4-byte downlink store.
            unsafe {
                let p = ptr.cast_mut();
                p.cast::<u16>().write((rightsib >> 16) as u16);
                p.add(2).cast::<u16>().write((rightsib & 0xffff) as u16);
            }
        }
        pm.index_tuple_delete(nextoffset);

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    let buffer = XLogInitBufferForRedo(record, 0)?;
    {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        bt_pageinit(&mut pm);
        write_opaque(
            &mut pm,
            &BTPageOpaqueData {
                btpo_prev: leftblk,
                btpo_next: rightblk,
                btpo_level: 0,
                btpo_flags: types_nbtree::BTP_HALF_DEAD | BTP_LEAF,
                btpo_cycleid: 0,
            },
        );

        let trunctuple = trunc_hikey(topparent);
        if pm.add_item(&trunctuple, P_HIKEY, 0).is_none() {
            return Err(error_err("could not add dummy high key to half-dead page".into()));
        }

        pm.set_lsn(lsn);
    }
    bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    unlock_release(buffer)
}

// The 8-byte truncated high key holding a top-parent link
// (BTreeTupleSetTopParent over a MemSet IndexTupleData).
fn trunc_hikey(topparent: u32) -> [u8; 8] {
    let mut b = [0u8; 8];
    b[0..2].copy_from_slice(&((topparent >> 16) as u16).to_ne_bytes());
    b[2..4].copy_from_slice(&((topparent & 0xffff) as u16).to_ne_bytes());
    let info = 8u16 | types_nbtree::INDEX_ALT_TID_MASK;
    b[6..8].copy_from_slice(&info.to_ne_bytes());
    b
}

fn btree_xlog_unlink_page(info: u8, record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let leftsib = u32::from_ne_bytes(xlrec[0..4].try_into().unwrap());
    let rightsib = u32::from_ne_bytes(xlrec[4..8].try_into().unwrap());
    let level = u32::from_ne_bytes(xlrec[8..12].try_into().unwrap());
    let safexid = u64::from_ne_bytes(xlrec[16..24].try_into().unwrap());
    let leafleftsib = u32::from_ne_bytes(xlrec[24..28].try_into().unwrap());
    let leafrightsib = u32::from_ne_bytes(xlrec[28..32].try_into().unwrap());
    let leaftopparent = u32::from_ne_bytes(xlrec[32..36].try_into().unwrap());
    let isleaf = level == 0;

    let mut leftbuf = InvalidBuffer;
    if leftsib != P_NONE {
        let (action, buf) = XLogReadBufferForRedo(record, 1)?;
        leftbuf = buf;
        if action == BLK_NEEDS_REDO {
            // SAFETY: pin + exclusive lock per the redo protocol.
            let mut pm = unsafe { page_mut(buf) };
            let mut opaque = page_opaque(&pm.as_ref());
            opaque.btpo_next = rightsib;
            write_opaque(&mut pm, &opaque);
            pm.set_lsn(lsn);
            bufmgr_seams::mark_buffer_dirty::call(buf)?;
        }
    }

    let target = XLogInitBufferForRedo(record, 0)?;
    {
        // SAFETY: pin + exclusive lock per the redo protocol.
        let mut pm = unsafe { page_mut(target) };
        bt_pageinit(&mut pm);
        write_opaque(
            &mut pm,
            &BTPageOpaqueData {
                btpo_prev: leftsib,
                btpo_next: rightsib,
                btpo_level: level,
                btpo_flags: if isleaf { BTP_LEAF } else { 0 },
                btpo_cycleid: 0,
            },
        );
        page_set_deleted(&mut pm, safexid);

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(target)?;
    }

    let (action, rightbuf) = XLogReadBufferForRedo(record, 2)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol.
        let mut pm = unsafe { page_mut(rightbuf) };
        let mut opaque = page_opaque(&pm.as_ref());
        opaque.btpo_prev = leftsib;
        write_opaque(&mut pm, &opaque);
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(rightbuf)?;
    }

    if leftbuf != InvalidBuffer {
        unlock_release(leftbuf)?;
    }
    if rightbuf != InvalidBuffer {
        unlock_release(rightbuf)?;
    }
    unlock_release(target)?;

    if record.block_tag_extended(3).is_some() {
        debug_assert!(!isleaf);
        let leafbuf = XLogInitBufferForRedo(record, 3)?;
        {
            // SAFETY: pin + exclusive lock per the redo protocol.
            let mut pm = unsafe { page_mut(leafbuf) };
            bt_pageinit(&mut pm);
            write_opaque(
                &mut pm,
                &BTPageOpaqueData {
                    btpo_prev: leafleftsib,
                    btpo_next: leafrightsib,
                    btpo_level: 0,
                    btpo_flags: types_nbtree::BTP_HALF_DEAD | BTP_LEAF,
                    btpo_cycleid: 0,
                },
            );

            let trunctuple = trunc_hikey(leaftopparent);
            if pm.add_item(&trunctuple, P_HIKEY, 0).is_none() {
                return Err(error_err(
                    "could not add dummy high key to half-dead page".into(),
                ));
            }
            pm.set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(leafbuf)?;
        unlock_release(leafbuf)?;
    }

    if info == XLOG_BTREE_UNLINK_PAGE_META {
        bt_restore_meta(record, 4)?;
    }
    Ok(())
}

// BTPageSetDeleted over a freshly initialized page image.
fn page_set_deleted(pm: &mut PageMut<'_>, safexid: u64) {
    let mut opaque = page_opaque(&pm.as_ref());
    opaque.btpo_flags &= !types_nbtree::BTP_HALF_DEAD;
    opaque.btpo_flags |= types_nbtree::BTP_DELETED | types_nbtree::BTP_HAS_FULLXID;
    write_opaque(pm, &opaque);
    let contents_off = maxalign(SizeOfPageHeaderData);
    pm.set_pd_lower((contents_off + 8) as u16);
    pm.set_pd_upper(pm.as_ref().pd_special());
    // SAFETY: PageGetContents, 8-aligned, 8B in-bounds under the redo lock.
    unsafe {
        pm.as_ref()
            .as_ptr()
            .cast_mut()
            .add(contents_off)
            .cast::<u64>()
            .write(safexid)
    };
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
        XLOG_BTREE_DEDUP => btree_xlog_dedup(record),
        XLOG_BTREE_VACUUM => btree_xlog_vacuum(record),
        XLOG_BTREE_DELETE => btree_xlog_delete(record),
        XLOG_BTREE_MARK_PAGE_HALFDEAD => btree_xlog_mark_page_halfdead(record),
        XLOG_BTREE_UNLINK_PAGE | XLOG_BTREE_UNLINK_PAGE_META => {
            btree_xlog_unlink_page(info, record)
        }
        XLOG_BTREE_REUSE_PAGE => {
            // Conflict point for hot standby only; nothing to replay.
            if xlogutils::InHotStandby() {
                let xlrec = main_data(record);
                let locator = types_storage::RelFileLocator::new(
                    u32::from_ne_bytes(xlrec[0..4].try_into().unwrap()),
                    u32::from_ne_bytes(xlrec[4..8].try_into().unwrap()),
                    u32::from_ne_bytes(xlrec[8..12].try_into().unwrap()),
                );
                let horizon = types_core::FullTransactionId::from_u64(u64::from_ne_bytes(
                    xlrec[16..24].try_into().unwrap(),
                ));
                let is_catalog_rel = xlrec[24] != 0;
                standby::ResolveRecoveryConflictWithSnapshotFullXid(
                    horizon,
                    is_catalog_rel,
                    locator,
                )?;
            }
            Ok(())
        }
        XLOG_BTREE_META_CLEANUP => bt_restore_meta(record, 0),
        other => Err(panic_err(format!("btree_redo: unknown op code {other}"))),
    }
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
