//! heapam_xlog.c — heap/heap2 rmgr redo. Live arms: INSERT, DELETE, UPDATE,
//! HOT_UPDATE, CONFIRM, LOCK, INPLACE, MULTI_INSERT, LOCK_UPDATED,
//! PRUNE_ON_ACCESS/PRUNE_VACUUM_SCAN/PRUNE_VACUUM_CLEANUP, VISIBLE (the
//! shapes our write side emits plus what C 18.3 writes into a shared datadir)
//! and the C no-op arms (TRUNCATE, HEAP2_NEW_CID). Everything else is a loud
//! panic naming its op.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{Buffer, InvalidBuffer, OffsetNumber, TransactionId, BLCKSZ};
use types_error::{PgError, PgResult};
use types_storage::bufpage::{
    MaxHeapTupleSize, MaxHeapTuplesPerPage, PageMut, SizeofHeapTupleHeader, PAI_IS_HEAP,
    PAI_OVERWRITE,
};
use types_tuple::{
    HeapTupleHeaderData, ItemPointerData, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_XMAX_BITS,
    HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_IS_LOCKED_ONLY, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK,
    HEAP_XMAX_LOCK_ONLY,
};
use xlogreader_seams::XLogReaderState;
use xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo, BLK_NEEDS_REDO};

pub const XLOG_HEAP_INSERT: u8 = 0x00;
pub const XLOG_HEAP_DELETE: u8 = 0x10;
pub const XLOG_HEAP_UPDATE: u8 = 0x20;
pub const XLOG_HEAP_TRUNCATE: u8 = 0x30;
pub const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
pub const XLOG_HEAP_CONFIRM: u8 = 0x50;
pub const XLOG_HEAP_LOCK: u8 = 0x60;
pub const XLOG_HEAP_INPLACE: u8 = 0x70;
pub const XLOG_HEAP_OPMASK: u8 = 0x70;
pub const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

pub const XLOG_HEAP2_REWRITE: u8 = 0x00;
pub const XLOG_HEAP2_PRUNE_ON_ACCESS: u8 = 0x10;
pub const XLOG_HEAP2_PRUNE_VACUUM_SCAN: u8 = 0x20;
pub const XLOG_HEAP2_PRUNE_VACUUM_CLEANUP: u8 = 0x30;
pub const XLOG_HEAP2_VISIBLE: u8 = 0x40;
pub const XLOG_HEAP2_MULTI_INSERT: u8 = 0x50;
pub const XLOG_HEAP2_LOCK_UPDATED: u8 = 0x60;
pub const XLOG_HEAP2_NEW_CID: u8 = 0x70;

pub const XLH_INSERT_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_INSERT_CONTAINS_NEW_TUPLE: u8 = 1 << 3;
pub const XLH_INSERT_ON_TOAST_RELATION: u8 = 1 << 4;
pub const XLH_INSERT_ALL_FROZEN_SET: u8 = 1 << 5;
pub const XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED: u8 = 1 << 1;
pub const XLH_UPDATE_CONTAINS_OLD_TUPLE: u8 = 1 << 2;
pub const XLH_UPDATE_CONTAINS_OLD_KEY: u8 = 1 << 3;
pub const XLH_UPDATE_CONTAINS_NEW_TUPLE: u8 = 1 << 4;
pub const XLH_UPDATE_PREFIX_FROM_OLD: u8 = 1 << 5;
pub const XLH_UPDATE_SUFFIX_FROM_OLD: u8 = 1 << 6;
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_DELETE_CONTAINS_OLD_TUPLE: u8 = 1 << 1;
pub const XLH_DELETE_CONTAINS_OLD_KEY: u8 = 1 << 2;
pub const XLH_DELETE_IS_SUPER: u8 = 1 << 3;
pub const XLH_DELETE_IS_PARTITION_MOVE: u8 = 1 << 4;
pub const XLH_LOCK_ALL_FROZEN_CLEARED: u8 = 1 << 0;

// xl_heap_new_cid (heapam_xlog.h); the record payload is the SizeOfHeapNewCid
// prefix (trailing tid padding excluded).
#[repr(C)]
pub struct XlHeapNewCid {
    pub top_xid: TransactionId,
    pub cmin: u32,
    pub cmax: u32,
    pub combocid: u32,
    pub target_locator: types_storage::RelFileLocator,
    pub target_tid: ItemPointerData,
}

pub const SizeOfHeapNewCid: usize =
    core::mem::offset_of!(XlHeapNewCid, target_tid) + core::mem::size_of::<ItemPointerData>();
const _: () = assert!(core::mem::offset_of!(XlHeapNewCid, target_locator) == 16);
const _: () = assert!(SizeOfHeapNewCid == 34);

pub const XLHP_IS_CATALOG_REL: u8 = 1 << 1;
pub const XLHP_CLEANUP_LOCK: u8 = 1 << 2;
pub const XLHP_HAS_CONFLICT_HORIZON: u8 = 1 << 3;
pub const XLHP_HAS_FREEZE_PLANS: u8 = 1 << 4;
pub const XLHP_HAS_REDIRECTIONS: u8 = 1 << 5;
pub const XLHP_HAS_DEAD_ITEMS: u8 = 1 << 6;
pub const XLHP_HAS_NOW_UNUSED_ITEMS: u8 = 1 << 7;

pub const XLH_FREEZE_XVAC: u8 = 0x02;
pub const XLH_INVALID_XVAC: u8 = 0x04;

pub const XLHL_XMAX_IS_MULTI: u8 = 0x01;
pub const XLHL_XMAX_LOCK_ONLY: u8 = 0x02;
pub const XLHL_XMAX_EXCL_LOCK: u8 = 0x04;
pub const XLHL_XMAX_KEYSHR_LOCK: u8 = 0x08;
pub const XLHL_KEYS_UPDATED: u8 = 0x10;

const XLR_INFO_MASK: u8 = 0x0F;
const SizeOfHeapHeader: usize = 5;
const FirstCommandId: u32 = 0;

fn main_data<'a>(record: &'a XLogReaderState) -> &'a [u8] {
    let rec = record.record.as_ref().expect("heap redo with no decoded record");
    // SAFETY: points into the reader's decode buffer, valid for the redo
    // callback's duration.
    unsafe { rec.main_data_bytes() }
}

fn record_xid(record: &XLogReaderState) -> TransactionId {
    record.record.as_ref().expect("heap redo with no decoded record").xl_xid
}

fn panic_err(msg: String) -> Box<PgError> {
    Box::new(PgError::new(types_error::PANIC, msg))
}

// SAFETY contract shared by both redo arms: the buffer is pinned and
// exclusively locked (XLogReadBufferForRedo protocol), so the PageMut is the
// sole writer of the image until the unlock below.
unsafe fn page_mut<'p>(buffer: Buffer) -> PageMut<'p> {
    unsafe { PageMut::from_raw(bufmgr_seams::buffer_get_page::call(buffer)) }
}

fn unlock_release(buffer: Buffer) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(buffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;
    bufmgr_seams::release_buffer::call(buffer)
}

fn fix_infomask_from_infobits(infobits: u8, infomask: &mut u16, infomask2: &mut u16) {
    *infomask &= !(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
    *infomask2 &= !HEAP_KEYS_UPDATED;
    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        *infomask |= HEAP_XMAX_IS_MULTI;
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        *infomask |= HEAP_XMAX_LOCK_ONLY;
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        *infomask |= HEAP_XMAX_EXCL_LOCK;
    }
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        *infomask |= HEAP_XMAX_KEYSHR_LOCK;
    }
    if infobits & XLHL_KEYS_UPDATED != 0 {
        *infomask2 |= HEAP_KEYS_UPDATED;
    }
}

fn clear_vm_bits(
    rlocator: types_storage::RelFileLocator,
    blkno: types_core::BlockNumber,
    flags: u8,
) -> PgResult<()> {
    let reln = xlogutils::CreateFakeRelcacheEntry(rlocator);
    let mut vmbuffer = visibilitymap::VmBuffer::new();
    visibilitymap::visibilitymap_pin(&reln, blkno, &mut vmbuffer)?;
    visibilitymap::visibilitymap_clear(&reln, blkno, &vmbuffer, flags)?;
    vmbuffer.release();
    xlogutils::FreeFakeRelcacheEntry(reln);
    Ok(())
}

fn page_set_prunable(pm: &mut PageMut<'_>, xid: TransactionId) {
    debug_assert!(xid != 0);
    let old = pm.as_ref().prune_xid();
    if old == 0 || types_core::xact::TransactionIdPrecedes(xid, old) {
        pm.set_prune_xid(xid);
    }
}

fn heap_xlog_delete(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let xmax = u32::from_ne_bytes(xlrec[0..4].try_into().unwrap());
    let offnum = u16::from_ne_bytes(xlrec[4..6].try_into().unwrap());
    let infobits_set = xlrec[6];
    let flags = xlrec[7];

    let (target_locator, _fork, blkno, _) = record
        .block_tag_extended(0)
        .expect("heap_xlog_delete: no block 0");
    let target_tid = ItemPointerData::new(blkno, offnum);

    if flags & XLH_DELETE_ALL_VISIBLE_CLEARED != 0 {
        clear_vm_bits(target_locator, blkno, visibilitymap::VISIBILITYMAP_VALID_BITS)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        let page = pm.as_ref();

        let lp = if page.max_offset_number() >= offnum {
            Some(page.item_id(offnum))
        } else {
            None
        };
        let Some(lp) = lp.filter(|id| id.is_normal()) else {
            return Err(panic_err("invalid lp".into()));
        };

        let (ptr, len) = page.item_raw(lp);
        // SAFETY: in-page tuple image under the pin+lock; exclusive for this arm.
        let htup =
            unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) };
        let _ = len;

        htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
        htup.clear_hot_updated();
        let (mut im, mut im2) = (htup.t_infomask, htup.t_infomask2);
        fix_infomask_from_infobits(infobits_set, &mut im, &mut im2);
        htup.t_infomask = im;
        htup.t_infomask2 = im2;
        if flags & XLH_DELETE_IS_SUPER == 0 {
            htup.set_xmax(xmax);
        } else {
            htup.set_xmin(0);
        }
        htup.set_cmax(FirstCommandId, false);

        page_set_prunable(&mut pm, record_xid(record));

        if flags & XLH_DELETE_ALL_VISIBLE_CLEARED != 0 {
            pm.clear_all_visible();
        }

        if flags & XLH_DELETE_IS_PARTITION_MOVE != 0 {
            htup.set_moved_partitions();
        } else {
            htup.t_ctid = target_tid;
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn heap_xlog_insert(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = main_data(record);
    let offnum = u16::from_ne_bytes(xlrec[0..2].try_into().unwrap());
    let flags = xlrec[2];

    let (target_locator, _fork, blkno, _) = record
        .block_tag_extended(0)
        .expect("heap_xlog_insert: no block 0");
    let target_tid = ItemPointerData::new(blkno, offnum);

    debug_assert!(flags & XLH_INSERT_ALL_FROZEN_SET == 0);

    if flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
        clear_vm_bits(target_locator, blkno, visibilitymap::VISIBILITYMAP_VALID_BITS)?;
    }

    let info = record.record.as_ref().unwrap().xl_info & !XLR_INFO_MASK;
    let (action, buffer) = if info & XLOG_HEAP_INIT_PAGE != 0 {
        let buffer = XLogInitBufferForRedo(record, 0)?;
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        pm.init(0);
        (BLK_NEEDS_REDO, buffer)
    } else {
        XLogReadBufferForRedo(record, 0)?
    };

    let mut freespace = 0usize;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        if pm.as_ref().max_offset_number() + 1 < offnum {
            return Err(panic_err("invalid max offset number".into()));
        }

        let blk = record.block(0);
        debug_assert!(blk.has_data);
        // SAFETY: block data points into the decode buffer, live for this arm.
        let data = unsafe { blk.data_bytes() };
        debug_assert!(data.len() > SizeOfHeapHeader);
        let newlen = data.len() - SizeOfHeapHeader;
        debug_assert!(newlen <= MaxHeapTupleSize);

        // xl_heap_header { uint16 t_infomask2; uint16 t_infomask; uint8 t_hoff }
        let xl_infomask2 = u16::from_ne_bytes(data[0..2].try_into().unwrap());
        let xl_infomask = u16::from_ne_bytes(data[2..4].try_into().unwrap());
        let xl_hoff = data[4];

        #[repr(align(8))]
        struct TBuf([u8; MaxHeapTupleSize + SizeofHeapTupleHeader]);
        let mut tbuf = TBuf([0u8; MaxHeapTupleSize + SizeofHeapTupleHeader]);
        tbuf.0[SizeofHeapTupleHeader..SizeofHeapTupleHeader + newlen]
            .copy_from_slice(&data[SizeOfHeapHeader..]);
        let tuple_len = SizeofHeapTupleHeader + newlen;
        {
            // SAFETY: 8-aligned zeroed buffer at least header-sized.
            let htup = unsafe { &mut *(tbuf.0.as_mut_ptr().cast::<HeapTupleHeaderData>()) };
            htup.t_infomask2 = xl_infomask2;
            htup.t_infomask = xl_infomask;
            htup.t_hoff = xl_hoff;
            htup.set_xmin(record_xid(record));
            htup.set_cmin(FirstCommandId);
            htup.t_ctid = target_tid;
        }

        if pm
            .add_item(&tbuf.0[..tuple_len], offnum, PAI_OVERWRITE | PAI_IS_HEAP)
            .is_none()
        {
            return Err(panic_err("failed to add tuple".into()));
        }

        freespace = pm.as_ref().heap_free_space();

        pm.set_lsn(lsn);

        if flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
            pm.clear_all_visible();
        }

        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    // C updates the FSM only when the page fills past 80% and the block was
    // not restored from a full-page image.
    if action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5 {
        freespace::XLogRecordPageWithFreeSpace(target_locator, blkno, freespace)?;
    }
    Ok(())
}

fn heap_xlog_multi_insert(record: &mut XLogReaderState) -> PgResult<()> {
    const SizeOfHeapMultiInsert: usize = 4;
    const SizeOfMultiInsertTuple: usize = 7;

    let lsn = record.EndRecPtr;
    let xlrec = main_data(record).to_vec();
    let flags = xlrec[0];
    let ntuples = u16::from_ne_bytes(xlrec[2..4].try_into().unwrap()) as usize;

    let (target_locator, _fork, blkno, _) = record
        .block_tag_extended(0)
        .expect("heap_xlog_multi_insert: no block 0");

    debug_assert!(
        !(flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 && flags & XLH_INSERT_ALL_FROZEN_SET != 0)
    );

    if flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
        clear_vm_bits(target_locator, blkno, visibilitymap::VISIBILITYMAP_VALID_BITS)?;
    }

    let info = record.record.as_ref().unwrap().xl_info & !XLR_INFO_MASK;
    let isinit = info & XLOG_HEAP_INIT_PAGE != 0;
    let (action, buffer) = if isinit {
        let buffer = XLogInitBufferForRedo(record, 0)?;
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        pm.init(0);
        (BLK_NEEDS_REDO, buffer)
    } else {
        XLogReadBufferForRedo(record, 0)?
    };

    let mut freespace = 0usize;
    if action == BLK_NEEDS_REDO {
        let blk = record.block(0);
        debug_assert!(blk.has_data);
        // SAFETY: block data points into the decode buffer, live for this arm.
        let tupdata = unsafe { blk.data_bytes() }.to_vec();

        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };

        let mut off = 0usize;
        for i in 0..ntuples {
            let offnum = if isinit {
                i as u16 + 1
            } else {
                u16::from_ne_bytes(
                    xlrec[SizeOfHeapMultiInsert + 2 * i..SizeOfHeapMultiInsert + 2 * i + 2]
                        .try_into()
                        .unwrap(),
                )
            };
            if pm.as_ref().max_offset_number() + 1 < offnum {
                return Err(panic_err("invalid max offset number".into()));
            }

            // xl_multi_insert_tuple is SHORTALIGNed relative to the block
            // data base (matches the writer's scratch-relative padding).
            off = (off + 1) & !1;
            let datalen =
                u16::from_ne_bytes(tupdata[off..off + 2].try_into().unwrap()) as usize;
            let xl_infomask2 = u16::from_ne_bytes(tupdata[off + 2..off + 4].try_into().unwrap());
            let xl_infomask = u16::from_ne_bytes(tupdata[off + 4..off + 6].try_into().unwrap());
            let xl_hoff = tupdata[off + 6];
            off += SizeOfMultiInsertTuple;
            debug_assert!(datalen <= MaxHeapTupleSize);

            #[repr(align(8))]
            struct TBuf([u8; MaxHeapTupleSize + SizeofHeapTupleHeader]);
            let mut tbuf = TBuf([0u8; MaxHeapTupleSize + SizeofHeapTupleHeader]);
            tbuf.0[SizeofHeapTupleHeader..SizeofHeapTupleHeader + datalen]
                .copy_from_slice(&tupdata[off..off + datalen]);
            off += datalen;
            let tuple_len = SizeofHeapTupleHeader + datalen;
            {
                // SAFETY: 8-aligned zeroed buffer at least header-sized.
                let htup = unsafe { &mut *(tbuf.0.as_mut_ptr().cast::<HeapTupleHeaderData>()) };
                htup.t_infomask2 = xl_infomask2;
                htup.t_infomask = xl_infomask;
                htup.t_hoff = xl_hoff;
                htup.set_xmin(record_xid(record));
                htup.set_cmin(FirstCommandId);
                htup.t_ctid = ItemPointerData::new(blkno, offnum);
            }

            if pm
                .add_item(&tbuf.0[..tuple_len], offnum, PAI_OVERWRITE | PAI_IS_HEAP)
                .is_none()
            {
                return Err(panic_err("failed to add tuple".into()));
            }
        }
        if off != tupdata.len() {
            return Err(panic_err("total tuple length mismatch".into()));
        }

        freespace = pm.as_ref().heap_free_space();

        pm.set_lsn(lsn);

        if flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
            pm.clear_all_visible();
        }
        if flags & XLH_INSERT_ALL_FROZEN_SET != 0 {
            pm.set_all_visible();
        }

        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    if action == BLK_NEEDS_REDO && freespace < BLCKSZ / 5 {
        freespace::XLogRecordPageWithFreeSpace(target_locator, blkno, freespace)?;
    }
    Ok(())
}

fn heap_xlog_inplace(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    // xl_heap_inplace layout (heapam_xlog.h).
    const MIN_SIZE_OF_HEAP_INPLACE: usize = 20;
    let xlrec = main_data(record).to_vec();
    let offnum = u16::from_ne_bytes(xlrec[0..2].try_into().unwrap());
    let db_id = u32::from_ne_bytes(xlrec[4..8].try_into().unwrap());
    let ts_id = u32::from_ne_bytes(xlrec[8..12].try_into().unwrap());
    let relcache_init_file_inval = xlrec[12] != 0;
    let nmsgs = i32::from_ne_bytes(xlrec[16..20].try_into().unwrap());

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        let blk = record.block(0);
        debug_assert!(blk.has_data);
        // SAFETY: block data points into the decode buffer, live for this arm.
        let newtup = unsafe { blk.data_bytes() };
        let newlen = newtup.len();

        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        let page = pm.as_ref();
        let lp = if page.max_offset_number() >= offnum {
            Some(page.item_id(offnum))
        } else {
            None
        };
        let Some(lp) = lp.filter(|id| id.is_normal()) else {
            return Err(panic_err("invalid lp".into()));
        };
        let (ptr, len) = page.item_raw(lp);
        // SAFETY: in-page tuple image under the pin+lock; exclusive for this arm.
        let hoff = unsafe { (*(ptr.cast::<HeapTupleHeaderData>())).t_hoff } as usize;
        if len as usize - hoff != newlen {
            return Err(panic_err("wrong tuple length".into()));
        }
        // SAFETY: destination within the item's storage, bounds checked above.
        unsafe {
            core::ptr::copy_nonoverlapping(newtup.as_ptr(), ptr.cast_mut().add(hoff), newlen);
        }
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }

    // C sends unconditionally; the port's shared-inval queue only attaches
    // for standby redo (xact_redo_commit's STANDBY_DISABLED guard shape).
    if xlogutils::standby_state() == xlogutils::STANDBY_DISABLED {
        return Ok(());
    }
    let mut msgs = Vec::with_capacity(nmsgs.max(0) as usize);
    for i in 0..nmsgs as usize {
        let off = MIN_SIZE_OF_HEAP_INPLACE + i * 16;
        let raw: [u8; 16] = xlrec[off..off + 16].try_into().unwrap();
        if let Some(m) = types_storage::SharedInvalidationMessage::from_wire_bytes(raw) {
            msgs.push(m);
        }
    }
    inval::eoxact::ProcessCommittedInvalidationMessages(
        &msgs,
        relcache_init_file_inval,
        db_id,
        ts_id,
    )
}

fn heap_xlog_update(record: &mut XLogReaderState, hot_update: bool) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let (old_xmax, old_offnum, old_infobits_set, flags, new_xmax, new_offnum) = {
        let x = main_data(record);
        (
            u32::from_ne_bytes(x[0..4].try_into().unwrap()),
            u16::from_ne_bytes(x[4..6].try_into().unwrap()),
            x[6],
            x[7],
            u32::from_ne_bytes(x[8..12].try_into().unwrap()),
            u16::from_ne_bytes(x[12..14].try_into().unwrap()),
        )
    };

    let (rlocator, _fork, newblk, _) = record
        .block_tag_extended(0)
        .expect("heap_xlog_update: no block 0");
    let oldblk = match record.block_tag_extended(1) {
        Some((_, _, blk, _)) => {
            debug_assert!(!hot_update);
            blk
        }
        None => newblk,
    };
    let newtid = ItemPointerData::new(newblk, new_offnum);

    if flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED != 0 {
        clear_vm_bits(rlocator, oldblk, visibilitymap::VISIBILITYMAP_VALID_BITS)?;
    }

    // The old page stays locked until the new tuple is in place (C's Hot
    // Standby consistency rule); the raw old-tuple slice below relies on it.
    let (oldaction, obuffer) =
        XLogReadBufferForRedo(record, if oldblk == newblk { 0 } else { 1 })?;
    let mut old_item: Option<(*const u8, u32)> = None;
    if oldaction == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(obuffer) };
        let page = pm.as_ref();
        let lp = if page.max_offset_number() >= old_offnum {
            Some(page.item_id(old_offnum))
        } else {
            None
        };
        let Some(lp) = lp.filter(|id| id.is_normal()) else {
            return Err(panic_err("invalid lp".into()));
        };
        let (ptr, len) = page.item_raw(lp);
        old_item = Some((ptr, len));
        // SAFETY: in-page tuple image under the pin+lock; exclusive for this arm.
        let htup = unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) };

        htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
        if hot_update {
            htup.set_hot_updated();
        } else {
            htup.clear_hot_updated();
        }
        let (mut im, mut im2) = (htup.t_infomask, htup.t_infomask2);
        fix_infomask_from_infobits(old_infobits_set, &mut im, &mut im2);
        htup.t_infomask = im;
        htup.t_infomask2 = im2;
        htup.set_xmax(old_xmax);
        htup.set_cmax(FirstCommandId, false);
        htup.t_ctid = newtid;

        page_set_prunable(&mut pm, record_xid(record));

        if flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED != 0 {
            pm.clear_all_visible();
        }

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(obuffer)?;
    }

    let info = record.record.as_ref().unwrap().xl_info & !XLR_INFO_MASK;
    let (newaction, nbuffer) = if oldblk == newblk {
        (oldaction, obuffer)
    } else if info & XLOG_HEAP_INIT_PAGE != 0 {
        let buffer = XLogInitBufferForRedo(record, 0)?;
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        pm.init(0);
        (BLK_NEEDS_REDO, buffer)
    } else {
        XLogReadBufferForRedo(record, 0)?
    };

    if flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED != 0 {
        clear_vm_bits(rlocator, newblk, visibilitymap::VISIBILITYMAP_VALID_BITS)?;
    }

    let mut freespace = 0usize;
    if newaction == BLK_NEEDS_REDO {
        let blk = record.block(0);
        debug_assert!(blk.has_data);
        // SAFETY: block data points into the decode buffer, live for this arm.
        let data = unsafe { blk.data_bytes() };

        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(nbuffer) };
        if pm.as_ref().max_offset_number() + 1 < new_offnum {
            return Err(panic_err("invalid max offset number".into()));
        }

        let mut rec = 0usize;
        let mut prefixlen = 0usize;
        let mut suffixlen = 0usize;
        if flags & XLH_UPDATE_PREFIX_FROM_OLD != 0 {
            debug_assert!(newblk == oldblk);
            prefixlen = u16::from_ne_bytes(data[rec..rec + 2].try_into().unwrap()) as usize;
            rec += 2;
        }
        if flags & XLH_UPDATE_SUFFIX_FROM_OLD != 0 {
            debug_assert!(newblk == oldblk);
            suffixlen = u16::from_ne_bytes(data[rec..rec + 2].try_into().unwrap()) as usize;
            rec += 2;
        }

        let xl_infomask2 = u16::from_ne_bytes(data[rec..rec + 2].try_into().unwrap());
        let xl_infomask = u16::from_ne_bytes(data[rec + 2..rec + 4].try_into().unwrap());
        let xl_hoff = data[rec + 4];
        rec += SizeOfHeapHeader;

        let tuplen = data.len() - rec;
        debug_assert!(tuplen <= MaxHeapTupleSize);

        #[repr(align(8))]
        struct TBuf([u8; MaxHeapTupleSize + SizeofHeapTupleHeader]);
        let mut tbuf = TBuf([0u8; MaxHeapTupleSize + SizeofHeapTupleHeader]);
        let mut newp = SizeofHeapTupleHeader;
        if prefixlen > 0 {
            let (optr, olen) = old_item.expect("prefix decode without old tuple");
            // SAFETY: old tuple image under obuffer's pin + lock, still held.
            let old = unsafe { core::slice::from_raw_parts(optr, olen as usize) };
            // SAFETY: header-sized normal item under the same pin + lock.
            let old_hoff =
                unsafe { (*(optr.cast::<HeapTupleHeaderData>())).t_hoff } as usize;

            let len = xl_hoff as usize - SizeofHeapTupleHeader;
            tbuf.0[newp..newp + len].copy_from_slice(&data[rec..rec + len]);
            rec += len;
            newp += len;

            tbuf.0[newp..newp + prefixlen]
                .copy_from_slice(&old[old_hoff..old_hoff + prefixlen]);
            newp += prefixlen;

            let len = tuplen - (xl_hoff as usize - SizeofHeapTupleHeader);
            tbuf.0[newp..newp + len].copy_from_slice(&data[rec..rec + len]);
            rec += len;
            newp += len;
        } else {
            tbuf.0[newp..newp + tuplen].copy_from_slice(&data[rec..rec + tuplen]);
            rec += tuplen;
            newp += tuplen;
        }
        debug_assert_eq!(rec, data.len());

        if suffixlen > 0 {
            let (optr, olen) = old_item.expect("suffix decode without old tuple");
            // SAFETY: old tuple image under obuffer's pin + lock, still held.
            let old = unsafe { core::slice::from_raw_parts(optr, olen as usize) };
            tbuf.0[newp..newp + suffixlen]
                .copy_from_slice(&old[olen as usize - suffixlen..]);
        }

        let newlen = SizeofHeapTupleHeader + tuplen + prefixlen + suffixlen;
        {
            // SAFETY: 8-aligned zeroed buffer at least header-sized.
            let htup = unsafe { &mut *(tbuf.0.as_mut_ptr().cast::<HeapTupleHeaderData>()) };
            htup.t_infomask2 = xl_infomask2;
            htup.t_infomask = xl_infomask;
            htup.t_hoff = xl_hoff;
            htup.set_xmin(record_xid(record));
            htup.set_cmin(FirstCommandId);
            htup.set_xmax(new_xmax);
            htup.t_ctid = newtid;
        }

        if pm
            .add_item(&tbuf.0[..newlen], new_offnum, PAI_OVERWRITE | PAI_IS_HEAP)
            .is_none()
        {
            return Err(panic_err("failed to add tuple".into()));
        }

        if flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED != 0 {
            pm.clear_all_visible();
        }

        freespace = pm.as_ref().heap_free_space();

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(nbuffer)?;
    }

    if nbuffer != InvalidBuffer && nbuffer != obuffer {
        unlock_release(nbuffer)?;
    }
    if obuffer != InvalidBuffer {
        unlock_release(obuffer)?;
    }

    // No FSM update on HOT updates: post-recovery pruning restores the space.
    if newaction == BLK_NEEDS_REDO && !hot_update && freespace < BLCKSZ / 5 {
        freespace::XLogRecordPageWithFreeSpace(rlocator, newblk, freespace)?;
    }
    Ok(())
}

fn heap_xlog_confirm(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let offnum = {
        let x = main_data(record);
        u16::from_ne_bytes(x[0..2].try_into().unwrap())
    };

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        let page = pm.as_ref();
        let lp = if page.max_offset_number() >= offnum {
            Some(page.item_id(offnum))
        } else {
            None
        };
        let Some(lp) = lp.filter(|id| id.is_normal()) else {
            return Err(panic_err("invalid lp".into()));
        };
        let (ptr, _len) = page.item_raw(lp);
        // SAFETY: in-page tuple image under the pin+lock; exclusive for this arm.
        let htup = unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) };

        htup.t_ctid = ItemPointerData::new(
            bufmgr_seams::buffer_get_block_number::call(buffer),
            offnum,
        );

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

// xl_heap_lock and xl_heap_lock_updated share one layout; `lock_updated`
// selects XLOG_HEAP2_LOCK_UPDATED's semantics (no lock-only ctid reset, no
// cmax stamp).
fn heap_xlog_lock_common(record: &mut XLogReaderState, lock_updated: bool) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let (xmax, offnum, infobits_set, flags) = {
        let x = main_data(record);
        (
            u32::from_ne_bytes(x[0..4].try_into().unwrap()),
            u16::from_ne_bytes(x[4..6].try_into().unwrap()),
            x[6],
            x[7],
        )
    };

    if flags & XLH_LOCK_ALL_FROZEN_CLEARED != 0 {
        let (rlocator, _fork, block, _) = record
            .block_tag_extended(0)
            .expect("heap_xlog_lock: no block 0");
        clear_vm_bits(rlocator, block, visibilitymap::VISIBILITYMAP_ALL_FROZEN)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == BLK_NEEDS_REDO {
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        let page = pm.as_ref();
        let lp = if page.max_offset_number() >= offnum {
            Some(page.item_id(offnum))
        } else {
            None
        };
        let Some(lp) = lp.filter(|id| id.is_normal()) else {
            return Err(panic_err("invalid lp".into()));
        };
        let (ptr, _len) = page.item_raw(lp);
        // SAFETY: in-page tuple image under the pin+lock; exclusive for this arm.
        let htup = unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) };

        htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
        htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
        let (mut im, mut im2) = (htup.t_infomask, htup.t_infomask2);
        fix_infomask_from_infobits(infobits_set, &mut im, &mut im2);
        htup.t_infomask = im;
        htup.t_infomask2 = im2;

        if !lock_updated {
            if HEAP_XMAX_IS_LOCKED_ONLY(htup.t_infomask) {
                htup.clear_hot_updated();
                htup.t_ctid = ItemPointerData::new(
                    bufmgr_seams::buffer_get_block_number::call(buffer),
                    offnum,
                );
            }
            htup.set_xmax(xmax);
            htup.set_cmax(FirstCommandId, false);
        } else {
            htup.set_xmax(xmax);
        }

        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }
    if buffer != InvalidBuffer {
        unlock_release(buffer)?;
    }
    Ok(())
}

fn heap_execute_freeze_tuple(
    htup: &mut HeapTupleHeaderData,
    xmax: TransactionId,
    t_infomask2: u16,
    t_infomask: u16,
    frzflags: u8,
) {
    htup.set_xmax(xmax);
    if frzflags & XLH_FREEZE_XVAC != 0 {
        htup.set_xvac(types_core::FrozenTransactionId);
    }
    if frzflags & XLH_INVALID_XVAC != 0 {
        htup.set_xvac(types_core::InvalidTransactionId);
    }
    htup.t_infomask = t_infomask;
    htup.t_infomask2 = t_infomask2;
}

fn heap_xlog_prune_freeze(record: &mut XLogReaderState) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    // xl_heap_prune { uint8 reason; uint8 flags }; the conflict horizon
    // follows unaligned when XLHP_HAS_CONFLICT_HORIZON is set (its only
    // consumer is the Hot Standby conflict arm, gated below).
    let flags = main_data(record)[1];

    let (rlocator, _fork, blkno, _) = record
        .block_tag_extended(0)
        .expect("heap_xlog_prune_freeze: no block 0");

    debug_assert!(
        flags & XLHP_CLEANUP_LOCK != 0
            || flags & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS) == 0
    );

    if flags & XLHP_HAS_CONFLICT_HORIZON != 0 && xlogutils::InHotStandby() {
        let md = main_data(record);
        let horizon = u32::from_ne_bytes(md[2..6].try_into().unwrap());
        standby::ResolveRecoveryConflictWithSnapshot(
            horizon,
            flags & XLHP_IS_CATALOG_REL != 0,
            rlocator,
        )?;
    }

    let (action, buffer) = xlogutils::XLogReadBufferForRedoExtended(
        record,
        0,
        types_storage::ReadBufferMode::Normal,
        flags & XLHP_CLEANUP_LOCK != 0,
    )?;
    if action == BLK_NEEDS_REDO {
        let blk = record.block(0);
        debug_assert!(blk.has_data);
        // SAFETY: block data points into the decode buffer, live for this arm.
        let data = unsafe { blk.data_bytes() };
        let mut cur = 0usize;
        let rd = |cur: usize| u16::from_ne_bytes(data[cur..cur + 2].try_into().unwrap());

        let mut nplans = 0usize;
        let plans_off;
        if flags & XLHP_HAS_FREEZE_PLANS != 0 {
            nplans = rd(cur) as usize;
            debug_assert!(nplans > 0);
            // xlhp_freeze_plans: uint16 nplans, 2 pad bytes, 12-byte plans.
            cur += 4;
            plans_off = cur;
            cur += 12 * nplans;
        } else {
            plans_off = 0;
        }

        let mut redirected = [0 as OffsetNumber; MaxHeapTuplesPerPage];
        let mut nowdead = [0 as OffsetNumber; MaxHeapTuplesPerPage];
        let mut nowunused = [0 as OffsetNumber; MaxHeapTuplesPerPage];
        let mut nredirected = 0usize;
        let mut ndead = 0usize;
        let mut nunused = 0usize;
        let read_items = |cur: &mut usize, out: &mut [OffsetNumber], pairs: bool| {
            let n = rd(*cur) as usize;
            debug_assert!(n > 0);
            *cur += 2;
            let count = if pairs { 2 * n } else { n };
            for slot in out[..count].iter_mut() {
                *slot = rd(*cur);
                *cur += 2;
            }
            n
        };
        if flags & XLHP_HAS_REDIRECTIONS != 0 {
            nredirected = read_items(&mut cur, &mut redirected, true);
        }
        if flags & XLHP_HAS_DEAD_ITEMS != 0 {
            ndead = read_items(&mut cur, &mut nowdead, false);
        }
        if flags & XLHP_HAS_NOW_UNUSED_ITEMS != 0 {
            nunused = read_items(&mut cur, &mut nowunused, false);
        }

        if nredirected > 0 || ndead > 0 || nunused > 0 {
            pruneheap_seams::heap_page_prune_execute::call(
                buffer,
                flags & XLHP_CLEANUP_LOCK == 0,
                &redirected[..2 * nredirected],
                &nowdead[..ndead],
                &nowunused[..nunused],
            );
        }

        // SAFETY: pin + (cleanup or exclusive) lock per the redo protocol.
        let mut pm = unsafe { page_mut(buffer) };
        for p in 0..nplans {
            let plan = plans_off + 12 * p;
            let xmax = u32::from_ne_bytes(data[plan..plan + 4].try_into().unwrap());
            let t_infomask2 = rd(plan + 4);
            let t_infomask = rd(plan + 6);
            let frzflags = data[plan + 8];
            let ntuples = rd(plan + 10) as usize;
            for _ in 0..ntuples {
                let offset = rd(cur);
                cur += 2;
                let page = pm.as_ref();
                let lp = page.item_id(offset);
                let (ptr, _len) = page.item_raw(lp);
                // SAFETY: in-page tuple image under the pin+lock.
                let htup = unsafe { &mut *(ptr.cast_mut().cast::<HeapTupleHeaderData>()) };
                heap_execute_freeze_tuple(htup, xmax, t_infomask2, t_infomask, frzflags);
            }
        }
        debug_assert_eq!(cur, data.len());

        // C leaves the page's prunability hints stale here; at worst an extra
        // prune cycle occurs soon.
        pm.set_lsn(lsn);
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }

    if buffer != InvalidBuffer {
        if flags & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS | XLHP_HAS_NOW_UNUSED_ITEMS) != 0
        {
            // SAFETY: pin + lock still held until the unlock below.
            let freespace = unsafe { page_mut(buffer) }.as_ref().heap_free_space();
            unlock_release(buffer)?;
            freespace::XLogRecordPageWithFreeSpace(rlocator, blkno, freespace)?;
        } else {
            unlock_release(buffer)?;
        }
    }
    Ok(())
}

fn heap_xlog_visible(record: &mut XLogReaderState) -> PgResult<()> {
    const VISIBILITYMAP_XLOG_CATALOG_REL: u8 = 0x04;
    let lsn = record.EndRecPtr;
    let (conflict_horizon, flags) = {
        let x = main_data(record);
        (u32::from_ne_bytes(x[0..4].try_into().unwrap()), x[4])
    };
    debug_assert!(
        flags & (visibilitymap::VISIBILITYMAP_VALID_BITS | VISIBILITYMAP_XLOG_CATALOG_REL)
            == flags
    );

    let (rlocator, _fork, blkno, _) = record
        .block_tag_extended(1)
        .expect("heap_xlog_visible: no block 1");

    if xlogutils::InHotStandby() {
        standby::ResolveRecoveryConflictWithSnapshot(
            conflict_horizon,
            flags & VISIBILITYMAP_XLOG_CATALOG_REL != 0,
            rlocator,
        )?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
    if action == BLK_NEEDS_REDO {
        // The heap LSN only moves when checksums/wal_log_hints demand it; the
        // torn-page hazard is benign because redo never reads this page.
        // SAFETY: pin + exclusive lock per the redo protocol (module contract).
        let mut pm = unsafe { page_mut(buffer) };
        pm.set_all_visible();
        if visibilitymap::xlog_hint_bit_is_needed() {
            pm.set_lsn(lsn);
        }
        bufmgr_seams::mark_buffer_dirty::call(buffer)?;
    }

    if buffer != InvalidBuffer {
        // SAFETY: pin + lock still held until the unlock below.
        let space = unsafe { page_mut(buffer) }.as_ref().free_space();
        unlock_release(buffer)?;
        if flags & visibilitymap::VISIBILITYMAP_VALID_BITS != 0 {
            freespace::XLogRecordPageWithFreeSpace(rlocator, blkno, space)?;
        }
    }

    let (vmaction, vmbuffer) = xlogutils::XLogReadBufferForRedoExtended(
        record,
        0,
        types_storage::ReadBufferMode::ZeroOnError,
        false,
    )?;
    if vmaction == BLK_NEEDS_REDO {
        {
            // SAFETY: pin + exclusive lock from the redo read.
            let mut vp = unsafe { page_mut(vmbuffer) };
            if vp.as_ref().is_new() {
                vp.init(0);
            }
        }
        let vmbits = flags & visibilitymap::VISIBILITYMAP_VALID_BITS;

        // visibilitymap_set locks the VM buffer itself.
        bufmgr_seams::lock_buffer::call(vmbuffer, bufmgr_seams::BUFFER_LOCK_UNLOCK)?;

        let reln = xlogutils::CreateFakeRelcacheEntry(rlocator);
        let mut vmbuf = visibilitymap::VmBuffer::new();
        visibilitymap::visibilitymap_pin(&reln, blkno, &mut vmbuf)?;
        visibilitymap::visibilitymap_set(
            &reln,
            blkno,
            InvalidBuffer,
            lsn,
            &vmbuf,
            conflict_horizon,
            vmbits,
        )?;
        vmbuf.release();
        bufmgr_seams::release_buffer::call(vmbuffer)?;
        xlogutils::FreeFakeRelcacheEntry(reln);
    } else if vmbuffer != InvalidBuffer {
        unlock_release(vmbuffer)?;
    }
    Ok(())
}

pub fn heap_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record.record.as_ref().expect("heap_redo with no decoded record").xl_info
        & !XLR_INFO_MASK;
    match info & XLOG_HEAP_OPMASK {
        XLOG_HEAP_INSERT => heap_xlog_insert(record),
        XLOG_HEAP_DELETE => heap_xlog_delete(record),
        // TRUNCATE exists for logical decoding only; replay is a no-op.
        XLOG_HEAP_TRUNCATE => Ok(()),
        XLOG_HEAP_UPDATE => heap_xlog_update(record, false),
        XLOG_HEAP_HOT_UPDATE => heap_xlog_update(record, true),
        XLOG_HEAP_CONFIRM => heap_xlog_confirm(record),
        XLOG_HEAP_LOCK => heap_xlog_lock_common(record, false),
        XLOG_HEAP_INPLACE => heap_xlog_inplace(record),
        other => Err(panic_err(format!("heap_redo: unknown op code {other}"))),
    }
}

pub fn heap2_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record.record.as_ref().expect("heap2_redo with no decoded record").xl_info
        & !XLR_INFO_MASK;
    match info & XLOG_HEAP_OPMASK {
        XLOG_HEAP2_PRUNE_ON_ACCESS | XLOG_HEAP2_PRUNE_VACUUM_SCAN
        | XLOG_HEAP2_PRUNE_VACUUM_CLEANUP => heap_xlog_prune_freeze(record),
        XLOG_HEAP2_VISIBLE => heap_xlog_visible(record),
        XLOG_HEAP2_MULTI_INSERT => heap_xlog_multi_insert(record),
        XLOG_HEAP2_LOCK_UPDATED => heap_xlog_lock_common(record, true),
        // Logical decoding only; nothing to do on a real replay.
        XLOG_HEAP2_NEW_CID => Ok(()),
        XLOG_HEAP2_REWRITE => heap_xlog_logical_rewrite(record),
        other => Err(panic_err(format!("heap2_redo: unknown op code {other}"))),
    }
}

// heap_xlog_logical_rewrite (rewriteheap.c:1073): recreate the logical
// rewrite mapping file on the standby — truncate to the record's offset
// (bytes past it were not guaranteed durable at the primary), rewrite the
// tail, fsync. xl_heap_rewrite_mapping layout (with C alignment holes):
// xid(0..4) db(4..8) rel(8..12) pad offset(16..24) num_mappings(24..28) pad
// start_lsn(32..40), then the raw LogicalRewriteMappingData entries.
fn heap_xlog_logical_rewrite(record: &mut XLogReaderState) -> PgResult<()> {
    const LOGICAL_REWRITE_MAPPING_SIZE: usize = 36;
    let md = main_data(record);
    let mapped_xid = u32::from_ne_bytes(md[0..4].try_into().unwrap());
    let mapped_db = u32::from_ne_bytes(md[4..8].try_into().unwrap());
    let mapped_rel = u32::from_ne_bytes(md[8..12].try_into().unwrap());
    let offset = i64::from_ne_bytes(md[16..24].try_into().unwrap()) as u64;
    let num_mappings = u32::from_ne_bytes(md[24..28].try_into().unwrap());
    let start_lsn = u64::from_ne_bytes(md[32..40].try_into().unwrap());
    let create_xid = record_xid(record);

    let datadir = init_small::globals::DataDir().expect("heap_xlog_logical_rewrite: DataDir unset");
    let dir = std::path::PathBuf::from(datadir).join("pg_logical/mappings");
    let path = dir.join(format!(
        "map-{:x}-{:x}-{:X}_{:X}-{:x}-{:x}",
        mapped_db,
        mapped_rel,
        (start_lsn >> 32) as u32,
        start_lsn as u32,
        mapped_xid,
        create_xid,
    ));

    let file_err = |what: &str, e: std::io::Error| {
        elog::ereport(types_error::ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not {what} file \"{}\": {e}", path.display()))
            .finish(types_error::ErrorLocation::new(
                "src/backend/access/heap/rewriteheap.c",
                1073,
                "heap_xlog_logical_rewrite",
            ))
    };

    let file = match std::fs::OpenOptions::new().create(true).write(true).open(&path) {
        Ok(f) => f,
        Err(e) => return file_err("create", e),
    };
    // Truncate all data that's not guaranteed to have been safely fsynced
    // (by a previous record or by the last checkpoint).
    if let Err(e) = file.set_len(offset) {
        return file_err("truncate", e);
    }
    let len = num_mappings as usize * LOGICAL_REWRITE_MAPPING_SIZE;
    let data = &md[40..40 + len];
    // Write out the tail end of the mapping file (again).
    // positional write; on non-unix (wasm) targets the FileExt trait is
    // unstable, so seek+write_all — equivalent here (nothing reads the
    // cursor after; sync_all follows).
    #[cfg(unix)]
    let write_res = std::os::unix::fs::FileExt::write_all_at(&file, data, offset);
    #[cfg(not(unix))]
    let write_res = (|| {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = &file;
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(data)
    })();
    if let Err(e) = write_res {
        return file_err("write to", e);
    }
    // fsync all previously written data.
    if let Err(e) = file.sync_all() {
        return file_err("fsync", e);
    }
    Ok(())
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
