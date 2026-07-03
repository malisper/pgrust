//! heapam_xlog.c — heap/heap2 rmgr redo. Live arms: XLOG_HEAP_INSERT and
//! XLOG_HEAP_DELETE (the record types the write side emits and round-trip
//! proves) plus the C no-op arms (TRUNCATE, HEAP2_NEW_CID). Everything else
//! is a loud panic naming its op.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use types_core::{Buffer, InvalidBuffer, TransactionId, BLCKSZ};
use types_error::{PgError, PgResult};
use types_storage::bufpage::{
    MaxHeapTupleSize, PageMut, SizeofHeapTupleHeader, PAI_IS_HEAP, PAI_OVERWRITE,
};
use types_tuple::{
    HeapTupleHeaderData, ItemPointerData, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_XMAX_BITS,
    HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK, HEAP_XMAX_LOCK_ONLY,
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
pub const XLH_INSERT_ALL_FROZEN_SET: u8 = 1 << 5;
pub const XLH_DELETE_ALL_VISIBLE_CLEARED: u8 = 1 << 0;
pub const XLH_DELETE_IS_SUPER: u8 = 1 << 3;
pub const XLH_DELETE_IS_PARTITION_MOVE: u8 = 1 << 4;

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

    let (_target_locator, _fork, blkno, _) = record
        .block_tag_extended(0)
        .expect("heap_xlog_delete: no block 0");
    let target_tid = ItemPointerData::new(blkno, offnum);

    if flags & XLH_DELETE_ALL_VISIBLE_CLEARED != 0 {
        panic!(
            "heap_xlog_delete: visibilitymap_clear redo not ported \
             (CreateFakeRelcacheEntry / rel vocab; land with the vacuum lane)"
        );
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
        panic!(
            "heap_xlog_insert: visibilitymap_clear redo not ported \
             (CreateFakeRelcacheEntry / rel vocab; land with the vacuum lane)"
        );
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
        freespace::XLogRecordPageWithFreeSpace(target_locator, blkno, freespace);
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
        XLOG_HEAP_UPDATE => panic!("heap_redo arm not ported: heap_xlog_update"),
        XLOG_HEAP_HOT_UPDATE => panic!("heap_redo arm not ported: heap_xlog_update (HOT)"),
        XLOG_HEAP_CONFIRM => panic!("heap_redo arm not ported: heap_xlog_confirm"),
        XLOG_HEAP_LOCK => panic!("heap_redo arm not ported: heap_xlog_lock"),
        XLOG_HEAP_INPLACE => panic!("heap_redo arm not ported: heap_xlog_inplace"),
        other => Err(panic_err(format!("heap_redo: unknown op code {other}"))),
    }
}

pub fn heap2_redo(record: &mut XLogReaderState) -> PgResult<()> {
    let info = record.record.as_ref().expect("heap2_redo with no decoded record").xl_info
        & !XLR_INFO_MASK;
    match info & XLOG_HEAP_OPMASK {
        XLOG_HEAP2_PRUNE_ON_ACCESS | XLOG_HEAP2_PRUNE_VACUUM_SCAN
        | XLOG_HEAP2_PRUNE_VACUUM_CLEANUP => {
            panic!("heap2_redo arm not ported: heap_xlog_prune_freeze")
        }
        XLOG_HEAP2_VISIBLE => panic!("heap2_redo arm not ported: heap_xlog_visible"),
        XLOG_HEAP2_MULTI_INSERT => panic!("heap2_redo arm not ported: heap_xlog_multi_insert"),
        XLOG_HEAP2_LOCK_UPDATED => panic!("heap2_redo arm not ported: heap_xlog_lock_updated"),
        // Logical decoding only; nothing to do on a real replay.
        XLOG_HEAP2_NEW_CID => Ok(()),
        XLOG_HEAP2_REWRITE => panic!("heap2_redo arm not ported: heap_xlog_logical_rewrite"),
        other => Err(panic_err(format!("heap2_redo: unknown op code {other}"))),
    }
}

pub fn init_seams() {}

#[cfg(test)]
mod tests;
