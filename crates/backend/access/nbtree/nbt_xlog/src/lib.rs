#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of `src/backend/access/nbtree/nbtxlog.c` (PostgreSQL 18.3).
//!
//! WAL replay (redo) and masking for nbtree. These run during recovery and
//! consume the [`XLogReaderState`] records emitted by the insert / split /
//! delete / vacuum / dedup paths.
//!
//! Functions ported 1:1 with C (original names preserved):
//! - [`btree_redo`]                  -- the rmgr redo dispatcher
//! - [`btree_xlog_startup`], [`btree_xlog_cleanup`]
//! - [`btree_mask`]                  -- the rmgr mask function
//! - `_bt_restore_page`, `_bt_restore_meta`
//! - `_bt_clear_incomplete_split`
//! - `btree_xlog_insert` / `_split` / `_dedup` / `_vacuum` / `_delete`
//! - `btree_xlog_updates`
//! - `btree_xlog_mark_page_halfdead` / `_unlink_page` / `_newroot`
//! - `btree_xlog_reuse_page`
//!
//! ## Model differences from the C tree
//!
//! The `record` is the value-typed [`XLogReaderState`] this repo decodes WAL
//! into; `XLogRecGet*` are its `DecodedXLogRecord` accessors. The WAL on-disk
//! structs (`xl_btree_insert`, `xl_btree_split`, ...) are decoded field-by-field
//! out of the (possibly unaligned) `XLogRecGetData` / `XLogRecGetBlockData` byte
//! buffers into the `types_nbtree` value structs.
//!
//! A redo buffer is mutated by handing the bufmgr the page bytes through
//! `with_buffer_page` (which gives a `&mut [u8]` view of the shared buffer's
//! page), applying the change, then stamping the record LSN with `page_set_lsn`
//! and `mark_buffer_dirty` — exactly the C `PageSetLSN` + `MarkBufferDirty`
//! sequence. The recovery buffer manager (`XLogReadBufferForRedo[Extended]`,
//! `XLogInitBufferForRedo`, `UnlockReleaseBuffer`) and the Hot-Standby
//! recovery-conflict machinery are reached through seams; the C control flow
//! (which blocks need redo, lock/release ordering, metapage rebuild) is
//! preserved exactly.
//!
//! `opCtx` (C `static MemoryContext`) is owned here as a thread-local recovery
//! working context, created in [`btree_xlog_startup`] and deleted in
//! [`btree_xlog_cleanup`]; `btree_redo` resets it after each record and uses it
//! as the allocation context for the redo working memory and conflict
//! resolution.

extern crate alloc;

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;

use dedup::{
    new_dedup_state, BTDedupState, _bt_dedup_finish_pending, _bt_dedup_save_htid,
    _bt_dedup_start_pending, _bt_swap_posting, _bt_update_posting,
};
use page::{
    ItemIdGetLength, ItemIdGetOffset, ItemPointerGetBlockNumberNoCheck, ItemPointerSetBlockNumber,
    ItemPointerSetOffsetNumber, PageAddItemExtended, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageGetSpecialPointer, PageGetTempPageCopySpecial, PageIndexMultiDelete,
    PageIndexTupleDelete, PageIndexTupleOverwrite, PageInit, PageMut, PageRef, PageRestoreTempPage,
};
use mcx::{MemoryContext, Mcx};
use ::types_core::primitive::{
    BlockNumber, InvalidBlockNumber, OffsetNumber, TransactionId, XLogRecPtr,
};
use ::types_core::xact::FullTransactionId;
use types_error::{PgError, PgResult, PANIC};
use types_nbtree::{
    xl_btree_dedup, xl_btree_delete, xl_btree_insert, xl_btree_mark_page_halfdead,
    xl_btree_metadata, xl_btree_newroot, xl_btree_reuse_page, xl_btree_split, xl_btree_unlink_page,
    BTDedupInterval, BTMetaPageData, BTPageOpaqueData, BTMaxItemSize, BTP_DELETED, BTP_HALF_DEAD,
    BTP_HAS_FULLXID, BTP_HAS_GARBAGE, BTP_INCOMPLETE_SPLIT, BTP_LEAF, BTP_META, BTP_ROOT,
    BTP_SPLIT_END, BTREE_MAGIC, BTREE_METAPAGE, BTREE_NOVAC_VERSION, BT_PIVOT_HEAP_TID_ATTR,
    INDEX_ALT_TID_MASK, P_FIRSTKEY, P_HIKEY, P_NONE, SizeOfBtreeUpdate,
    XLOG_BTREE_DEDUP, XLOG_BTREE_DELETE, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META,
    XLOG_BTREE_INSERT_POST, XLOG_BTREE_INSERT_UPPER, XLOG_BTREE_MARK_PAGE_HALFDEAD,
    XLOG_BTREE_META_CLEANUP, XLOG_BTREE_NEWROOT, XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_SPLIT_L,
    XLOG_BTREE_SPLIT_R, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use ::types_storage::storage::{Buffer, InvalidBuffer, ReadBufferMode};
use ::types_storage::RelFileLocator;
use ::types_tuple::heaptuple::{IndexTupleData, IndexTupleSize, ItemPointerData};
use ::wal::rmgr::XLogReaderState;
use ::wal::xlogutils::in_hot_standby;
use ::wal::XLogRedoAction;

use bufmask_seams as bufmask;
use xlogutils_seams as xlogutils;
use bufmgr_seams as bufmgr;
use standby_seams as standby;

#[cfg(test)]
mod tests;

// ===========================================================================
// `opCtx` — the C `static MemoryContext opCtx` recovery working context.
// ===========================================================================

thread_local! {
    /// `static MemoryContext opCtx` (nbtxlog.c) — working memory for redo
    /// operations, created at recovery startup and deleted at cleanup.
    static OP_CTX: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };
}

/// Run `f` with an [`Mcx`] borrowed from `opCtx` (mirrors C's
/// `MemoryContextSwitchTo(opCtx)`). Panics if `opCtx` was never created, which
/// in C would be a NULL-deref — the rmgr only calls redo between
/// `btree_xlog_startup` and `btree_xlog_cleanup`.
fn with_op_ctx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
    OP_CTX.with(|c| {
        let borrow = c.borrow();
        let ctx = borrow
            .as_ref()
            .expect("btree_redo called without btree_xlog_startup (opCtx is NULL)");
        f(ctx.mcx())
    })
}

// ===========================================================================
// Offset / page-flag helpers (storage/off.h, access/nbtree.h).
// ===========================================================================

/// `InvalidOffsetNumber` (`storage/off.h`).
const InvalidOffsetNumber: OffsetNumber = 0;

/// `MAXALIGN(len)` (c.h) — `ALIGNOF_LONG` (== `MAXIMUM_ALIGNOF`) is 8.
#[inline]
const fn maxalign(len: usize) -> usize {
    const ALIGNOF: usize = 8;
    (len + (ALIGNOF - 1)) & !(ALIGNOF - 1)
}

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberNext(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber + 1
}

/// `OffsetNumberPrev(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberPrev(offsetNumber: OffsetNumber) -> OffsetNumber {
    offsetNumber - 1
}

/// `SizeOfPageHeaderData` (`storage/bufpage.h`).
const SizeOfPageHeaderData: usize = 24;

// Page-header field byte offsets within a `PageHeaderData` (stable on-disk
// format). `pd_lower`/`pd_upper`/`pd_special` are uint16 fields. The page codec
// keeps its header setters private, so the redo routines that re-establish
// pd_lower (after rebuilding the metapage / marking a page deleted) write them
// here directly, matching C's `((PageHeader) page)->pd_lower = ...`.
const OFF_PD_LOWER: usize = 12;
const OFF_PD_UPPER: usize = 14;
const OFF_PD_SPECIAL: usize = 16;

#[inline]
fn read_pd_special(page_bytes: &[u8]) -> usize {
    u16::from_ne_bytes([page_bytes[OFF_PD_SPECIAL], page_bytes[OFF_PD_SPECIAL + 1]]) as usize
}

#[inline]
fn write_pd_lower(page_bytes: &mut [u8], value: u16) {
    page_bytes[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

#[inline]
fn write_pd_upper(page_bytes: &mut [u8], value: u16) {
    page_bytes[OFF_PD_UPPER..OFF_PD_UPPER + 2].copy_from_slice(&value.to_ne_bytes());
}

// ===========================================================================
// nbtree page opaque-area accessors (in-crate port; access/nbtree.h).
//
// nbtree pages carry a `BTPageOpaqueData` special area exactly 16 bytes wide:
//   { BlockNumber btpo_prev; BlockNumber btpo_next; uint32 btpo_level;
//     uint16 btpo_flags; BTCycleId btpo_cycleid; }
// ===========================================================================

/// `BTPageGetOpaque(page)` — decode the B-tree opaque special area.
fn BTPageGetOpaque(page: &PageRef<'_>) -> PgResult<BTPageOpaqueData> {
    let special = PageGetSpecialPointer(page)?;
    let rd_u32 = |off: usize| -> u32 {
        u32::from_ne_bytes([
            special[off],
            special[off + 1],
            special[off + 2],
            special[off + 3],
        ])
    };
    let rd_u16 = |off: usize| -> u16 { u16::from_ne_bytes([special[off], special[off + 1]]) };
    Ok(BTPageOpaqueData {
        btpo_prev: rd_u32(0),
        btpo_next: rd_u32(4),
        btpo_level: rd_u32(8),
        btpo_flags: rd_u16(12),
        btpo_cycleid: rd_u16(14),
    })
}

/// Write a [`BTPageOpaqueData`] back into a page's special area.
fn write_opaque(page: &mut PageMut<'_>, opaque: &BTPageOpaqueData) {
    let special_off = read_pd_special(page.as_bytes());
    let bytes = page.as_mut_bytes();
    bytes[special_off..special_off + 4].copy_from_slice(&opaque.btpo_prev.to_ne_bytes());
    bytes[special_off + 4..special_off + 8].copy_from_slice(&opaque.btpo_next.to_ne_bytes());
    bytes[special_off + 8..special_off + 12].copy_from_slice(&opaque.btpo_level.to_ne_bytes());
    bytes[special_off + 12..special_off + 14].copy_from_slice(&opaque.btpo_flags.to_ne_bytes());
    bytes[special_off + 14..special_off + 16].copy_from_slice(&opaque.btpo_cycleid.to_ne_bytes());
}

/// `P_RIGHTMOST(opaque)`.
#[inline]
fn P_RIGHTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.btpo_next == P_NONE
}

/// `P_HAS_GARBAGE(opaque)`.
#[inline]
fn P_HAS_GARBAGE(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_HAS_GARBAGE) != 0
}

/// `P_INCOMPLETE_SPLIT(opaque)`.
#[inline]
fn P_INCOMPLETE_SPLIT(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_INCOMPLETE_SPLIT) != 0
}

/// `P_ISLEAF(opaque)`.
#[inline]
fn P_ISLEAF(opaque: &BTPageOpaqueData) -> bool {
    (opaque.btpo_flags & BTP_LEAF) != 0
}

/// `P_FIRSTDATAKEY(opaque)`.
#[inline]
fn P_FIRSTDATAKEY(opaque: &BTPageOpaqueData) -> OffsetNumber {
    if P_RIGHTMOST(opaque) {
        P_HIKEY
    } else {
        P_FIRSTKEY
    }
}

/// `_bt_pageinit(page, size)` — initialise a btree page with a
/// `sizeof(BTPageOpaqueData)` special area.
fn _bt_pageinit(page_bytes: &mut [u8], size: usize) -> PgResult<()> {
    PageInit(page_bytes, size, core::mem::size_of::<BTPageOpaqueData>())
}

// ===========================================================================
// BTreeTupleData inline helpers (access/nbtree.h "Notes on B-Tree tuple
// format"). These operate on / produce the 8-byte `IndexTupleData` header.
// ===========================================================================

/// Interpret the leading 8 bytes of a page item as an [`IndexTupleData`] header.
/// Page items are not guaranteed aligned, so read field-by-field.
fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    debug_assert!(tuple.len() >= 8);
    let t_tid = read_ipd(&tuple[0..6]);
    let t_info = u16::from_ne_bytes([tuple[6], tuple[7]]);
    IndexTupleData { t_tid, t_info }
}

/// Decode a 6-byte on-page `ItemPointerData`.
fn read_ipd(bytes: &[u8]) -> ItemPointerData {
    let mut ipd = ItemPointerData::default();
    ipd.ip_blkid.bi_hi = u16::from_ne_bytes([bytes[0], bytes[1]]);
    ipd.ip_blkid.bi_lo = u16::from_ne_bytes([bytes[2], bytes[3]]);
    ipd.ip_posid = u16::from_ne_bytes([bytes[4], bytes[5]]);
    ipd
}

/// Write an 8-byte `IndexTupleData` header into the start of `bytes`.
fn write_index_tuple_header(bytes: &mut [u8], hdr: &IndexTupleData) {
    bytes[0..2].copy_from_slice(&hdr.t_tid.ip_blkid.bi_hi.to_ne_bytes());
    bytes[2..4].copy_from_slice(&hdr.t_tid.ip_blkid.bi_lo.to_ne_bytes());
    bytes[4..6].copy_from_slice(&hdr.t_tid.ip_posid.to_ne_bytes());
    bytes[6..8].copy_from_slice(&hdr.t_info.to_ne_bytes());
}

/// `BTreeTupleGetDownLink(pivot)`.
fn BTreeTupleGetDownLink(pivot: &IndexTupleData) -> BlockNumber {
    ItemPointerGetBlockNumberNoCheck(&pivot.t_tid)
}

/// `BTreeTupleSetDownLink(pivot, blkno)`.
fn BTreeTupleSetDownLink(pivot: &mut IndexTupleData, blkno: BlockNumber) {
    ItemPointerSetBlockNumber(&mut pivot.t_tid, blkno);
}

/// `BTreeTupleSetNAtts(itup, nkeyatts, heaptid)`.
fn BTreeTupleSetNAtts(itup: &mut IndexTupleData, nkeyatts: u16, heaptid: bool) {
    debug_assert!(!heaptid || nkeyatts > 0);

    itup.t_info |= INDEX_ALT_TID_MASK;

    let mut nkeyatts = nkeyatts;
    if heaptid {
        nkeyatts |= BT_PIVOT_HEAP_TID_ATTR;
    }
    // BT_IS_POSTING bit is deliberately unset here.
    ItemPointerSetOffsetNumber(&mut itup.t_tid, nkeyatts);
}

/// `BTreeTupleSetTopParent(leafhikey, blkno)`.
fn BTreeTupleSetTopParent(leafhikey: &mut IndexTupleData, blkno: BlockNumber) {
    ItemPointerSetBlockNumber(&mut leafhikey.t_tid, blkno);
    BTreeTupleSetNAtts(leafhikey, 0, false);
}

// ===========================================================================
// On-disk WAL-record decoders. `XLogRecGetData` / `XLogRecGetBlockData` return
// byte buffers that are not guaranteed aligned for the `xl_btree_*` structs, so
// decode field-by-field.
// ===========================================================================

#[inline]
fn rd_u16(b: &[u8], off: usize) -> u16 {
    u16::from_ne_bytes([b[off], b[off + 1]])
}

#[inline]
fn rd_u32(b: &[u8], off: usize) -> u32 {
    u32::from_ne_bytes([b[off], b[off + 1], b[off + 2], b[off + 3]])
}

#[inline]
fn rd_u64(b: &[u8], off: usize) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[off..off + 8]);
    u64::from_ne_bytes(a)
}

fn decode_insert(b: &[u8]) -> xl_btree_insert {
    xl_btree_insert {
        offnum: rd_u16(b, 0),
    }
}

fn decode_split(b: &[u8]) -> xl_btree_split {
    xl_btree_split {
        level: rd_u32(b, 0),
        firstrightoff: rd_u16(b, 4),
        newitemoff: rd_u16(b, 6),
        postingoff: rd_u16(b, 8),
    }
}

fn decode_dedup(b: &[u8]) -> xl_btree_dedup {
    xl_btree_dedup {
        nintervals: rd_u16(b, 0),
    }
}

fn decode_vacuum(b: &[u8]) -> ::types_nbtree::xl_btree_vacuum {
    ::types_nbtree::xl_btree_vacuum {
        ndeleted: rd_u16(b, 0),
        nupdated: rd_u16(b, 2),
    }
}

fn decode_delete(b: &[u8]) -> xl_btree_delete {
    xl_btree_delete {
        snapshotConflictHorizon: rd_u32(b, 0) as TransactionId,
        ndeleted: rd_u16(b, 4),
        nupdated: rd_u16(b, 6),
        isCatalogRel: b[8] != 0,
    }
}

fn decode_mark_page_halfdead(b: &[u8]) -> xl_btree_mark_page_halfdead {
    xl_btree_mark_page_halfdead {
        poffset: rd_u16(b, 0),
        leafblk: rd_u32(b, 4),
        leftblk: rd_u32(b, 8),
        rightblk: rd_u32(b, 12),
        topparent: rd_u32(b, 16),
    }
}

fn decode_unlink_page(b: &[u8]) -> xl_btree_unlink_page {
    xl_btree_unlink_page {
        leftsib: rd_u32(b, 0),
        rightsib: rd_u32(b, 4),
        level: rd_u32(b, 8),
        safexid: FullTransactionId {
            value: rd_u64(b, 16),
        },
        leafleftsib: rd_u32(b, 24),
        leafrightsib: rd_u32(b, 28),
        leaftopparent: rd_u32(b, 32),
    }
}

fn decode_newroot(b: &[u8]) -> xl_btree_newroot {
    xl_btree_newroot {
        rootblk: rd_u32(b, 0),
        level: rd_u32(b, 4),
    }
}

fn decode_reuse_page(b: &[u8]) -> xl_btree_reuse_page {
    // locator: spcOid(4) dbOid(4) relNumber(4); block(4); horizon(8);
    // isCatalogRel(1)
    let locator = RelFileLocator {
        spcOid: rd_u32(b, 0),
        dbOid: rd_u32(b, 4),
        relNumber: rd_u32(b, 8),
    };
    xl_btree_reuse_page {
        locator,
        block: rd_u32(b, 12),
        snapshotConflictHorizon: FullTransactionId {
            value: rd_u64(b, 16),
        },
        isCatalogRel: b[24] != 0,
    }
}

fn decode_metadata(b: &[u8]) -> xl_btree_metadata {
    xl_btree_metadata {
        version: rd_u32(b, 0),
        root: rd_u32(b, 4),
        level: rd_u32(b, 8),
        fastroot: rd_u32(b, 12),
        fastlevel: rd_u32(b, 16),
        last_cleanup_num_delpages: rd_u32(b, 20),
        allequalimage: b[24] != 0,
    }
}

/// Decode the array of `BTDedupInterval` that follows `xl_btree_dedup` in the
/// block data. Each interval is `{ OffsetNumber baseoff; uint16 nitems; }`.
fn decode_dedup_intervals(ptr: &[u8], nintervals: usize) -> Vec<BTDedupInterval> {
    let mut v: Vec<BTDedupInterval> = Vec::new();
    let _ = v.try_reserve(nintervals);
    let isz = 4usize; // sizeof(BTDedupInterval) == 4
    for i in 0..nintervals {
        let off = i * isz;
        v.push(BTDedupInterval {
            baseoff: rd_u16(ptr, off),
            nitems: rd_u16(ptr, off + 2),
        });
    }
    v
}

// ===========================================================================
// Record / block accessors over the value-typed reader.
// ===========================================================================

/// The decoded record currently held by the reader. Recovery always has a
/// decoded record when redo runs (the rmgr dispatcher only calls redo for a
/// freshly read record).
fn rec<'a, 'mcx>(
    record: &'a XLogReaderState<'mcx>,
) -> &'a ::wal::wal::DecodedXLogRecord<'mcx> {
    // Borrow the decoded record straight off the reader, preserving its arena
    // lifetime `'mcx`. Mirrors the hash/brin -xlog siblings' `record.record
    // .as_ref()`; no lifetime laundering is needed.
    record
        .record
        .as_ref()
        .expect("btree_redo: reader has no decoded record")
}

/// `XLogRecGetInfo(record) & ~XLR_INFO_MASK` — the rmgr info bits.
fn rec_info(record: &XLogReaderState<'_>) -> u8 {
    const XLR_INFO_MASK: u8 = 0x0F;
    rec(record).info() & !XLR_INFO_MASK
}

/// `XLogRecGetData(record)`.
fn rec_data(record: &XLogReaderState<'_>) -> Vec<u8> {
    rec(record).data().to_vec()
}

/// `XLogRecGetBlockData(record, block_id, NULL)`.
fn rec_block_data(record: &XLogReaderState<'_>, block_id: u8) -> Option<Vec<u8>> {
    rec(record).block_data(block_id as usize).map(|s| s.to_vec())
}

/// `XLogRecHasBlockRef(record, block_id)`.
fn rec_has_block_ref(record: &XLogReaderState<'_>, block_id: u8) -> bool {
    rec(record).has_block_ref(block_id as usize)
}

/// `XLogRecGetBlockTag(record, id, &rlocator, NULL, &blk)` — block number.
fn block_tag_blknum(record: &XLogReaderState<'_>, block_id: u8) -> BlockNumber {
    let d = rec(record);
    if d.has_block_ref(block_id as usize) {
        d.blocks()[block_id as usize].blkno()
    } else {
        InvalidBlockNumber
    }
}

/// `XLogRecGetBlockTag(record, id, &rlocator, ...)` — the relation locator.
fn block_tag_rlocator(record: &XLogReaderState<'_>, block_id: u8) -> RelFileLocator {
    let d = rec(record);
    let l = d.blocks()[block_id as usize].rlocator();
    RelFileLocator {
        spcOid: l.spc_oid(),
        dbOid: l.db_oid(),
        relNumber: l.rel_number(),
    }
}

/// `record->EndRecPtr` — the LSN to stamp on redone pages.
fn rec_lsn(record: &XLogReaderState<'_>) -> XLogRecPtr {
    record.EndRecPtr
}

// ===========================================================================
// Buffer helpers (storage/bufmgr.h).
// ===========================================================================

/// `BufferIsValid(buf)`.
#[inline]
fn buffer_is_valid(buf: Buffer) -> bool {
    buf != InvalidBuffer
}

/// Apply `f` to the page bytes of a redo buffer, then stamp the record LSN
/// (`PageSetLSN`) and `MarkBufferDirty`, mirroring the C
/// `PageSetLSN(page, lsn); MarkBufferDirty(buf);` sequence that follows every
/// in-place redo page mutation.
fn buffer_modify_page(
    buf: Buffer,
    lsn: XLogRecPtr,
    f: &mut dyn FnMut(&mut PageMut<'_>) -> PgResult<()>,
) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buf, &mut |bytes: &mut [u8]| {
        let mut page = PageMut::new(bytes)?;
        f(&mut page)
    })?;
    bufmgr::page_set_lsn::call(buf, lsn)?;
    bufmgr::mark_buffer_dirty::call(buf);
    Ok(())
}

/// `PageGetItem(page, PageGetItemId(page, off))` returning an owned copy.
fn page_item(page: &PageMut<'_>, off: OffsetNumber) -> PgResult<Vec<u8>> {
    let pr = page.as_ref();
    let itemid = PageGetItemId(&pr, off)?;
    Ok(PageGetItem(&pr, &itemid)?.to_vec())
}

/// `ItemIdGetLength(PageGetItemId(page, off))`.
fn page_item_len(page: &PageMut<'_>, off: OffsetNumber) -> PgResult<usize> {
    let pr = page.as_ref();
    let itemid = PageGetItemId(&pr, off)?;
    Ok(ItemIdGetLength(&itemid) as usize)
}

/// `BTPageSetDeleted(page, safexid)` (`nbtree.h`).
fn bt_page_set_deleted(page: &mut PageMut<'_>, safexid: FullTransactionId) -> PgResult<()> {
    let mut opaque = BTPageGetOpaque(&page.as_ref())?;
    opaque.btpo_flags &= !BTP_HALF_DEAD;
    opaque.btpo_flags |= BTP_DELETED | BTP_HAS_FULLXID;
    write_opaque(page, &opaque);

    // header->pd_lower = MAXALIGN(SizeOfPageHeaderData) + sizeof(BTDeletedPageData)
    // header->pd_upper = header->pd_special
    // BTDeletedPageData is a single FullTransactionId (8 bytes).
    let new_lower =
        (maxalign(SizeOfPageHeaderData) + core::mem::size_of::<FullTransactionId>()) as u16;
    let special = read_pd_special(page.as_bytes()) as u16;
    {
        let bytes = page.as_mut_bytes();
        write_pd_lower(bytes, new_lower);
        write_pd_upper(bytes, special);
    }

    // contents = (BTDeletedPageData *) PageGetContents(page); contents->safexid =
    // safexid
    let off = maxalign(SizeOfPageHeaderData);
    let bytes = page.as_mut_bytes();
    bytes[off..off + 8].copy_from_slice(&safexid.value.to_ne_bytes());
    Ok(())
}

/// Build the dummy `IndexTupleData` high key item that points to a half-dead
/// leaf's top parent (C: `MemSet(&trunctuple,0,...); trunctuple.t_info =
/// sizeof(IndexTupleData); BTreeTupleSetTopParent(&trunctuple, topparent);`).
fn make_trunctuple(topparent: BlockNumber) -> Vec<u8> {
    let mut trunctuple = IndexTupleData::default();
    trunctuple.t_info = core::mem::size_of::<IndexTupleData>() as u16;
    BTreeTupleSetTopParent(&mut trunctuple, topparent);
    let mut bytes = vec![0u8; core::mem::size_of::<IndexTupleData>()];
    write_index_tuple_header(&mut bytes, &trunctuple);
    bytes
}

/// `sizeof(BTMetaPageData)` on-disk: 7 uint32 fields (24 + 4) + pad(4) + float8
/// (8) + bool(1), padded to 8 => 48.
const SIZEOF_BT_META_PAGE_DATA: usize = 48;

/// Write a [`BTMetaPageData`] at `PageGetContents` (just past the page header).
fn write_meta(page: &mut PageMut<'_>, md: &BTMetaPageData) {
    let off = maxalign(SizeOfPageHeaderData);
    let bytes = page.as_mut_bytes();
    bytes[off..off + 4].copy_from_slice(&md.btm_magic.to_ne_bytes());
    bytes[off + 4..off + 8].copy_from_slice(&md.btm_version.to_ne_bytes());
    bytes[off + 8..off + 12].copy_from_slice(&md.btm_root.to_ne_bytes());
    bytes[off + 12..off + 16].copy_from_slice(&md.btm_level.to_ne_bytes());
    bytes[off + 16..off + 20].copy_from_slice(&md.btm_fastroot.to_ne_bytes());
    bytes[off + 20..off + 24].copy_from_slice(&md.btm_fastlevel.to_ne_bytes());
    bytes[off + 24..off + 28].copy_from_slice(&md.btm_last_cleanup_num_delpages.to_ne_bytes());
    // pad [off+28 .. off+32]
    bytes[off + 32..off + 40].copy_from_slice(&md.btm_last_cleanup_num_heap_tuples.to_ne_bytes());
    bytes[off + 40] = md.btm_allequalimage as u8;
}

// ===========================================================================
// `_bt_restore_page`
// ===========================================================================

/// `_bt_restore_page()` -- re-populate a freshly init'd page from the upper-part
/// copy logged in a WAL record.
fn _bt_restore_page(page: &mut PageMut<'_>, from: &[u8]) -> PgResult<()> {
    let len = from.len();
    // Add items in reverse so they regain their original order; scan forward
    // first to find item boundaries.
    let mut items: Vec<(usize, u16)> = Vec::new();
    let mut off = 0usize;
    while off < len {
        let itupdata = index_tuple_header(&from[off..]);
        let itemsz = maxalign(IndexTupleSize(&itupdata)) as u16;
        items.push((off, itemsz));
        off += itemsz as usize;
    }
    let nitems = items.len();

    for i in (0..nitems).rev() {
        let (start, itemsz) = items[i];
        let item = &from[start..start + itemsz as usize];
        if PageAddItemExtended(page, item, (nitems - i) as u16, 0)? == InvalidOffsetNumber {
            return Err(PgError::new(PANIC, "_bt_restore_page: cannot add item to page"));
        }
    }
    Ok(())
}

// ===========================================================================
// `_bt_restore_meta`
// ===========================================================================

fn _bt_restore_meta(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<()> {
    let lsn = rec_lsn(record);

    let metabuf = xlogutils::xlog_init_buffer_for_redo::call(record, block_id)?;
    let ptr = rec_block_data(record, block_id).expect("metapage block data present");

    debug_assert!(ptr.len() == 25); // sizeof(xl_btree_metadata) on-disk
    debug_assert!(bufmgr::buffer_get_block_number::call(metabuf) == BTREE_METAPAGE);
    let xlrec = decode_metadata(&ptr);

    buffer_modify_page(metabuf, lsn, &mut |metapg| {
        let page_size = metapg.as_bytes().len();
        _bt_pageinit(metapg.as_mut_bytes(), page_size)?;

        let md = BTMetaPageData {
            btm_magic: BTREE_MAGIC,
            btm_version: xlrec.version,
            btm_root: xlrec.root,
            btm_level: xlrec.level,
            btm_fastroot: xlrec.fastroot,
            btm_fastlevel: xlrec.fastlevel,
            btm_last_cleanup_num_delpages: xlrec.last_cleanup_num_delpages,
            btm_last_cleanup_num_heap_tuples: -1.0,
            btm_allequalimage: xlrec.allequalimage,
        };
        debug_assert!(md.btm_version >= BTREE_NOVAC_VERSION);
        write_meta(metapg, &md);

        let mut pageop = BTPageGetOpaque(&metapg.as_ref())?;
        pageop.btpo_flags = BTP_META;
        write_opaque(metapg, &pageop);

        // Set pd_lower just past the end of the metadata.
        let new_lower = (maxalign(SizeOfPageHeaderData) + SIZEOF_BT_META_PAGE_DATA) as u16;
        write_pd_lower(metapg.as_mut_bytes(), new_lower);

        Ok(())
    })?;

    bufmgr::unlock_release_buffer::call(metabuf);
    Ok(())
}

// ===========================================================================
// `_bt_clear_incomplete_split`
// ===========================================================================

/// `_bt_clear_incomplete_split` -- clear INCOMPLETE_SPLIT flag on a page.
fn _bt_clear_incomplete_split(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<()> {
    let lsn = rec_lsn(record);

    let (action, buf) = xlogutils::xlog_read_buffer_for_redo::call(record, block_id)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(buf, lsn, &mut |page| {
            let mut pageop = BTPageGetOpaque(&page.as_ref())?;
            debug_assert!(P_INCOMPLETE_SPLIT(&pageop));
            pageop.btpo_flags &= !BTP_INCOMPLETE_SPLIT;
            write_opaque(page, &pageop);
            Ok(())
        })?;
    }
    if buffer_is_valid(buf) {
        bufmgr::unlock_release_buffer::call(buf);
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_insert`
// ===========================================================================

fn btree_xlog_insert(
    record: &XLogReaderState<'_>,
    isleaf: bool,
    ismeta: bool,
    posting: bool,
) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_insert(&data);

    // Insertion to an internal page finishes an incomplete split at the child
    // level. Clear the incomplete-split flag in the child.
    if !isleaf {
        _bt_clear_incomplete_split(record, 1)?;
    }
    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let datapos = rec_block_data(record, 0).expect("insert block data present");

        with_op_ctx(|mcx| {
            buffer_modify_page(buffer, lsn, &mut |page| {
                if !posting {
                    // Simple retail insertion
                    if PageAddItemExtended(page, &datapos, xlrec.offnum, 0)? == InvalidOffsetNumber {
                        return Err(PgError::new(PANIC, "failed to add new item"));
                    }
                } else {
                    // A posting list split occurred during leaf page insertion.
                    // WAL record data starts with an offset number representing
                    // the point in an existing posting list that a split occurs
                    // at. Use _bt_swap_posting() to repeat the split steps.
                    let postingoff = rd_u16(&datapos, 0);
                    let datapos_rest = datapos[core::mem::size_of::<u16>()..].to_vec();
                    let datalen = datapos_rest.len();

                    let oposting = page_item(page, OffsetNumberPrev(xlrec.offnum))?;

                    debug_assert!(isleaf && postingoff > 0);
                    let mut newitem = datapos_rest;
                    let nposting =
                        _bt_swap_posting(mcx, &mut newitem, &oposting, postingoff as i32)?;

                    // Replace existing posting list with post-split version.
                    // nposting is the same size as the existing posting list, so
                    // an equal-size PageIndexTupleOverwrite equals C's in-place
                    // memcpy(oposting, nposting, MAXALIGN(IndexTupleSize)).
                    debug_assert!(
                        maxalign(IndexTupleSize(&index_tuple_header(&nposting))) == nposting.len()
                    );
                    let opoff = OffsetNumberPrev(xlrec.offnum);
                    if !PageIndexTupleOverwrite(page, opoff, &nposting)? {
                        return Err(PgError::new(
                            PANIC,
                            "failed to overwrite posting list during posting split",
                        ));
                    }

                    // Insert "final" new item (not orignewitem from WAL stream)
                    debug_assert!(IndexTupleSize(&index_tuple_header(&newitem)) == datalen);
                    if PageAddItemExtended(page, &newitem, xlrec.offnum, 0)? == InvalidOffsetNumber {
                        return Err(PgError::new(PANIC, "failed to add posting split new item"));
                    }
                }
                Ok(())
            })
        })?;
    }
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    if ismeta {
        _bt_restore_meta(record, 2)?;
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_split`
// ===========================================================================

fn btree_xlog_split(record: &XLogReaderState<'_>, newitemonleft: bool) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_split(&data);
    let isleaf = xlrec.level == 0;

    let origpagenumber = block_tag_blknum(record, 0);
    let rightpagenumber = block_tag_blknum(record, 1);
    let spagenumber = if rec_has_block_ref(record, 2) {
        block_tag_blknum(record, 2)
    } else {
        P_NONE
    };

    // Clear the incomplete split flag on the appropriate child page one level
    // down when origpage is internal.
    if !isleaf {
        _bt_clear_incomplete_split(record, 3)?;
    }

    // Reconstruct right (new) sibling page from scratch
    let rbuf = xlogutils::xlog_init_buffer_for_redo::call(record, 1)?;
    let datapos_r = rec_block_data(record, 1).expect("right block data present");
    buffer_modify_page(rbuf, lsn, &mut |rpage| {
        let rpage_size = rpage.as_bytes().len();
        _bt_pageinit(rpage.as_mut_bytes(), rpage_size)?;
        let ropaque = BTPageOpaqueData {
            btpo_prev: origpagenumber,
            btpo_next: spagenumber,
            btpo_level: xlrec.level,
            btpo_flags: if isleaf { BTP_LEAF } else { 0 },
            btpo_cycleid: 0,
        };
        write_opaque(rpage, &ropaque);

        _bt_restore_page(rpage, &datapos_r)?;
        Ok(())
    })?;

    // Now reconstruct original page (left half of split)
    let (action, buf) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let datapos = rec_block_data(record, 0).expect("left block data present");
        let mut datalen = datapos.len();
        let mut dataoff = 0usize;

        let mut newitem: Vec<u8> = Vec::new();
        let mut nposting: Vec<u8> = Vec::new();
        let mut newitemsz = 0usize;
        let mut replacepostingoff: OffsetNumber = InvalidOffsetNumber;

        with_op_ctx(|mcx| {
            buffer_modify_page(buf, lsn, &mut |origpage| {
                let oopaque = BTPageGetOpaque(&origpage.as_ref())?;

                if newitemonleft || xlrec.postingoff != 0 {
                    newitemsz =
                        maxalign(IndexTupleSize(&index_tuple_header(&datapos[dataoff..])));
                    newitem = datapos[dataoff..dataoff + newitemsz].to_vec();
                    dataoff += newitemsz;
                    datalen -= newitemsz;

                    if xlrec.postingoff != 0 {
                        replacepostingoff = OffsetNumberPrev(xlrec.newitemoff);
                        let oposting = page_item(origpage, replacepostingoff)?;
                        nposting = _bt_swap_posting(
                            mcx,
                            &mut newitem,
                            &oposting,
                            xlrec.postingoff as i32,
                        )?
                        .to_vec();
                    }
                }

                // Extract left hikey and its size.
                let left_hikeysz =
                    maxalign(IndexTupleSize(&index_tuple_header(&datapos[dataoff..])));
                let left_hikey = datapos[dataoff..dataoff + left_hikeysz].to_vec();
                dataoff += left_hikeysz;
                datalen -= left_hikeysz;

                debug_assert!(datalen == 0);

                let mut lefttemp = PageGetTempPageCopySpecial(&origpage.as_ref())?;
                {
                    let mut leftpage = PageMut::new(lefttemp.as_mut_bytes())?;

                    let mut leftoff = P_HIKEY;
                    if PageAddItemExtended(&mut leftpage, &left_hikey, P_HIKEY, 0)?
                        == InvalidOffsetNumber
                    {
                        return Err(PgError::error(
                            "failed to add high key to left page after split",
                        ));
                    }
                    leftoff = OffsetNumberNext(leftoff);

                    let mut off = P_FIRSTDATAKEY(&oopaque);
                    while off < xlrec.firstrightoff {
                        if off == replacepostingoff {
                            debug_assert!(
                                newitemonleft || xlrec.firstrightoff == xlrec.newitemoff
                            );
                            let nsz = maxalign(IndexTupleSize(&index_tuple_header(&nposting)));
                            if PageAddItemExtended(&mut leftpage, &nposting[..nsz], leftoff, 0)?
                                == InvalidOffsetNumber
                            {
                                return Err(PgError::error(
                                    "failed to add new posting list item to left page after split",
                                ));
                            }
                            leftoff = OffsetNumberNext(leftoff);
                            off = OffsetNumberNext(off);
                            continue; // don't insert oposting
                        } else if newitemonleft && off == xlrec.newitemoff {
                            if PageAddItemExtended(&mut leftpage, &newitem[..newitemsz], leftoff, 0)?
                                == InvalidOffsetNumber
                            {
                                return Err(PgError::error(
                                    "failed to add new item to left page after split",
                                ));
                            }
                            leftoff = OffsetNumberNext(leftoff);
                        }

                        let itemsz = page_item_len(origpage, off)?;
                        let item = page_item(origpage, off)?;
                        if PageAddItemExtended(&mut leftpage, &item[..itemsz], leftoff, 0)?
                            == InvalidOffsetNumber
                        {
                            return Err(PgError::error(
                                "failed to add old item to left page after split",
                            ));
                        }
                        leftoff = OffsetNumberNext(leftoff);
                        off = OffsetNumberNext(off);
                    }

                    // cope with possibility that newitem goes at the end
                    if newitemonleft && off == xlrec.newitemoff {
                        if PageAddItemExtended(&mut leftpage, &newitem[..newitemsz], leftoff, 0)?
                            == InvalidOffsetNumber
                        {
                            return Err(PgError::error(
                                "failed to add new item to left page after split",
                            ));
                        }
                        leftoff = OffsetNumberNext(leftoff);
                    }
                    let _ = leftoff;
                }

                PageRestoreTempPage(lefttemp, origpage)?;

                // Fix opaque fields
                let mut oopaque = BTPageGetOpaque(&origpage.as_ref())?;
                oopaque.btpo_flags = BTP_INCOMPLETE_SPLIT;
                if isleaf {
                    oopaque.btpo_flags |= BTP_LEAF;
                }
                oopaque.btpo_next = rightpagenumber;
                oopaque.btpo_cycleid = 0;
                write_opaque(origpage, &oopaque);
                Ok(())
            })
        })?;
    }

    // Fix left-link of the page to the right of the new right sibling
    if spagenumber != P_NONE {
        let (saction, sbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 2)?;
        if saction == XLogRedoAction::BlkNeedsRedo {
            buffer_modify_page(sbuf, lsn, &mut |spage| {
                let mut spageop = BTPageGetOpaque(&spage.as_ref())?;
                spageop.btpo_prev = rightpagenumber;
                write_opaque(spage, &spageop);
                Ok(())
            })?;
        }
        if buffer_is_valid(sbuf) {
            bufmgr::unlock_release_buffer::call(sbuf);
        }
    }

    // Release the remaining buffers together.
    bufmgr::unlock_release_buffer::call(rbuf);
    if buffer_is_valid(buf) {
        bufmgr::unlock_release_buffer::call(buf);
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_dedup`
// ===========================================================================

fn btree_xlog_dedup(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_dedup(&data);

    let (action, buf) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let ptr = rec_block_data(record, 0).expect("dedup block data present");

        with_op_ctx(|mcx| {
            buffer_modify_page(buf, lsn, &mut |page| {
                let opaque = BTPageGetOpaque(&page.as_ref())?;

                // state = (BTDedupState) palloc(sizeof(BTDedupStateData)); the
                // redo path conservatively uses BTMaxItemSize for maxpostingsize.
                let mut state: BTDedupState = new_dedup_state(mcx, BTMaxItemSize)?;

                let minoff = P_FIRSTDATAKEY(&opaque);
                let maxoff = PageGetMaxOffsetNumber(&page.as_ref());
                let mut newtemp = PageGetTempPageCopySpecial(&page.as_ref())?;

                let nintervals = xlrec.nintervals as usize;
                let logged_intervals = decode_dedup_intervals(&ptr, nintervals);

                {
                    let mut newpage = PageMut::new(newtemp.as_mut_bytes())?;

                    if !P_RIGHTMOST(&opaque) {
                        let itemsz = page_item_len(page, P_HIKEY)?;
                        let item = page_item(page, P_HIKEY)?;

                        if PageAddItemExtended(&mut newpage, &item[..itemsz], P_HIKEY, 0)?
                            == InvalidOffsetNumber
                        {
                            return Err(PgError::error("deduplication failed to add highkey"));
                        }
                    }

                    let mut offnum = minoff;
                    while offnum <= maxoff {
                        let itup = page_item(page, offnum)?;

                        if offnum == minoff {
                            _bt_dedup_start_pending(&mut state, &itup, offnum)?;
                        } else if state.nintervals < nintervals
                            && state.baseoff == logged_intervals[state.nintervals].baseoff
                            && (state.nitems as u16) < logged_intervals[state.nintervals].nitems
                        {
                            if !_bt_dedup_save_htid(&mut state, &itup)? {
                                return Err(PgError::error(
                                    "deduplication failed to add heap tid to pending posting list",
                                ));
                            }
                        } else {
                            _bt_dedup_finish_pending(mcx, &mut newpage, &mut state)?;
                            _bt_dedup_start_pending(&mut state, &itup, offnum)?;
                        }
                        offnum = OffsetNumberNext(offnum);
                    }

                    _bt_dedup_finish_pending(mcx, &mut newpage, &mut state)?;
                    debug_assert!(state.nintervals == nintervals);
                    debug_assert!(state.intervals[..state.nintervals]
                        .iter()
                        .zip(logged_intervals.iter())
                        .all(|(a, b)| a.baseoff == b.baseoff && a.nitems == b.nitems));

                    if P_HAS_GARBAGE(&opaque) {
                        let mut nopaque = BTPageGetOpaque(&newpage.as_ref())?;
                        nopaque.btpo_flags &= !BTP_HAS_GARBAGE;
                        write_opaque(&mut newpage, &nopaque);
                    }
                }

                PageRestoreTempPage(newtemp, page)?;
                Ok(())
            })
        })?;
    }

    if buffer_is_valid(buf) {
        bufmgr::unlock_release_buffer::call(buf);
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_updates`
// ===========================================================================

fn btree_xlog_updates(
    mcx: Mcx<'_>,
    page: &mut PageMut<'_>,
    updatedoffsets: &[OffsetNumber],
    updates_bytes: &[u8],
    nupdated: usize,
) -> PgResult<()> {
    let mut upoff = 0usize; // running offset into the updates array
    for &updoff in updatedoffsets.iter().take(nupdated) {
        let origtuple = page_item(page, updoff)?;

        // ndeletedtids = updates->ndeletedtids
        let ndeletedtids = rd_u16(updates_bytes, upoff) as usize;
        // deletetids array follows the SizeOfBtreeUpdate header
        let mut deletetids: Vec<u16> = Vec::new();
        let _ = deletetids.try_reserve(ndeletedtids);
        for k in 0..ndeletedtids {
            deletetids.push(rd_u16(updates_bytes, upoff + SizeOfBtreeUpdate + k * 2));
        }

        // _bt_update_posting builds the trimmed posting tuple from origtuple.
        let newtuple = _bt_update_posting(mcx, &origtuple, &deletetids)?;

        // Overwrite updated version of tuple
        let itemsz = maxalign(IndexTupleSize(&index_tuple_header(&newtuple)));
        if !PageIndexTupleOverwrite(page, updoff, &newtuple[..itemsz])? {
            return Err(PgError::new(PANIC, "failed to update partially dead item"));
        }

        // advance to next xl_btree_update from array
        upoff += SizeOfBtreeUpdate + ndeletedtids * core::mem::size_of::<u16>();
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_vacuum`
// ===========================================================================

fn btree_xlog_vacuum(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_vacuum(&data);

    // We need a cleanup lock here, just like btvacuumpage().
    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo_extended::call(
        record,
        0,
        ReadBufferMode::Normal,
        true,
    )?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let ptr = rec_block_data(record, 0).expect("vacuum block data present");

        let ndeleted = xlrec.ndeleted as usize;
        let nupdated = xlrec.nupdated as usize;
        let osz = core::mem::size_of::<OffsetNumber>();

        let mut deleted: Vec<OffsetNumber> = Vec::new();
        let _ = deleted.try_reserve(ndeleted);
        for i in 0..ndeleted {
            deleted.push(rd_u16(&ptr, i * osz));
        }

        with_op_ctx(|mcx| {
            buffer_modify_page(buffer, lsn, &mut |page| {
                if nupdated > 0 {
                    let upbase = ndeleted * osz;
                    let mut updatedoffsets: Vec<OffsetNumber> = Vec::new();
                    let _ = updatedoffsets.try_reserve(nupdated);
                    for i in 0..nupdated {
                        updatedoffsets.push(rd_u16(&ptr, upbase + i * osz));
                    }
                    let updates = &ptr[upbase + nupdated * osz..];
                    btree_xlog_updates(mcx, page, &updatedoffsets, updates, nupdated)?;
                }

                if ndeleted > 0 {
                    PageIndexMultiDelete(page, &deleted)?;
                }

                // Clear the vacuum cycle ID and the LP_DEAD-items hint.
                let mut opaque = BTPageGetOpaque(&page.as_ref())?;
                opaque.btpo_cycleid = 0;
                opaque.btpo_flags &= !BTP_HAS_GARBAGE;
                write_opaque(page, &opaque);
                Ok(())
            })
        })?;
    }
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_delete`
// ===========================================================================

fn btree_xlog_delete(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_delete(&data);

    // Conflict processing must happen before we update the page.
    if in_hot_standby(xlogutils::standby_state::call()) {
        let rlocator = block_tag_rlocator(record, 0);
        with_op_ctx(|mcx| {
            standby::resolve_recovery_conflict_with_snapshot::call(
                mcx,
                xlrec.snapshotConflictHorizon,
                xlrec.isCatalogRel,
                rlocator,
            )
        })?;
    }

    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let ptr = rec_block_data(record, 0).expect("delete block data present");

        let ndeleted = xlrec.ndeleted as usize;
        let nupdated = xlrec.nupdated as usize;
        let osz = core::mem::size_of::<OffsetNumber>();

        let mut deleted: Vec<OffsetNumber> = Vec::new();
        let _ = deleted.try_reserve(ndeleted);
        for i in 0..ndeleted {
            deleted.push(rd_u16(&ptr, i * osz));
        }

        with_op_ctx(|mcx| {
            buffer_modify_page(buffer, lsn, &mut |page| {
                if nupdated > 0 {
                    let upbase = ndeleted * osz;
                    let mut updatedoffsets: Vec<OffsetNumber> = Vec::new();
                    let _ = updatedoffsets.try_reserve(nupdated);
                    for i in 0..nupdated {
                        updatedoffsets.push(rd_u16(&ptr, upbase + i * osz));
                    }
                    let updates = &ptr[upbase + nupdated * osz..];
                    btree_xlog_updates(mcx, page, &updatedoffsets, updates, nupdated)?;
                }

                if ndeleted > 0 {
                    PageIndexMultiDelete(page, &deleted)?;
                }

                // Do *not* clear the vacuum cycle ID, but mark the page as not
                // containing any LP_DEAD items.
                let mut opaque = BTPageGetOpaque(&page.as_ref())?;
                opaque.btpo_flags &= !BTP_HAS_GARBAGE;
                write_opaque(page, &opaque);
                Ok(())
            })
        })?;
    }
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_mark_page_halfdead`
// ===========================================================================

fn btree_xlog_mark_page_halfdead(record: &XLogReaderState<'_>, _info: u8) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_mark_page_halfdead(&data);

    // to-be-deleted subtree's parent page
    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(buffer, lsn, &mut |page| {
            let poffset = xlrec.poffset;

            let nextoffset = OffsetNumberNext(poffset);
            let itup = page_item(page, nextoffset)?;
            let rightsib = BTreeTupleGetDownLink(&index_tuple_header(&itup));

            // Update the downlink in-place on the page item at `poffset`.
            let pr = page.as_ref();
            let itemid = PageGetItemId(&pr, poffset)?;
            let off = ItemIdGetOffset(&itemid) as usize;
            {
                let bytes = page.as_mut_bytes();
                let mut hdr = index_tuple_header(&bytes[off..]);
                BTreeTupleSetDownLink(&mut hdr, rightsib);
                write_index_tuple_header(&mut bytes[off..off + 8], &hdr);
            }
            let nextoffset = OffsetNumberNext(poffset);
            PageIndexTupleDelete(page, nextoffset)?;
            Ok(())
        })?;
    }

    // Don't need to couple cross-level locks; release internal page now.
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    // Rewrite the leaf page as a halfdead page
    let buffer = xlogutils::xlog_init_buffer_for_redo::call(record, 0)?;
    buffer_modify_page(buffer, lsn, &mut |page| {
        let page_size = page.as_bytes().len();
        _bt_pageinit(page.as_mut_bytes(), page_size)?;
        let pageop = BTPageOpaqueData {
            btpo_prev: xlrec.leftblk,
            btpo_next: xlrec.rightblk,
            btpo_level: 0,
            btpo_flags: BTP_HALF_DEAD | BTP_LEAF,
            btpo_cycleid: 0,
        };
        write_opaque(page, &pageop);

        // Construct a dummy high key item that points to top parent page.
        let trunctuple = make_trunctuple(xlrec.topparent);
        if PageAddItemExtended(page, &trunctuple, P_HIKEY, 0)? == InvalidOffsetNumber {
            return Err(PgError::error("could not add dummy high key to half-dead page"));
        }
        Ok(())
    })?;
    bufmgr::unlock_release_buffer::call(buffer);
    Ok(())
}

// ===========================================================================
// `btree_xlog_unlink_page`
// ===========================================================================

fn btree_xlog_unlink_page(record: &XLogReaderState<'_>, info: u8) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_unlink_page(&data);

    let leftsib = xlrec.leftsib;
    let rightsib = xlrec.rightsib;
    let level = xlrec.level;
    let isleaf = level == 0;
    let safexid = xlrec.safexid;

    // No leaftopparent for level 0 (leaf) or level 1 target.
    debug_assert!(xlrec.leaftopparent == InvalidBlockNumber || level > 1);

    // Fix right-link of left sibling, if any
    let leftbuf;
    if leftsib != P_NONE {
        let (action, lbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            buffer_modify_page(lbuf, lsn, &mut |page| {
                let mut pageop = BTPageGetOpaque(&page.as_ref())?;
                pageop.btpo_next = rightsib;
                write_opaque(page, &pageop);
                Ok(())
            })?;
        }
        leftbuf = lbuf;
    } else {
        leftbuf = InvalidBuffer;
    }

    // Rewrite target page as empty deleted page
    let target = xlogutils::xlog_init_buffer_for_redo::call(record, 0)?;
    buffer_modify_page(target, lsn, &mut |page| {
        let page_size = page.as_bytes().len();
        _bt_pageinit(page.as_mut_bytes(), page_size)?;
        let mut pageop = BTPageOpaqueData {
            btpo_prev: leftsib,
            btpo_next: rightsib,
            btpo_level: level,
            btpo_flags: 0,
            btpo_cycleid: 0,
        };
        write_opaque(page, &pageop);
        bt_page_set_deleted(page, safexid)?;
        // BTPageSetDeleted rewrote the opaque; re-read to set BTP_LEAF.
        pageop = BTPageGetOpaque(&page.as_ref())?;
        if isleaf {
            pageop.btpo_flags |= BTP_LEAF;
        }
        pageop.btpo_cycleid = 0;
        write_opaque(page, &pageop);
        Ok(())
    })?;

    // Fix left-link of right sibling
    let (raction, rightbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 2)?;
    if raction == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(rightbuf, lsn, &mut |page| {
            let mut pageop = BTPageGetOpaque(&page.as_ref())?;
            pageop.btpo_prev = leftsib;
            write_opaque(page, &pageop);
            Ok(())
        })?;
    }

    // Release siblings
    if buffer_is_valid(leftbuf) {
        bufmgr::unlock_release_buffer::call(leftbuf);
    }
    if buffer_is_valid(rightbuf) {
        bufmgr::unlock_release_buffer::call(rightbuf);
    }

    // Release target
    bufmgr::unlock_release_buffer::call(target);

    // If we deleted a parent of the targeted leaf page, update the leaf to point
    // to the next remaining child in the to-be-deleted subtree.
    if rec_has_block_ref(record, 3) {
        debug_assert!(!isleaf);

        let leafbuf = xlogutils::xlog_init_buffer_for_redo::call(record, 3)?;
        buffer_modify_page(leafbuf, lsn, &mut |page| {
            let lpage_size = page.as_bytes().len();
            _bt_pageinit(page.as_mut_bytes(), lpage_size)?;
            let pageop = BTPageOpaqueData {
                btpo_prev: xlrec.leafleftsib,
                btpo_next: xlrec.leafrightsib,
                btpo_level: 0,
                btpo_flags: BTP_HALF_DEAD | BTP_LEAF,
                btpo_cycleid: 0,
            };
            write_opaque(page, &pageop);

            let trunctuple = make_trunctuple(xlrec.leaftopparent);
            if PageAddItemExtended(page, &trunctuple, P_HIKEY, 0)? == InvalidOffsetNumber {
                return Err(PgError::error("could not add dummy high key to half-dead page"));
            }
            Ok(())
        })?;
        bufmgr::unlock_release_buffer::call(leafbuf);
    }

    // Update metapage if needed
    if info == XLOG_BTREE_UNLINK_PAGE_META {
        _bt_restore_meta(record, 4)?;
    }
    Ok(())
}

// ===========================================================================
// `btree_xlog_newroot`
// ===========================================================================

fn btree_xlog_newroot(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let data = rec_data(record);
    let xlrec = decode_newroot(&data);

    let buffer = xlogutils::xlog_init_buffer_for_redo::call(record, 0)?;

    let block0 = if xlrec.level > 0 {
        rec_block_data(record, 0)
    } else {
        None
    };

    buffer_modify_page(buffer, lsn, &mut |page| {
        let page_size = page.as_bytes().len();
        _bt_pageinit(page.as_mut_bytes(), page_size)?;
        let mut pageop = BTPageOpaqueData {
            btpo_prev: P_NONE,
            btpo_next: P_NONE,
            btpo_level: xlrec.level,
            btpo_flags: BTP_ROOT,
            btpo_cycleid: 0,
        };
        if xlrec.level == 0 {
            pageop.btpo_flags |= BTP_LEAF;
        }
        write_opaque(page, &pageop);

        if xlrec.level > 0 {
            let ptr = block0.as_ref().expect("newroot block data present");
            _bt_restore_page(page, ptr)?;
        }
        Ok(())
    })?;

    if xlrec.level > 0 {
        // Clear the incomplete-split flag in left child
        _bt_clear_incomplete_split(record, 1)?;
    }

    bufmgr::unlock_release_buffer::call(buffer);

    _bt_restore_meta(record, 2)?;
    Ok(())
}

// ===========================================================================
// `btree_xlog_reuse_page`
// ===========================================================================

fn btree_xlog_reuse_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let data = rec_data(record);
    let xlrec = decode_reuse_page(&data);

    if in_hot_standby(xlogutils::standby_state::call()) {
        with_op_ctx(|mcx| {
            standby::resolve_recovery_conflict_with_snapshot_full_xid::call(
                mcx,
                xlrec.snapshotConflictHorizon,
                xlrec.isCatalogRel,
                xlrec.locator,
            )
        })?;
    }
    Ok(())
}

// ===========================================================================
// `btree_redo`
// ===========================================================================

/// `btree_redo()` -- the nbtree rmgr redo dispatcher (`rm_redo` slot).
pub fn btree_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = rec_info(record);

    // oldCtx = MemoryContextSwitchTo(opCtx); ... MemoryContextReset(opCtx);
    let result = (|| match info {
        x if x == XLOG_BTREE_INSERT_LEAF => btree_xlog_insert(record, true, false, false),
        x if x == XLOG_BTREE_INSERT_UPPER => btree_xlog_insert(record, false, false, false),
        x if x == XLOG_BTREE_INSERT_META => btree_xlog_insert(record, false, true, false),
        x if x == XLOG_BTREE_SPLIT_L => btree_xlog_split(record, true),
        x if x == XLOG_BTREE_SPLIT_R => btree_xlog_split(record, false),
        x if x == XLOG_BTREE_INSERT_POST => btree_xlog_insert(record, true, false, true),
        x if x == XLOG_BTREE_DEDUP => btree_xlog_dedup(record),
        x if x == XLOG_BTREE_VACUUM => btree_xlog_vacuum(record),
        x if x == XLOG_BTREE_DELETE => btree_xlog_delete(record),
        x if x == XLOG_BTREE_MARK_PAGE_HALFDEAD => btree_xlog_mark_page_halfdead(record, info),
        x if x == XLOG_BTREE_UNLINK_PAGE || x == XLOG_BTREE_UNLINK_PAGE_META => {
            btree_xlog_unlink_page(record, info)
        }
        x if x == XLOG_BTREE_NEWROOT => btree_xlog_newroot(record),
        x if x == XLOG_BTREE_REUSE_PAGE => btree_xlog_reuse_page(record),
        x if x == XLOG_BTREE_META_CLEANUP => _bt_restore_meta(record, 0),
        _ => Err(PgError::new(
            PANIC,
            format!("btree_redo: unknown op code {info}"),
        )),
    })();
    result?;

    // MemoryContextReset(opCtx).
    OP_CTX.with(|c| {
        if let Some(ctx) = c.borrow_mut().as_mut() {
            ctx.reset();
        }
    });
    Ok(())
}

// ===========================================================================
// `btree_xlog_startup` / `btree_xlog_cleanup`
// ===========================================================================

/// `btree_xlog_startup()` -- create the recovery working-memory context
/// (`rm_startup` slot). `parent` is the recovery `CurrentMemoryContext` the
/// rmgr passes; the new context is a regular AllocSet under it.
pub fn btree_xlog_startup(_parent: Mcx<'_>) -> PgResult<()> {
    OP_CTX.with(|c| {
        *c.borrow_mut() = Some(MemoryContext::new("Btree recovery temporary context"));
    });
    Ok(())
}

/// `btree_xlog_cleanup()` -- delete the recovery working-memory context
/// (`rm_cleanup` slot).
pub fn btree_xlog_cleanup() {
    OP_CTX.with(|c| {
        *c.borrow_mut() = None;
    });
}

// ===========================================================================
// `btree_mask`
// ===========================================================================

/// `btree_mask()` -- mask a btree page before WAL consistency checking
/// (`rm_mask` slot).
pub fn btree_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum::call(pagedata);

    bufmask::mask_page_hint_bits::call(pagedata);
    bufmask::mask_unused_space::call(pagedata)?;

    // P_ISLEAF on the masked page's opaque area.
    let is_leaf = {
        let page = PageRef::new(pagedata)?;
        let maskopaq = BTPageGetOpaque(&page)?;
        P_ISLEAF(&maskopaq)
    };

    if is_leaf {
        // Leaf LP_FLAGS can change without WAL; mask the line pointer flags.
        bufmask::mask_lp_flags::call(pagedata);
    }

    // BTP_HAS_GARBAGE/BTP_SPLIT_END are un-logged hints; mask them and the
    // cycle id (the split redo leaves these unset/zero on the right sibling).
    {
        let mut page = PageMut::new(pagedata)?;
        let mut maskopaq = BTPageGetOpaque(&page.as_ref())?;
        maskopaq.btpo_flags &= !BTP_HAS_GARBAGE;
        maskopaq.btpo_flags &= !BTP_SPLIT_END;
        maskopaq.btpo_cycleid = 0;
        write_opaque(&mut page, &maskopaq);
    }
    Ok(())
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this unit's owned rmgr-callback seams (consumed by
/// `backend-access-transam-rmgr`'s `RmgrTable`).
pub fn init_seams() {
    nbt_xlog_seams::btree_redo::set(btree_redo);
    nbt_xlog_seams::btree_xlog_startup::set(btree_xlog_startup);
    nbt_xlog_seams::btree_xlog_cleanup::set(btree_xlog_cleanup);
    nbt_xlog_seams::btree_mask::set(btree_mask);
}
