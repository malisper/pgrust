#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

//! Port of `src/backend/access/heap/heapam_xlog.c` (PostgreSQL 18.3).
//!
//! WAL replay (redo) and masking for the heap access method. These run during
//! recovery and consume the [`XLogReaderState`] records emitted by the heap
//! insert / delete / update / multi-insert / lock / inplace / prune-freeze /
//! visibility-map / speculative-confirm paths.
//!
//! Functions ported 1:1 with C (original names preserved):
//! - [`heap_redo`]                  — the RM_HEAP rmgr redo dispatcher
//! - [`heap2_redo`]                 — the RM_HEAP2 rmgr redo dispatcher
//! - [`heap_mask`]                  — the rmgr `rm_mask` function
//! - `heap_xlog_insert` / `_delete` / `_update` (+HOT) / `_multi_insert`
//! - `heap_xlog_confirm` / `_lock` / `_lock_updated` / `_inplace`
//! - `heap_xlog_prune_freeze` / `_visible`
//! - `fix_infomask_from_infobits`
//!
//! ## Model differences from the C tree
//!
//! `record` is the value-typed [`XLogReaderState`] this repo decodes WAL into;
//! `XLogRecGet*` are its `DecodedXLogRecord` accessors (read off
//! `record.record`). The WAL on-disk structs (`xl_heap_insert`, `xl_heap_prune`,
//! ...) are decoded field-by-field out of the (possibly unaligned)
//! `XLogRecGetData` / `XLogRecGetBlockData` byte buffers into the
//! `xlog_records` value structs.
//!
//! A redo buffer is mutated by handing the bufmgr the page bytes through
//! `with_buffer_page` (a `&mut [u8]` view of the shared buffer's page), applying
//! the change with the `page` codec (`PageInit`, `PageAddItem`,
//! `PageSetAllVisible`, the line-pointer reads, the
//! `HeapTupleHeaderData::read_on_page` / `write_on_page` tuple-header
//! read/write), then stamping the record LSN with `PageSetLSN` and
//! `MarkBufferDirty` — exactly the C `PageSetLSN` + `MarkBufferDirty` sequence.
//! The recovery buffer manager (`XLogReadBufferForRedo[Extended]`,
//! `XLogInitBufferForRedo`, `UnlockReleaseBuffer`) is the (real) xlogutils crate;
//! the visibility map, the fake relcache, the FSM, the prune executor, the
//! freeze applier (a tiny header-math mirror, below), the Hot-Standby conflict
//! machinery, the logical-rewrite replay, and the shared-inval replay are
//! reached through seams / owner crates; the C control flow (which blocks need
//! redo, lock/release ordering, vm/FSM bookkeeping) is preserved exactly.
//!
//! `CurrentMemoryContext` (used by the C to `palloc` the transient fake relcache
//! entries and the conflict-resolution working memory) is modelled by a
//! thread-local recovery context created lazily and reset after each redo
//! dispatch — the C frees those allocations promptly within each record's
//! replay, and the heap rmgr has no `rm_startup` / `rm_cleanup` hooks.

extern crate alloc;

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;

use ::mcx::{MemoryContext, Mcx};
use ::types_core::primitive::{BlockNumber, OffsetNumber, TransactionId, XLogRecPtr, Oid, BLCKSZ};
use ::types_error::{PgError, PgResult, PANIC};
use ::types_storage::storage::ReadBufferMode;
use ::types_storage::RelFileLocator;
use ::types_storage::buf::{Buffer, BufferIsValid, InvalidBuffer};
use ::types_storage::sinval::SharedInvalidationMessage;
use ::types_storage::bufpage::SizeofHeapTupleHeader;
use ::types_tuple::heaptuple::{
    BlockIdData, HeapTupleField3, HeapTupleFields, HeapTupleHeaderChoice, HeapTupleHeaderData,
    ItemPointerData, HEAP_KEYS_UPDATED, HEAP_MOVED, HEAP_XACT_MASK, HEAP_XMAX_COMMITTED,
    HEAP_XMAX_EXCL_LOCK, HEAP_XMAX_INVALID, HEAP_XMAX_IS_MULTI, HEAP_XMAX_KEYSHR_LOCK,
    HEAP_XMAX_LOCK_ONLY, HEAP_XMIN_FROZEN,
};
use ::types_vacuum::vacuum::HeapTupleFreeze;
use ::wal::rmgr::XLogReaderState;
use ::wal::XLogRedoAction;

use ::xlog_records::heapam_xlog::{
    xl_heap_confirm, xl_heap_delete, xl_heap_header, xl_heap_inplace, xl_heap_insert, xl_heap_lock,
    xl_heap_lock_updated, xl_heap_multi_insert, xl_heap_prune, xl_heap_update, xl_heap_visible,
    xl_multi_insert_tuple, SizeOfHeapHeader, SizeOfMultiInsertTuple,
    XLH_DELETE_ALL_VISIBLE_CLEARED, XLH_DELETE_IS_PARTITION_MOVE, XLH_DELETE_IS_SUPER,
    XLH_INSERT_ALL_FROZEN_SET, XLH_INSERT_ALL_VISIBLE_CLEARED, XLH_LOCK_ALL_FROZEN_CLEARED,
    XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED, XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED,
    XLH_UPDATE_PREFIX_FROM_OLD, XLH_UPDATE_SUFFIX_FROM_OLD,
};

use ::page::{
    ItemIdGetLength, ItemIdGetOffset, ItemIdHasStorage, ItemIdIsNormal, PageAddItemExtended,
    PageClearAllVisible, PageGetFreeSpace, PageGetHeapFreeSpace, PageGetItem, PageGetItemId,
    PageGetMaxOffsetNumber, PageInit, PageIsNew, PageMut, PageRef, PageSetAllVisible, PageSetLSN,
    PageSetPrunable,
};
use ::types_storage::bufpage::{PAI_IS_HEAP, PAI_OVERWRITE};

use ::xlogutils::{
    standby_state, XLogInitBufferForRedo, XLogReadBufferForRedo, XLogReadBufferForRedoExtended,
};
use ::wal::xlogutils::in_hot_standby;
use ::freespace::XLogRecordPageWithFreeSpace;

// opcode + flag constants live in the ported heapdesc (heapam_xlog.h).
use ::rmgrdesc_next::heapdesc::{
    XLHL_KEYS_UPDATED, XLHL_XMAX_EXCL_LOCK, XLHL_XMAX_IS_MULTI, XLHL_XMAX_KEYSHR_LOCK,
    XLHL_XMAX_LOCK_ONLY, XLHP_CLEANUP_LOCK, XLHP_HAS_CONFLICT_HORIZON, XLHP_HAS_DEAD_ITEMS,
    XLHP_HAS_NOW_UNUSED_ITEMS, XLHP_HAS_REDIRECTIONS, XLHP_IS_CATALOG_REL, XLOG_HEAP2_LOCK_UPDATED,
    XLOG_HEAP2_MULTI_INSERT, XLOG_HEAP2_NEW_CID, XLOG_HEAP2_PRUNE_ON_ACCESS,
    XLOG_HEAP2_PRUNE_VACUUM_CLEANUP, XLOG_HEAP2_PRUNE_VACUUM_SCAN, XLOG_HEAP2_REWRITE,
    XLOG_HEAP2_VISIBLE, XLOG_HEAP_CONFIRM, XLOG_HEAP_DELETE, XLOG_HEAP_HOT_UPDATE,
    XLOG_HEAP_INIT_PAGE, XLOG_HEAP_INPLACE, XLOG_HEAP_INSERT, XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK,
    XLOG_HEAP_TRUNCATE, XLOG_HEAP_UPDATE,
};
use ::rmgrdesc_next::heapdesc::heap_xlog_deserialize_prune_and_freeze;

use bufmask_seams as bufmask;
use pruneheap_seams as pruneheap;
use rewriteheap_seams as rewrite;
use ::xlogreader_seams::xlog_rec_get_block_tag_extended;
use bufmgr_seams as bufmgr;
use standby_seams as standby;
use inval_seams as inval;
use relcache_seams as relcache;

#[cfg(test)]
mod tests;

// ===========================================================================
// `CurrentMemoryContext` during recovery — a transient working context. The C
// `palloc`s the fake relcache entries (and the conflict-resolution scratch) in
// `CurrentMemoryContext` and frees them within each record's replay; we mirror
// that with a lazily-created thread-local context reset after each dispatch.
// ===========================================================================

thread_local! {
    static REDO_CTX: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };
}

/// Run `f` with an [`Mcx`] borrowed from the recovery context (creating it on
/// first use), mirroring C's `CurrentMemoryContext`.
fn with_redo_ctx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
    REDO_CTX.with(|c| {
        {
            let mut b = c.borrow_mut();
            if b.is_none() {
                *b = Some(MemoryContext::new("Heap recovery temporary context"));
            }
        }
        let borrow = c.borrow();
        let ctx = borrow.as_ref().expect("recovery context created above");
        f(ctx.mcx())
    })
}

/// Reset the recovery context (drop the per-record transient allocations).
fn reset_redo_ctx() {
    REDO_CTX.with(|c| {
        if let Some(ctx) = c.borrow_mut().as_mut() {
            ctx.reset();
        }
    });
}

// ===========================================================================
// Visibility-map constants (visibilitymap.h / heapam_xlog.h). Only
// `VISIBILITYMAP_XLOG_*` are not re-exported by the vm crate, so define them.
// ===========================================================================

/// `VISIBILITYMAP_VALID_BITS` (visibilitymap.h).
const VISIBILITYMAP_VALID_BITS: u8 = 0x03;
/// `VISIBILITYMAP_ALL_FROZEN` (visibilitymap.h).
const VISIBILITYMAP_ALL_FROZEN: u8 = 0x02;
/// `VISIBILITYMAP_XLOG_CATALOG_REL` (heapam_xlog.h) — VM bit that means the heap
/// page belongs to a catalog relation (carried only in the WAL record's flags).
const VISIBILITYMAP_XLOG_CATALOG_REL: u8 = 0x04;
/// `VISIBILITYMAP_XLOG_VALID_BITS` (heapam_xlog.h).
const VISIBILITYMAP_XLOG_VALID_BITS: u8 = VISIBILITYMAP_VALID_BITS | VISIBILITYMAP_XLOG_CATALOG_REL;

/// `HEAP_XMAX_BITS` (htup_details.h) — every infomask bit that participates in
/// the xmax/lock state, cleared before re-deriving it from the record.
const HEAP_XMAX_BITS: u16 = HEAP_XMAX_COMMITTED
    | HEAP_XMAX_INVALID
    | HEAP_XMAX_IS_MULTI
    | HEAP_XMAX_LOCK_ONLY
    | HEAP_XMAX_KEYSHR_LOCK
    | HEAP_XMAX_EXCL_LOCK;

/// `FirstCommandId` (c.h).
const FirstCommandId: u32 = 0;
/// `InvalidTransactionId` (transam.h).
const InvalidTransactionId: TransactionId = 0;
/// `InvalidOffsetNumber` (off.h).
const InvalidOffsetNumber: OffsetNumber = 0;
/// `SpecTokenOffsetNumber` (itemptr.h).
const SpecTokenOffsetNumber: OffsetNumber = 0xfffe;
/// `MASK_MARKER` (bufmask.h) — byte written into masked regions (== 0).
const MASK_MARKER: u8 = 0;
/// `MovedPartitionsBlockNumber` / `MovedPartitionsOffsetNumber` (itemptr.h).
const MovedPartitionsBlockNumber: BlockNumber = 0xffff_ffff;
const MovedPartitionsOffsetNumber: OffsetNumber = 0xfffd;
/// `XLH_FREEZE_XVAC` / `XLH_INVALID_XVAC` (heapam_xlog.h) — freeze-plan flags.
const XLH_FREEZE_XVAC: u8 = 0x02;
const XLH_INVALID_XVAC: u8 = 0x04;
/// `FrozenTransactionId` (transam.h).
const FrozenTransactionId: TransactionId = 2;
/// `sizeof(xl_heap_rewrite_mapping)` (heapam_xlog.h; see rewriteheap.c).
const SIZEOF_XL_HEAP_REWRITE_MAPPING: usize = 40;
/// `XLR_INFO_MASK` (xlogrecord.h).
const XLR_INFO_MASK: u8 = 0x0F;

// ===========================================================================
// Decoded-record accessors (read off `record.record`, owned by xlogreader).
// ===========================================================================

/// `XLogRecGetData(record)` — the record's main data.
fn record_get_data<'a>(record: &'a XLogReaderState<'_>) -> &'a [u8] {
    record.record.as_ref().map(|r| r.data()).unwrap_or(&[])
}

/// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
fn record_get_info(record: &XLogReaderState<'_>) -> u8 {
    record.record.as_ref().map(|r| r.info()).unwrap_or(0)
}

/// `XLogRecGetXid(record)`.
fn record_get_xid(record: &XLogReaderState<'_>) -> TransactionId {
    record.record.as_ref().map(|r| r.xid()).unwrap_or(0)
}

/// `XLogRecGetBlockData(record, block_id, &len)` — per-block data.
fn record_get_block_data<'a>(record: &'a XLogReaderState<'_>, block_id: u8) -> &'a [u8] {
    record
        .record
        .as_ref()
        .and_then(|r| r.block_data(block_id as usize))
        .unwrap_or(&[])
}

/// `XLogRecGetBlockTagExtended(record, id, &rlocator, NULL, &blk, NULL)` — block
/// reference, or `None` (C `false`) for a bogus id.
fn block_tag(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<Option<(RelFileLocator, BlockNumber)>> {
    Ok(xlog_rec_get_block_tag_extended::call(record, block_id)?.map(|t| (t.rlocator, t.blkno)))
}

/// `XLogRecGetBlockTag(record, id, ...)` — the (rlocator, blkno) for a block id
/// that the caller knows is present.
fn block_tag_unwrap(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<(RelFileLocator, BlockNumber)> {
    block_tag(record, block_id)?
        .ok_or_else(|| PgError::new(PANIC, "heap redo: missing expected block reference"))
}

/// `record->EndRecPtr`.
fn rec_lsn(record: &XLogReaderState<'_>) -> XLogRecPtr {
    record.EndRecPtr
}

// ===========================================================================
// Page / tuple helpers (storage/bufpage.h, storage/itemptr.h).
// ===========================================================================

/// `ItemPointerSet(&tid, blk, off)`.
fn item_pointer_set(blk: BlockNumber, off: OffsetNumber) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: BlockIdData::new(blk),
        ip_posid: off,
    }
}

/// Read the page-resident tuple header at `offnum`, running the
/// `PageGetMaxOffsetNumber(page) < offnum || !ItemIdIsNormal(lp)` "invalid lp"
/// guard in-crate (PANIC exactly where the C does). Returns the owned header.
fn read_normal_tuple_header<'mcx>(
    mcx: Mcx<'mcx>,
    page: &PageRef<'_>,
    offnum: OffsetNumber,
) -> PgResult<HeapTupleHeaderData<'mcx>> {
    let max_off = PageGetMaxOffsetNumber(page);
    let lp_normal = if max_off >= offnum {
        let lp = PageGetItemId(page, offnum)?;
        ItemIdIsNormal(&lp)
    } else {
        false
    };
    if max_off < offnum || !lp_normal {
        return Err(PgError::new(PANIC, "invalid lp"));
    }
    let lp = PageGetItemId(page, offnum)?;
    let item = PageGetItem(page, &lp)?;
    HeapTupleHeaderData::read_on_page(mcx, item)
}

/// Write a materialized header back over the on-page item at `offnum`.
fn overwrite_tuple_header(page: &mut PageMut<'_>, offnum: OffsetNumber, htup: &HeapTupleHeaderData<'_>) -> PgResult<()> {
    let (off, len) = {
        let pr = page.as_ref();
        let lp = PageGetItemId(&pr, offnum)?;
        (ItemIdGetOffset(&lp) as usize, ItemIdGetLength(&lp) as usize)
    };
    let item = page
        .as_mut_bytes()
        .get_mut(off..off + len)
        .ok_or_else(|| PgError::error("heap redo: item storage outside page"))?;
    htup.write_on_page(item)
}

// ===========================================================================
// htup_details.h header-field setters (mirrors heapam delete.rs / lock.rs).
// ===========================================================================

fn HeapTupleHeaderSetXmin(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmin = xid;
    }
}

fn HeapTupleHeaderSetXmax(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_xmax = xid;
    }
}

/// `HeapTupleHeaderSetXvac(tup, xid)` — `t_field3.t_xvac` (only on HEAP_MOVED).
fn HeapTupleHeaderSetXvac(hdr: &mut HeapTupleHeaderData<'_>, xid: TransactionId) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_field3 = HeapTupleField3::TXvac(xid);
    }
}

/// `HeapTupleHeaderSetCmin(tup, cid)` — `t_field3.t_cid = cid; clear COMBOCID`.
fn HeapTupleHeaderSetCmin(hdr: &mut HeapTupleHeaderData<'_>, cid: u32) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_field3 = HeapTupleField3::TCid(cid);
    }
    hdr.t_infomask &= !HEAP_COMBOCID;
}

/// `HeapTupleHeaderSetCmax(tup, cid, iscombo)`.
fn HeapTupleHeaderSetCmax(hdr: &mut HeapTupleHeaderData<'_>, cid: u32, iscombo: bool) {
    if let HeapTupleHeaderChoice::THeap(f) = &mut hdr.t_choice {
        f.t_field3 = HeapTupleField3::TCid(cid);
    }
    if iscombo {
        hdr.t_infomask |= HEAP_COMBOCID;
    } else {
        hdr.t_infomask &= !HEAP_COMBOCID;
    }
}

fn HeapTupleHeaderClearHotUpdated(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 &= !HEAP_HOT_UPDATED;
}

fn HeapTupleHeaderSetHotUpdated(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_infomask2 |= HEAP_HOT_UPDATED;
}

/// `HeapTupleHeaderSetMovedPartitions(tup)`.
fn HeapTupleHeaderSetMovedPartitions(hdr: &mut HeapTupleHeaderData<'_>) {
    hdr.t_ctid.ip_blkid = BlockIdData::new(MovedPartitionsBlockNumber);
    hdr.t_ctid.ip_posid = MovedPartitionsOffsetNumber;
}

/// `HeapTupleHeaderXminFrozen(tup)` (htup_details.h).
fn HeapTupleHeaderXminFrozen(hdr: &HeapTupleHeaderData<'_>) -> bool {
    (hdr.t_infomask & HEAP_XMIN_FROZEN) == HEAP_XMIN_FROZEN
}

/// `HeapTupleHeaderIsSpeculative(tup)` (htup_details.h).
fn HeapTupleHeaderIsSpeculative(hdr: &HeapTupleHeaderData<'_>) -> bool {
    hdr.t_ctid.ip_posid == SpecTokenOffsetNumber
}

/// `HEAP_XMAX_IS_LOCKED_ONLY(infomask)` (htup_details.h).
fn HEAP_XMAX_IS_LOCKED_ONLY(infomask: u16) -> bool {
    (infomask & HEAP_XMAX_LOCK_ONLY) != 0
        || (infomask & (HEAP_XMAX_IS_MULTI | HEAP_LOCK_MASK)) == HEAP_XMAX_EXCL_LOCK
}

/// `set page_htup->t_choice.t_heap.t_field3.t_cid = cid` (heap_mask).
fn set_t_cid_raw(hdr: &mut HeapTupleHeaderData<'_>, cid: u32) {
    match &mut hdr.t_choice {
        HeapTupleHeaderChoice::THeap(f) => f.t_field3 = HeapTupleField3::TCid(cid),
        other => {
            *other = HeapTupleHeaderChoice::THeap(HeapTupleFields {
                t_xmin: 0,
                t_xmax: 0,
                t_field3: HeapTupleField3::TCid(cid),
            });
        }
    }
}

/// `HEAP_COMBOCID` (htup_details.h).
const HEAP_COMBOCID: u16 = 0x0020;
/// `HEAP_HOT_UPDATED` (htup_details.h).
const HEAP_HOT_UPDATED: u16 = 0x4000;
/// `HEAP_LOCK_MASK` (htup_details.h).
const HEAP_LOCK_MASK: u16 = HEAP_XMAX_EXCL_LOCK | HEAP_XMAX_KEYSHR_LOCK;

/// `MAXALIGN(len)` (c.h).
#[inline]
const fn maxalign(len: usize) -> usize {
    const ALIGNOF: usize = 8;
    (len + (ALIGNOF - 1)) & !(ALIGNOF - 1)
}

/// `SHORTALIGN(len)` (c.h).
#[inline]
const fn shortalign(len: usize) -> usize {
    const ALIGNOF: usize = 2;
    (len + (ALIGNOF - 1)) & !(ALIGNOF - 1)
}

// ===========================================================================
// fix_infomask_from_infobits (heapam_xlog.c)
// ===========================================================================

/// Given an "infobits" field from an XLog record, set the correct bits in the
/// given infomask and infomask2. (Reverse of `compute_infobits`.)
fn fix_infomask_from_infobits(infobits: u8, infomask: &mut u16, infomask2: &mut u16) {
    *infomask &=
        !(HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
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
    // note HEAP_XMAX_SHR_LOCK isn't considered here
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        *infomask |= HEAP_XMAX_KEYSHR_LOCK;
    }

    if infobits & XLHL_KEYS_UPDATED != 0 {
        *infomask2 |= HEAP_KEYS_UPDATED;
    }
}

/// `heap_execute_freeze_tuple(tuple, frz)` (heapam.c) — mirrored locally (pure
/// header math) to avoid depending on the heavy heapam crate.
fn heap_execute_freeze_tuple(tuple: &mut HeapTupleHeaderData<'_>, frz: &HeapTupleFreeze) {
    HeapTupleHeaderSetXmax(tuple, frz.xmax);

    if frz.frzflags & XLH_FREEZE_XVAC != 0 {
        HeapTupleHeaderSetXvac(tuple, FrozenTransactionId);
    }
    if frz.frzflags & XLH_INVALID_XVAC != 0 {
        HeapTupleHeaderSetXvac(tuple, InvalidTransactionId);
    }

    tuple.t_infomask = frz.t_infomask;
    tuple.t_infomask2 = frz.t_infomask2;
}

// ===========================================================================
// visibility-map helpers — `visibilitymap_clear` over a fake relcache entry.
// ===========================================================================

/// The C "create fake relcache entry, pin, clear, release, free" unit used by
/// the redo routines to fix the VM bit even when the heap page is up-to-date.
fn visibilitymap_clear_fake(rlocator: RelFileLocator, blkno: BlockNumber, flags: u8) -> PgResult<()> {
    with_redo_ctx(|mcx| {
        // CreateFakeRelcacheEntry(rlocator) — wrapped as a transient relcache
        // handle; its allocation is reclaimed when the redo context resets after
        // the dispatch (== C's FreeFakeRelcacheEntry pfree of CurrentMemoryContext).
        let reln = rel::Relation::open(
            relcache::create_fake_relcache_entry::call(mcx, rlocator)?,
            None,
        );
        let mut vmbuffer: Buffer = InvalidBuffer;
        visibilitymap::visibilitymap_pin(&reln, blkno, &mut vmbuffer)?;
        visibilitymap::visibilitymap_clear(&reln, blkno, vmbuffer, flags)?;
        if BufferIsValid(vmbuffer) {
            bufmgr::release_buffer::call(vmbuffer);
        }
        Ok(())
    })
}

// ===========================================================================
// heap_xlog_prune_freeze (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_prune_freeze(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let maindata = record_get_data(record);
    let xlrec = xl_heap_prune::from_bytes(maindata);

    let (rlocator, blkno) = block_tag_unwrap(record, 0)?;

    // Assert((flags & XLHP_CLEANUP_LOCK) != 0 ||
    //        (flags & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS)) == 0);
    debug_assert!(
        (xlrec.flags & XLHP_CLEANUP_LOCK) != 0
            || (xlrec.flags & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS)) == 0
    );

    // The conflict horizon XID comes after xl_heap_prune (unaligned).
    if (xlrec.flags & XLHP_HAS_CONFLICT_HORIZON) != 0 {
        let snapshot_conflict_horizon = xl_heap_prune::conflict_horizon(maindata);

        if in_hot_standby(standby_state()) {
            with_redo_ctx(|mcx| {
                standby::resolve_recovery_conflict_with_snapshot::call(
                    mcx,
                    snapshot_conflict_horizon,
                    (xlrec.flags & XLHP_IS_CATALOG_REL) != 0,
                    rlocator,
                )
            })?;
        }
    }

    // If we have a full-page image, restore it and we're done.
    let (action, buffer) = XLogReadBufferForRedoExtended(
        record,
        0,
        ReadBufferMode::Normal,
        (xlrec.flags & XLHP_CLEANUP_LOCK) != 0,
    )?;
    let mut buffer = buffer;
    if action == XLogRedoAction::BlkNeedsRedo {
        let dataptr = record_get_block_data(record, 0);
        let decoded = heap_xlog_deserialize_prune_and_freeze(dataptr, xlrec.flags);

        let nredirected = decoded.nredirected;
        let ndead = decoded.ndead;
        let nunused = decoded.nunused;

        // Update all line pointers per the record, and repair fragmentation if
        // needed (the prune executor owns the page-byte line-pointer ops).
        if nredirected > 0 || ndead > 0 || nunused > 0 {
            let mut redirected: Vec<OffsetNumber> = Vec::new();
            for i in 0..(nredirected as usize) {
                let (from, to) = decoded.redirected.get(i);
                redirected.push(from);
                redirected.push(to);
            }
            let mut nowdead: Vec<OffsetNumber> = Vec::new();
            for i in 0..(ndead as usize) {
                nowdead.push(decoded.nowdead.get(i));
            }
            let mut nowunused: Vec<OffsetNumber> = Vec::new();
            for i in 0..(nunused as usize) {
                nowunused.push(decoded.nowunused.get(i));
            }
            pruneheap::heap_page_prune_execute::call(
                buffer,
                (xlrec.flags & XLHP_CLEANUP_LOCK) == 0,
                redirected,
                nowdead,
                nowunused,
            )?;
        }

        // Freeze tuples.
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
                let mut page = PageMut::new(bytes)?;
                let mut frz_idx: usize = 0;
                for p in 0..(decoded.nplans as usize) {
                    let plan = decoded.plans.get(p);
                    // Convert freeze plan from WAL record into per-tuple format.
                    let frz = HeapTupleFreeze {
                        xmax: plan.xmax,
                        t_infomask2: plan.t_infomask2,
                        t_infomask: plan.t_infomask,
                        frzflags: plan.frzflags,
                        checkflags: 0,
                        offset: InvalidOffsetNumber,
                    };
                    for _i in 0..(plan.ntuples as usize) {
                        let offset = decoded.frz_offsets.get(frz_idx);
                        frz_idx += 1;
                        let mut htup = {
                            let pr = page.as_ref();
                            let lp = PageGetItemId(&pr, offset)?;
                            let item = PageGetItem(&pr, &lp)?;
                            HeapTupleHeaderData::read_on_page(mcx, item)?
                        };
                        heap_execute_freeze_tuple(&mut htup, &frz);
                        overwrite_tuple_header(&mut page, offset, &htup)?;
                    }
                }
                // Note: we don't worry about updating the page's prunability hints.
                PageSetLSN(&mut page, lsn);
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }

    // If we released any space or line pointers, update the free space map.
    if BufferIsValid(buffer) {
        if xlrec.flags & (XLHP_HAS_REDIRECTIONS | XLHP_HAS_DEAD_ITEMS | XLHP_HAS_NOW_UNUSED_ITEMS)
            != 0
        {
            let freespace = with_buffer_freespace(buffer, false)?;
            bufmgr::unlock_release_buffer::call(buffer);
            buffer = InvalidBuffer;
            XLogRecordPageWithFreeSpace(rlocator, blkno, freespace)?;
        } else {
            bufmgr::unlock_release_buffer::call(buffer);
            buffer = InvalidBuffer;
        }
    }
    let _ = buffer;
    Ok(())
}

/// `PageGetHeapFreeSpace(BufferGetPage(buffer))` (heap) or `PageGetFreeSpace`
/// (`heap=false`), read through the page bytes.
fn with_buffer_freespace(buffer: Buffer, plain: bool) -> PgResult<usize> {
    let mut space = 0usize;
    bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
        let page = PageRef::new(bytes)?;
        space = if plain {
            PageGetFreeSpace(&page)
        } else {
            PageGetHeapFreeSpace(&page)
        };
        Ok(())
    })?;
    Ok(space)
}

// ===========================================================================
// heap_xlog_visible (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_visible(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let xlrec = xl_heap_visible::from_bytes(record_get_data(record));

    debug_assert_eq!(xlrec.flags & VISIBILITYMAP_XLOG_VALID_BITS, xlrec.flags);

    let (rlocator, blkno) = block_tag_unwrap(record, 1)?;

    // Hot-Standby conflict resolution.
    if in_hot_standby(standby_state()) {
        with_redo_ctx(|mcx| {
            standby::resolve_recovery_conflict_with_snapshot::call(
                mcx,
                xlrec.snapshotConflictHorizon,
                xlrec.flags & VISIBILITYMAP_XLOG_CATALOG_REL != 0,
                rlocator,
            )
        })?;
    }

    // Read the heap page, if it still exists.
    let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
    let mut buffer = buffer;
    if action == XLogRedoAction::BlkNeedsRedo {
        // We don't bump the LSN of the heap page when setting the visibility map
        // bit (unless checksums or wal_hint_bits is enabled).
        bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
            let mut page = PageMut::new(bytes)?;
            PageSetAllVisible(&mut page);
            if backend_access_transam_xlog_hint_bit_is_needed() {
                PageSetLSN(&mut page, lsn);
            }
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    // else if action == BlkRestored: full-page image already restored.

    if BufferIsValid(buffer) {
        let space = with_buffer_freespace(buffer, true)?;
        bufmgr::unlock_release_buffer::call(buffer);
        buffer = InvalidBuffer;

        // Forestall stale FSM problems by updating FSM's idea about a page that
        // is becoming all-visible or all-frozen.
        if xlrec.flags & VISIBILITYMAP_VALID_BITS != 0 {
            XLogRecordPageWithFreeSpace(rlocator, blkno, space)?;
        }
    }
    let _ = buffer;

    // Even if we skipped the heap page update due to the LSN interlock, it's
    // still safe to update the visibility map.
    let (vmaction, vmbuffer) =
        XLogReadBufferForRedoExtended(record, 0, ReadBufferMode::ZeroOnError, false)?;
    let mut vmbuffer = vmbuffer;
    if vmaction == XLogRedoAction::BlkNeedsRedo {
        // Initialize the page if it was read as zeros.
        bufmgr::with_buffer_page::call(vmbuffer, &mut |bytes| {
            let isnew = {
                let page = PageRef::new(bytes)?;
                PageIsNew(&page)
            };
            if isnew {
                PageInit(bytes, BLCKSZ, 0)?;
            }
            Ok(())
        })?;

        let vmbits = xlrec.flags & VISIBILITYMAP_VALID_BITS;

        // XLogReadBufferForRedoExtended locked the buffer; visibilitymap_set
        // handles locking itself.
        bufmgr::lock_buffer::call(vmbuffer, BUFFER_LOCK_UNLOCK)?;

        with_redo_ctx(|mcx| {
            let reln = rel::Relation::open(
                relcache::create_fake_relcache_entry::call(mcx, rlocator)?,
                None,
            );
            visibilitymap::visibilitymap_pin(&reln, blkno, &mut vmbuffer)?;
            visibilitymap::visibilitymap_set(
                &reln,
                blkno,
                InvalidBuffer,
                lsn,
                vmbuffer,
                xlrec.snapshotConflictHorizon,
                vmbits,
            )?;
            bufmgr::release_buffer::call(vmbuffer);
            // FreeFakeRelcacheEntry: reclaimed on redo-context reset.
            Ok(())
        })?;
    } else if BufferIsValid(vmbuffer) {
        bufmgr::unlock_release_buffer::call(vmbuffer);
    }
    Ok(())
}

/// `BUFFER_LOCK_UNLOCK` (bufmgr.h).
const BUFFER_LOCK_UNLOCK: i32 = 0;

/// `XLogHintBitIsNeeded()` (xlog.h) — reached through the xlog seam.
fn backend_access_transam_xlog_hint_bit_is_needed() -> bool {
    transam_xlog_seams::xlog_hint_bit_is_needed::call()
}

// ===========================================================================
// heap_xlog_delete (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_delete(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let xlrec = xl_heap_delete::from_bytes(record_get_data(record));

    let (target_locator, blkno) = block_tag_unwrap(record, 0)?;
    let target_tid = item_pointer_set(blkno, xlrec.offnum);

    // The visibility map may need to be fixed even if the page is up-to-date.
    if xlrec.flags & XLH_DELETE_ALL_VISIBLE_CLEARED != 0 {
        visibilitymap_clear_fake(target_locator, blkno, VISIBILITYMAP_VALID_BITS)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let xid = record_get_xid(record);
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
                let mut htup = {
                    let page = PageRef::new(bytes)?;
                    read_normal_tuple_header(mcx, &page, xlrec.offnum)?
                };

                htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
                htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
                HeapTupleHeaderClearHotUpdated(&mut htup);
                let (mut im, mut im2) = (htup.t_infomask, htup.t_infomask2);
                fix_infomask_from_infobits(xlrec.infobits_set, &mut im, &mut im2);
                htup.t_infomask = im;
                htup.t_infomask2 = im2;
                if xlrec.flags & XLH_DELETE_IS_SUPER == 0 {
                    HeapTupleHeaderSetXmax(&mut htup, xlrec.xmax);
                } else {
                    HeapTupleHeaderSetXmin(&mut htup, InvalidTransactionId);
                }
                HeapTupleHeaderSetCmax(&mut htup, FirstCommandId, false);

                // Make sure t_ctid is set correctly.
                if xlrec.flags & XLH_DELETE_IS_PARTITION_MOVE != 0 {
                    HeapTupleHeaderSetMovedPartitions(&mut htup);
                } else {
                    htup.t_ctid = target_tid;
                }

                let mut page = PageMut::new(bytes)?;
                // Mark the page as a candidate for pruning.
                PageSetPrunable(&mut page, xid);
                if xlrec.flags & XLH_DELETE_ALL_VISIBLE_CLEARED != 0 {
                    PageClearAllVisible(&mut page);
                }
                overwrite_tuple_header(&mut page, xlrec.offnum, &htup)?;
                PageSetLSN(&mut page, lsn);
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// build the on-page heap-tuple item from an xl_heap_header + user-data bytes.
// ===========================================================================

/// Assemble an on-page item: a `newlen`-byte buffer whose 23-byte header is
/// stamped from `hdr` and whose `[SizeofHeapTupleHeader..]` tail is `userdata`.
/// `userdata` is the WAL "bitmap [+ padding] [+ oid] + data" (already in C's
/// `(char*)htup + SizeofHeapTupleHeader` layout).
fn build_item(hdr: &HeapTupleHeaderData<'_>, userdata: &[u8]) -> PgResult<Vec<u8>> {
    let mut item = vec![0u8; SizeofHeapTupleHeader + userdata.len()];
    hdr.write_on_page(&mut item)?;
    item[SizeofHeapTupleHeader..].copy_from_slice(userdata);
    Ok(item)
}

/// A scratch `HeapTupleHeaderData` matching C's `MemSet(htup, 0, ...)` over the
/// union/fields (`THeap` arm, all-zero), then set `t_infomask2/t_infomask/t_hoff`
/// from the xlhdr.
fn new_tuple_header<'mcx>(mcx: Mcx<'mcx>, xlhdr: &xl_heap_header) -> HeapTupleHeaderData<'mcx> {
    HeapTupleHeaderData {
        t_choice: HeapTupleHeaderChoice::THeap(HeapTupleFields {
            t_xmin: 0,
            t_xmax: 0,
            t_field3: HeapTupleField3::TCid(0),
        }),
        t_ctid: ItemPointerData::default(),
        t_infomask2: xlhdr.t_infomask2,
        t_infomask: xlhdr.t_infomask,
        t_hoff: xlhdr.t_hoff,
        t_bits: ::mcx::PgVec::new_in(mcx),
    }
}

// ===========================================================================
// heap_xlog_insert (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_insert(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let xlrec = xl_heap_insert::from_bytes(record_get_data(record));
    let mut freespace: usize = 0;

    let (target_locator, blkno) = block_tag_unwrap(record, 0)?;
    let target_tid = item_pointer_set(blkno, xlrec.offnum);

    // No freezing in the heap_insert() code path.
    debug_assert!(xlrec.flags & XLH_INSERT_ALL_FROZEN_SET == 0);

    if xlrec.flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
        visibilitymap_clear_fake(target_locator, blkno, VISIBILITYMAP_VALID_BITS)?;
    }

    // If we inserted the first and only tuple on the page, re-initialize it.
    let action;
    let mut buffer;
    if record_get_info(record) & XLOG_HEAP_INIT_PAGE != 0 {
        buffer = XLogInitBufferForRedo(record, 0)?;
        bufmgr::with_buffer_page::call(buffer, &mut |bytes| PageInit(bytes, BLCKSZ, 0))?;
        action = XLogRedoAction::BlkNeedsRedo;
    } else {
        let (a, b) = XLogReadBufferForRedo(record, 0)?;
        buffer = b;
        action = a;
    }
    if action == XLogRedoAction::BlkNeedsRedo {
        let block = record_get_block_data(record, 0);
        let xid = record_get_xid(record);
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
                {
                    let page = PageRef::new(bytes)?;
                    if PageGetMaxOffsetNumber(&page) + 1 < xlrec.offnum {
                        return Err(PgError::new(PANIC, "invalid max offset number"));
                    }
                }

                // newlen = datalen - SizeOfHeapHeader; tuple body follows xlhdr.
                let xlhdr = xl_heap_header::from_bytes(block);
                let userdata = &block[SizeOfHeapHeader..];

                let mut htup = new_tuple_header(mcx, &xlhdr);
                HeapTupleHeaderSetXmin(&mut htup, xid);
                HeapTupleHeaderSetCmin(&mut htup, FirstCommandId);
                htup.t_ctid = target_tid;

                let item = build_item(&htup, userdata)?;
                let mut page = PageMut::new(bytes)?;
                if PageAddItemExtended(&mut page, &item, xlrec.offnum, PAI_OVERWRITE | PAI_IS_HEAP)?
                    == InvalidOffsetNumber
                {
                    return Err(PgError::new(PANIC, "failed to add tuple"));
                }

                freespace = PageGetHeapFreeSpace(&page.as_ref());
                PageSetLSN(&mut page, lsn);
                if xlrec.flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
                    PageClearAllVisible(&mut page);
                }
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
        buffer = InvalidBuffer;
    }
    let _ = buffer;

    // If the page is running low on free space, update the FSM as well.
    if action == XLogRedoAction::BlkNeedsRedo && freespace < (BLCKSZ as usize) / 5 {
        XLogRecordPageWithFreeSpace(target_locator, blkno, freespace)?;
    }
    Ok(())
}

// ===========================================================================
// heap_xlog_multi_insert (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_multi_insert(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let mut freespace: usize = 0;
    let isinit = (record_get_info(record) & XLOG_HEAP_INIT_PAGE) != 0;

    let maindata = record_get_data(record);
    let xlrec = xl_heap_multi_insert::from_bytes(maindata);
    let offsets = xl_heap_multi_insert::offsets(maindata);

    let (rlocator, blkno) = block_tag_unwrap(record, 0)?;

    // check that the mutually exclusive flags are not both set
    debug_assert!(
        !((xlrec.flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0)
            && (xlrec.flags & XLH_INSERT_ALL_FROZEN_SET != 0))
    );

    if xlrec.flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
        visibilitymap_clear_fake(rlocator, blkno, VISIBILITYMAP_VALID_BITS)?;
    }

    let action;
    let mut buffer;
    if isinit {
        buffer = XLogInitBufferForRedo(record, 0)?;
        bufmgr::with_buffer_page::call(buffer, &mut |bytes| PageInit(bytes, BLCKSZ, 0))?;
        action = XLogRedoAction::BlkNeedsRedo;
    } else {
        let (a, b) = XLogReadBufferForRedo(record, 0)?;
        buffer = b;
        action = a;
    }
    if action == XLogRedoAction::BlkNeedsRedo {
        let block = record_get_block_data(record, 0);
        let xid = record_get_xid(record);
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
                // Tuples are stored as block data; walk SHORTALIGNed records.
                let mut tupdata = 0usize; // cursor into `block`
                for i in 0..(xlrec.ntuples as usize) {
                    // Reinit page => tuples are in order from FirstOffsetNumber;
                    // else an offsets array precedes the tuples.
                    let offnum = if isinit {
                        (1 + i) as OffsetNumber // FirstOffsetNumber == 1
                    } else {
                        offsets.get(i)
                    };
                    {
                        let page = PageRef::new(bytes)?;
                        if PageGetMaxOffsetNumber(&page) + 1 < offnum {
                            return Err(PgError::new(PANIC, "invalid max offset number"));
                        }
                    }

                    // xlhdr = (xl_multi_insert_tuple *) SHORTALIGN(tupdata);
                    tupdata = shortalign(tupdata);
                    let xlhdr = xl_multi_insert_tuple::from_bytes(&block[tupdata..]);
                    tupdata += SizeOfMultiInsertTuple;

                    let newlen = xlhdr.datalen as usize;
                    let userdata = &block[tupdata..tupdata + newlen];
                    tupdata += newlen;

                    let mut htup = new_tuple_header(
                        mcx,
                        &xl_heap_header {
                            t_infomask2: xlhdr.t_infomask2,
                            t_infomask: xlhdr.t_infomask,
                            t_hoff: xlhdr.t_hoff,
                        },
                    );
                    HeapTupleHeaderSetXmin(&mut htup, xid);
                    HeapTupleHeaderSetCmin(&mut htup, FirstCommandId);
                    htup.t_ctid = item_pointer_set(blkno, offnum);

                    let item = build_item(&htup, userdata)?;
                    let mut page = PageMut::new(bytes)?;
                    if PageAddItemExtended(&mut page, &item, offnum, PAI_OVERWRITE | PAI_IS_HEAP)?
                        == InvalidOffsetNumber
                    {
                        return Err(PgError::new(PANIC, "failed to add tuple"));
                    }
                }
                if tupdata != block.len() {
                    return Err(PgError::new(PANIC, "total tuple length mismatch"));
                }

                let mut page = PageMut::new(bytes)?;
                freespace = PageGetHeapFreeSpace(&page.as_ref());
                PageSetLSN(&mut page, lsn);
                if xlrec.flags & XLH_INSERT_ALL_VISIBLE_CLEARED != 0 {
                    PageClearAllVisible(&mut page);
                }
                // XLH_INSERT_ALL_FROZEN_SET implies all tuples are visible.
                if xlrec.flags & XLH_INSERT_ALL_FROZEN_SET != 0 {
                    PageSetAllVisible(&mut page);
                }
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
        buffer = InvalidBuffer;
    }
    let _ = buffer;

    if action == XLogRedoAction::BlkNeedsRedo && freespace < (BLCKSZ as usize) / 5 {
        XLogRecordPageWithFreeSpace(rlocator, blkno, freespace)?;
    }
    Ok(())
}

// ===========================================================================
// heap_xlog_update (+ HOT) (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_update(record: &XLogReaderState<'_>, hot_update: bool) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let xlrec = xl_heap_update::from_bytes(record_get_data(record));
    let mut freespace: usize = 0;

    let (rlocator, newblk) = block_tag_unwrap(record, 0)?;
    let oldblk;
    if let Some((_l, ob)) = block_tag(record, 1)? {
        // HOT updates are never done across pages.
        debug_assert!(!hot_update);
        oldblk = ob;
    } else {
        oldblk = newblk;
    }
    let newtid = item_pointer_set(newblk, xlrec.new_offnum);
    let same_page = oldblk == newblk;

    if xlrec.flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED != 0 {
        visibilitymap_clear_fake(rlocator, oldblk, VISIBILITYMAP_VALID_BITS)?;
    }

    // Deal with old tuple version. We capture the old tuple's on-page hoff/len
    // and user-data while obuffer is pinned, for the prefix/suffix splice (which
    // only occurs same-page, per the XLH_UPDATE_*_FROM_OLD asserts).
    let (oldaction, obuffer) =
        XLogReadBufferForRedo(record, if same_page { 0 } else { 1 })?;
    let mut old_hoff: usize = 0;
    let mut old_len: usize = 0;
    let mut old_userdata: Vec<u8> = Vec::new();
    if oldaction == XLogRedoAction::BlkNeedsRedo {
        let offnum = xlrec.old_offnum;
        let xid = record_get_xid(record);
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(obuffer, &mut |bytes| {
                let (mut htup, lp_len, lp_off) = {
                    let page = PageRef::new(bytes)?;
                    let max_off = PageGetMaxOffsetNumber(&page);
                    let lp_normal = if max_off >= offnum {
                        ItemIdIsNormal(&PageGetItemId(&page, offnum)?)
                    } else {
                        false
                    };
                    if max_off < offnum || !lp_normal {
                        return Err(PgError::new(PANIC, "invalid lp"));
                    }
                    let lp = PageGetItemId(&page, offnum)?;
                    let item = PageGetItem(&page, &lp)?;
                    (
                        HeapTupleHeaderData::read_on_page(mcx, item)?,
                        ItemIdGetLength(&lp) as usize,
                        ItemIdGetOffset(&lp) as usize,
                    )
                };

                // Capture the old tuple bytes for a same-page prefix/suffix splice.
                old_hoff = htup.t_hoff as usize;
                old_len = lp_len;
                old_userdata = bytes[lp_off..lp_off + lp_len].to_vec();

                htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
                htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
                if hot_update {
                    HeapTupleHeaderSetHotUpdated(&mut htup);
                } else {
                    HeapTupleHeaderClearHotUpdated(&mut htup);
                }
                let (mut im, mut im2) = (htup.t_infomask, htup.t_infomask2);
                fix_infomask_from_infobits(xlrec.old_infobits_set, &mut im, &mut im2);
                htup.t_infomask = im;
                htup.t_infomask2 = im2;
                HeapTupleHeaderSetXmax(&mut htup, xlrec.old_xmax);
                HeapTupleHeaderSetCmax(&mut htup, FirstCommandId, false);
                // Set forward chain link in t_ctid.
                htup.t_ctid = newtid;

                let mut page = PageMut::new(bytes)?;
                PageSetPrunable(&mut page, xid);
                if xlrec.flags & XLH_UPDATE_OLD_ALL_VISIBLE_CLEARED != 0 {
                    PageClearAllVisible(&mut page);
                }
                overwrite_tuple_header(&mut page, offnum, &htup)?;
                PageSetLSN(&mut page, lsn);
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(obuffer);
    }

    // Read the page the new tuple goes into, if different from old.
    let newaction;
    let nbuffer;
    if same_page {
        nbuffer = obuffer;
        newaction = oldaction;
    } else if record_get_info(record) & XLOG_HEAP_INIT_PAGE != 0 {
        nbuffer = XLogInitBufferForRedo(record, 0)?;
        bufmgr::with_buffer_page::call(nbuffer, &mut |bytes| PageInit(bytes, BLCKSZ, 0))?;
        newaction = XLogRedoAction::BlkNeedsRedo;
    } else {
        let (a, b) = XLogReadBufferForRedo(record, 0)?;
        nbuffer = b;
        newaction = a;
    }

    if xlrec.flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED != 0 {
        visibilitymap_clear_fake(rlocator, newblk, VISIBILITYMAP_VALID_BITS)?;
    }

    // Deal with new tuple.
    if newaction == XLogRedoAction::BlkNeedsRedo {
        let offnum = xlrec.new_offnum;
        let recdata = record_get_block_data(record, 0);
        let xid = record_get_xid(record);
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(nbuffer, &mut |bytes| {
                {
                    let page = PageRef::new(bytes)?;
                    if PageGetMaxOffsetNumber(&page) + 1 < offnum {
                        return Err(PgError::new(PANIC, "invalid max offset number"));
                    }
                }

                let mut cur = 0usize; // cursor into recdata
                let mut prefixlen: usize = 0;
                let mut suffixlen: usize = 0;
                if xlrec.flags & XLH_UPDATE_PREFIX_FROM_OLD != 0 {
                    debug_assert!(same_page);
                    prefixlen = u16::from_ne_bytes([recdata[cur], recdata[cur + 1]]) as usize;
                    cur += 2;
                }
                if xlrec.flags & XLH_UPDATE_SUFFIX_FROM_OLD != 0 {
                    debug_assert!(same_page);
                    suffixlen = u16::from_ne_bytes([recdata[cur], recdata[cur + 1]]) as usize;
                    cur += 2;
                }

                let xlhdr = xl_heap_header::from_bytes(&recdata[cur..]);
                cur += SizeOfHeapHeader;

                let tuplen = recdata.len() - cur;

                // Reconstruct the new tuple's user-data area (the bytes after the
                // 23-byte header), splicing prefix/suffix from the OLD tuple.
                let mut userdata: Vec<u8> = Vec::new();
                if prefixlen > 0 {
                    // bitmap [+ padding] [+ oid] from WAL record.
                    let len = xlhdr.t_hoff as usize - SizeofHeapTupleHeader;
                    userdata.extend_from_slice(&recdata[cur..cur + len]);
                    cur += len;
                    // prefix from old tuple: old user-data starts at old_hoff.
                    userdata.extend_from_slice(&old_userdata[old_hoff..old_hoff + prefixlen]);
                    // new tuple data from WAL record.
                    let len2 = tuplen - (xlhdr.t_hoff as usize - SizeofHeapTupleHeader);
                    userdata.extend_from_slice(&recdata[cur..cur + len2]);
                    cur += len2;
                } else {
                    // bitmap [+ padding] [+ oid] + data, all in one go.
                    userdata.extend_from_slice(&recdata[cur..cur + tuplen]);
                    cur += tuplen;
                }
                debug_assert_eq!(cur, recdata.len());

                // suffix from old tuple: the last `suffixlen` bytes of the old item.
                if suffixlen > 0 {
                    userdata.extend_from_slice(&old_userdata[old_len - suffixlen..old_len]);
                }

                let mut htup = new_tuple_header(mcx, &xlhdr);
                HeapTupleHeaderSetXmin(&mut htup, xid);
                HeapTupleHeaderSetCmin(&mut htup, FirstCommandId);
                HeapTupleHeaderSetXmax(&mut htup, xlrec.new_xmax);
                // Make sure there is no forward chain link in t_ctid.
                htup.t_ctid = newtid;

                let item = build_item(&htup, &userdata)?;
                let mut page = PageMut::new(bytes)?;
                if PageAddItemExtended(&mut page, &item, offnum, PAI_OVERWRITE | PAI_IS_HEAP)?
                    == InvalidOffsetNumber
                {
                    return Err(PgError::new(PANIC, "failed to add tuple"));
                }

                if xlrec.flags & XLH_UPDATE_NEW_ALL_VISIBLE_CLEARED != 0 {
                    PageClearAllVisible(&mut page);
                }
                freespace = PageGetHeapFreeSpace(&page.as_ref());
                PageSetLSN(&mut page, lsn);
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(nbuffer);
    }

    if BufferIsValid(nbuffer) && nbuffer != obuffer {
        bufmgr::unlock_release_buffer::call(nbuffer);
    }
    if BufferIsValid(obuffer) {
        bufmgr::unlock_release_buffer::call(obuffer);
    }

    // If the new page is running low on free space, update the FSM (not on HOT).
    if newaction == XLogRedoAction::BlkNeedsRedo && !hot_update && freespace < (BLCKSZ as usize) / 5
    {
        XLogRecordPageWithFreeSpace(rlocator, newblk, freespace)?;
    }
    Ok(())
}

// ===========================================================================
// heap_xlog_confirm (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_confirm(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let xlrec = xl_heap_confirm::from_bytes(record_get_data(record));

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let offnum = xlrec.offnum;
        let blkno = bufmgr::buffer_get_block_number::call(buffer);
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
                let mut htup = {
                    let page = PageRef::new(bytes)?;
                    read_normal_tuple_header(mcx, &page, offnum)?
                };
                // Confirm tuple as actually inserted.
                htup.t_ctid = item_pointer_set(blkno, offnum);

                let mut page = PageMut::new(bytes)?;
                overwrite_tuple_header(&mut page, offnum, &htup)?;
                PageSetLSN(&mut page, lsn);
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// heap_xlog_lock (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_lock(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let xlrec = xl_heap_lock::from_bytes(record_get_data(record));

    if xlrec.flags & XLH_LOCK_ALL_FROZEN_CLEARED != 0 {
        let (rlocator, block) = block_tag_unwrap(record, 0)?;
        visibilitymap_clear_fake(rlocator, block, VISIBILITYMAP_ALL_FROZEN)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let offnum = xlrec.offnum;
        let blkno = bufmgr::buffer_get_block_number::call(buffer);
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
                let mut htup = {
                    let page = PageRef::new(bytes)?;
                    read_normal_tuple_header(mcx, &page, offnum)?
                };

                htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
                htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
                let (mut im, mut im2) = (htup.t_infomask, htup.t_infomask2);
                fix_infomask_from_infobits(xlrec.infobits_set, &mut im, &mut im2);
                htup.t_infomask = im;
                htup.t_infomask2 = im2;

                // Clear update flags only if the modified infomask says there's
                // no update.
                if HEAP_XMAX_IS_LOCKED_ONLY(htup.t_infomask) {
                    HeapTupleHeaderClearHotUpdated(&mut htup);
                    // Make sure there is no forward chain link in t_ctid.
                    htup.t_ctid = item_pointer_set(blkno, offnum);
                }
                HeapTupleHeaderSetXmax(&mut htup, xlrec.xmax);
                HeapTupleHeaderSetCmax(&mut htup, FirstCommandId, false);

                let mut page = PageMut::new(bytes)?;
                overwrite_tuple_header(&mut page, offnum, &htup)?;
                PageSetLSN(&mut page, lsn);
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// heap_xlog_lock_updated (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_lock_updated(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let xlrec = xl_heap_lock_updated::from_bytes(record_get_data(record));

    if xlrec.flags & XLH_LOCK_ALL_FROZEN_CLEARED != 0 {
        let (rlocator, block) = block_tag_unwrap(record, 0)?;
        visibilitymap_clear_fake(rlocator, block, VISIBILITYMAP_ALL_FROZEN)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let offnum = xlrec.offnum;
        with_redo_ctx(|mcx| {
            bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
                let mut htup = {
                    let page = PageRef::new(bytes)?;
                    read_normal_tuple_header(mcx, &page, offnum)?
                };

                htup.t_infomask &= !(HEAP_XMAX_BITS | HEAP_MOVED);
                htup.t_infomask2 &= !HEAP_KEYS_UPDATED;
                let (mut im, mut im2) = (htup.t_infomask, htup.t_infomask2);
                fix_infomask_from_infobits(xlrec.infobits_set, &mut im, &mut im2);
                htup.t_infomask = im;
                htup.t_infomask2 = im2;
                HeapTupleHeaderSetXmax(&mut htup, xlrec.xmax);

                let mut page = PageMut::new(bytes)?;
                overwrite_tuple_header(&mut page, offnum, &htup)?;
                PageSetLSN(&mut page, lsn);
                Ok(())
            })
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// heap_xlog_inplace (heapam_xlog.c)
// ===========================================================================

fn heap_xlog_inplace(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = rec_lsn(record);
    let maindata = record_get_data(record);
    let xlrec = xl_heap_inplace::from_bytes(maindata);

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let newtup = record_get_block_data(record, 0);
        let newlen = newtup.len();
        let offnum = xlrec.offnum;
        bufmgr::with_buffer_page::call(buffer, &mut |bytes| {
            // invalid-lp guard + oldlen == ItemIdGetLength(lp) - htup->t_hoff.
            let (lp_off, lp_len, hoff) = {
                let page = PageRef::new(bytes)?;
                let max_off = PageGetMaxOffsetNumber(&page);
                let lp_normal = if max_off >= offnum {
                    ItemIdIsNormal(&PageGetItemId(&page, offnum)?)
                } else {
                    false
                };
                if max_off < offnum || !lp_normal {
                    return Err(PgError::new(PANIC, "invalid lp"));
                }
                let lp = PageGetItemId(&page, offnum)?;
                let item = PageGetItem(&page, &lp)?;
                // t_hoff is byte 22 of the on-page item.
                let hoff = item[22] as usize;
                (ItemIdGetOffset(&lp) as usize, ItemIdGetLength(&lp) as usize, hoff)
            };
            let oldlen = lp_len - hoff;
            if oldlen != newlen {
                return Err(PgError::new(PANIC, "wrong tuple length"));
            }
            // memcpy((char*)htup + htup->t_hoff, newtup, newlen).
            let dst_off = lp_off + hoff;
            bytes[dst_off..dst_off + newlen].copy_from_slice(newtup);

            let mut page = PageMut::new(bytes)?;
            PageSetLSN(&mut page, lsn);
            Ok(())
        })?;
        bufmgr::mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    // Decode the trailing SharedInvalidationMessage[] and apply them.
    let nmsgs = xlrec.nmsgs.max(0) as usize;
    let msgs_view = xl_heap_inplace::msgs(maindata);
    let mut msgs: Vec<SharedInvalidationMessage> = Vec::new();
    for i in 0..nmsgs {
        if let Some(m) = msgs_view.get(i) {
            msgs.push(m);
        }
    }
    inval::process_committed_invalidation_messages::call(
        &msgs,
        xlrec.relcacheInitFileInval,
        xlrec.dbId,
        xlrec.tsId,
    )?;
    Ok(())
}

// ===========================================================================
// heap_xlog_logical_rewrite (XLOG_HEAP2_REWRITE) — decode header, route to seam.
// ===========================================================================

fn heap_xlog_logical_rewrite(record: &XLogReaderState<'_>) -> PgResult<()> {
    let data = record_get_data(record);
    // xl_heap_rewrite_mapping layout (40 bytes, see rewriteheap.c):
    //   mapped_xid(u32)@0 mapped_db(u32)@4 mapped_rel(u32)@8 [pad@12]
    //   offset(i64)@16 num_mappings(u32)@24 [pad@28] start_lsn(u64)@32
    let u32_at = |o: usize| u32::from_ne_bytes([data[o], data[o + 1], data[o + 2], data[o + 3]]);
    let u64_at = |o: usize| {
        let mut a = [0u8; 8];
        a.copy_from_slice(&data[o..o + 8]);
        u64::from_ne_bytes(a)
    };
    let mapped_xid = u32_at(0) as TransactionId;
    let mapped_db: Oid = u32_at(4);
    let mapped_rel: Oid = u32_at(8);
    let offset = u64_at(16) as i64;
    let num_mappings = u32_at(24);
    let start_lsn = u64_at(32);
    let mappings = &data[SIZEOF_XL_HEAP_REWRITE_MAPPING..];
    rewrite::heap_xlog_logical_rewrite::call(
        mapped_xid,
        mapped_db,
        mapped_rel,
        offset,
        num_mappings,
        start_lsn,
        record_get_xid(record),
        mappings,
    )
}

// ===========================================================================
// heap_redo / heap2_redo (heapam_xlog.c)
// ===========================================================================

/// RM_HEAP (heap1) redo dispatch. (`heap_redo`)
pub fn heap_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !XLR_INFO_MASK;

    // These operations don't overwrite MVCC data so no conflict processing is
    // required. The ones in heap2 rmgr do.
    let result = match info & XLOG_HEAP_OPMASK {
        x if x == XLOG_HEAP_INSERT => heap_xlog_insert(record),
        x if x == XLOG_HEAP_DELETE => heap_xlog_delete(record),
        x if x == XLOG_HEAP_UPDATE => heap_xlog_update(record, false),
        x if x == XLOG_HEAP_TRUNCATE => {
            // TRUNCATE is a no-op (logged as SMGR records); WAL record only
            // exists for logical decoding.
            Ok(())
        }
        x if x == XLOG_HEAP_HOT_UPDATE => heap_xlog_update(record, true),
        x if x == XLOG_HEAP_CONFIRM => heap_xlog_confirm(record),
        x if x == XLOG_HEAP_LOCK => heap_xlog_lock(record),
        x if x == XLOG_HEAP_INPLACE => heap_xlog_inplace(record),
        _ => Err(PgError::new(PANIC, format!("heap_redo: unknown op code {info}"))),
    };
    reset_redo_ctx();
    result
}

/// RM_HEAP2 (heap2) redo dispatch. (`heap2_redo`)
pub fn heap2_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !XLR_INFO_MASK;

    let result = match info & XLOG_HEAP_OPMASK {
        x if x == XLOG_HEAP2_PRUNE_ON_ACCESS
            || x == XLOG_HEAP2_PRUNE_VACUUM_SCAN
            || x == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP =>
        {
            heap_xlog_prune_freeze(record)
        }
        x if x == XLOG_HEAP2_VISIBLE => heap_xlog_visible(record),
        x if x == XLOG_HEAP2_MULTI_INSERT => heap_xlog_multi_insert(record),
        x if x == XLOG_HEAP2_LOCK_UPDATED => heap_xlog_lock_updated(record),
        x if x == XLOG_HEAP2_NEW_CID => {
            // Nothing to do on a real replay; only used during logical decoding.
            Ok(())
        }
        x if x == XLOG_HEAP2_REWRITE => heap_xlog_logical_rewrite(record),
        _ => Err(PgError::new(PANIC, format!("heap2_redo: unknown op code {info}"))),
    };
    reset_redo_ctx();
    result
}

// ===========================================================================
// heap_mask (heapam_xlog.c)
// ===========================================================================

/// Mask a heap page before performing consistency checks on it. (`heap_mask`)
pub fn heap_mask(pagedata: &mut [u8], blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum::call(pagedata);

    bufmask::mask_page_hint_bits::call(pagedata);
    bufmask::mask_unused_space::call(pagedata)?;

    let max_off = {
        let page = PageRef::new(pagedata)?;
        PageGetMaxOffsetNumber(&page)
    };

    with_redo_ctx(|mcx| {
        let mut off: OffsetNumber = 1;
        while off <= max_off {
            let (lp_off, is_normal, has_storage, lp_len) = {
                let page = PageRef::new(pagedata)?;
                let iid = PageGetItemId(&page, off)?;
                (
                    ItemIdGetOffset(&iid) as usize,
                    ItemIdIsNormal(&iid),
                    ItemIdHasStorage(&iid),
                    ItemIdGetLength(&iid) as usize,
                )
            };

            if is_normal {
                // Read the page header, decide the masked bits, write it back.
                let mut page_htup = {
                    let page = PageRef::new(pagedata)?;
                    let iid = PageGetItemId(&page, off)?;
                    let item = PageGetItem(&page, &iid)?;
                    HeapTupleHeaderData::read_on_page(mcx, item)?
                };

                // If xmin not yet frozen, ignore hint-bit differences.
                if !HeapTupleHeaderXminFrozen(&page_htup) {
                    page_htup.t_infomask &= !HEAP_XACT_MASK;
                } else {
                    // Still mask xmax hint bits.
                    page_htup.t_infomask &= !HEAP_XMAX_INVALID;
                    page_htup.t_infomask &= !HEAP_XMAX_COMMITTED;
                }

                // During replay, Command Id is FirstCommandId. Mask it.
                set_t_cid_raw(&mut page_htup, MASK_MARKER as u32);

                // For a speculative tuple, set t_ctid to current (blkno, off).
                if HeapTupleHeaderIsSpeculative(&page_htup) {
                    page_htup.t_ctid = item_pointer_set(blkno, off);
                }

                // NB: not ignoring ctid changes due to tuple having moved.
                page_htup.write_on_page(&mut pagedata[lp_off..lp_off + lp_len])?;
            }

            // Ignore padding bytes after the tuple (MAXALIGN(len) - len).
            if has_storage {
                let padlen = maxalign(lp_len) - lp_len;
                if padlen > 0 {
                    for b in &mut pagedata[lp_off + lp_len..lp_off + lp_len + padlen] {
                        *b = MASK_MARKER;
                    }
                }
            }

            off += 1;
        }
        Ok(())
    })
}

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install this unit's owned rmgr-callback seams (consumed by
/// `backend-access-transam-rmgr`'s `RmgrTable`).
pub fn init_seams() {
    heapam_xlog_seams::heap_redo::set(heap_redo);
    heapam_xlog_seams::heap2_redo::set(heap2_redo);
    heapam_xlog_seams::heap_mask::set(heap_mask);
}
