//! WAL replay + record-builder layer for GiST (`access/gist/gistxlog.c`):
//!
//!   * the rmgr-table callbacks `gist_redo` / `gist_xlog_startup` /
//!     `gist_xlog_cleanup` / `gist_mask`, dispatching the redo of page-update,
//!     delete, page-reuse, page-split and page-delete records (the
//!     `XLOG_GIST_ASSIGN_LSN` op is a no-op), with the shared
//!     `gistRedoClearFollowRight` helper and the `decodePageSplitRecord` tuple
//!     decoder;
//!   * the WAL-write builders `gistXLogSplit` / `gistXLogUpdate` /
//!     `gistXLogDelete` / `gistXLogPageDelete` / `gistXLogPageReuse` /
//!     `gistXLogAssignLSN`, plus `gistGetFakeLSN` (gistutil.c) — these are
//!     the GiST WAL-write seams that the insert spine reaches through
//!     `backend-access-gist-core-seams`, installed here in F7.
//!
//! The record structs are decoded/encoded against the exact on-disk byte layout
//! of `access/gistxlog.h`. The redo path reads the record through the xlogreader
//! value accessors, fetches buffers via xlogutils' `XLogReadBufferForRedo` /
//! `XLogInitBufferForRedo`, and edits page bytes via the bufpage primitives and
//! the GiST page-opaque accessors in [`crate::gist_page`].
//!
//! `opCtx` (C `static MemoryContext opCtx`) is a thread-local recovery context
//! created by `gist_xlog_startup`, reset after each redo and deleted by
//! `gist_xlog_cleanup` — mirroring the gin/nbtree redo crates.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate std;

use alloc::format;
use alloc::vec::Vec;
use core::cell::RefCell;
use std::thread_local;

use ::bufmask_seams::{
    mask_lp_flags, mask_page_hint_bits, mask_page_lsn_and_checksum, mask_unused_space,
};
use ::xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo};
use ::bufmgr_seams::{
    mark_buffer_dirty, unlock_release_buffer, with_buffer_page,
};
use ::page::{
    PageAddItemExtended, PageGetMaxOffsetNumber, PageIndexMultiDelete, PageIndexTupleDelete,
    PageIndexTupleOverwrite, PageIsEmpty, PageMut, PageRef, PageSetLSN,
};
use ::utils_error::{ereport, PgError, PgResult};
use ::mcx::{Mcx, MemoryContext};
use ::types_core::primitive::{
    BlockNumber, OffsetNumber, RmgrId, TransactionId, XLogRecPtr,
};
use ::types_core::xact::FullTransactionId;
use ::types_error::error::{ERROR, PANIC};
use ::types_storage::buf::{Buffer, BufferIsValid};
use ::types_storage::storage::RelFileLocator;
use ::types_tuple::access::{
    RELPERSISTENCE_PERMANENT, RELPERSISTENCE_TEMP, RELPERSISTENCE_UNLOGGED,
};
use ::types_tuple::heaptuple::{FIRST_OFFSET_NUMBER, INDEX_SIZE_MASK, INVALID_OFFSET_NUMBER};
use ::wal::rmgr::XLogReaderState;
use ::wal::XLogRedoAction;

use transam_xlog_seams as xlog_seams;
use xloginsert_seams as xloginsert;
use standby_seams as standby;

use crate::gist_page::{
    gist_page_set_nsn, gistinitpage, set_gist_page_rightlink, GistClearFollowRight,
    GistClearPageHasGarbage, GistMarkFollowRight, GistMarkTuplesDeleted, GistPageIsLeaf,
    GistPageSetDeleted,
};

use ::rel::Relation;
use ::gist::SplitPageLayout;

// ===========================================================================
// gistxlog.h op-info bytes + REGBUF flags.
// ===========================================================================

/// `XLR_INFO_MASK` (xlogrecord.h) = `0x0F`: the WAL framework's per-record info
/// flags, masked off before the GiST opcode dispatch (the opcode is the high
/// nibble).
const XLR_INFO_MASK: u8 = 0x0F;

/// `XLOG_GIST_PAGE_UPDATE`.
const XLOG_GIST_PAGE_UPDATE: u8 = 0x00;
/// `XLOG_GIST_DELETE`.
const XLOG_GIST_DELETE: u8 = 0x10;
/// `XLOG_GIST_PAGE_REUSE`.
const XLOG_GIST_PAGE_REUSE: u8 = 0x20;
/// `XLOG_GIST_PAGE_SPLIT`.
const XLOG_GIST_PAGE_SPLIT: u8 = 0x30;
/// `XLOG_GIST_PAGE_DELETE`.
const XLOG_GIST_PAGE_DELETE: u8 = 0x60;
/// `XLOG_GIST_ASSIGN_LSN`.
const XLOG_GIST_ASSIGN_LSN: u8 = 0x70;

/// `RM_GIST_ID` (`access/rmgrlist.h`) — GiST is the 12th resource manager
/// (XLOG=0 … HEAP2=9, HEAP=10, BTREE=11, HASH=12... wait): counting the
/// `PG_RMGR` list (XLOG,XACT,SMGR,CLOG,DBASE,TBLSPC,MULTIXACT,RELMAP,STANDBY,
/// HEAP2,HEAP,BTREE,HASH,GIN,GIST) places GiST at index 14.
const RM_GIST_ID: RmgrId = 14;

/// `GIST_ROOT_BLKNO` (gist_private.h).
const GIST_ROOT_BLKNO: BlockNumber = 0;

/// `F_LEAF` (gist.h) — the leaf-page flag passed to `gistinitpage` during split
/// redo.
const F_LEAF: u16 = 1 << 0;

/// `InvalidBlockNumber` (storage/block.h).
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;

// REGBUF_* flags (xloginsert.h).
/// `REGBUF_STANDARD` (xloginsert.h).
const REGBUF_STANDARD: u8 = 0x08;
/// `REGBUF_WILL_INIT` (xloginsert.h) = `REGBUF_WILL_INIT (0x02) | REGBUF_NO_IMAGE (0x04)`.
const REGBUF_WILL_INIT: u8 = 0x04 | 0x02;

/// `XLOG_MARK_UNIMPORTANT` (xlog.h).
const XLOG_MARK_UNIMPORTANT: u8 = 0x02;

/// `MASK_MARKER` (bufmask.h) = 0.
const MASK_MARKER: u64 = 0;

/// `FirstNormalUnloggedLSN` (xlogdefs.h) = 1000 — the seed for the temp-rel fake
/// LSN counter.
const FirstNormalUnloggedLSN: XLogRecPtr = 1000;

// ===========================================================================
// Record structs (gistxlog.h) + on-disk byte (de)serialization.
// ===========================================================================

/// `gistxlogPageUpdate` (gistxlog.h): `{ uint16 ntodelete; uint16 ntoinsert; }`.
#[derive(Clone, Copy, Debug, Default)]
struct GistxlogPageUpdate {
    ntodelete: u16,
    ntoinsert: u16,
}

impl GistxlogPageUpdate {
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(4);
        v.extend_from_slice(&self.ntodelete.to_ne_bytes());
        v.extend_from_slice(&self.ntoinsert.to_ne_bytes());
        v
    }

    fn decode(data: &[u8]) -> Self {
        GistxlogPageUpdate {
            ntodelete: u16_at(data, 0),
            ntoinsert: u16_at(data, 2),
        }
    }
}

/// `gistxlogDelete` (gistxlog.h): `{ TransactionId snapshotConflictHorizon;
/// uint16 ntodelete; bool isCatalogRel; OffsetNumber offsets[]; }`. The fixed
/// header is `SizeOfGistxlogDelete` = `offsetof(.., offsets)` = 8 bytes
/// (TransactionId u32 @0, ntodelete u16 @4, isCatalogRel bool @6, then the
/// `offsets[]` flexible array begins at @8 on the 2-byte-aligned OffsetNumber).
#[derive(Clone, Copy, Debug, Default)]
struct GistxlogDelete {
    snapshot_conflict_horizon: TransactionId,
    ntodelete: u16,
    is_catalog_rel: bool,
}

/// `SizeOfGistxlogDelete`.
const SIZE_OF_GISTXLOG_DELETE: usize = 8;

impl GistxlogDelete {
    fn encode_header(&self) -> Vec<u8> {
        let mut v = alloc::vec![0u8; SIZE_OF_GISTXLOG_DELETE];
        v[0..4].copy_from_slice(&self.snapshot_conflict_horizon.to_ne_bytes());
        v[4..6].copy_from_slice(&self.ntodelete.to_ne_bytes());
        v[6] = self.is_catalog_rel as u8;
        v
    }

    fn decode(data: &[u8]) -> Self {
        GistxlogDelete {
            snapshot_conflict_horizon: u32_at(data, 0),
            ntodelete: u16_at(data, 4),
            is_catalog_rel: bool_at(data, 6),
        }
    }

    /// The `offsets[]` flexible array, read from the same `XLogRecGetData`
    /// buffer just past the fixed header (`xldata->offsets`).
    fn offsets(data: &[u8], ntodelete: u16) -> Vec<OffsetNumber> {
        read_offsets(&data[SIZE_OF_GISTXLOG_DELETE..], ntodelete as usize)
    }
}

/// `gistxlogPageSplit` (gistxlog.h): `{ BlockNumber origrlink /*0*/;
/// GistNSN orignsn /*8-aligned @8*/; bool origleaf /*16*/; uint16 npage /*18*/;
/// bool markfollowright /*20*/; }`, struct size MAXALIGN'd to 24.
#[derive(Clone, Copy, Debug, Default)]
struct GistxlogPageSplit {
    origrlink: BlockNumber,
    orignsn: u64,
    origleaf: bool,
    npage: u16,
    markfollowright: bool,
}

/// `sizeof(gistxlogPageSplit)` = 24.
const SIZE_OF_GISTXLOG_PAGE_SPLIT: usize = 24;

impl GistxlogPageSplit {
    fn encode(&self) -> Vec<u8> {
        let mut v = alloc::vec![0u8; SIZE_OF_GISTXLOG_PAGE_SPLIT];
        v[0..4].copy_from_slice(&self.origrlink.to_ne_bytes());
        v[8..16].copy_from_slice(&self.orignsn.to_ne_bytes());
        v[16] = self.origleaf as u8;
        v[18..20].copy_from_slice(&self.npage.to_ne_bytes());
        v[20] = self.markfollowright as u8;
        v
    }

    fn decode(data: &[u8]) -> Self {
        GistxlogPageSplit {
            origrlink: u32_at(data, 0),
            orignsn: u64_at(data, 8),
            origleaf: bool_at(data, 16),
            npage: u16_at(data, 18),
            markfollowright: bool_at(data, 20),
        }
    }
}

/// `gistxlogPageDelete` (gistxlog.h): `{ FullTransactionId deleteXid /*0*/;
/// OffsetNumber downlinkOffset /*8*/; }`.
#[derive(Clone, Copy, Debug, Default)]
struct GistxlogPageDelete {
    delete_xid: FullTransactionId,
    downlink_offset: OffsetNumber,
}

/// `SizeOfGistxlogPageDelete` = `offsetof(.., downlinkOffset) + sizeof(OffsetNumber)`
/// = 8 + 2 = 10.
const SIZE_OF_GISTXLOG_PAGE_DELETE: usize = 10;

impl GistxlogPageDelete {
    fn encode(&self) -> Vec<u8> {
        let mut v = alloc::vec![0u8; SIZE_OF_GISTXLOG_PAGE_DELETE];
        v[0..8].copy_from_slice(&full_xid_value(self.delete_xid).to_ne_bytes());
        v[8..10].copy_from_slice(&self.downlink_offset.to_ne_bytes());
        v
    }

    fn decode(data: &[u8]) -> Self {
        GistxlogPageDelete {
            delete_xid: full_xid_from(u64_at(data, 0)),
            downlink_offset: u16_at(data, 8),
        }
    }
}

/// `gistxlogPageReuse` (gistxlog.h): `{ RelFileLocator locator /*0..12*/;
/// BlockNumber block /*12*/; FullTransactionId snapshotConflictHorizon
/// /*8-aligned @16*/; bool isCatalogRel /*24*/; }`.
#[derive(Clone, Copy, Debug)]
struct GistxlogPageReuse {
    locator: RelFileLocator,
    block: BlockNumber,
    snapshot_conflict_horizon: FullTransactionId,
    is_catalog_rel: bool,
}

/// `SizeOfGistxlogPageReuse` = `offsetof(.., isCatalogRel) + sizeof(bool)`
/// = 24 + 1 = 25.
const SIZE_OF_GISTXLOG_PAGE_REUSE: usize = 25;

impl GistxlogPageReuse {
    fn encode(&self) -> Vec<u8> {
        let mut v = alloc::vec![0u8; SIZE_OF_GISTXLOG_PAGE_REUSE];
        v[0..4].copy_from_slice(&self.locator.spcOid.to_ne_bytes());
        v[4..8].copy_from_slice(&self.locator.dbOid.to_ne_bytes());
        v[8..12].copy_from_slice(&self.locator.relNumber.to_ne_bytes());
        v[12..16].copy_from_slice(&self.block.to_ne_bytes());
        v[16..24].copy_from_slice(&full_xid_value(self.snapshot_conflict_horizon).to_ne_bytes());
        v[24] = self.is_catalog_rel as u8;
        v
    }

    fn decode(data: &[u8]) -> Self {
        GistxlogPageReuse {
            locator: RelFileLocator {
                spcOid: u32_at(data, 0),
                dbOid: u32_at(data, 4),
                relNumber: u32_at(data, 8),
            },
            block: u32_at(data, 12),
            snapshot_conflict_horizon: full_xid_from(u64_at(data, 16)),
            is_catalog_rel: bool_at(data, 24),
        }
    }
}

// ===========================================================================
// little fixed-width readers (native byte order, as records are on-disk).
// ===========================================================================

fn u16_at(data: &[u8], offset: usize) -> u16 {
    u16::from_ne_bytes(data[offset..offset + 2].try_into().unwrap())
}

fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_ne_bytes(data[offset..offset + 4].try_into().unwrap())
}

fn u64_at(data: &[u8], offset: usize) -> u64 {
    u64::from_ne_bytes(data[offset..offset + 8].try_into().unwrap())
}

fn bool_at(data: &[u8], offset: usize) -> bool {
    data[offset] != 0
}

/// Read `n` `OffsetNumber`s (u16, native order) from the front of `data`.
fn read_offsets(data: &[u8], n: usize) -> Vec<OffsetNumber> {
    let mut v: Vec<OffsetNumber> = Vec::with_capacity(n);
    for i in 0..n {
        v.push(u16_at(data, i * 2));
    }
    v
}

/// Serialize an `OffsetNumber` slice to its native on-disk bytes.
fn offsets_to_bytes(offsets: &[OffsetNumber]) -> Vec<u8> {
    let mut v: Vec<u8> = Vec::with_capacity(offsets.len() * 2);
    for off in offsets {
        v.extend_from_slice(&off.to_ne_bytes());
    }
    v
}

/// `IndexTupleSize(itup)` over a raw IndexTuple byte stream: the low 13 bits of
/// `t_info` (the u16 at offset 6, after the 6-byte `ItemPointerData`).
fn index_tuple_size_bytes(data: &[u8], pos: usize) -> usize {
    let t_info = u16_at(data, pos + 6);
    (t_info & INDEX_SIZE_MASK) as usize
}

/// `U64ToFullTransactionId` — read the on-disk `FullTransactionId` (a bare u64).
fn full_xid_from(v: u64) -> FullTransactionId {
    FullTransactionId { value: v }
}

/// `U64FromFullTransactionId` — the on-disk u64 value of a `FullTransactionId`.
fn full_xid_value(x: FullTransactionId) -> u64 {
    x.value
}

// ===========================================================================
// Decoded-record accessors (read off `record.record`, owned by xlogreader).
// ===========================================================================

/// `XLogRecGetData(record)` — the record's main data.
fn record_get_data(record: &XLogReaderState<'_>) -> Vec<u8> {
    record
        .record
        .as_ref()
        .map(|r| r.data().to_vec())
        .unwrap_or_default()
}

/// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
fn record_get_info(record: &XLogReaderState<'_>) -> u8 {
    record.record.as_ref().map(|r| r.info()).unwrap_or(0)
}

/// `XLogRecGetBlockData(record, block_id, &len)` — per-block data.
fn record_get_block_data(record: &XLogReaderState<'_>, block_id: u8) -> Vec<u8> {
    record
        .record
        .as_ref()
        .and_then(|r| r.block_data(block_id as usize))
        .map(|s| s.to_vec())
        .unwrap_or_default()
}

/// `XLogRecHasBlockRef(record, block_id)`.
fn record_has_block_ref(record: &XLogReaderState<'_>, block_id: u8) -> bool {
    record
        .record
        .as_ref()
        .map(|r| r.has_block_ref(block_id as usize))
        .unwrap_or(false)
}

/// `XLogRecGetBlockTag(record, block_id, NULL, NULL, &blkno)` — the block number.
fn record_get_block_tag_blkno(
    record: &XLogReaderState<'_>,
    block_id: u8,
) -> PgResult<BlockNumber> {
    let tag = xlogreader_seams::xlog_rec_get_block_tag_extended::call(
        record, block_id,
    )?;
    Ok(tag.map(|t| t.blkno).unwrap_or(InvalidBlockNumber))
}

/// `XLogRecGetBlockTag(record, block_id, &rlocator, NULL, NULL)` — the rlocator.
fn record_get_block_tag_locator(
    record: &XLogReaderState<'_>,
    block_id: u8,
) -> PgResult<RelFileLocator> {
    let tag = xlogreader_seams::xlog_rec_get_block_tag_extended::call(
        record, block_id,
    )?;
    Ok(tag.map(|t| t.rlocator).unwrap_or(RelFileLocator {
        spcOid: 0,
        dbOid: 0,
        relNumber: 0,
    }))
}

/// Borrow the replay `opCtx` as an `Mcx` for the conflict-resolution seams.
/// Mirrors nbtree's `with_op_ctx`; the C runs each redo op in `opCtx`.
fn with_op_ctx<R>(f: impl FnOnce(Mcx<'_>) -> R) -> R {
    OP_CTX.with(|c| {
        let borrow = c.borrow();
        let ctx = borrow
            .as_ref()
            .expect("gist_redo called without gist_xlog_startup (opCtx is NULL)");
        f(ctx.mcx())
    })
}

// ===========================================================================
// `opCtx` — the C `static MemoryContext opCtx` recovery working context.
// ===========================================================================

thread_local! {
    /// `static MemoryContext opCtx` (gistxlog.c) — working memory for redo
    /// operations, created at recovery startup and deleted at cleanup.
    static OP_CTX: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };
}

// ===========================================================================
// redo replay.
// ===========================================================================

/// `PageSetLSN(page, lsn)` against the page bytes.
fn page_set_lsn(page: &mut [u8], lsn: XLogRecPtr) -> PgResult<()> {
    let mut pm = PageMut::new(page)?;
    PageSetLSN(&mut pm, lsn);
    Ok(())
}

/// Replay the clearing of `F_FOLLOW_RIGHT` on a child page
/// (`gistRedoClearFollowRight`, gistxlog.c:39).
///
/// Even if the WAL record includes a full-page image, we still update the
/// follow-right flag, because that change is not part of the full-page image.
fn gist_redo_clear_follow_right(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<()> {
    let lsn = record.EndRecPtr;

    let (action, buffer) = XLogReadBufferForRedo(record, block_id)?;
    if action == XLogRedoAction::BlkNeedsRedo || action == XLogRedoAction::BlkRestored {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            gist_page_set_nsn(page, lsn)?;
            GistClearFollowRight(page)?;
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

/// redo any page update (except page split) (`gistRedoPageUpdateRecord`,
/// gistxlog.c:69).
fn gist_redo_page_update_record(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = GistxlogPageUpdate::decode(&record_get_data(record));

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let begin = record_get_block_data(record, 0);
        let datalen = begin.len();
        // `data` is the running cursor into `begin` (the C `data - begin`).
        let mut data: usize = 0;

        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            if xldata.ntodelete == 1 && xldata.ntoinsert == 1 {
                // Replacing one tuple with one other tuple: use
                // PageIndexTupleOverwrite for consistency with gistplacetopage.
                let offnum = u16_at(&begin, data);
                data += core::mem::size_of::<OffsetNumber>();
                let itupsize = index_tuple_size_bytes(&begin, data);
                let ok = {
                    let mut pm = PageMut::new(page)?;
                    PageIndexTupleOverwrite(&mut pm, offnum, &begin[data..data + itupsize])?
                };
                if !ok {
                    return Err(panic_failed_to_add(itupsize as i32));
                }
                data += itupsize;
                // Should be nothing left after consuming 1 tuple.
                debug_assert_eq!(data, datalen);
            } else if xldata.ntodelete > 0 {
                // Otherwise, delete old tuples if any.
                let todelete = read_offsets(&begin, xldata.ntodelete as usize);
                data += core::mem::size_of::<OffsetNumber>() * xldata.ntodelete as usize;

                {
                    let mut pm = PageMut::new(page)?;
                    PageIndexMultiDelete(&mut pm, &todelete)?;
                }
                if GistPageIsLeaf(page)? {
                    GistMarkTuplesDeleted(page)?;
                }
            }

            // Add new tuples if any.
            if data < datalen {
                let mut off = {
                    let pref = PageRef::new(page)?;
                    if PageIsEmpty(&pref) {
                        FIRST_OFFSET_NUMBER
                    } else {
                        PageGetMaxOffsetNumber(&pref) + 1
                    }
                };

                while data < datalen {
                    let sz = index_tuple_size_bytes(&begin, data);
                    let l = {
                        let mut pm = PageMut::new(page)?;
                        PageAddItemExtended(&mut pm, &begin[data..data + sz], off, 0)?
                    };
                    data += sz;
                    if l == INVALID_OFFSET_NUMBER {
                        return Err(panic_failed_to_add(sz as i32));
                    }
                    off += 1;
                }
            }

            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }

    // Fix follow-right data on left child page. Must be done while still
    // holding the lock on the target page. Even if the target page no longer
    // exists, we still attempt to replay the change on the child page.
    if record_has_block_ref(record, 1) {
        gist_redo_clear_follow_right(record, 1)?;
    }

    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

/// redo delete on gist index page to remove tuples marked as DEAD during index
/// tuple insertion (`gistRedoDeleteRecord`, gistxlog.c:171).
fn gist_redo_delete_record(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    let xldata = GistxlogDelete::decode(&data);
    let to_delete = GistxlogDelete::offsets(&data, xldata.ntodelete);

    // If we have any conflict processing to do, it must happen before we update
    // the page. GiST delete records can conflict with standby queries; vacuum
    // records' conflicts have already been handled by
    // XLOG_HEAP2_PRUNE_VACUUM_SCAN.
    if in_hot_standby() {
        let rlocator = record_get_block_tag_locator(record, 0)?;
        with_op_ctx(|mcx| {
            standby::resolve_recovery_conflict_with_snapshot::call(
                mcx,
                xldata.snapshot_conflict_horizon,
                xldata.is_catalog_rel,
                rlocator,
            )
        })?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            {
                let mut pm = PageMut::new(page)?;
                PageIndexMultiDelete(&mut pm, &to_delete)?;
            }
            GistClearPageHasGarbage(page)?;
            GistMarkTuplesDeleted(page)?;
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

/// Decode a split record's per-page payload into the raw index-tuple byte blobs
/// (`decodePageSplitRecord`, gistxlog.c:222). Returns the tuple blobs and `*n`.
fn decode_page_split_record(begin: &[u8]) -> (Vec<Vec<u8>>, i32) {
    let len = begin.len();
    // extract the number of tuples
    let n = i32::from_ne_bytes(begin[0..4].try_into().unwrap());
    let mut ptr: usize = core::mem::size_of::<i32>();

    let mut tuples: Vec<Vec<u8>> = Vec::with_capacity(n as usize);
    for _ in 0..n {
        debug_assert!(ptr < len);
        let sz = index_tuple_size_bytes(begin, ptr);
        tuples.push(begin[ptr..ptr + sz].to_vec());
        ptr += sz;
    }
    debug_assert_eq!(ptr, len);

    (tuples, n)
}

/// redo a page split (`gistRedoPageSplitRecord`, gistxlog.c:246).
fn gist_redo_page_split_record(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = GistxlogPageSplit::decode(&record_get_data(record));
    let mut firstbuffer: Buffer = Buffer::default();
    let mut isrootsplit = false;

    // We must hold lock on the first-listed page throughout the action,
    // including while updating the left child page (if any). We can unlock
    // remaining pages as soon as they've been written, because there is no path
    // for concurrent queries to reach those pages without first visiting the
    // first-listed page.

    for i in 0..xldata.npage as i32 {
        let blkno = record_get_block_tag_blkno(record, (i + 1) as u8)?;
        if blkno == GIST_ROOT_BLKNO {
            debug_assert_eq!(i, 0);
            isrootsplit = true;
        }

        let buffer = XLogInitBufferForRedo(record, (i + 1) as u8)?;
        let data = record_get_block_data(record, (i + 1) as u8);

        let (tuples, _num) = decode_page_split_record(&data);

        // ok, clear buffer
        let flags = if xldata.origleaf && blkno != GIST_ROOT_BLKNO {
            F_LEAF
        } else {
            0
        };

        // GISTInitBuffer(buffer, flags); gistfillbuffer(page, tuples, num, ...);
        // and the rightlink/NSN/follow-right edits, all under the page lock.
        let nextblkno = if blkno != GIST_ROOT_BLKNO && i < xldata.npage as i32 - 1 {
            Some(record_get_block_tag_blkno(record, (i + 2) as u8)?)
        } else {
            None
        };

        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            gistinitpage(page, flags)?;

            // gistfillbuffer(page, tuples, num, FirstOffsetNumber);
            crate::gist_page::gistfillbuffer(page, &tuples, FIRST_OFFSET_NUMBER)?;

            if blkno == GIST_ROOT_BLKNO {
                set_gist_page_rightlink(page, InvalidBlockNumber)?;
                gist_page_set_nsn(page, xldata.orignsn)?;
                GistClearFollowRight(page)?;
            } else {
                if i < xldata.npage as i32 - 1 {
                    set_gist_page_rightlink(page, nextblkno.unwrap())?;
                } else {
                    set_gist_page_rightlink(page, xldata.origrlink)?;
                }
                gist_page_set_nsn(page, xldata.orignsn)?;
                if i < xldata.npage as i32 - 1 && !isrootsplit && xldata.markfollowright {
                    GistMarkFollowRight(page)?;
                } else {
                    GistClearFollowRight(page)?;
                }
            }

            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);

        if i == 0 {
            firstbuffer = buffer;
        } else {
            unlock_release_buffer::call(buffer);
        }
    }

    // Fix follow-right data on left child page, if any.
    if record_has_block_ref(record, 0) {
        gist_redo_clear_follow_right(record, 0)?;
    }

    // Finally, release lock on the first page.
    unlock_release_buffer::call(firstbuffer);
    Ok(())
}

/// redo page deletion (`gistRedoPageDelete`, gistxlog.c:341).
fn gist_redo_page_delete(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = GistxlogPageDelete::decode(&record_get_data(record));

    let (leaf_action, leaf_buffer) = XLogReadBufferForRedo(record, 0)?;
    if leaf_action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(leaf_buffer, &mut |page: &mut [u8]| {
            GistPageSetDeleted(page, xldata.delete_xid)?;
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(leaf_buffer);
    }

    let (parent_action, parent_buffer) = XLogReadBufferForRedo(record, 1)?;
    if parent_action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(parent_buffer, &mut |page: &mut [u8]| {
            {
                let mut pm = PageMut::new(page)?;
                PageIndexTupleDelete(&mut pm, xldata.downlink_offset)?;
            }
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(parent_buffer);
    }

    if BufferIsValid(parent_buffer) {
        unlock_release_buffer::call(parent_buffer);
    }
    if BufferIsValid(leaf_buffer) {
        unlock_release_buffer::call(leaf_buffer);
    }
    Ok(())
}

/// redo page reuse (`gistRedoPageReuse`, gistxlog.c:375).
fn gist_redo_page_reuse(record: &XLogReaderState<'_>) -> PgResult<()> {
    let xlrec = GistxlogPageReuse::decode(&record_get_data(record));

    // PAGE_REUSE records exist to provide a conflict point when we reuse pages
    // in the index via the FSM. That's all they do though.
    //
    // snapshotConflictHorizon was the page's deleteXid. The
    // GlobalVisCheckRemovableFullXid(deleteXid) test in gistPageRecyclable()
    // conceptually mirrors the PGPROC->xmin > limitXmin test in
    // GetConflictingVirtualXIDs(); one XID value achieves the same exclusion
    // effect on primary and standby.
    if in_hot_standby() {
        with_op_ctx(|mcx| {
            standby::resolve_recovery_conflict_with_snapshot_full_xid::call(
                mcx,
                xlrec.snapshot_conflict_horizon,
                xlrec.is_catalog_rel,
                xlrec.locator,
            )
        })?;
    }
    Ok(())
}

/// `gist_redo(record)` (gistxlog.c:396) — the `rm_redo` slot.
///
/// GiST indexes do not require any conflict processing.
pub fn gist_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !XLR_INFO_MASK;

    // The C runs each op in opCtx and resets it afterward; the recovery
    // working context is held thread-local (`with_op_ctx`).
    let result = (|| match info {
        XLOG_GIST_PAGE_UPDATE => gist_redo_page_update_record(record),
        XLOG_GIST_DELETE => gist_redo_delete_record(record),
        XLOG_GIST_PAGE_REUSE => gist_redo_page_reuse(record),
        XLOG_GIST_PAGE_SPLIT => gist_redo_page_split_record(record),
        XLOG_GIST_PAGE_DELETE => gist_redo_page_delete(record),
        // nop. See gistGetFakeLSN().
        XLOG_GIST_ASSIGN_LSN => Ok(()),
        // elog(PANIC, "gist_redo: unknown op code %u", info)
        _ => Err(PgError::new(
            PANIC,
            format!("gist_redo: unknown op code {info}"),
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
// opCtx lifecycle (gist_xlog_startup / gist_xlog_cleanup).
// ===========================================================================

/// `gist_xlog_startup()` (gistxlog.c:437): create the replay working context
/// (`rm_startup` slot). `parent` is the recovery `CurrentMemoryContext`; the new
/// context is a regular AllocSet under it.
pub fn gist_xlog_startup(_parent: Mcx<'_>) -> PgResult<()> {
    OP_CTX.with(|c| {
        *c.borrow_mut() = Some(MemoryContext::new("GiST temporary context"));
    });
    Ok(())
}

/// `gist_xlog_cleanup()` (gistxlog.c:443): delete the replay working context
/// (`rm_cleanup` slot).
pub fn gist_xlog_cleanup() {
    OP_CTX.with(|c| {
        *c.borrow_mut() = None;
    });
}

// ===========================================================================
// gist_mask (gistxlog.c:452).
// ===========================================================================

/// Mask a Gist page before running consistency checks on it (`gist_mask`,
/// gistxlog.c:452) — the `rm_mask` slot.
pub fn gist_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    mask_page_lsn_and_checksum::call(pagedata);

    mask_page_hint_bits::call(pagedata);
    mask_unused_space::call(pagedata)?;

    // NSN is nothing but a special purpose LSN. Hence, mask it for the same
    // reason as mask_page_lsn_and_checksum.
    gist_page_set_nsn(pagedata, MASK_MARKER)?;

    // We update F_FOLLOW_RIGHT flag on the left child after writing WAL record.
    // Hence, mask this flag. See gistplacetopage() for details.
    GistMarkFollowRight(pagedata)?;

    if GistPageIsLeaf(pagedata)? {
        // In gist leaf pages, it is possible to modify the LP_FLAGS without
        // emitting any WAL record. Hence, mask the line pointer flags. See
        // gistkillitems() for details.
        mask_lp_flags::call(pagedata);
    }

    // During gist redo, we never mark a page as garbage. Hence, mask it to
    // ignore any differences.
    GistClearPageHasGarbage(pagedata)?;
    Ok(())
}

// ===========================================================================
// WAL record builders (insert side).
// ===========================================================================

/// Write WAL record of a page split (`gistXLogSplit`, gistxlog.c:494).
///
/// `dist` is the in-order list of split-page descriptors (the C
/// `SplitPageLayout *dist` linked list, walked once to count, once to register).
/// `leftchildbuf` is `InvalidBuffer` when there is no left child to fix.
pub fn gist_xlog_split(
    page_is_leaf: bool,
    dist: &[SplitPageLayout<'_>],
    origrlink: BlockNumber,
    orignsn: u64,
    leftchildbuf: Buffer,
    markfollowright: bool,
) -> PgResult<XLogRecPtr> {
    let npage = dist.len() as i32;

    let xlrec = GistxlogPageSplit {
        origrlink,
        orignsn,
        origleaf: page_is_leaf,
        npage: npage as u16,
        markfollowright,
    };

    xloginsert::xlog_begin_insert::call()?;

    // Include a full page image of the child buf. (only necessary if a
    // checkpoint happened since the child page was split)
    if BufferIsValid(leftchildbuf) {
        xloginsert::xlog_register_buffer::call(0, leftchildbuf, REGBUF_STANDARD)?;
    }

    // NOTE: We register a lot of data. The caller must've called
    // XLogEnsureRecordSpace() to prepare for that.
    xloginsert::xlog_register_data::call(&xlrec.encode())?;

    let mut i: u8 = 1;
    for ptr in dist {
        xloginsert::xlog_register_buffer::call(i, ptr.buffer, REGBUF_WILL_INIT)?;
        xloginsert::xlog_register_buf_data::call(i, &ptr.block.num.to_ne_bytes())?;
        xloginsert::xlog_register_buf_data::call(i, &ptr.list)?;
        i += 1;
    }

    xloginsert::xlog_insert_record::call(RM_GIST_ID, XLOG_GIST_PAGE_SPLIT)
}

/// Write XLOG record describing a page deletion, including removal of the
/// downlink from the parent page (`gistXLogPageDelete`, gistxlog.c:551).
pub fn gist_xlog_page_delete(
    buffer: Buffer,
    xid: FullTransactionId,
    parent_buffer: Buffer,
    downlink_offset: OffsetNumber,
) -> PgResult<XLogRecPtr> {
    let xlrec = GistxlogPageDelete {
        delete_xid: xid,
        downlink_offset,
    };

    xloginsert::xlog_begin_insert::call()?;
    xloginsert::xlog_register_data::call(&xlrec.encode())?;

    xloginsert::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;
    xloginsert::xlog_register_buffer::call(1, parent_buffer, REGBUF_STANDARD)?;

    xloginsert::xlog_insert_record::call(RM_GIST_ID, XLOG_GIST_PAGE_DELETE)
}

/// Write an empty XLOG record to assign a distinct LSN (`gistXLogAssignLSN`,
/// gistxlog.c:575).
pub fn gist_xlog_assign_lsn() -> PgResult<XLogRecPtr> {
    let dummy: i32 = 0;

    // Records other than XLOG_SWITCH must have content. We use an integer 0 to
    // follow the restriction.
    xloginsert::xlog_begin_insert::call()?;
    xloginsert::xlog_set_record_flags::call(XLOG_MARK_UNIMPORTANT);
    xloginsert::xlog_register_data::call(&dummy.to_ne_bytes())?;
    xloginsert::xlog_insert_record::call(RM_GIST_ID, XLOG_GIST_ASSIGN_LSN)
}

/// Write XLOG record about reuse of a deleted page (`gistXLogPageReuse`,
/// gistxlog.c:593).
pub fn gist_xlog_page_reuse(
    rel: &Relation<'_>,
    heaprel: &Relation<'_>,
    blkno: BlockNumber,
    delete_xid: FullTransactionId,
) -> PgResult<()> {
    // Note that we don't register the buffer with the record, because this
    // operation doesn't modify the page. This record only exists to provide a
    // conflict point for Hot Standby.
    let xlrec_reuse = GistxlogPageReuse {
        is_catalog_rel:
            relcache_seams::relation_is_accessible_in_logical_decoding::call(
                heaprel,
            )?,
        locator: rel.rd_locator,
        block: blkno,
        snapshot_conflict_horizon: delete_xid,
    };

    xloginsert::xlog_begin_insert::call()?;
    xloginsert::xlog_register_data::call(&xlrec_reuse.encode())?;

    xloginsert::xlog_insert_record::call(RM_GIST_ID, XLOG_GIST_PAGE_REUSE)?;
    Ok(())
}

/// Write XLOG record describing a page update (`gistXLogUpdate`,
/// gistxlog.c:628).
///
/// The update can include any number of deletions and/or insertions of tuples
/// on a single index page. `itup` carries the raw IndexTuple byte images. If
/// this update inserts a downlink for a split page, `leftchildbuf` is the child
/// whose `F_FOLLOW_RIGHT` flag is cleared and NSN set.
pub fn gist_xlog_update(
    buffer: Buffer,
    todelete: &[OffsetNumber],
    itup: &[&[u8]],
    leftchildbuf: Buffer,
) -> PgResult<XLogRecPtr> {
    let xlrec = GistxlogPageUpdate {
        ntodelete: todelete.len() as u16,
        ntoinsert: itup.len() as u16,
    };

    xloginsert::xlog_begin_insert::call()?;
    xloginsert::xlog_register_data::call(&xlrec.encode())?;

    xloginsert::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;
    xloginsert::xlog_register_buf_data::call(0, &offsets_to_bytes(todelete))?;

    // new tuples
    for itup_i in itup {
        xloginsert::xlog_register_buf_data::call(0, itup_i)?;
    }

    // Include a full page image of the child buf. (only necessary if a
    // checkpoint happened since the child page was split)
    if BufferIsValid(leftchildbuf) {
        xloginsert::xlog_register_buffer::call(1, leftchildbuf, REGBUF_STANDARD)?;
    }

    xloginsert::xlog_insert_record::call(RM_GIST_ID, XLOG_GIST_PAGE_UPDATE)
}

/// Write XLOG record describing a delete of leaf index tuples marked as DEAD
/// during new tuple insertion (`gistXLogDelete`, gistxlog.c:669).
///
/// One may think this case is already covered by `gistXLogUpdate`, but deletion
/// of index tuples might conflict with standby queries and needs special
/// handling.
pub fn gist_xlog_delete(
    buffer: Buffer,
    todelete: &[OffsetNumber],
    snapshot_conflict_horizon: TransactionId,
    heaprel: &Relation<'_>,
) -> PgResult<XLogRecPtr> {
    let xlrec = GistxlogDelete {
        is_catalog_rel:
            relcache_seams::relation_is_accessible_in_logical_decoding::call(
                heaprel,
            )?,
        snapshot_conflict_horizon,
        ntodelete: todelete.len() as u16,
    };

    xloginsert::xlog_begin_insert::call()?;
    xloginsert::xlog_register_data::call(&xlrec.encode_header())?;

    // We need the target-offsets array whether or not we store the whole
    // buffer, to allow us to find the snapshotConflictHorizon on a standby
    // server.
    xloginsert::xlog_register_data::call(&offsets_to_bytes(todelete))?;

    xloginsert::xlog_register_buffer::call(0, buffer, REGBUF_STANDARD)?;

    xloginsert::xlog_insert_record::call(RM_GIST_ID, XLOG_GIST_DELETE)
}

// ===========================================================================
// gistGetFakeLSN (gistutil.c).
// ===========================================================================

thread_local! {
    /// `static XLogRecPtr counter` in `gistGetFakeLSN` (temp-rel branch).
    static FAKE_LSN_COUNTER: RefCell<XLogRecPtr> = const { RefCell::new(FirstNormalUnloggedLSN) };
    /// `static XLogRecPtr lastlsn` in `gistGetFakeLSN` (permanent-rel branch).
    static FAKE_LSN_LASTLSN: RefCell<XLogRecPtr> = const { RefCell::new(0 /* InvalidXLogRecPtr */) };
}

/// `gistGetFakeLSN(rel)` (gistutil.c): produce a fake LSN for an unlogged/temp
/// GiST index so NSN interlocks still order correctly without real WAL.
pub fn gist_get_fake_lsn(rel: &Relation<'_>) -> PgResult<XLogRecPtr> {
    let persistence = rel.rd_rel.relpersistence;
    if persistence == RELPERSISTENCE_TEMP {
        // Temporary relations are only accessible in our session, so a simple
        // backend-local counter will do.
        FAKE_LSN_COUNTER.with(|c| {
            let mut c = c.borrow_mut();
            let cur = *c;
            *c += 1;
            Ok(cur)
        })
    } else if persistence == RELPERSISTENCE_PERMANENT {
        // WAL-logging on this relation will start after commit, so its LSNs
        // must be distinct numbers smaller than the LSN at the next commit.
        // Emit a dummy WAL record if insert-LSN hasn't advanced after the last
        // call.
        let mut currlsn = xlog_seams::get_xlog_insert_rec_ptr::call();

        // Shouldn't be called for WAL-logging relations.
        debug_assert!(!relcache_seams::relation_needs_wal::call(rel));

        FAKE_LSN_LASTLSN.with(|l| -> PgResult<()> {
            let lastlsn = *l.borrow();
            // No need for an actual record if we already have a distinct LSN.
            if lastlsn != 0 && lastlsn == currlsn {
                currlsn = gist_xlog_assign_lsn()?;
            }
            *l.borrow_mut() = currlsn;
            Ok(())
        })?;
        Ok(currlsn)
    } else {
        // Unlogged relations are accessible from other backends, and survive
        // (clean) restarts. GetFakeLSNForUnloggedRel() handles that for us.
        debug_assert_eq!(persistence, RELPERSISTENCE_UNLOGGED);
        Ok(xlog_seams::get_fake_lsn_for_unlogged_rel::call())
    }
}

// ===========================================================================
// `InHotStandby` (procarray.h) — `standbyState >= STANDBY_SNAPSHOT_PENDING`.
// ===========================================================================

/// `InHotStandby` macro — true once the standby has begun (or is pending) a
/// running-xacts snapshot during recovery.
fn in_hot_standby() -> bool {
    ::wal::xlogutils::in_hot_standby(
        ::xlogutils::standby_state(),
    )
}

// ===========================================================================
// Error reporters.
// ===========================================================================

/// `elog(ERROR, "failed to add item to GiST index page, size %d bytes", sz)`.
fn panic_failed_to_add(sz: i32) -> PgError {
    ereport(ERROR)
        .errmsg(format!(
            "failed to add item to GiST index page, size {sz} bytes"
        ))
        .into_error()
}
