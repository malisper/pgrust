//! `backend-access-gin-ginxlog` — an owned-tree Rust port of
//! `src/backend/access/gin/ginxlog.c` (PostgreSQL 18.3): the WAL redo
//! (`gin_redo`), recovery-context startup/cleanup (`gin_xlog_startup` /
//! `gin_xlog_cleanup`), and consistency-mask (`gin_mask`) resource-manager
//! callbacks for GIN inverted indexes.
//!
//! `gin_redo` dispatches a decoded WAL record to its per-op handler
//! (`ginRedoCreatePTree` / `ginRedoInsert` / `ginRedoSplit` /
//! `ginRedoVacuumPage` / `ginRedoVacuumDataLeafPage` / `ginRedoDeletePage` /
//! `ginRedoUpdateMetapage` / `ginRedoInsertListPage` /
//! `ginRedoDeleteListPages`), reading the record through the xlogreader
//! value-typed accessors, fetching the redo buffers via xlogutils'
//! `XLogReadBufferForRedo` / `XLogInitBufferForRedo`, and applying the page
//! edits with the bufpage primitives and the already-merged GIN page crates.
//!
//! ## What is grounded in-crate vs. what is seamed
//!
//! The page-byte edits (posting-list recompression, metapage / list-page
//! re-initialization, downlink and deletion bookkeeping) are pure safe-Rust
//! over the 8 KB page buffer, reusing the in-repo GIN page crates:
//! [`gindatapage`] (`ginpage`/`postinglist` accessors),
//! [`core_probe`] (the posting-list codec
//! `ginCompressPostingList`/`ginPostingListDecode`/`ginMergeItemPointers`),
//! and [`ginutil`] (`GinInitBuffer`/`GinInitMetabuffer`).
//! The few GIN index-tuple/metapage byte macros the redo path needs that the
//! page crates keep private (`GinSetDownlink`, `IndexTupleSize`, the
//! `GinMetaPageData` on-disk (de)serialization, the `pd_prune_xid` /
//! `pd_lower` header bytes) are transcribed 1:1 here, exactly as the
//! src-idiomatic port does.
//!
//! The genuinely-external WAL-recovery substrate crosses seams / owner crates:
//! `XLogReadBufferForRedo` / `XLogInitBufferForRedo` (xlogutils), the
//! `XLogReaderState` block-tag accessor (xlogreader), the buffer manager
//! (`with_buffer_page` / `BufferGetBlockNumber` / `MarkBufferDirty` /
//! `UnlockReleaseBuffer`), and the page-masking helpers (bufmask). The decoded
//! record's main data / per-block data / info byte are read off
//! `record.record` (the decoded payload owned by xlogreader).
//!
//! `opCtx` (C `static MemoryContext`) is owned here as a thread-local recovery
//! context, created by `gin_xlog_startup`, reset after each redo, and deleted
//! by `gin_xlog_cleanup` — mirroring nbtree's `btree_xlog_startup`.
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use core::cell::RefCell;

use ::bufmask_seams::{
    mask_page_content, mask_page_hint_bits, mask_page_lsn_and_checksum, mask_unused_space,
};
use ::xlogreader_seams::xlog_rec_get_block_tag_extended;
use ::xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo};
use ::bufmgr_seams::{
    buffer_get_block_number, mark_buffer_dirty, unlock_release_buffer, with_buffer_page,
};
use ::page::{
    PageAddItemExtended, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageIndexTupleDelete,
    PageIsEmpty, PageMut, PageRef, PageSetLSN,
};
use ::utils_error::{PgError, PgResult};

use ::core_probe::ginpostinglist::{
    ginCompressPostingList, ginMergeItemPointers, ginPostingListDecode,
};
use gindatapage as gdp;
use ::gindatapage::datatree::GinDataPageAddPostingItem;
use ::ginutil::{GinInitBuffer, GinInitMetabuffer};

use ::mcx::{Mcx, MemoryContext};
use ::types_core::primitive::{BlockNumber, OffsetNumber, BLCKSZ};
use ::types_error::error::PANIC;
use ::gin::{
    GinMetaPageData, GIN_COMPRESSED, GIN_DATA, GIN_DELETED, GIN_INCOMPLETE_SPLIT, GIN_LEAF,
    GIN_LIST, GIN_LIST_FULLROW, GIN_METAPAGE_BLKNO,
};
use ::types_storage::bufpage::SizeOfPageHeaderData;
use ::types_storage::buf::{Buffer, BufferIsValid};
use ::types_tuple::heaptuple::{
    ItemPointerData, FIRST_OFFSET_NUMBER, INDEX_SIZE_MASK, INVALID_OFFSET_NUMBER,
};
use ::wal::rmgr::XLogReaderState;
use ::wal::XLogRedoAction;

#[cfg(test)]
mod tests;

// ===========================================================================
// `opCtx` — the C `static MemoryContext opCtx` recovery working context.
// ===========================================================================

thread_local! {
    /// `static MemoryContext opCtx` (ginxlog.c) — working memory for redo
    /// operations, created at recovery startup and deleted at cleanup.
    static OP_CTX: RefCell<Option<MemoryContext>> = const { RefCell::new(None) };
}

// ===========================================================================
// XLOG_GIN_* resource-manager info bytes (ginxlog.h).
// ===========================================================================

/// `XLOG_GIN_CREATE_PTREE`.
const XLOG_GIN_CREATE_PTREE: u8 = 0x10;
/// `XLOG_GIN_INSERT`.
const XLOG_GIN_INSERT: u8 = 0x20;
/// `XLOG_GIN_SPLIT`.
const XLOG_GIN_SPLIT: u8 = 0x30;
/// `XLOG_GIN_VACUUM_PAGE`.
const XLOG_GIN_VACUUM_PAGE: u8 = 0x40;
/// `XLOG_GIN_DELETE_PAGE`.
const XLOG_GIN_DELETE_PAGE: u8 = 0x50;
/// `XLOG_GIN_UPDATE_META_PAGE`.
const XLOG_GIN_UPDATE_META_PAGE: u8 = 0x60;
/// `XLOG_GIN_INSERT_LISTPAGE`.
const XLOG_GIN_INSERT_LISTPAGE: u8 = 0x70;
/// `XLOG_GIN_DELETE_LISTPAGE`.
const XLOG_GIN_DELETE_LISTPAGE: u8 = 0x80;
/// `XLOG_GIN_VACUUM_DATA_LEAF_PAGE`.
const XLOG_GIN_VACUUM_DATA_LEAF_PAGE: u8 = 0x90;

// `ginxlogInsert`/`ginxlogSplit` flag bits (ginxlog.h:124..126).
const GIN_INSERT_ISDATA: u16 = 0x01;
const GIN_INSERT_ISLEAF: u16 = 0x02;
const GIN_SPLIT_ROOT: u16 = 0x04;

// `GIN_SEGMENT_*` action codes (ginxlog.h:91..95).
const GIN_SEGMENT_DELETE: u8 = 1;
const GIN_SEGMENT_INSERT: u8 = 2;
const GIN_SEGMENT_REPLACE: u8 = 3;
const GIN_SEGMENT_ADDITEMS: u8 = 4;

/// `XLR_INFO_MASK` (xlogrecord.h): the low four bits of the info byte, masked
/// off before dispatch so the rmgr's opcode (high nibble) remains.
const XLR_INFO_MASK: u8 = 0x0F;

/// `InvalidBlockNumber` (storage/block.h).
const InvalidBlockNumber: BlockNumber = 0xFFFF_FFFF;

/// `sizeof(BlockIdData)` == 4.
const SIZE_OF_BLOCK_ID_DATA: usize = 4;

/// `sizeof(ItemPointerData)` on the on-disk ABI: 6 bytes.
const SIZE_OF_ITEM_POINTER: usize = 6;

/// `sizeof(GinMetaPageData)` as laid out on disk (LP64). Two int64 members force
/// 8-byte alignment and a trailing pad after the final int32 `ginVersion`.
const SIZE_OF_GIN_META_PAGE_DATA: usize = 56;

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

/// `XLogRecGetBlockData(record, block_id, &len)` — per-block data registered
/// with the record.
fn record_get_block_data<'a>(record: &'a XLogReaderState<'_>, block_id: u8) -> &'a [u8] {
    record
        .record
        .as_ref()
        .and_then(|r| r.block_data(block_id as usize))
        .unwrap_or(&[])
}

// ===========================================================================
// Byte-level page / index-tuple helpers (transcribed from ginblock.h /
// bufpage.h — the page crates keep these header/tuple bytes private).
// ===========================================================================

/// `MAXALIGN(x)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// `SHORTALIGN(LEN)` — round up to a multiple of `ALIGNOF_SHORT` (2).
#[inline]
const fn shortalign(len: usize) -> usize {
    (len + 1) & !1
}

/// Byte offset of `pd_lower` within the page header.
const OFF_PD_LOWER: usize = 12;
/// Byte offset of `pd_special` within the page header.
const OFF_PD_SPECIAL: usize = 16;
/// Byte offset of `pd_prune_xid` within the page header.
const OFF_PD_PRUNE_XID: usize = 20;

/// `PageGetSpecialPointer(page)` byte offset (`pd_special`); also the GIN
/// opaque offset.
#[inline]
fn special_pointer_offset(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[OFF_PD_SPECIAL], page[OFF_PD_SPECIAL + 1]]) as usize
}

/// Read the page header's `pd_lower` field (offset 12).
#[inline]
fn read_pd_lower(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[OFF_PD_LOWER], page[OFF_PD_LOWER + 1]]) as usize
}

/// `GinPageSetDeleteXid(page, xid)`: write `pd_prune_xid` (offset 20).
#[inline]
fn write_pd_prune_xid(page: &mut [u8], xid: u32) {
    page[OFF_PD_PRUNE_XID..OFF_PD_PRUNE_XID + 4].copy_from_slice(&xid.to_ne_bytes());
}

/// `IndexTupleSize(itup)` over raw bytes: `t_info & INDEX_SIZE_MASK`. `t_info`
/// is the uint16 right after the 6-byte `t_tid` (`ItemPointerData`).
#[inline]
fn index_tuple_size(tuple: &[u8]) -> usize {
    let t_info = u16::from_ne_bytes([tuple[SIZE_OF_ITEM_POINTER], tuple[SIZE_OF_ITEM_POINTER + 1]]);
    (t_info & INDEX_SIZE_MASK) as usize
}

/// `GinSetDownlink(itup, blkno)` — set `t_tid = (blkno, InvalidOffsetNumber)`.
#[inline]
fn gin_set_downlink(tuple: &mut [u8], blkno: BlockNumber) {
    let tid = ItemPointerData::new(blkno, INVALID_OFFSET_NUMBER);
    gdp::write_item_pointer(tuple, &tid);
}

/// `OffsetNumberNext(offsetNumber)` (off.h).
#[inline]
fn offset_number_next(off: OffsetNumber) -> OffsetNumber {
    off + 1
}

/// `BlockIdGetBlockNumber((BlockId) ptr)` from a 4-byte `BlockIdData` image
/// (`{ bi_hi: u16, bi_lo: u16 }`).
fn block_id_get_block_number(buf: &[u8]) -> BlockNumber {
    let bi_hi = u16::from_ne_bytes([buf[0], buf[1]]);
    let bi_lo = u16::from_ne_bytes([buf[2], buf[3]]);
    ((bi_hi as u32) << 16) | (bi_lo as u32)
}

// ===========================================================================
// ginRedoClearIncompleteSplit (ginxlog.c:24)
// ===========================================================================

/// `ginRedoClearIncompleteSplit(record, block_id)` (ginxlog.c:24): clear the
/// `GIN_INCOMPLETE_SPLIT` flag on a child page once a split is finished.
fn ginRedoClearIncompleteSplit(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<()> {
    let lsn = record.EndRecPtr;

    let (action, buffer) = XLogReadBufferForRedo(record, block_id)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            // GinPageGetOpaque(page)->flags &= ~GIN_INCOMPLETE_SPLIT;
            let f = gdp::gin_page_get_flags(page);
            gdp::gin_page_set_flags(page, f & !GIN_INCOMPLETE_SPLIT);
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

// ===========================================================================
// ginRedoCreatePTree (ginxlog.c:43)
// ===========================================================================

/// `sizeof(ginxlogCreatePostingTree)` (ginxlog.h): a single `uint32 size`.
const SIZE_OF_GINXLOG_CREATE_POSTING_TREE: usize = 4;

/// `ginRedoCreatePTree(record)` (ginxlog.c:43): replay creation of a posting
/// tree's root leaf page.
fn ginRedoCreatePTree(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    // ginxlogCreatePostingTree { uint32 size; }
    let size = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]) as usize;

    let buffer = XLogInitBufferForRedo(record, 0)?;
    // ptr = XLogRecGetData(record) + sizeof(ginxlogCreatePostingTree);
    let payload = data[SIZE_OF_GINXLOG_CREATE_POSTING_TREE..].to_vec();

    with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        GinInitBuffer(page, (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) as u32)?;

        // Place page data: memcpy(GinDataLeafPageGetPostingList(page), ptr, size).
        let off = gdp::gin_data_page_data_offset();
        page[off..off + size].copy_from_slice(&payload[..size]);

        gdp::GinDataPageSetDataSize(page, size);
        page_set_lsn(page, lsn)?;
        Ok(())
    })?;

    mark_buffer_dirty::call(buffer);
    unlock_release_buffer::call(buffer);
    Ok(())
}

// ===========================================================================
// ginRedoInsertEntry (ginxlog.c:70)
// ===========================================================================

/// `offsetof(ginxlogInsertEntry, tuple)` (ginxlog.h): an `OffsetNumber offset`
/// (2 bytes), a `bool isDelete` (1 byte), and 1 pad byte before the IndexTuple.
const OFFSET_OF_GINXLOG_INSERT_ENTRY_TUPLE: usize = 4;

/// `ginRedoInsertEntry(buffer, isLeaf, rightblkno, rdata)` (ginxlog.c:70):
/// replay insertion of one tuple onto an entry-tree page. `rdata` is the
/// per-block payload (`ginxlogInsertEntry`).
fn ginRedoInsertEntry(
    record: &XLogReaderState<'_>,
    buffer: Buffer,
    _is_leaf: bool,
    rightblkno: BlockNumber,
    rdata: &[u8],
) -> PgResult<()> {
    // ginxlogInsertEntry { OffsetNumber offset; bool isDelete; IndexTupleData tuple; }
    let offset: OffsetNumber = u16::from_ne_bytes([rdata[0], rdata[1]]);
    let is_delete = rdata[2] != 0;
    let tuple_all = rdata[OFFSET_OF_GINXLOG_INSERT_ENTRY_TUPLE..].to_vec();
    let tuple_size = index_tuple_size(&tuple_all);
    let tuple = tuple_all[..tuple_size].to_vec();

    // For the error message: BufferGetTag(buffer) is not directly available, so
    // read the block tag off the record (block_id 0 for the inserted-into page).
    let tag = xlog_rec_get_block_tag_extended::call(record, 0)?;
    let (spc_oid, db_oid, rel_number) = match tag {
        Some(t) => (t.rlocator.spcOid, t.rlocator.dbOid, t.rlocator.relNumber),
        None => (0, 0, 0),
    };

    with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        if rightblkno != InvalidBlockNumber {
            // update link to right page after split
            // Assert(!GinPageIsLeaf(page));
            let (item_off, item_len, mut itup) = {
                let pr = PageRef::new(page)?;
                let iid = PageGetItemId(&pr, offset)?;
                let itup = PageGetItem(&pr, &iid)?.to_vec();
                (iid.lp_off() as usize, iid.lp_len() as usize, itup)
            };
            gin_set_downlink(&mut itup, rightblkno);
            // Write the patched downlink back into the page item.
            page[item_off..item_off + item_len].copy_from_slice(&itup[..item_len]);
        }

        if is_delete {
            // Assert(GinPageIsLeaf(page));
            let mut pm = PageMut::new(page)?;
            PageIndexTupleDelete(&mut pm, offset)?;
        }

        // itup = &data->tuple;
        let added = {
            let mut pm = PageMut::new(page)?;
            PageAddItemExtended(&mut pm, &tuple, offset, 0)?
        };
        if added == INVALID_OFFSET_NUMBER {
            return Err(PgError::new(
                PANIC,
                format!(
                    "failed to add item to index page in {spc_oid}/{db_oid}/{rel_number}"
                ),
            ));
        }
        Ok(())
    })
}

// ===========================================================================
// ginRedoRecompress (ginxlog.c:116)
// ===========================================================================

/// `ginRedoRecompress(page, data)` (ginxlog.c:116): redo recompression of a
/// posting list. `data` is the `ginxlogRecompressDataLeaf` payload (a `uint16
/// nactions` then the per-action stream).
///
/// Doing all the changes in-place is not always possible, because it might
/// require more space than there is on the page. We snapshot the live posting
/// list region up front (`page_snapshot`) — this is always the source of
/// segment reads — and write directly to the page, exactly reproducing the C
/// `tailCopy` semantics while remaining byte-identical.
fn ginRedoRecompress(page: &mut [u8], data: &[u8]) -> PgResult<()> {
    // ginxlogRecompressDataLeaf { uint16 nactions; }
    let nactions = u16::from_ne_bytes([data[0], data[1]]) as i32;

    // PageGetSpecialPointer(page): the upper bound the writes may not exceed.
    let special = special_pointer_offset(page);

    // If the page is in pre-9.4 format, convert to new format first.
    if !gdp::GinPageIsCompressed(page) {
        let nuncompressed = gdp::gin_page_get_maxoff(page) as i32;
        let totalsize;

        if nuncompressed > 0 {
            // Read the uncompressed ItemPointer array from GinDataPageGetData.
            let dataoff = gdp::gin_data_page_data_offset();
            let mut uncompressed: Vec<ItemPointerData> = Vec::new();
            uncompressed
                .try_reserve(nuncompressed as usize)
                .expect("OOM");
            for i in 0..nuncompressed as usize {
                let o = dataoff + i * SIZE_OF_ITEM_POINTER;
                uncompressed.push(gdp::read_item_pointer(&page[o..]));
            }
            let mut npacked = 0;
            let plist = ginCompressPostingList(
                &uncompressed,
                nuncompressed,
                BLCKSZ as i32,
                Some(&mut npacked),
            );
            let ts = plist.bytes.len();
            // Assert(npacked == nuncompressed);
            if npacked != nuncompressed {
                return Err(PgError::new(
                    PANIC,
                    format!("ginRedoRecompress: npacked != nuncompressed"),
                ));
            }
            // memcpy(GinDataLeafPageGetPostingList(page), plist, totalsize).
            page[dataoff..dataoff + ts].copy_from_slice(&plist.bytes);
            totalsize = ts;
        } else {
            totalsize = 0;
        }

        gdp::GinDataPageSetDataSize(page, totalsize);
        gdp::GinPageSetCompressed(page);
        gdp::gin_page_set_maxoff(page, INVALID_OFFSET_NUMBER);
    }

    // The on-disk posting-list region begins at GinDataLeafPageGetPostingList.
    let posting_list_off = gdp::gin_data_page_data_offset();
    let initial_list_size = gdp::GinDataLeafPageGetPostingListSize(page);

    // Snapshot the live posting-list region once, up front; this is always the
    // source of segment reads (equivalent to the C `tailCopy`, but simpler).
    let mut page_snapshot: Vec<u8> = Vec::new();
    page_snapshot.try_reserve(initial_list_size).expect("OOM");
    page_snapshot.extend_from_slice(&page[posting_list_off..posting_list_off + initial_list_size]);

    let mut write_off = posting_list_off; // writePtr, as a page offset
    let mut read_pos: usize = 0; // (Pointer) oldseg, relative to posting_list_off
    let segment_end: usize = initial_list_size; // segmentend, relative to posting_list_off
    let mut segno: i32 = 0;

    // The WAL action stream begins right after the uint16 nactions field.
    let mut walpos: usize = 2;

    for _actionno in 0..nactions {
        let a_segno = data[walpos];
        walpos += 1;
        let mut a_action = data[walpos];
        walpos += 1;

        let mut newseg: Vec<u8> = Vec::new();
        let mut items: Vec<ItemPointerData> = Vec::new();
        let mut nitems: u16 = 0;

        // Extract all the information we need from the WAL record.
        if a_action == GIN_SEGMENT_INSERT || a_action == GIN_SEGMENT_REPLACE {
            let newsegsize = gdp::size_of_gin_posting_list(&data[walpos..]);
            newseg = data[walpos..walpos + newsegsize].to_vec();
            walpos += shortalign(newsegsize);
        }

        if a_action == GIN_SEGMENT_ADDITEMS {
            nitems = u16::from_ne_bytes([data[walpos], data[walpos + 1]]);
            walpos += 2;
            items.try_reserve(nitems as usize).expect("OOM");
            for _ in 0..nitems as usize {
                items.push(gdp::read_item_pointer(&data[walpos..]));
                walpos += SIZE_OF_ITEM_POINTER;
            }
        }

        // Skip to the segment that this action concerns.
        // Assert(segno <= a_segno);
        while segno < a_segno as i32 {
            let segsize = gdp::size_of_gin_posting_list(&page_snapshot[read_pos..]);
            // Assert(writePtr + segsize < PageGetSpecialPointer(page));
            if write_off + segsize > special {
                return Err(PgError::new(
                    PANIC,
                    format!("ginRedoRecompress: overflow copying segment"),
                ));
            }
            page[write_off..write_off + segsize]
                .copy_from_slice(&page_snapshot[read_pos..read_pos + segsize]);
            write_off += segsize;
            read_pos += segsize;
            segno += 1;
        }

        // ADDITEMS is handled like REPLACE, but the new segment is reconstructed
        // using the old segment from disk plus the new items from the WAL record.
        if a_action == GIN_SEGMENT_ADDITEMS {
            let mut nolditems = 0;
            let olditems = ginPostingListDecode(&page_snapshot[read_pos..], Some(&mut nolditems));

            let mut nnewitems = 0;
            let newitems = ginMergeItemPointers(
                &items,
                nitems as u32,
                &olditems,
                nolditems as u32,
                &mut nnewitems,
            );
            // Assert(nnewitems == nolditems + nitems);
            let mut npacked = 0;
            let compressed =
                ginCompressPostingList(&newitems, nnewitems, BLCKSZ as i32, Some(&mut npacked));
            // Assert(npacked == nnewitems);
            newseg = compressed.bytes;
            a_action = GIN_SEGMENT_REPLACE;
        }

        // segptr = (Pointer) oldseg; either at a real segment or at segmentend.
        let segsize;
        if read_pos != segment_end {
            segsize = gdp::size_of_gin_posting_list(&page_snapshot[read_pos..]);
        } else {
            // Positioned after the last existing segment. Only INSERTs expected.
            // Assert(a_action == GIN_SEGMENT_INSERT);
            segsize = 0;
        }

        let newsegsize = newseg.len();
        match a_action {
            GIN_SEGMENT_DELETE => {
                read_pos += segsize;
                segno += 1;
            }
            GIN_SEGMENT_INSERT => {
                // copy the new segment in place
                // Assert(writePtr + newsegsize <= PageGetSpecialPointer(page));
                if write_off + newsegsize > special {
                    return Err(PgError::new(PANIC, format!("ginRedoRecompress: insert overflow")));
                }
                page[write_off..write_off + newsegsize].copy_from_slice(&newseg);
                write_off += newsegsize;
            }
            GIN_SEGMENT_REPLACE => {
                // copy the new version of the segment in place
                // Assert(writePtr + newsegsize <= PageGetSpecialPointer(page));
                if write_off + newsegsize > special {
                    return Err(PgError::new(
                        PANIC,
                        format!("ginRedoRecompress: replace overflow"),
                    ));
                }
                page[write_off..write_off + newsegsize].copy_from_slice(&newseg);
                write_off += newsegsize;
                read_pos += segsize;
                segno += 1;
            }
            _ => {
                return Err(PgError::new(
                    PANIC,
                    format!("unexpected GIN leaf action: {a_action}"),
                ));
            }
        }
    }

    // Copy the rest of the unmodified segments, if any.
    if read_pos != segment_end {
        let rest_size = segment_end - read_pos;
        // Assert(writePtr + restSize <= PageGetSpecialPointer(page));
        if write_off + rest_size > special {
            return Err(PgError::new(PANIC, format!("ginRedoRecompress: tail overflow")));
        }
        page[write_off..write_off + rest_size]
            .copy_from_slice(&page_snapshot[read_pos..read_pos + rest_size]);
        write_off += rest_size;
    }

    let totalsize = write_off - posting_list_off;
    gdp::GinDataPageSetDataSize(page, totalsize);
    Ok(())
}

// ===========================================================================
// ginRedoInsertData (ginxlog.c:318)
// ===========================================================================

/// `offsetof(ginxlogInsertDataInternal, newitem)` (ginxlog.h): `OffsetNumber
/// offset` (2) before the PostingItem `newitem`. `PostingItem` (BlockIdData +
/// ItemPointerData) is only 2-byte aligned, so there is no padding.
const OFFSET_OF_GINXLOG_INSERT_DATA_INTERNAL_NEWITEM: usize = 2;

/// `ginRedoInsertData(buffer, isLeaf, rightblkno, rdata)` (ginxlog.c:318).
fn ginRedoInsertData(
    buffer: Buffer,
    is_leaf: bool,
    rightblkno: BlockNumber,
    rdata: &[u8],
) -> PgResult<()> {
    if is_leaf {
        // ginxlogRecompressDataLeaf
        let data = rdata.to_vec();
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            // Assert(GinPageIsLeaf(page));
            ginRedoRecompress(page, &data)
        })
    } else {
        // ginxlogInsertDataInternal { OffsetNumber offset; PostingItem newitem; }
        let offset: OffsetNumber = u16::from_ne_bytes([rdata[0], rdata[1]]);
        let newitem =
            gdp::read_posting_item(&rdata[OFFSET_OF_GINXLOG_INSERT_DATA_INTERNAL_NEWITEM..]);
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            // Assert(!GinPageIsLeaf(page));
            // update link to right page after split
            let mut oldpitem = gdp::GinDataPageGetPostingItem(page, offset);
            gdp::PostingItemSetBlockNumber(&mut oldpitem, rightblkno);
            gdp::GinDataPageSetPostingItem(page, offset, &oldpitem);

            GinDataPageAddPostingItem(page, &newitem, offset);
            Ok(())
        })
    }
}

// ===========================================================================
// ginRedoInsert (ginxlog.c:346)
// ===========================================================================

/// `sizeof(ginxlogInsert)` (ginxlog.h:37-55): the struct is `{ uint16 flags; }`
/// with no trailing padding; the optional non-leaf payload (two `BlockIdData`)
/// begins at offset 2.
const SIZE_OF_GINXLOG_INSERT: usize = 2;

/// `ginRedoInsert(record)` (ginxlog.c:346).
fn ginRedoInsert(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    // ginxlogInsert { uint16 flags; ... }
    let flags = u16::from_ne_bytes([data[0], data[1]]);
    let mut right_child_blkno = InvalidBlockNumber;
    let is_leaf = (flags & GIN_INSERT_ISLEAF) != 0;

    // First clear incomplete-split flag on child page if this finishes a split.
    if !is_leaf {
        // payload = XLogRecGetData(record) + sizeof(ginxlogInsert);
        // leftChildBlkno = BlockIdGetBlockNumber((BlockId) payload);  [NOT_USED]
        // payload += sizeof(BlockIdData);
        let mut payload = SIZE_OF_GINXLOG_INSERT;
        payload += SIZE_OF_BLOCK_ID_DATA;
        // rightChildBlkno = BlockIdGetBlockNumber((BlockId) payload);
        right_child_blkno = block_id_get_block_number(&data[payload..]);

        ginRedoClearIncompleteSplit(record, 1)?;
    }

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let payload = record_get_block_data(record, 0).to_vec();
        // How to insert the payload is tree-type specific.
        if flags & GIN_INSERT_ISDATA != 0 {
            // Assert(GinPageIsData(page));
            ginRedoInsertData(buffer, is_leaf, right_child_blkno, &payload)?;
        } else {
            // Assert(!GinPageIsData(page));
            ginRedoInsertEntry(record, buffer, is_leaf, right_child_blkno, &payload)?;
        }
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| page_set_lsn(page, lsn))?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// ginRedoSplit (ginxlog.c:401)
// ===========================================================================

/// `ginRedoSplit(record)` (ginxlog.c:401).
fn ginRedoSplit(record: &XLogReaderState<'_>) -> PgResult<()> {
    let data = record_get_data(record);
    // ginxlogSplit { ... uint16 flags; ... } -- flags is the only field we read.
    let flags = read_split_flags(data);
    let is_leaf = (flags & GIN_INSERT_ISLEAF) != 0;
    let is_root = (flags & GIN_SPLIT_ROOT) != 0;

    // First clear incomplete-split flag on child page if this finishes a split.
    if !is_leaf {
        ginRedoClearIncompleteSplit(record, 3)?;
    }

    let (laction, lbuffer) = XLogReadBufferForRedo(record, 0)?;
    if laction != XLogRedoAction::BlkRestored {
        return Err(PgError::new(
            PANIC,
            format!("GIN split record did not contain a full-page image of left page"),
        ));
    }

    let (raction, rbuffer) = XLogReadBufferForRedo(record, 1)?;
    if raction != XLogRedoAction::BlkRestored {
        return Err(PgError::new(
            PANIC,
            format!("GIN split record did not contain a full-page image of right page"),
        ));
    }

    if is_root {
        let (rootaction, rootbuf) = XLogReadBufferForRedo(record, 2)?;
        if rootaction != XLogRedoAction::BlkRestored {
            return Err(PgError::new(
                PANIC,
                format!("GIN split record did not contain a full-page image of root page"),
            ));
        }
        unlock_release_buffer::call(rootbuf);
    }

    unlock_release_buffer::call(rbuffer);
    unlock_release_buffer::call(lbuffer);
    Ok(())
}

/// Read `ginxlogSplit.flags`. Layout (ginxlog.h): `BlockNumber rrlink` (4),
/// `BlockNumber leftChildBlkno` (4), `BlockNumber rightChildBlkno` (4),
/// `uint16 flags` (2). `flags` sits at byte offset 12.
fn read_split_flags(data: &[u8]) -> u16 {
    u16::from_ne_bytes([data[12], data[13]])
}

// ===========================================================================
// ginRedoVacuumPage (ginxlog.c:439)
// ===========================================================================

/// `ginRedoVacuumPage(record)` (ginxlog.c:439): a VACUUM_PAGE record contains
/// simply a full image of the page, similar to an XLOG_FPI record.
fn ginRedoVacuumPage(record: &XLogReaderState<'_>) -> PgResult<()> {
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action != XLogRedoAction::BlkRestored {
        return Err(PgError::new(
            PANIC,
            format!("replay of gin entry tree page vacuum did not restore the page"),
        ));
    }
    unlock_release_buffer::call(buffer);
    Ok(())
}

// ===========================================================================
// ginRedoVacuumDataLeafPage (ginxlog.c:451)
// ===========================================================================

/// `ginRedoVacuumDataLeafPage(record)` (ginxlog.c:451).
fn ginRedoVacuumDataLeafPage(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        // ginxlogVacuumDataLeafPage { ginxlogRecompressDataLeaf data; } -- the
        // recompress payload is the whole block data.
        let xlrec = record_get_block_data(record, 0).to_vec();
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            // Assert(GinPageIsLeaf(page) && GinPageIsData(page));
            ginRedoRecompress(page, &xlrec)?;
            page_set_lsn(page, lsn)
        })?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// ginRedoDeletePage (ginxlog.c:476)
// ===========================================================================

/// `ginRedoDeletePage(record)` (ginxlog.c:476).
fn ginRedoDeletePage(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    // ginxlogDeletePage { OffsetNumber parentOffset; BlockNumber rightLink;
    //                     TransactionId deleteXid; }
    let parent_offset: OffsetNumber = u16::from_ne_bytes([data[0], data[1]]);
    // (1 pad word so rightLink/deleteXid are 4-aligned; rightLink at offset 4.)
    let right_link = u32::from_ne_bytes([data[4], data[5], data[6], data[7]]) as BlockNumber;
    let delete_xid = u32::from_ne_bytes([data[8], data[9], data[10], data[11]]);

    // Lock left page first to prevent possible deadlock with ginStepRight().
    let (laction, lbuffer) = XLogReadBufferForRedo(record, 2)?;
    if laction == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(lbuffer, &mut |page: &mut [u8]| {
            // Assert(GinPageIsData(page));
            gdp::gin_page_set_rightlink(page, right_link);
            page_set_lsn(page, lsn)
        })?;
        mark_buffer_dirty::call(lbuffer);
    }

    let (daction, dbuffer) = XLogReadBufferForRedo(record, 0)?;
    if daction == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(dbuffer, &mut |page: &mut [u8]| {
            // Assert(GinPageIsData(page));
            // GinPageSetDeleted(page): flags |= GIN_DELETED.
            let f = gdp::gin_page_get_flags(page);
            gdp::gin_page_set_flags(page, f | GIN_DELETED);
            // GinPageSetDeleteXid(page, data->deleteXid): pd_prune_xid = xid.
            write_pd_prune_xid(page, delete_xid);
            page_set_lsn(page, lsn)
        })?;
        mark_buffer_dirty::call(dbuffer);
    }

    let (paction, pbuffer) = XLogReadBufferForRedo(record, 1)?;
    if paction == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(pbuffer, &mut |page: &mut [u8]| {
            // Assert(GinPageIsData(page) && !GinPageIsLeaf(page));
            gdp::GinPageDeletePostingItem(page, parent_offset);
            page_set_lsn(page, lsn)
        })?;
        mark_buffer_dirty::call(pbuffer);
    }

    if BufferIsValid(lbuffer) {
        unlock_release_buffer::call(lbuffer);
    }
    if BufferIsValid(pbuffer) {
        unlock_release_buffer::call(pbuffer);
    }
    if BufferIsValid(dbuffer) {
        unlock_release_buffer::call(dbuffer);
    }
    Ok(())
}

// ===========================================================================
// ginRedoUpdateMetapage (ginxlog.c:527)
// ===========================================================================

/// `ginRedoUpdateMetapage(record)` (ginxlog.c:527).
fn ginRedoUpdateMetapage(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    // ginxlogUpdateMeta { RelFileLocator locator; GinMetaPageData metadata;
    //                     BlockNumber prevTail; BlockNumber newRightlink;
    //                     int32 ntuples; }
    let meta = parse_update_meta(data);

    // Restore the metapage unconditionally (torn-page hazard), like a full-page
    // image.
    let metabuffer = XLogInitBufferForRedo(record, 0)?;
    // Assert(BufferGetBlockNumber(metabuffer) == GIN_METAPAGE_BLKNO);
    debug_assert!(buffer_get_block_number::call(metabuffer) == GIN_METAPAGE_BLKNO);
    let metadata = meta.metadata.clone();
    with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
        GinInitMetabuffer(page, BLCKSZ)?;
        // memcpy(GinPageGetMeta(metapage), &data->metadata, sizeof(...)).
        write_meta(page, &metadata);
        page_set_lsn(page, lsn)
    })?;
    mark_buffer_dirty::call(metabuffer);

    if meta.ntuples > 0 {
        // insert into tail page
        let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            let payload = record_get_block_data(record, 1).to_vec();
            let ntuples = meta.ntuples;
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                // off = PageIsEmpty(page) ? FirstOffsetNumber
                //                         : OffsetNumberNext(PageGetMaxOffsetNumber(page));
                let mut off = {
                    let pr = PageRef::new(page)?;
                    if PageIsEmpty(&pr) {
                        FIRST_OFFSET_NUMBER
                    } else {
                        offset_number_next(PageGetMaxOffsetNumber(&pr))
                    }
                };
                // Walk the concatenated tuples and add each.
                let total = payload.len();
                let mut pos = 0usize;
                for _ in 0..ntuples {
                    let tupsize = index_tuple_size(&payload[pos..]);
                    let tuple = payload[pos..pos + tupsize].to_vec();
                    let added = {
                        let mut pm = PageMut::new(page)?;
                        PageAddItemExtended(&mut pm, &tuple, off, 0)?
                    };
                    if added == INVALID_OFFSET_NUMBER {
                        return Err(PgError::new(
                            PANIC,
                            format!("failed to add item to index page"),
                        ));
                    }
                    pos += tupsize;
                    off += 1;
                }
                // Assert(payload + totaltupsize == (char *) tuples).
                if pos != total {
                    return Err(PgError::new(
                        PANIC,
                        format!("ginRedoUpdateMetapage: tuple stream size mismatch"),
                    ));
                }
                // Increase counter of heap tuples: GinPageGetOpaque(page)->maxoff++.
                let m = gdp::gin_page_get_maxoff(page);
                gdp::gin_page_set_maxoff(page, m + 1);
                page_set_lsn(page, lsn)
            })?;
            mark_buffer_dirty::call(buffer);
        }
        if BufferIsValid(buffer) {
            unlock_release_buffer::call(buffer);
        }
    } else if meta.prev_tail != InvalidBlockNumber {
        // New tail.
        let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            let new_rightlink = meta.new_rightlink;
            with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
                gdp::gin_page_set_rightlink(page, new_rightlink);
                page_set_lsn(page, lsn)
            })?;
            mark_buffer_dirty::call(buffer);
        }
        if BufferIsValid(buffer) {
            unlock_release_buffer::call(buffer);
        }
    }

    unlock_release_buffer::call(metabuffer);
    Ok(())
}

/// Deserialized `ginxlogUpdateMeta` fields the redo logic uses.
struct UpdateMeta {
    /// `data->metadata`.
    metadata: GinMetaPageData,
    /// `data->prevTail`.
    prev_tail: BlockNumber,
    /// `data->newRightlink`.
    new_rightlink: BlockNumber,
    /// `data->ntuples`.
    ntuples: i32,
}

/// `sizeof(RelFileLocator)` (relfilelocator.h): `Oid spcOid; Oid dbOid;
/// RelFileNumber relNumber;` == 12.
const SIZE_OF_REL_FILE_LOCATOR: usize = 12;

fn parse_update_meta(data: &[u8]) -> UpdateMeta {
    // ginxlogUpdateMeta { RelFileLocator locator; GinMetaPageData metadata;
    //                     BlockNumber prevTail; BlockNumber newRightlink;
    //                     int32 ntuples; }
    let meta_off = SIZE_OF_REL_FILE_LOCATOR;
    let metadata = parse_gin_meta(&data[meta_off..]);
    let after_meta = meta_off + SIZE_OF_GIN_META_PAGE_DATA;
    let prev_tail = u32::from_ne_bytes([
        data[after_meta],
        data[after_meta + 1],
        data[after_meta + 2],
        data[after_meta + 3],
    ]);
    let nr = after_meta + 4;
    let new_rightlink = u32::from_ne_bytes([data[nr], data[nr + 1], data[nr + 2], data[nr + 3]]);
    let nt = nr + 4;
    let ntuples = i32::from_ne_bytes([data[nt], data[nt + 1], data[nt + 2], data[nt + 3]]);
    UpdateMeta {
        metadata,
        prev_tail,
        new_rightlink,
        ntuples,
    }
}

// ===========================================================================
// GinMetaPageData on-disk byte (de)serialization.
//
// Field byte offsets within `GinMetaPageData` (matching the C struct layout
// with natural alignment, LP64 — identical to the layout `ginutil`'s metapage
// writer uses).
// ===========================================================================

const OFF_GIN_HEAD: usize = 0; // uint32 (head)
const OFF_GIN_TAIL: usize = 4; // uint32 (tail)
const OFF_GIN_TAILFREESIZE: usize = 8; // uint32
const OFF_GIN_NPENDINGPAGES: usize = 12; // uint32
const OFF_GIN_NPENDINGHEAPTUPLES: usize = 16; // int64 (16 is already 8-aligned)
const OFF_GIN_NTOTALPAGES: usize = 24; // uint32
const OFF_GIN_NENTRYPAGES: usize = 28; // uint32
const OFF_GIN_NDATAPAGES: usize = 32; // uint32
                                      // 4 bytes of pad (36..40) before the 8-aligned int64
const OFF_GIN_NENTRIES: usize = 40; // int64
const OFF_GIN_VERSION: usize = 48; // int32

/// Decode a `GinMetaPageData` from its raw on-disk bytes.
fn parse_gin_meta(src: &[u8]) -> GinMetaPageData {
    let get_u32 = |o: usize| u32::from_ne_bytes([src[o], src[o + 1], src[o + 2], src[o + 3]]);
    let get_i64 = |o: usize| {
        i64::from_ne_bytes([
            src[o],
            src[o + 1],
            src[o + 2],
            src[o + 3],
            src[o + 4],
            src[o + 5],
            src[o + 6],
            src[o + 7],
        ])
    };
    let get_i32 = |o: usize| i32::from_ne_bytes([src[o], src[o + 1], src[o + 2], src[o + 3]]);
    GinMetaPageData {
        head: get_u32(OFF_GIN_HEAD),
        tail: get_u32(OFF_GIN_TAIL),
        tailFreeSize: get_u32(OFF_GIN_TAILFREESIZE),
        nPendingPages: get_u32(OFF_GIN_NPENDINGPAGES),
        nPendingHeapTuples: get_i64(OFF_GIN_NPENDINGHEAPTUPLES),
        nTotalPages: get_u32(OFF_GIN_NTOTALPAGES),
        nEntryPages: get_u32(OFF_GIN_NENTRYPAGES),
        nDataPages: get_u32(OFF_GIN_NDATAPAGES),
        nEntries: get_i64(OFF_GIN_NENTRIES),
        ginVersion: get_i32(OFF_GIN_VERSION),
    }
}

/// `memcpy(GinPageGetMeta(metapage), &metadata, sizeof(GinMetaPageData))`: write
/// the metadata into the page contents (`PageGetContents`), each field at its
/// exact on-disk byte offset so the image is byte-identical to the C struct.
fn write_meta(page: &mut [u8], meta: &GinMetaPageData) {
    let off = gdp::page_contents_offset();
    let put_u32 = |page: &mut [u8], field_off: usize, val: u32| {
        let p = off + field_off;
        page[p..p + 4].copy_from_slice(&val.to_ne_bytes());
    };
    let put_i64 = |page: &mut [u8], field_off: usize, val: i64| {
        let p = off + field_off;
        page[p..p + 8].copy_from_slice(&val.to_ne_bytes());
    };
    let put_i32 = |page: &mut [u8], field_off: usize, val: i32| {
        let p = off + field_off;
        page[p..p + 4].copy_from_slice(&val.to_ne_bytes());
    };
    put_u32(page, OFF_GIN_HEAD, meta.head);
    put_u32(page, OFF_GIN_TAIL, meta.tail);
    put_u32(page, OFF_GIN_TAILFREESIZE, meta.tailFreeSize);
    put_u32(page, OFF_GIN_NPENDINGPAGES, meta.nPendingPages);
    put_i64(page, OFF_GIN_NPENDINGHEAPTUPLES, meta.nPendingHeapTuples);
    put_u32(page, OFF_GIN_NTOTALPAGES, meta.nTotalPages);
    put_u32(page, OFF_GIN_NENTRYPAGES, meta.nEntryPages);
    put_u32(page, OFF_GIN_NDATAPAGES, meta.nDataPages);
    put_i64(page, OFF_GIN_NENTRIES, meta.nEntries);
    put_i32(page, OFF_GIN_VERSION, meta.ginVersion);
}

// ===========================================================================
// ginRedoInsertListPage (ginxlog.c:619)
// ===========================================================================

/// `ginRedoInsertListPage(record)` (ginxlog.c:619).
fn ginRedoInsertListPage(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    // ginxlogInsertListPage { BlockNumber rightlink; int32 ntuples; }
    let rightlink = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]) as BlockNumber;
    let ntuples = i32::from_ne_bytes([data[4], data[5], data[6], data[7]]);

    // We always re-initialize the page.
    let buffer = XLogInitBufferForRedo(record, 0)?;
    let payload = record_get_block_data(record, 0).to_vec();

    with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        GinInitBuffer(page, GIN_LIST as u32)?;
        gdp::gin_page_set_rightlink(page, rightlink);
        if rightlink == InvalidBlockNumber {
            // tail of sublist
            // GinPageSetFullRow(page): flags |= GIN_LIST_FULLROW.
            let f = gdp::gin_page_get_flags(page);
            gdp::gin_page_set_flags(page, f | GIN_LIST_FULLROW);
            gdp::gin_page_set_maxoff(page, 1);
        } else {
            gdp::gin_page_set_maxoff(page, 0);
        }

        let total = payload.len();
        let mut pos = 0usize;
        let mut off = FIRST_OFFSET_NUMBER;
        for _ in 0..ntuples {
            let tupsize = index_tuple_size(&payload[pos..]);
            let tuple = payload[pos..pos + tupsize].to_vec();
            let l = {
                let mut pm = PageMut::new(page)?;
                PageAddItemExtended(&mut pm, &tuple, off, 0)?
            };
            if l == INVALID_OFFSET_NUMBER {
                return Err(PgError::new(PANIC, format!("failed to add item to index page")));
            }
            pos += tupsize;
            off += 1;
        }
        // Assert((char *) tuples == payload + totaltupsize).
        if pos != total {
            return Err(PgError::new(
                PANIC,
                format!("ginRedoInsertListPage: tuple stream size mismatch"),
            ));
        }
        page_set_lsn(page, lsn)
    })?;

    mark_buffer_dirty::call(buffer);
    unlock_release_buffer::call(buffer);
    Ok(())
}

// ===========================================================================
// ginRedoDeleteListPages (ginxlog.c:674)
// ===========================================================================

/// `ginRedoDeleteListPages(record)` (ginxlog.c:674).
fn ginRedoDeleteListPages(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    // ginxlogDeleteListPages { GinMetaPageData metadata; int32 ndeleted; }
    let metadata = parse_gin_meta(data);
    let nd_off = SIZE_OF_GIN_META_PAGE_DATA;
    let ndeleted = i32::from_ne_bytes([
        data[nd_off],
        data[nd_off + 1],
        data[nd_off + 2],
        data[nd_off + 3],
    ]);

    let metabuffer = XLogInitBufferForRedo(record, 0)?;
    // Assert(BufferGetBlockNumber(metabuffer) == GIN_METAPAGE_BLKNO);
    debug_assert!(buffer_get_block_number::call(metabuffer) == GIN_METAPAGE_BLKNO);
    with_buffer_page::call(metabuffer, &mut |page: &mut [u8]| {
        GinInitMetabuffer(page, BLCKSZ)?;
        write_meta(page, &metadata);
        page_set_lsn(page, lsn)
    })?;
    mark_buffer_dirty::call(metabuffer);

    // During replay we re-initialize the deleted pages as empty, deleted pages,
    // one at a time. No full-page images are taken; right-links need not be
    // preserved (new readers can't see the pages, see the C comment).
    for i in 0..ndeleted {
        let buffer = XLogInitBufferForRedo(record, (i + 1) as u8)?;
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            GinInitBuffer(page, GIN_DELETED as u32)?;
            page_set_lsn(page, lsn)
        })?;
        mark_buffer_dirty::call(buffer);
        unlock_release_buffer::call(buffer);
    }
    unlock_release_buffer::call(metabuffer);
    Ok(())
}

// ===========================================================================
// gin_redo (ginxlog.c:725)
// ===========================================================================

/// `gin_redo(record)` (ginxlog.c:725): dispatch a GIN WAL record to its redo
/// handler (`rm_redo` slot).
///
/// GIN indexes do not require any conflict processing.
pub fn gin_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !XLR_INFO_MASK;

    // oldCtx = MemoryContextSwitchTo(opCtx); ... MemoryContextReset(opCtx);
    let result = (|| match info {
        XLOG_GIN_CREATE_PTREE => ginRedoCreatePTree(record),
        XLOG_GIN_INSERT => ginRedoInsert(record),
        XLOG_GIN_SPLIT => ginRedoSplit(record),
        XLOG_GIN_VACUUM_PAGE => ginRedoVacuumPage(record),
        XLOG_GIN_VACUUM_DATA_LEAF_PAGE => ginRedoVacuumDataLeafPage(record),
        XLOG_GIN_DELETE_PAGE => ginRedoDeletePage(record),
        XLOG_GIN_UPDATE_META_PAGE => ginRedoUpdateMetapage(record),
        XLOG_GIN_INSERT_LISTPAGE => ginRedoInsertListPage(record),
        XLOG_GIN_DELETE_LISTPAGE => ginRedoDeleteListPages(record),
        // C: elog(PANIC, "gin_redo: unknown op code %u", info) -- fail-stop.
        _ => Err(PgError::new(
            PANIC,
            format!("gin_redo: unknown op code {info}"),
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
// gin_xlog_startup / gin_xlog_cleanup (ginxlog.c:774/782)
// ===========================================================================

/// `gin_xlog_startup()` (ginxlog.c:774): create the GIN replay temporary
/// context (`rm_startup` slot). `parent` is the recovery `CurrentMemoryContext`
/// the rmgr passes; the new context is a regular AllocSet under it.
pub fn gin_xlog_startup(_parent: Mcx<'_>) -> PgResult<()> {
    OP_CTX.with(|c| {
        *c.borrow_mut() = Some(MemoryContext::new("GIN recovery temporary context"));
    });
    Ok(())
}

/// `gin_xlog_cleanup()` (ginxlog.c:782): delete the GIN replay temporary
/// context (`rm_cleanup` slot).
pub fn gin_xlog_cleanup() {
    OP_CTX.with(|c| {
        *c.borrow_mut() = None;
    });
}

// ===========================================================================
// gin_mask (ginxlog.c:792)
// ===========================================================================

/// `gin_mask(pagedata, blkno)` (ginxlog.c:792): mask a GIN page before running
/// consistency checks on it (`rm_mask` slot).
pub fn gin_mask(page: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    mask_page_lsn_and_checksum::call(page);

    // opaque = GinPageGetOpaque(page); read flags before masking hint bits.
    let flags = gdp::gin_page_get_flags(page);

    mask_page_hint_bits::call(page);

    // For a GIN_DELETED page, the page is initialized to empty, so mask the
    // whole content. For other pages, mask the hole if pd_lower appears to have
    // been set correctly.
    if flags & GIN_DELETED != 0 {
        mask_page_content::call(page);
    } else {
        let pd_lower = read_pd_lower(page);
        if pd_lower > SizeOfPageHeaderData {
            mask_unused_space::call(page)?;
        }
    }
    Ok(())
}

// ===========================================================================
// Helpers.
// ===========================================================================

/// `PageSetLSN(page, lsn)` against the page bytes.
fn page_set_lsn(page: &mut [u8], lsn: u64) -> PgResult<()> {
    let mut pm = PageMut::new(page)?;
    PageSetLSN(&mut pm, lsn);
    Ok(())
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the rmgr-table callbacks this unit owns
/// (`gin_redo` / `gin_xlog_startup` / `gin_xlog_cleanup` / `gin_mask`).
pub fn init_seams() {
    gin_core_seams::gin_redo::set(gin_redo);
    gin_core_seams::gin_xlog_startup::set(gin_xlog_startup);
    gin_core_seams::gin_xlog_cleanup::set(gin_xlog_cleanup);
    gin_core_seams::gin_mask::set(gin_mask);
}

// Compile-time cross-checks of the record-header byte layouts (LP64).
const _: () = {
    assert!(SIZE_OF_GIN_META_PAGE_DATA == 56);
    assert!(SIZE_OF_ITEM_POINTER == 6);
    // Used by ginRedoRecompress for the pre-9.4 maxalign data origin.
    assert!(maxalign(SizeOfPageHeaderData).is_multiple_of(8));
};
