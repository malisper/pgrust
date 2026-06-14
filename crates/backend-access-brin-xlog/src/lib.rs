//! `backend-access-brin-xlog` — an owned-tree Rust port of
//! `src/backend/access/brin/brin_xlog.c` (PostgreSQL 18.3): the WAL redo
//! (`brin_redo`) and consistency-mask (`brin_mask`) resource-manager callbacks
//! for BRIN indexes.
//!
//! `brin_redo` dispatches a decoded WAL record to its per-op handler
//! (`brin_xlog_createidx` / `brin_xlog_insert` / `brin_xlog_update` /
//! `brin_xlog_samepage_update` / `brin_xlog_revmap_extend` /
//! `brin_xlog_desummarize_page`), reading the record through the xlogreader
//! value-typed accessors, fetching the redo buffers via xlogutils'
//! `XLogReadBufferForRedo` / `XLogInitBufferForRedo`, and applying the page
//! edits with the bufpage primitives (`PageInit`, `PageAddItemExtended`,
//! `PageIndexTupleDeleteNoCompact`, `PageIndexTupleOverwrite`).
//!
//! ## What is grounded in-crate vs. what is seamed
//!
//! There is no ported sibling `brin_pageops` / `brin_page` / `brin_revmap`
//! crate yet (`backend-access-brin-core` is still `todo`), so the few
//! BRIN-specific page-byte primitives the redo path needs —
//! `brin_metapage_init` / `brin_page_init` (brin_pageops.c),
//! `BrinPageType`/`BrinPageFlags`/`BrinMetaPageData` accessors (brin_page.h),
//! and the `brinSetHeapBlockItemptr` revmap item-pointer arithmetic
//! (brin_revmap.c) — are transcribed 1:1 here against the `BLCKSZ` page bytes,
//! exactly as the src-idiomatic port does. They move to the brin-core crate
//! once it lands.
//!
//! The genuinely-external WAL-recovery substrate crosses seams /
//! owner crates: `XLogReadBufferForRedo` / `XLogInitBufferForRedo`
//! (xlogutils), the `XLogReaderState` block-tag accessor (xlogreader), the
//! buffer manager (`with_buffer_page` / `BufferGetBlockNumber` /
//! `MarkBufferDirty` / `UnlockReleaseBuffer`), and the page-masking helpers
//! (bufmask). The decoded-record main data / per-block data / info byte are
//! read off `record.record` (the decoded payload owned by xlogreader).
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;

use backend_access_common_bufmask_seams::{
    mask_page_hint_bits, mask_page_lsn_and_checksum, mask_unused_space,
};
use backend_access_transam_xlogreader_seams::xlog_rec_get_block_tag_extended;
use backend_access_transam_xlogutils::{XLogInitBufferForRedo, XLogReadBufferForRedo};
use backend_storage_buffer_bufmgr_seams::{
    buffer_get_block_number, mark_buffer_dirty, unlock_release_buffer, with_buffer_page,
};
use backend_storage_page::{
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid, ItemPointerSet,
    ItemPointerSetInvalid, PageAddItemExtended, PageGetMaxOffsetNumber,
    PageIndexTupleDeleteNoCompact, PageIndexTupleOverwrite, PageInit, PageMut, PageRef, PageSetLSN,
};
use backend_utils_error::{ereport, PgError, PgResult};
use types_core::primitive::{BlockNumber, OffsetNumber, BLCKSZ};
use types_error::error::PANIC;
use types_storage::buf::{Buffer, BufferIsValid};
use types_storage::bufpage::{SizeOfPageHeaderData, PAI_OVERWRITE};
use types_tuple::heaptuple::{ItemPointerData, INVALID_OFFSET_NUMBER};
use types_wal::rmgr::XLogReaderState;
use types_wal::XLogRedoAction;
use types_core::XLogRecPtr;

// ===========================================================================
// brin_xlog.h opcodes (access/brin_xlog.h).
// ===========================================================================

/// `XLR_INFO_MASK` (xlogrecord.h) = `0x0F`: the low four bits of the info byte
/// (the WAL framework's per-record flags), masked OFF before the BRIN opcode
/// dispatch. The opcode lives in the high nibble.
const XLR_INFO_MASK: u8 = 0x0F;

const XLOG_BRIN_CREATE_INDEX: u8 = 0x00;
const XLOG_BRIN_INSERT: u8 = 0x10;
const XLOG_BRIN_UPDATE: u8 = 0x20;
const XLOG_BRIN_SAMEPAGE_UPDATE: u8 = 0x30;
const XLOG_BRIN_REVMAP_EXTEND: u8 = 0x40;
const XLOG_BRIN_DESUMMARIZE: u8 = 0x50;

/// `XLOG_BRIN_OPMASK` (brin_xlog.h) — isolates the opcode.
const XLOG_BRIN_OPMASK: u8 = 0x70;

/// `XLOG_BRIN_INIT_PAGE` (brin_xlog.h) — set when the page should be
/// re-initialized from scratch.
const XLOG_BRIN_INIT_PAGE: u8 = 0x80;

// ===========================================================================
// brin_page.h constants.
// ===========================================================================

/// `BRIN_PAGETYPE_META` (brin_page.h).
const BRIN_PAGETYPE_META: u16 = 0xF091;
/// `BRIN_PAGETYPE_REVMAP` (brin_page.h).
const BRIN_PAGETYPE_REVMAP: u16 = 0xF092;
/// `BRIN_PAGETYPE_REGULAR` (brin_page.h).
const BRIN_PAGETYPE_REGULAR: u16 = 0xF093;

/// `BRIN_EVACUATE_PAGE` (brin_page.h) — flag bit (not WAL-logged).
const BRIN_EVACUATE_PAGE: u16 = 1 << 0;

/// `BRIN_META_MAGIC` (brin_page.h).
const BRIN_META_MAGIC: u32 = 0xA8109CFA;

/// `BRIN_IS_META_PAGE(page)` (brin_page.h).
#[inline]
fn BRIN_IS_META_PAGE_TYPE(ty: u16) -> bool {
    ty == BRIN_PAGETYPE_META
}

/// `BRIN_IS_REGULAR_PAGE(page)` (brin_page.h).
#[inline]
fn BRIN_IS_REGULAR_PAGE_TYPE(ty: u16) -> bool {
    ty == BRIN_PAGETYPE_REGULAR
}

// ===========================================================================
// Byte-level page layout helpers (transcribed from brin_page.h / bufpage.h).
//
// There is no ported sibling brin_page / brin_pageops crate yet, so these
// BRIN-specific page-byte primitives are grounded here, 1:1 with the C macros.
// ===========================================================================

/// `MAXALIGN(x)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// `PageGetContents(page)` offset (bufpage.h): the area after the MAXALIGN'd
/// page header. `BrinMetaPageData` / `RevmapContents.rm_tids` start here.
const CONTENTS_OFFSET: usize = maxalign(SizeOfPageHeaderData);

/// `sizeof(BrinSpecialSpace)` (brin_page.h) = `MAXALIGN(1)` = 8 bytes; it
/// always occupies the last MAXALIGN-sized element of the page.
const SIZEOF_BRIN_SPECIAL_SPACE: usize = maxalign(1);

/// `sizeof(ItemPointerData)` on the on-disk ABI: 6 bytes.
const SIZEOF_ITEM_POINTER_DATA: usize = 6;

/// Byte offset of `pd_lower` within `PageHeaderData` (the uint16 at offset 12).
const OFF_PD_LOWER: usize = 12;

/// `REVMAP_CONTENT_SIZE` (brin_page.h): the bytes available for the revmap's
/// `rm_tids` array (offsetof(RevmapContents, rm_tids) == 0).
const REVMAP_CONTENT_SIZE: usize =
    BLCKSZ - maxalign(SizeOfPageHeaderData) - 0 - maxalign(SIZEOF_BRIN_SPECIAL_SPACE);

/// `REVMAP_PAGE_MAXITEMS` (brin_page.h): max number of revmap entries per page.
const REVMAP_PAGE_MAXITEMS: usize = REVMAP_CONTENT_SIZE / SIZEOF_ITEM_POINTER_DATA;

/// `BrinPageType(page)` (brin_page.h): the last half-word of the page
/// (`BrinSpecialSpace.vector[MAXALIGN(1)/2 - 1]`).
fn brin_page_type(page: &[u8]) -> u16 {
    let off = BLCKSZ - 2;
    u16::from_ne_bytes([page[off], page[off + 1]])
}

/// `BrinPageType(page) = type` — write the page-type half-word.
fn set_brin_page_type(page: &mut [u8], ty: u16) {
    let off = BLCKSZ - 2;
    page[off..off + 2].copy_from_slice(&ty.to_ne_bytes());
}

/// `BrinPageFlags(page)` (brin_page.h): the second-to-last half-word of the
/// page (`BrinSpecialSpace.vector[MAXALIGN(1)/2 - 2]`).
fn brin_page_flags(page: &[u8]) -> u16 {
    let off = BLCKSZ - 4;
    u16::from_ne_bytes([page[off], page[off + 1]])
}

/// `BrinPageFlags(page) = flags` — write the flags half-word.
fn set_brin_page_flags(page: &mut [u8], flags: u16) {
    let off = BLCKSZ - 4;
    page[off..off + 2].copy_from_slice(&flags.to_ne_bytes());
}

/// `((PageHeader) page)->pd_lower`.
fn read_pd_lower(page: &[u8]) -> u16 {
    u16::from_ne_bytes([page[OFF_PD_LOWER], page[OFF_PD_LOWER + 1]])
}

/// `((PageHeader) page)->pd_lower = value`.
fn set_pd_lower(page: &mut [u8], value: u16) {
    page[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

// --- metapage accessors (BrinMetaPageData lives at PageGetContents) ----------
//
// struct BrinMetaPageData { uint32 brinMagic; uint32 brinVersion;
//                           BlockNumber pagesPerRange; BlockNumber lastRevmapPage; }
// — four 4-byte fields, no padding (offsets 0, 4, 8, 12 within the contents).

const META_OFF_MAGIC: usize = CONTENTS_OFFSET; // brinMagic
const META_OFF_VERSION: usize = CONTENTS_OFFSET + 4; // brinVersion
const META_OFF_PAGES_PER_RANGE: usize = CONTENTS_OFFSET + 8; // pagesPerRange
const META_OFF_LAST_REVMAP_PAGE: usize = CONTENTS_OFFSET + 12; // lastRevmapPage

/// `sizeof(BrinMetaPageData)` = 16 bytes.
const SIZEOF_BRIN_META_PAGE_DATA: usize = 16;

fn meta_last_revmap_page(page: &[u8]) -> BlockNumber {
    BlockNumber::from_ne_bytes([
        page[META_OFF_LAST_REVMAP_PAGE],
        page[META_OFF_LAST_REVMAP_PAGE + 1],
        page[META_OFF_LAST_REVMAP_PAGE + 2],
        page[META_OFF_LAST_REVMAP_PAGE + 3],
    ])
}

fn set_meta_last_revmap_page(page: &mut [u8], blk: BlockNumber) {
    page[META_OFF_LAST_REVMAP_PAGE..META_OFF_LAST_REVMAP_PAGE + 4]
        .copy_from_slice(&blk.to_ne_bytes());
}

/// `((PageHeader) metapg)->pd_lower = ((char *) metadata +
/// sizeof(BrinMetaPageData)) - (char *) metapg` (brin_xlog.c): set pd_lower
/// just past the end of the metadata.
fn set_meta_pd_lower(page: &mut [u8]) {
    set_pd_lower(page, (CONTENTS_OFFSET + SIZEOF_BRIN_META_PAGE_DATA) as u16);
}

// ===========================================================================
// brin_page_init / brin_metapage_init (brin_pageops.c).
// ===========================================================================

/// `brin_page_init(page, type)` (brin_pageops.c): `PageInit` the page and stamp
/// its special-area page type.
fn brin_page_init(page: &mut [u8], page_type: u16) -> PgResult<()> {
    PageInit(page, BLCKSZ, SIZEOF_BRIN_SPECIAL_SPACE)?;
    set_brin_page_type(page, page_type);
    Ok(())
}

/// `brin_metapage_init(page, pagesPerRange, version)` (brin_pageops.c): create
/// the metapage.
fn brin_metapage_init(page: &mut [u8], pages_per_range: BlockNumber, version: u16) -> PgResult<()> {
    brin_page_init(page, BRIN_PAGETYPE_META)?;

    // metadata->brinMagic = BRIN_META_MAGIC;
    page[META_OFF_MAGIC..META_OFF_MAGIC + 4].copy_from_slice(&BRIN_META_MAGIC.to_ne_bytes());
    // metadata->brinVersion = version;
    page[META_OFF_VERSION..META_OFF_VERSION + 4].copy_from_slice(&(version as u32).to_ne_bytes());
    // metadata->pagesPerRange = pagesPerRange;
    page[META_OFF_PAGES_PER_RANGE..META_OFF_PAGES_PER_RANGE + 4]
        .copy_from_slice(&pages_per_range.to_ne_bytes());
    // metadata->lastRevmapPage = 0;
    set_meta_last_revmap_page(page, 0);

    // ((PageHeader) page)->pd_lower = ... just past the metadata.
    set_meta_pd_lower(page);
    Ok(())
}

// ===========================================================================
// revmap addressing (brin_revmap.c) + brinSetHeapBlockItemptr.
// ===========================================================================

/// `HEAPBLK_TO_REVMAP_INDEX(pagesPerRange, heapBlk)` (brin_revmap.c): index
/// within the revmap page's `rm_tids` array.
#[inline]
fn heapblk_to_revmap_index(pages_per_range: BlockNumber, heap_blk: BlockNumber) -> usize {
    ((heap_blk / pages_per_range) % REVMAP_PAGE_MAXITEMS as BlockNumber) as usize
}

/// Byte offset of `rm_tids[index]` within the page: the `RevmapContents`
/// struct begins directly at `PageGetContents`, so the array starts at
/// `CONTENTS_OFFSET`.
#[inline]
fn revmap_tid_offset(index: usize) -> usize {
    CONTENTS_OFFSET + index * SIZEOF_ITEM_POINTER_DATA
}

/// Read an on-disk `ItemPointerData` (6 bytes: 2-byte hi blkid, 2-byte lo
/// blkid, 2-byte posid) at byte offset `off`.
fn read_item_pointer(page: &[u8], off: usize) -> ItemPointerData {
    let bi_hi = u16::from_ne_bytes([page[off], page[off + 1]]);
    let bi_lo = u16::from_ne_bytes([page[off + 2], page[off + 3]]);
    let posid = u16::from_ne_bytes([page[off + 4], page[off + 5]]);
    let mut iptr = ItemPointerData::default();
    iptr.ip_blkid.bi_hi = bi_hi;
    iptr.ip_blkid.bi_lo = bi_lo;
    iptr.ip_posid = posid;
    iptr
}

/// Write an `ItemPointerData` (6 bytes) at byte offset `off`.
fn write_item_pointer(page: &mut [u8], off: usize, iptr: &ItemPointerData) {
    page[off..off + 2].copy_from_slice(&iptr.ip_blkid.bi_hi.to_ne_bytes());
    page[off + 2..off + 4].copy_from_slice(&iptr.ip_blkid.bi_lo.to_ne_bytes());
    page[off + 4..off + 6].copy_from_slice(&iptr.ip_posid.to_ne_bytes());
}

/// `brinSetHeapBlockItemptr(buffer, pagesPerRange, heapBlk, tid)`
/// (brin_revmap.c) replayed directly against the page bytes of an
/// already-locked revmap buffer. Used by the redo path exactly as the live path
/// uses the engine routine.
fn brin_set_heap_block_itemptr(
    page: &mut [u8],
    pages_per_range: BlockNumber,
    heap_blk: BlockNumber,
    tid: ItemPointerData,
) {
    let index = heapblk_to_revmap_index(pages_per_range, heap_blk);
    let off = revmap_tid_offset(index);
    let mut iptr = read_item_pointer(page, off);
    if ItemPointerIsValid(Some(&tid)) {
        ItemPointerSet(
            &mut iptr,
            ItemPointerGetBlockNumber(&tid),
            ItemPointerGetOffsetNumber(&tid),
        );
    } else {
        ItemPointerSetInvalid(&mut iptr);
    }
    write_item_pointer(page, off, &iptr);
}

// ===========================================================================
// xl_brin_* record decoders (native-endian, mirroring the on-disk structs).
// ===========================================================================

/// `xl_brin_createidx` (brin_xlog.h): `{ BlockNumber pagesPerRange; uint16
/// version; }`.
#[derive(Clone, Copy, Debug)]
struct XlBrinCreateidx {
    version: u16,
    pages_per_range: BlockNumber,
}

fn parse_createidx(data: &[u8]) -> XlBrinCreateidx {
    let pages_per_range = BlockNumber::from_ne_bytes([data[0], data[1], data[2], data[3]]);
    let version = u16::from_ne_bytes([data[4], data[5]]);
    XlBrinCreateidx {
        version,
        pages_per_range,
    }
}

/// `xl_brin_insert` (brin_xlog.h): `{ BlockNumber heapBlk; BlockNumber
/// pagesPerRange; OffsetNumber offnum; }`.
#[derive(Clone, Copy, Debug)]
struct XlBrinInsert {
    heap_blk: BlockNumber,
    pages_per_range: BlockNumber,
    offnum: OffsetNumber,
}

fn parse_insert(data: &[u8]) -> XlBrinInsert {
    XlBrinInsert {
        heap_blk: BlockNumber::from_ne_bytes([data[0], data[1], data[2], data[3]]),
        pages_per_range: BlockNumber::from_ne_bytes([data[4], data[5], data[6], data[7]]),
        offnum: u16::from_ne_bytes([data[8], data[9]]),
    }
}

/// `xl_brin_update` (brin_xlog.h): `{ OffsetNumber oldOffnum; xl_brin_insert
/// insert; }`.
#[derive(Clone, Copy, Debug)]
struct XlBrinUpdate {
    old_offnum: OffsetNumber,
    insert: XlBrinInsert,
}

fn parse_update(data: &[u8]) -> XlBrinUpdate {
    let old_offnum = u16::from_ne_bytes([data[0], data[1]]);
    // offsetof(xl_brin_update, insert) == 4 (oldOffnum is OffsetNumber, padded
    // to the 4-byte alignment of the embedded struct's leading BlockNumber).
    XlBrinUpdate {
        old_offnum,
        insert: parse_insert(&data[4..]),
    }
}

/// `xl_brin_samepage_update` (brin_xlog.h): `{ OffsetNumber offnum; }`.
#[derive(Clone, Copy, Debug)]
struct XlBrinSamepageUpdate {
    offnum: OffsetNumber,
}

fn parse_samepage_update(data: &[u8]) -> XlBrinSamepageUpdate {
    XlBrinSamepageUpdate {
        offnum: u16::from_ne_bytes([data[0], data[1]]),
    }
}

/// `xl_brin_revmap_extend` (brin_xlog.h): `{ BlockNumber targetBlk; }`.
#[derive(Clone, Copy, Debug)]
struct XlBrinRevmapExtend {
    target_blk: BlockNumber,
}

fn parse_revmap_extend(data: &[u8]) -> XlBrinRevmapExtend {
    XlBrinRevmapExtend {
        target_blk: BlockNumber::from_ne_bytes([data[0], data[1], data[2], data[3]]),
    }
}

/// `xl_brin_desummarize` (brin_xlog.h): `{ BlockNumber pagesPerRange;
/// BlockNumber heapBlk; OffsetNumber regOffset; }`.
#[derive(Clone, Copy, Debug)]
struct XlBrinDesummarize {
    pages_per_range: BlockNumber,
    heap_blk: BlockNumber,
    reg_offset: OffsetNumber,
}

fn parse_desummarize(data: &[u8]) -> XlBrinDesummarize {
    XlBrinDesummarize {
        pages_per_range: BlockNumber::from_ne_bytes([data[0], data[1], data[2], data[3]]),
        heap_blk: BlockNumber::from_ne_bytes([data[4], data[5], data[6], data[7]]),
        reg_offset: u16::from_ne_bytes([data[8], data[9]]),
    }
}

// ===========================================================================
// Decoded-record accessors (read off `record.record`, owned by xlogreader).
// ===========================================================================

/// `XLogRecGetData(record)` — the record's main data.
fn record_get_data<'a>(record: &'a XLogReaderState<'_>) -> &'a [u8] {
    record
        .record
        .as_ref()
        .map(|r| r.data())
        .unwrap_or(&[])
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
// brin_xlog_createidx (brin_xlog.c:23)
// ===========================================================================

/// `brin_xlog_createidx` (brin_xlog.c:23): create the index's metapage.
fn brin_xlog_createidx(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = parse_createidx(record_get_data(record));

    /* create the index' metapage */
    let buf = XLogInitBufferForRedo(record, 0)?;
    debug_assert!(BufferIsValid(buf));
    with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        brin_metapage_init(page, xlrec.pages_per_range, xlrec.version)?;
        page_set_lsn(page, lsn)?;
        Ok(())
    })?;
    mark_buffer_dirty::call(buf);
    unlock_release_buffer::call(buf);
    Ok(())
}

// ===========================================================================
// brin_xlog_insert_update (brin_xlog.c:45)
// ===========================================================================

/// `brin_xlog_insert_update` (brin_xlog.c:45): common part of an insert or
/// update — insert the new tuple and update the revmap.
fn brin_xlog_insert_update(record: &XLogReaderState<'_>, xlrec: &XlBrinInsert) -> PgResult<()> {
    let lsn = record.EndRecPtr;

    /*
     * If we inserted the first and only tuple on the page, re-initialize the
     * page from scratch.
     */
    let mut buffer: Buffer;
    let action: XLogRedoAction;
    if (record_get_info(record) & XLOG_BRIN_INIT_PAGE) != 0 {
        buffer = XLogInitBufferForRedo(record, 0)?;
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            brin_page_init(page, BRIN_PAGETYPE_REGULAR)?;
            Ok(())
        })?;
        action = XLogRedoAction::BlkNeedsRedo;
    } else {
        let (a, b) = XLogReadBufferForRedo(record, 0)?;
        action = a;
        buffer = b;
    }

    /* need this page's blkno to store in revmap */
    let regpgno = buffer_get_block_number::call(buffer);

    /* insert the index item into the page */
    if action == XLogRedoAction::BlkNeedsRedo {
        let tuple = record_get_block_data(record, 0);
        let tuplen = tuple.len();

        // Assert(tuple->bt_blkno == xlrec->heapBlk);
        debug_assert_eq!(brin_tuple_get_blkno(tuple), xlrec.heap_blk);

        let offnum = xlrec.offnum;
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            {
                let pref = PageRef::new(page)?;
                if PageGetMaxOffsetNumber(&pref) + 1 < offnum {
                    return Err(panic_invalid_max_offset());
                }
            }
            let placed = {
                let mut pmut = PageMut::new(page)?;
                PageAddItemExtended(&mut pmut, &tuple[..tuplen], offnum, PAI_OVERWRITE)?
            };
            if placed == INVALID_OFFSET_NUMBER {
                return Err(panic_failed_to_add());
            }
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }

    /* update the revmap */
    let (a, b) = XLogReadBufferForRedo(record, 1)?;
    let action = a;
    buffer = b;
    if action == XLogRedoAction::BlkNeedsRedo {
        let mut tid = ItemPointerData::default();
        ItemPointerSet(&mut tid, regpgno, xlrec.offnum);
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            brin_set_heap_block_itemptr(page, xlrec.pages_per_range, xlrec.heap_blk, tid);
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }

    /* XXX no FSM updates here ... */
    Ok(())
}

// ===========================================================================
// brin_xlog_insert (brin_xlog.c:123)
// ===========================================================================

/// `brin_xlog_insert` (brin_xlog.c:123): replay a BRIN index insertion.
fn brin_xlog_insert(record: &XLogReaderState<'_>) -> PgResult<()> {
    let xlrec = parse_insert(record_get_data(record));
    brin_xlog_insert_update(record, &xlrec)
}

// ===========================================================================
// brin_xlog_update (brin_xlog.c:134)
// ===========================================================================

/// `brin_xlog_update` (brin_xlog.c:134): replay a BRIN index update.
fn brin_xlog_update(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = parse_update(record_get_data(record));

    /* First remove the old tuple */
    let (action, buffer) = XLogReadBufferForRedo(record, 2)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let offnum = xlrec.old_offnum;
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            {
                let mut pmut = PageMut::new(page)?;
                PageIndexTupleDeleteNoCompact(&mut pmut, offnum)?;
            }
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }

    /* Then insert the new tuple and update revmap, like in an insertion. */
    brin_xlog_insert_update(record, &xlrec.insert)?;

    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// brin_xlog_samepage_update (brin_xlog.c:170)
// ===========================================================================

/// `brin_xlog_samepage_update` (brin_xlog.c:170): update a tuple on a single
/// page.
fn brin_xlog_samepage_update(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = parse_samepage_update(record_get_data(record));

    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let brintuple = record_get_block_data(record, 0);
        let tuplen = brintuple.len();
        let offnum = xlrec.offnum;
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            let overwritten = {
                let mut pmut = PageMut::new(page)?;
                PageIndexTupleOverwrite(&mut pmut, offnum, &brintuple[..tuplen])?
            };
            if !overwritten {
                return Err(panic_failed_to_replace());
            }
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }

    /* XXX no FSM updates here ... */
    Ok(())
}

// ===========================================================================
// brin_xlog_revmap_extend (brin_xlog.c:208)
// ===========================================================================

/// `brin_xlog_revmap_extend` (brin_xlog.c:208): replay a revmap page extension.
fn brin_xlog_revmap_extend(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = parse_revmap_extend(record_get_data(record));

    // XLogRecGetBlockTag(record, 1, NULL, NULL, &targetBlk);
    // Assert(xlrec->targetBlk == targetBlk);
    #[cfg(debug_assertions)]
    {
        if let Some(tag) = xlog_rec_get_block_tag_extended::call(record, 1)? {
            debug_assert_eq!(xlrec.target_blk, tag.blkno);
        }
    }

    /* Update the metapage */
    let (action, metabuf) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        with_buffer_page::call(metabuf, &mut |metapg: &mut [u8]| {
            // metadata = (BrinMetaPageData *) PageGetContents(metapg);
            // Assert(metadata->lastRevmapPage == xlrec->targetBlk - 1);
            debug_assert_eq!(meta_last_revmap_page(metapg), xlrec.target_blk - 1);
            set_meta_last_revmap_page(metapg, xlrec.target_blk);

            page_set_lsn(metapg, lsn)?;

            /*
             * Set pd_lower just past the end of the metadata.  This is
             * essential, because without doing so, metadata will be lost if
             * xlog.c compresses the page.  (We must do this here because pre-v11
             * versions of PG did not set the metapage's pd_lower correctly, so a
             * pg_upgraded index might contain the wrong value.)
             */
            set_meta_pd_lower(metapg);
            Ok(())
        })?;
        mark_buffer_dirty::call(metabuf);
    }

    /*
     * Re-init the target block as a revmap page.  There's never a full-page
     * image here.
     */
    let buf = XLogInitBufferForRedo(record, 1)?;
    with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        brin_page_init(page, BRIN_PAGETYPE_REVMAP)?;
        page_set_lsn(page, lsn)?;
        Ok(())
    })?;
    mark_buffer_dirty::call(buf);

    unlock_release_buffer::call(buf);
    if BufferIsValid(metabuf) {
        unlock_release_buffer::call(metabuf);
    }
    Ok(())
}

// ===========================================================================
// brin_xlog_desummarize_page (brin_xlog.c:268)
// ===========================================================================

/// `brin_xlog_desummarize_page` (brin_xlog.c:268): replay a range
/// desummarization.
fn brin_xlog_desummarize_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = parse_desummarize(record_get_data(record));

    /* Update the revmap */
    let (action, buffer) = XLogReadBufferForRedo(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let mut iptr = ItemPointerData::default();
        ItemPointerSetInvalid(&mut iptr);
        with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
            brin_set_heap_block_itemptr(page, xlrec.pages_per_range, xlrec.heap_blk, iptr);
            page_set_lsn(page, lsn)?;
            Ok(())
        })?;
        mark_buffer_dirty::call(buffer);
    }
    if BufferIsValid(buffer) {
        unlock_release_buffer::call(buffer);
    }

    /* remove the leftover entry from the regular page */
    let (action, buffer) = XLogReadBufferForRedo(record, 1)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let reg_offset = xlrec.reg_offset;
        with_buffer_page::call(buffer, &mut |reg_pg: &mut [u8]| {
            {
                let mut pmut = PageMut::new(reg_pg)?;
                PageIndexTupleDeleteNoCompact(&mut pmut, reg_offset)?;
            }
            page_set_lsn(reg_pg, lsn)?;
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
// brin_redo (brin_xlog.c:309)
// ===========================================================================

/// `brin_redo(XLogReaderState *record)` (brin_xlog.c:309): dispatch a BRIN WAL
/// record to its handler.
pub fn brin_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !XLR_INFO_MASK;

    match info & XLOG_BRIN_OPMASK {
        XLOG_BRIN_CREATE_INDEX => brin_xlog_createidx(record),
        XLOG_BRIN_INSERT => brin_xlog_insert(record),
        XLOG_BRIN_UPDATE => brin_xlog_update(record),
        XLOG_BRIN_SAMEPAGE_UPDATE => brin_xlog_samepage_update(record),
        XLOG_BRIN_REVMAP_EXTEND => brin_xlog_revmap_extend(record),
        XLOG_BRIN_DESUMMARIZE => brin_xlog_desummarize_page(record),
        _ => Err(panic_unknown_opcode(info)),
    }
}

// ===========================================================================
// brin_mask (brin_xlog.c:342)
// ===========================================================================

/// `brin_mask(pagedata, blkno)` (brin_xlog.c:342): mask a BRIN page before
/// consistency checks, operating on the raw page bytes.
pub fn brin_mask(page: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    // mask_page_lsn_and_checksum(page); mask_page_hint_bits(page);
    mask_page_lsn_and_checksum::call(page);
    mask_page_hint_bits::call(page);

    /*
     * Regular brin pages contain unused space which needs to be masked.
     * Similarly for meta pages, but mask it only if pd_lower appears to have
     * been set correctly.
     */
    let pd_lower = read_pd_lower(page) as usize;
    let ty = brin_page_type(page);
    if BRIN_IS_REGULAR_PAGE_TYPE(ty) || (BRIN_IS_META_PAGE_TYPE(ty) && pd_lower > SizeOfPageHeaderData)
    {
        mask_unused_space::call(page)?;
    }

    /*
     * BRIN_EVACUATE_PAGE is not WAL-logged, since it's of no use in recovery.
     * Mask it.  See brin_start_evacuating_page() for details.
     */
    let flags = brin_page_flags(page);
    set_brin_page_flags(page, flags & !BRIN_EVACUATE_PAGE);
    Ok(())
}

// ===========================================================================
// Helpers.
// ===========================================================================

/// `PageSetLSN(page, lsn)` against the page bytes.
fn page_set_lsn(page: &mut [u8], lsn: XLogRecPtr) -> PgResult<()> {
    let mut pmut = PageMut::new(page)?;
    PageSetLSN(&mut pmut, lsn);
    Ok(())
}

/// `((BrinTuple *) blockData)->bt_blkno` — the heap block number recorded in
/// the on-disk tuple header (`BrinTuple` begins with the 4-byte `bt_blkno`).
fn brin_tuple_get_blkno(tuple: &[u8]) -> BlockNumber {
    BlockNumber::from_ne_bytes([tuple[0], tuple[1], tuple[2], tuple[3]])
}

// ===========================================================================
// Error reporters (PANIC).
// ===========================================================================

/// `elog(PANIC, "brin_xlog_insert_update: invalid max offset number")`.
fn panic_invalid_max_offset() -> PgError {
    ereport(PANIC)
        .errmsg_internal("brin_xlog_insert_update: invalid max offset number")
        .into_error()
}

/// `elog(PANIC, "brin_xlog_insert_update: failed to add tuple")`.
fn panic_failed_to_add() -> PgError {
    ereport(PANIC)
        .errmsg_internal("brin_xlog_insert_update: failed to add tuple")
        .into_error()
}

/// `elog(PANIC, "brin_xlog_samepage_update: failed to replace tuple")`.
fn panic_failed_to_replace() -> PgError {
    ereport(PANIC)
        .errmsg_internal("brin_xlog_samepage_update: failed to replace tuple")
        .into_error()
}

/// `elog(PANIC, "brin_redo: unknown op code %u", info)`.
fn panic_unknown_opcode(info: u8) -> PgError {
    ereport(PANIC)
        .errmsg_internal(format!("brin_redo: unknown op code {info}"))
        .into_error()
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the rmgr-table callbacks this unit owns
/// (`brin_redo` / `brin_mask`).
pub fn init_seams() {
    backend_access_brin_xlog_seams::brin_redo::set(brin_redo);
    backend_access_brin_xlog_seams::brin_mask::set(brin_mask);
}
