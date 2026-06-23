//! Shared internals: the `BrinRevmap` access object (`brin_revmap.c`), the
//! `RevmapContents` item-pointer arithmetic, the `xl_brin_*` record encoders
//! (`brin_xlog.h`), the page read/modify helpers over the bufmgr seam, and the
//! small shared helpers (BufferIsValid, error reporters).

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;

use ::bufmgr_seams::with_buffer_page;
use page::{
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid, ItemPointerSet,
    ItemPointerSetInvalid,
};
use utils_error::{ereport, PgError};
use ::types_error::error::ERROR;
use ::types_core::primitive::{BlockNumber, OffsetNumber};
use ::types_error::PgResult;
use ::rel::Relation;
use ::types_storage::buf::Buffer;
use ::types_tuple::heaptuple::ItemPointerData;

use crate::brin_page::{CONTENTS_OFFSET, SIZEOF_ITEM_POINTER_DATA};

// ===========================================================================
// BrinRevmap (brin_revmap.c) — the range-map access object.
// ===========================================================================

/// `struct BrinRevmap` (brin_revmap.c): an access object for a BRIN range map.
/// Must be released with `brinRevmapTerminate`.
pub struct BrinRevmap<'mcx> {
    /// `rm_irel`: the index relation.
    pub rm_irel: Relation<'mcx>,
    /// `rm_pagesPerRange`.
    pub rm_pagesPerRange: BlockNumber,
    /// `rm_lastRevmapPage`: cached from the metapage.
    pub rm_lastRevmapPage: BlockNumber,
    /// `rm_metaBuf`.
    pub rm_metaBuf: Buffer,
    /// `rm_currBuf`.
    pub rm_currBuf: Buffer,
}

// ===========================================================================
// HEAPBLK_TO_REVMAP_* (brin_revmap.c).
// ===========================================================================

/// `HEAPBLK_TO_REVMAP_BLK(pagesPerRange, heapBlk)`: logical revmap page number.
#[inline]
pub fn heapblk_to_revmap_blk(pages_per_range: BlockNumber, heap_blk: BlockNumber) -> BlockNumber {
    (heap_blk / pages_per_range) / crate::brin_page::REVMAP_PAGE_MAXITEMS as BlockNumber
}

/// `HEAPBLK_TO_REVMAP_INDEX(pagesPerRange, heapBlk)`: index within revmap page.
#[inline]
pub fn heapblk_to_revmap_index(pages_per_range: BlockNumber, heap_blk: BlockNumber) -> usize {
    ((heap_blk / pages_per_range) % crate::brin_page::REVMAP_PAGE_MAXITEMS as BlockNumber) as usize
}

/// Byte offset of `RevmapContents.rm_tids[index]` within the page. The
/// `RevmapContents` struct begins directly at `PageGetContents`.
#[inline]
pub fn revmap_tid_offset(index: usize) -> usize {
    CONTENTS_OFFSET + index * SIZEOF_ITEM_POINTER_DATA
}

/// Read an on-disk `ItemPointerData` (6 bytes) at byte offset `off`.
pub fn read_item_pointer(page: &[u8], off: usize) -> ItemPointerData {
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
pub fn write_item_pointer(page: &mut [u8], off: usize, iptr: &ItemPointerData) {
    page[off..off + 2].copy_from_slice(&iptr.ip_blkid.bi_hi.to_ne_bytes());
    page[off + 2..off + 4].copy_from_slice(&iptr.ip_blkid.bi_lo.to_ne_bytes());
    page[off + 4..off + 6].copy_from_slice(&iptr.ip_posid.to_ne_bytes());
}

/// `brinSetHeapBlockItemptr` core, against already-locked revmap page bytes:
/// set the element for `heap_blk` to `tid` (or invalid if `tid` is invalid).
pub fn set_heap_block_itemptr_bytes(
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
// `((BrinTuple *) item)->bt_blkno` — the heap block in the on-disk header.
// ===========================================================================

/// `tup->bt_blkno` — the 4-byte BlockNumber at offset 0 of a BRIN tuple image.
pub fn brin_tuple_get_blkno(tuple: &[u8]) -> BlockNumber {
    BlockNumber::from_ne_bytes([tuple[0], tuple[1], tuple[2], tuple[3]])
}

// ===========================================================================
// Page read/modify helpers over the bufmgr seam.
//
// The bufmgr exposes only `with_buffer_page` (a `&mut [u8]` closure). For
// reads we run it with a closure that copies out the value; for in-place edits
// we run the mutating closure directly. This mirrors hash-core's `with_metap`.
// ===========================================================================

/// Read a value off a (caller-locked) buffer page.
pub fn page_read<R>(buf: Buffer, f: impl FnOnce(&[u8]) -> R) -> PgResult<R> {
    let mut slot: Option<R> = None;
    let mut once = Some(f);
    with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let g = once.take().expect("page_read closure run once");
        slot = Some(g(page));
        Ok(())
    })?;
    Ok(slot.expect("page_read produced a value"))
}

/// Run a fallible in-place edit on a (caller-locked) buffer page.
pub fn page_modify<R>(buf: Buffer, f: impl FnOnce(&mut [u8]) -> PgResult<R>) -> PgResult<R> {
    let mut slot: Option<PgResult<R>> = None;
    let mut once = Some(f);
    with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        let g = once.take().expect("page_modify closure run once");
        let r = g(page);
        let ok = r.is_ok();
        slot = Some(r);
        if ok {
            Ok(())
        } else {
            // Surface a sentinel error so with_buffer_page does not commit a
            // half-applied edit on failure; the real error is in `slot`.
            Err(edit_failed())
        }
    })
    .ok();
    slot.expect("page_modify produced a value")
}

fn edit_failed() -> PgError {
    ereport(ERROR)
        .errmsg_internal("brin page edit failed")
        .into_error()
}

// ===========================================================================
// Shared small helpers.
// ===========================================================================

/// `BufferIsValid(buffer)`.
#[inline]
pub fn buffer_is_valid(buffer: Buffer) -> bool {
    ::types_storage::buf::BufferIsValid(buffer)
}

/// `MAXALIGN(x)`.
#[inline]
pub fn MAXALIGN(x: usize) -> usize {
    (x + 7) & !7
}

// ===========================================================================
// xl_brin_* record encoders (brin_xlog.h), native-endian, mirroring the
// on-disk struct layout exactly. SizeOf* constants from brin_xlog.h.
// ===========================================================================

// (`xl_brin_createidx` is emitted by `brin.c`'s `brinbuildempty`, not by these
// two files, so it is not encoded here.)

/// `xl_brin_insert`: `{ BlockNumber heapBlk; BlockNumber pagesPerRange;
/// OffsetNumber offnum; }`. `SizeOfBrinInsert` = offsetof(offnum) +
/// sizeof(OffsetNumber) = 10.
pub fn encode_xl_brin_insert(
    heap_blk: BlockNumber,
    pages_per_range: BlockNumber,
    offnum: OffsetNumber,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(10);
    v.extend_from_slice(&heap_blk.to_ne_bytes());
    v.extend_from_slice(&pages_per_range.to_ne_bytes());
    v.extend_from_slice(&offnum.to_ne_bytes());
    v
}

/// `xl_brin_update`: `{ OffsetNumber oldOffnum; xl_brin_insert insert; }`.
/// `insert` is 4-byte aligned (leads with BlockNumber), so `oldOffnum` is
/// padded to offset 4. `SizeOfBrinUpdate` = offsetof(insert) +
/// SizeOfBrinInsert = 4 + 10 = 14.
pub fn encode_xl_brin_update(
    old_offnum: OffsetNumber,
    heap_blk: BlockNumber,
    pages_per_range: BlockNumber,
    offnum: OffsetNumber,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(14);
    v.extend_from_slice(&old_offnum.to_ne_bytes());
    v.extend_from_slice(&[0u8; 2]); // padding to 4-byte align of `insert`
    v.extend_from_slice(&encode_xl_brin_insert(heap_blk, pages_per_range, offnum));
    v
}

/// `xl_brin_samepage_update`: `{ OffsetNumber offnum; }`.
/// `SizeOfBrinSamepageUpdate` = 2.
pub fn encode_xl_brin_samepage_update(offnum: OffsetNumber) -> Vec<u8> {
    offnum.to_ne_bytes().to_vec()
}

/// `xl_brin_revmap_extend`: `{ BlockNumber targetBlk; }`.
/// `SizeOfBrinRevmapExtend` = 4.
pub fn encode_xl_brin_revmap_extend(target_blk: BlockNumber) -> Vec<u8> {
    target_blk.to_ne_bytes().to_vec()
}

/// `xl_brin_desummarize`: `{ BlockNumber pagesPerRange; BlockNumber heapBlk;
/// OffsetNumber regOffset; }`. `SizeOfBrinDesummarize` = 10.
pub fn encode_xl_brin_desummarize(
    pages_per_range: BlockNumber,
    heap_blk: BlockNumber,
    reg_offset: OffsetNumber,
) -> Vec<u8> {
    let mut v = Vec::with_capacity(10);
    v.extend_from_slice(&pages_per_range.to_ne_bytes());
    v.extend_from_slice(&heap_blk.to_ne_bytes());
    v.extend_from_slice(&reg_offset.to_ne_bytes());
    v
}

// ===========================================================================
// Error reporters shared by the two files.
// ===========================================================================

/// `ereport(ERROR, errcode(ERRCODE_INDEX_CORRUPTED), errmsg("corrupted BRIN
/// index: inconsistent range map"))`.
pub fn corrupted_inconsistent() -> PgError {
    ereport(ERROR)
        .errcode(::types_error::error::ERRCODE_INDEX_CORRUPTED)
        .errmsg("corrupted BRIN index: inconsistent range map")
        .into_error()
}

/// The `errmsg_internal` variant (brin_revmap.c:258).
pub fn corrupted_inconsistent_internal() -> PgError {
    ereport(ERROR)
        .errcode(::types_error::error::ERRCODE_INDEX_CORRUPTED)
        .errmsg_internal("corrupted BRIN index: inconsistent range map")
        .into_error()
}

/// `elog(ERROR, "revmap does not cover heap block %u", heapBlk)`.
pub fn elog_revmap_does_not_cover(heap_blk: BlockNumber) -> PgError {
    ereport(ERROR)
        .errmsg_internal(format!("revmap does not cover heap block {heap_blk}"))
        .into_error()
}

/// `ereport(ERROR, errcode(ERRCODE_INDEX_CORRUPTED), errmsg("unexpected page
/// type 0x%04X in BRIN index \"%s\" block %u", ...))`.
pub fn unexpected_page_type(page_ty: u16, relname: &str, blkno: BlockNumber) -> PgError {
    ereport(ERROR)
        .errcode(::types_error::error::ERRCODE_INDEX_CORRUPTED)
        .errmsg(format!(
            "unexpected page type 0x{page_ty:04X} in BRIN index \"{relname}\" block {blkno}"
        ))
        .into_error()
}

/// `ereport(ERROR, errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED), errmsg("index row
/// size %zu exceeds maximum %zu for index \"%s\"", ...))`.
pub fn index_row_size_error(sz: usize, max: usize, relname: &str) -> PgError {
    ereport(ERROR)
        .errcode(::types_error::error::ERRCODE_PROGRAM_LIMIT_EXCEEDED)
        .errmsg(format!(
            "index row size {sz} exceeds maximum {max} for index \"{relname}\""
        ))
        .into_error()
}

/// `elog(ERROR, "failed to replace BRIN tuple")`.
pub fn elog_failed_replace() -> PgError {
    ereport(ERROR)
        .errmsg_internal("failed to replace BRIN tuple")
        .into_error()
}

/// `elog(ERROR, "failed to add BRIN tuple to new page")`.
pub fn elog_failed_add_new_page() -> PgError {
    ereport(ERROR)
        .errmsg_internal("failed to add BRIN tuple to new page")
        .into_error()
}

/// OOM during a fallible page-item copy (AGENTS.md: no abort on OOM).
pub fn oom_error() -> PgError {
    ereport(ERROR)
        .errcode(::types_error::error::ERRCODE_OUT_OF_MEMORY)
        .errmsg("out of memory")
        .into_error()
}

#[allow(dead_code)]
type _Relname = String;
