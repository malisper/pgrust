//! `backend-storage-page` — idiomatic Rust port of PostgreSQL's standard
//! buffer-page code.
//!
//! Ports `src/backend/storage/page/bufpage.c` (the page support functions) and
//! `src/backend/storage/page/itemptr.c` (the out-of-line item-pointer helpers),
//! together with the inline accessors/macros from `storage/bufpage.h`,
//! `storage/itemptr.h`, `storage/itemid.h`, and `storage/block.h` that those
//! `.c` files rely on. (`checksum.c` is the separate sibling crate
//! `backend-storage-page-checksum`.)
//!
//! A PostgreSQL page is a fixed `BLCKSZ`-byte buffer with a precise on-disk
//! layout. We keep that byte-exact layout: a page is a `&[u8]` / `&mut [u8]`,
//! and the header fields and line-pointer (`ItemIdData`) array are read and
//! written at their fixed native-endian byte offsets. This reproduces the C
//! `(PageHeader) page` struct overlay bit-for-bit without raw-pointer casts.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use backend_storage_page_checksum::pg_checksum_page;
use backend_utils_error::{elog, ereport};
use mcx::{vec_with_capacity_in, MemoryContext, PgVec};
use types_core::{
    BlockNumber, OffsetNumber, Size, TransactionId, XLogRecPtr, InvalidBlockNumber, BLCKSZ,
};
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_DATA_CORRUPTED, ERROR, LOG, PANIC, WARNING,
};
use types_storage::bufpage::{
    ItemIdData, ItemLength, ItemOffset, MaxHeapTuplesPerPage, MaxOffsetNumber,
    MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber, PageTemp, PageXLogRecPtr, PAI_IS_HEAP,
    PAI_OVERWRITE, PD_ALL_VISIBLE, PD_HAS_FREE_LINES, PD_PAGE_FULL, PD_VALID_FLAG_BITS,
    PG_PAGE_LAYOUT_VERSION, PIV_IGNORE_CHECKSUM_FAILURE, PIV_LOG_LOG, PIV_LOG_WARNING,
    SizeOfPageHeaderData,
};
use types_storage::storage::LocationIndex;
use types_tuple::heaptuple::{BlockIdData, ItemPointerData, FIRST_OFFSET_NUMBER, INVALID_OFFSET_NUMBER};

pub use types_storage::bufpage::PG_IO_ALIGN_SIZE;

/// `sizeof(ItemIdData)` — every line pointer is 4 bytes.
const ITEM_ID_SIZE: usize = core::mem::size_of::<ItemIdData>();

// Fixed byte offsets of each `PageHeaderData` field within a page buffer (the
// native-endian layout of the C struct). `SizeOfPageHeaderData` (== 24) is the
// offset of `pd_linp`, the start of the line-pointer array.
const OFF_PD_LSN: usize = 0; // PageXLogRecPtr: xlogid @0, xrecoff @4
const OFF_PD_CHECKSUM: usize = 8;
const OFF_PD_FLAGS: usize = 10;
const OFF_PD_LOWER: usize = 12;
const OFF_PD_UPPER: usize = 14;
const OFF_PD_SPECIAL: usize = 16;
const OFF_PD_PAGESIZE_VERSION: usize = 18;
const OFF_PD_PRUNE_XID: usize = 20;

// ---------------------------------------------------------------------------
// block.h — BlockId helpers
// ---------------------------------------------------------------------------

/// `BlockIdSet` (block.h).
pub fn BlockIdSet(blockId: &mut BlockIdData, blockNumber: BlockNumber) {
    blockId.set_block_number(blockNumber);
}

/// `BlockIdGetBlockNumber` (block.h).
pub fn BlockIdGetBlockNumber(blockId: &BlockIdData) -> BlockNumber {
    blockId.block_number()
}

// ---------------------------------------------------------------------------
// itemptr.h / itemptr.c — ItemPointer helpers
// ---------------------------------------------------------------------------

/// `ItemPointerIsValid` (itemptr.h). The argument is `None` for a NULL pointer.
pub fn ItemPointerIsValid(pointer: Option<&ItemPointerData>) -> bool {
    pointer.is_some_and(|pointer| pointer.ip_posid != INVALID_OFFSET_NUMBER)
}

/// `ItemPointerGetBlockNumberNoCheck` (itemptr.h).
pub fn ItemPointerGetBlockNumberNoCheck(pointer: &ItemPointerData) -> BlockNumber {
    pointer.ip_blkid.block_number()
}

/// `ItemPointerGetBlockNumber` (itemptr.h). C asserts `ItemPointerIsValid`.
pub fn ItemPointerGetBlockNumber(pointer: &ItemPointerData) -> BlockNumber {
    ItemPointerGetBlockNumberNoCheck(pointer)
}

/// `ItemPointerGetOffsetNumberNoCheck` (itemptr.h).
pub fn ItemPointerGetOffsetNumberNoCheck(pointer: &ItemPointerData) -> OffsetNumber {
    pointer.ip_posid
}

/// `ItemPointerGetOffsetNumber` (itemptr.h). C asserts `ItemPointerIsValid`.
pub fn ItemPointerGetOffsetNumber(pointer: &ItemPointerData) -> OffsetNumber {
    ItemPointerGetOffsetNumberNoCheck(pointer)
}

/// `ItemPointerSet` (itemptr.h).
pub fn ItemPointerSet(pointer: &mut ItemPointerData, blockNumber: BlockNumber, offNum: OffsetNumber) {
    *pointer = ItemPointerData::new(blockNumber, offNum);
}

/// `ItemPointerSetBlockNumber` (itemptr.h).
pub fn ItemPointerSetBlockNumber(pointer: &mut ItemPointerData, blockNumber: BlockNumber) {
    pointer.ip_blkid.set_block_number(blockNumber);
}

/// `ItemPointerSetOffsetNumber` (itemptr.h).
pub fn ItemPointerSetOffsetNumber(pointer: &mut ItemPointerData, offsetNumber: OffsetNumber) {
    pointer.ip_posid = offsetNumber;
}

/// `ItemPointerCopy` (itemptr.h).
pub fn ItemPointerCopy(fromPointer: &ItemPointerData, toPointer: &mut ItemPointerData) {
    *toPointer = *fromPointer;
}

/// `ItemPointerSetInvalid` (itemptr.h).
pub fn ItemPointerSetInvalid(pointer: &mut ItemPointerData) {
    *pointer = ItemPointerData::new(InvalidBlockNumber, INVALID_OFFSET_NUMBER);
}

/// `ItemPointerIndicatesMovedPartitions` (itemptr.h).
pub fn ItemPointerIndicatesMovedPartitions(pointer: &ItemPointerData) -> bool {
    ItemPointerGetOffsetNumber(pointer) == MovedPartitionsOffsetNumber
        && ItemPointerGetBlockNumberNoCheck(pointer) == MovedPartitionsBlockNumber
}

/// `ItemPointerSetMovedPartitions` (itemptr.h).
pub fn ItemPointerSetMovedPartitions(pointer: &mut ItemPointerData) {
    ItemPointerSet(pointer, MovedPartitionsBlockNumber, MovedPartitionsOffsetNumber);
}

/// `ItemPointerEquals` (itemptr.c).
pub fn ItemPointerEquals(pointer1: &ItemPointerData, pointer2: &ItemPointerData) -> bool {
    ItemPointerGetBlockNumber(pointer1) == ItemPointerGetBlockNumber(pointer2)
        && ItemPointerGetOffsetNumber(pointer1) == ItemPointerGetOffsetNumber(pointer2)
}

/// `ItemPointerCompare` (itemptr.c): generic btree-style comparison.
pub fn ItemPointerCompare(arg1: &ItemPointerData, arg2: &ItemPointerData) -> i32 {
    let b1 = ItemPointerGetBlockNumberNoCheck(arg1);
    let b2 = ItemPointerGetBlockNumberNoCheck(arg2);
    if b1 < b2 {
        -1
    } else if b1 > b2 {
        1
    } else {
        let o1 = ItemPointerGetOffsetNumberNoCheck(arg1);
        let o2 = ItemPointerGetOffsetNumberNoCheck(arg2);
        if o1 < o2 {
            -1
        } else if o1 > o2 {
            1
        } else {
            0
        }
    }
}

/// `ItemPointerInc` (itemptr.c).
pub fn ItemPointerInc(pointer: &mut ItemPointerData) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);
    if off == u16::MAX {
        if blk != InvalidBlockNumber {
            off = 0;
            blk += 1;
        }
    } else {
        off += 1;
    }
    ItemPointerSet(pointer, blk, off);
}

/// `ItemPointerDec` (itemptr.c).
pub fn ItemPointerDec(pointer: &mut ItemPointerData) {
    let mut blk = ItemPointerGetBlockNumberNoCheck(pointer);
    let mut off = ItemPointerGetOffsetNumberNoCheck(pointer);
    if off == 0 {
        if blk != 0 {
            off = u16::MAX;
            blk -= 1;
        }
    } else {
        off -= 1;
    }
    ItemPointerSet(pointer, blk, off);
}

// ---------------------------------------------------------------------------
// itemid.h — ItemId accessors / mutators (thin wrappers over ItemIdData)
// ---------------------------------------------------------------------------

/// `ItemIdGetLength` (itemid.h).
pub fn ItemIdGetLength(itemId: &ItemIdData) -> ItemLength {
    itemId.lp_len()
}

/// `ItemIdGetOffset` (itemid.h).
pub fn ItemIdGetOffset(itemId: &ItemIdData) -> ItemOffset {
    itemId.lp_off()
}

/// `ItemIdGetFlags` (itemid.h).
pub fn ItemIdGetFlags(itemId: &ItemIdData) -> u32 {
    itemId.lp_flags()
}

/// `ItemIdGetRedirect` (itemid.h): a redirect stores its link in `lp_off`.
pub fn ItemIdGetRedirect(itemId: &ItemIdData) -> OffsetNumber {
    itemId.lp_off()
}

/// `ItemIdIsUsed` (itemid.h).
pub fn ItemIdIsUsed(itemId: &ItemIdData) -> bool {
    itemId.is_used()
}

/// `ItemIdIsNormal` (itemid.h).
pub fn ItemIdIsNormal(itemId: &ItemIdData) -> bool {
    itemId.is_normal()
}

/// `ItemIdIsRedirected` (itemid.h).
pub fn ItemIdIsRedirected(itemId: &ItemIdData) -> bool {
    itemId.is_redirected()
}

/// `ItemIdIsDead` (itemid.h).
pub fn ItemIdIsDead(itemId: &ItemIdData) -> bool {
    itemId.is_dead()
}

/// `ItemIdHasStorage` (itemid.h).
pub fn ItemIdHasStorage(itemId: &ItemIdData) -> bool {
    itemId.has_storage()
}

/// `ItemIdSetUnused` (itemid.h).
pub fn ItemIdSetUnused(itemId: &mut ItemIdData) {
    itemId.set_unused();
}

/// `ItemIdSetNormal` (itemid.h).
pub fn ItemIdSetNormal(itemId: &mut ItemIdData, off: ItemOffset, len: ItemLength) {
    itemId.set_normal(off, len);
}

/// `ItemIdSetRedirect` (itemid.h).
pub fn ItemIdSetRedirect(itemId: &mut ItemIdData, link: OffsetNumber) {
    itemId.set_redirect(link);
}

/// `ItemIdSetDead` (itemid.h).
pub fn ItemIdSetDead(itemId: &mut ItemIdData) {
    itemId.set_dead();
}

/// `ItemIdMarkDead` (itemid.h).
pub fn ItemIdMarkDead(itemId: &mut ItemIdData) {
    itemId.mark_dead();
}

// ---------------------------------------------------------------------------
// bufpage.h — PageXLogRecPtr helpers
// ---------------------------------------------------------------------------

/// `PageXLogRecPtrGet` (bufpage.h).
pub fn PageXLogRecPtrGet(val: PageXLogRecPtr) -> XLogRecPtr {
    val.lsn()
}

/// `PageXLogRecPtrSet` (bufpage.h).
pub fn PageXLogRecPtrSet(ptr: &mut PageXLogRecPtr, lsn: XLogRecPtr) {
    *ptr = PageXLogRecPtr::from_lsn(lsn);
}

// ---------------------------------------------------------------------------
// ItemIdData <-> raw u32 (the on-page 4-byte layout)
//
// The page's 4-byte line-pointer slots use the fixed bitfield layout
// (lp_off: bits 0-14, lp_flags: bits 15-16, lp_len: bits 17-31). We marshal
// `ItemIdData` to/from those 4 bytes through its public `new`/getter API.
// ---------------------------------------------------------------------------

fn item_id_from_raw(raw: u32) -> ItemIdData {
    let lp_off = (raw & 0x7fff) as ItemOffset;
    let lp_flags = (raw >> 15) & 0x0003;
    let lp_len = ((raw >> 17) & 0x7fff) as ItemLength;
    ItemIdData::new(lp_off, lp_flags, lp_len)
}

fn item_id_to_raw(item: &ItemIdData) -> u32 {
    (item.lp_off() as u32 & 0x7fff)
        | ((item.lp_flags() & 0x0003) << 15)
        | ((item.lp_len() as u32 & 0x7fff) << 17)
}

// ---------------------------------------------------------------------------
// Byte-level page views
//
// `PageRef` / `PageMut` are zero-cost wrappers over a validated, exactly
// `BLCKSZ`-long byte slice — the idiomatic stand-in for C's `Page` (a
// `char *`) and `PageHeader` (a `(PageHeader) page` overlay). Header fields and
// line pointers are accessed at their fixed byte offsets.
// ---------------------------------------------------------------------------

/// A read-only view of a formatted page buffer.
#[derive(Clone, Copy, Debug)]
pub struct PageRef<'a> {
    bytes: &'a [u8],
}

/// A mutable view of a formatted page buffer.
#[derive(Debug)]
pub struct PageMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> PageRef<'a> {
    /// Wrap a byte buffer that is at least `BLCKSZ` bytes, taking the first
    /// `BLCKSZ` of them as the page.
    pub fn new(bytes: &'a [u8]) -> PgResult<Self> {
        if bytes.len() < BLCKSZ {
            return Err(PgError::error("page buffer is smaller than BLCKSZ"));
        }
        Ok(Self {
            bytes: &bytes[..BLCKSZ],
        })
    }

    /// The page's bytes (exactly `BLCKSZ`).
    pub fn as_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    fn read_u16(&self, off: usize) -> u16 {
        u16::from_ne_bytes([self.bytes[off], self.bytes[off + 1]])
    }

    fn read_u32(&self, off: usize) -> u32 {
        u32::from_ne_bytes([
            self.bytes[off],
            self.bytes[off + 1],
            self.bytes[off + 2],
            self.bytes[off + 3],
        ])
    }

    fn pd_lsn(&self) -> PageXLogRecPtr {
        PageXLogRecPtr {
            xlogid: self.read_u32(OFF_PD_LSN),
            xrecoff: self.read_u32(OFF_PD_LSN + 4),
        }
    }

    /// `((PageHeader) page)->pd_checksum` — public read accessor (basebackup's
    /// per-page checksum verification reads this header field directly).
    pub fn pd_checksum(&self) -> u16 {
        self.read_u16(OFF_PD_CHECKSUM)
    }

    fn pd_flags(&self) -> u16 {
        self.read_u16(OFF_PD_FLAGS)
    }

    /// `((PageHeader) page)->pd_prune_xid` — public read accessor (heap page
    /// pruning reads the prune-hint XID directly, mirroring C's `PageHeader`
    /// field read).
    pub fn pd_prune_xid(&self) -> TransactionId {
        self.read_u32(OFF_PD_PRUNE_XID)
    }

    /// `((PageHeader) page)->pd_lower` — public read accessor (the heap-AM
    /// in-place update builds a post-mutation FPI image and must read the page's
    /// free-space boundaries directly, mirroring C's `PageHeader` field reads).
    pub fn pd_lower(&self) -> LocationIndex {
        self.read_u16(OFF_PD_LOWER)
    }

    /// `((PageHeader) page)->pd_upper` — public read accessor (see `pd_lower`).
    pub fn pd_upper(&self) -> LocationIndex {
        self.read_u16(OFF_PD_UPPER)
    }

    fn pd_special(&self) -> LocationIndex {
        self.read_u16(OFF_PD_SPECIAL)
    }

    fn pd_pagesize_version(&self) -> u16 {
        self.read_u16(OFF_PD_PAGESIZE_VERSION)
    }

    /// Read the line pointer at `offsetNumber` (1-based), or `None` if it is
    /// invalid or lies outside the buffer.
    fn item_id(&self, offsetNumber: OffsetNumber) -> Option<ItemIdData> {
        let index = item_index(offsetNumber).ok()?;
        let start = SizeOfPageHeaderData + index * ITEM_ID_SIZE;
        self.bytes
            .get(start..start + ITEM_ID_SIZE)
            .map(|chunk| item_id_from_raw(u32::from_ne_bytes(chunk.try_into().unwrap())))
    }
}

impl<'a> PageMut<'a> {
    /// Wrap a mutable byte buffer that is at least `BLCKSZ` bytes.
    pub fn new(bytes: &'a mut [u8]) -> PgResult<Self> {
        if bytes.len() < BLCKSZ {
            return Err(PgError::error("page buffer is smaller than BLCKSZ"));
        }
        Ok(Self {
            bytes: &mut bytes[..BLCKSZ],
        })
    }

    /// Read-only view of the same page.
    pub fn as_ref(&self) -> PageRef<'_> {
        PageRef { bytes: self.bytes }
    }

    /// The page's bytes (exactly `BLCKSZ`).
    pub fn as_bytes(&self) -> &[u8] {
        self.bytes
    }

    /// The page's bytes (exactly `BLCKSZ`), mutable.
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        self.bytes
    }

    fn write_u16(&mut self, off: usize, value: u16) {
        self.bytes[off..off + 2].copy_from_slice(&value.to_ne_bytes());
    }

    fn write_u32(&mut self, off: usize, value: u32) {
        self.bytes[off..off + 4].copy_from_slice(&value.to_ne_bytes());
    }

    fn pd_lower(&self) -> LocationIndex {
        self.as_ref().pd_lower()
    }

    fn pd_upper(&self) -> LocationIndex {
        self.as_ref().pd_upper()
    }

    fn pd_special(&self) -> LocationIndex {
        self.as_ref().pd_special()
    }

    fn pd_flags(&self) -> u16 {
        self.as_ref().pd_flags()
    }

    /// `((PageHeader) page)->pd_prune_xid` — public read accessor (heap page
    /// pruning reads the prune-hint XID directly).
    pub fn pd_prune_xid(&self) -> TransactionId {
        self.as_ref().read_u32(OFF_PD_PRUNE_XID)
    }

    fn set_pd_lsn(&mut self, lsn: PageXLogRecPtr) {
        self.write_u32(OFF_PD_LSN, lsn.xlogid);
        self.write_u32(OFF_PD_LSN + 4, lsn.xrecoff);
    }

    fn set_pd_checksum(&mut self, value: u16) {
        self.write_u16(OFF_PD_CHECKSUM, value);
    }

    fn set_pd_flags(&mut self, value: u16) {
        self.write_u16(OFF_PD_FLAGS, value);
    }

    fn set_pd_lower(&mut self, value: LocationIndex) {
        self.write_u16(OFF_PD_LOWER, value);
    }

    fn set_pd_upper(&mut self, value: LocationIndex) {
        self.write_u16(OFF_PD_UPPER, value);
    }

    fn set_pd_special(&mut self, value: LocationIndex) {
        self.write_u16(OFF_PD_SPECIAL, value);
    }

    fn set_pd_pagesize_version(&mut self, value: u16) {
        self.write_u16(OFF_PD_PAGESIZE_VERSION, value);
    }

    /// `((PageHeader) page)->pd_prune_xid = value` — public write accessor.
    /// `heap_page_prune_and_freeze`'s `do_hint` step sets the prune-hint XID to
    /// either zero or the lowest soon-prunable XID directly.
    pub fn set_pd_prune_xid(&mut self, value: TransactionId) {
        self.write_u32(OFF_PD_PRUNE_XID, value);
    }

    fn item_id(&self, offsetNumber: OffsetNumber) -> Option<ItemIdData> {
        self.as_ref().item_id(offsetNumber)
    }

    /// Write the line pointer at `offsetNumber` (1-based).
    ///
    /// `pub` so index kill-items write-back (hash `_hash_kill_items`
    /// `ItemIdMarkDead`) can re-stamp a mutated line pointer; in C the `ItemId`
    /// is an lvalue into the page, so marking it dead IS this write.
    pub fn set_item_id(&mut self, offsetNumber: OffsetNumber, item: ItemIdData) -> PgResult<()> {
        let index = item_index(offsetNumber)?;
        let start = SizeOfPageHeaderData + index * ITEM_ID_SIZE;
        let chunk = self
            .bytes
            .get_mut(start..start + ITEM_ID_SIZE)
            .ok_or_else(|| PgError::error("line pointer offset is outside page"))?;
        chunk.copy_from_slice(&item_id_to_raw(&item).to_ne_bytes());
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// bufpage.h — page size / version / special-space accessors
// ---------------------------------------------------------------------------

/// `PageIsEmpty` (bufpage.h).
pub fn PageIsEmpty(page: &PageRef<'_>) -> bool {
    page.pd_lower() as Size <= SizeOfPageHeaderData
}

/// `PageIsNew` (bufpage.h).
pub fn PageIsNew(page: &PageRef<'_>) -> bool {
    page.pd_upper() == 0
}

/// `PageGetPageSize` (bufpage.h): `pd_pagesize_version & 0xFF00`.
pub fn PageGetPageSize(page: &PageRef<'_>) -> Size {
    (page.pd_pagesize_version() & 0xff00) as Size
}

/// `PageGetPageLayoutVersion` (bufpage.h): `pd_pagesize_version & 0x00FF`.
pub fn PageGetPageLayoutVersion(page: &PageRef<'_>) -> u8 {
    (page.pd_pagesize_version() & 0x00ff) as u8
}

/// `PageSetPageSizeAndVersion` (bufpage.h).
pub fn PageSetPageSizeAndVersion(page: &mut PageMut<'_>, size: Size, version: u8) {
    debug_assert_eq!(size & 0xff00, size);
    page.set_pd_pagesize_version((size | version as Size) as u16);
}

/// `PageGetSpecialSize` (bufpage.h).
pub fn PageGetSpecialSize(page: &PageRef<'_>) -> u16 {
    (PageGetPageSize(page) - page.pd_special() as Size) as u16
}

/// `PageGetMaxOffsetNumber` (bufpage.h).
pub fn PageGetMaxOffsetNumber(page: &PageRef<'_>) -> OffsetNumber {
    let lower = page.pd_lower() as Size;
    if lower <= SizeOfPageHeaderData {
        0
    } else {
        ((lower - SizeOfPageHeaderData) / ITEM_ID_SIZE) as OffsetNumber
    }
}

/// `PageGetItemId` (bufpage.h): the line pointer at `offsetNumber` (1-based).
///
/// C returns an lvalue `ItemId` (a pointer into the page); here we return a
/// copy of the 4-byte line pointer. Mutating callers write back via
/// [`PageSetItemId`].
pub fn PageGetItemId(page: &PageRef<'_>, offsetNumber: OffsetNumber) -> PgResult<ItemIdData> {
    page.item_id(offsetNumber)
        .ok_or_else(|| PgError::error("line pointer offset is outside page"))
}

/// Write the line pointer at `offsetNumber` (1-based) back onto the page — the
/// C lvalue store through `PageGetItemId` (e.g.
/// `ItemIdSetUnused(PageGetItemId(page, off))`).
pub fn PageSetItemId(page: &mut PageMut<'_>, offsetNumber: OffsetNumber, item: ItemIdData) -> PgResult<()> {
    page.set_item_id(offsetNumber, item)
}

/// `PageGetContents` (bufpage.h): the area after the MAXALIGN'd page header,
/// for pages that do not use line pointers.
pub fn PageGetContents<'a>(page: &PageRef<'a>) -> PgResult<&'a [u8]> {
    page.bytes
        .get(maxalign(SizeOfPageHeaderData)..)
        .ok_or_else(|| PgError::error("page contents offset is outside page"))
}

/// `PageGetSpecialPointer` (bufpage.h) + the `PageValidateSpecialPointer`
/// assertions (here promoted to real range checks).
pub fn PageGetSpecialPointer<'a>(page: &PageRef<'a>) -> PgResult<&'a [u8]> {
    let special = page.pd_special() as usize;
    if special > BLCKSZ || special < SizeOfPageHeaderData {
        return Err(PgError::error("invalid page special pointer"));
    }
    page.bytes
        .get(special..PageGetPageSize(page))
        .ok_or_else(|| PgError::error("page special pointer is outside page"))
}

/// `PageGetItem` (bufpage.h): the on-page item data referenced by `itemId`.
pub fn PageGetItem<'a>(page: &PageRef<'a>, itemId: &ItemIdData) -> PgResult<&'a [u8]> {
    if !itemId.has_storage() {
        return Err(PgError::error("item identifier has no storage"));
    }
    let offset = itemId.lp_off() as usize;
    let len = itemId.lp_len() as usize;
    page.bytes
        .get(offset..offset + len)
        .ok_or_else(|| PgError::error("item storage is outside page"))
}

/// `PageGetLSN` (bufpage.h).
pub fn PageGetLSN(page: &PageRef<'_>) -> XLogRecPtr {
    page.pd_lsn().lsn()
}

/// `PageSetLSN` (bufpage.h).
pub fn PageSetLSN(page: &mut PageMut<'_>, lsn: XLogRecPtr) {
    page.set_pd_lsn(PageXLogRecPtr::from_lsn(lsn));
}

/// `PageHasFreeLinePointers` (bufpage.h).
pub fn PageHasFreeLinePointers(page: &PageRef<'_>) -> bool {
    page.pd_flags() & PD_HAS_FREE_LINES != 0
}

/// `PageSetHasFreeLinePointers` (bufpage.h).
pub fn PageSetHasFreeLinePointers(page: &mut PageMut<'_>) {
    let flags = page.pd_flags() | PD_HAS_FREE_LINES;
    page.set_pd_flags(flags);
}

/// `PageClearHasFreeLinePointers` (bufpage.h).
pub fn PageClearHasFreeLinePointers(page: &mut PageMut<'_>) {
    let flags = page.pd_flags() & !PD_HAS_FREE_LINES;
    page.set_pd_flags(flags);
}

/// `PageIsFull` (bufpage.h).
pub fn PageIsFull(page: &PageRef<'_>) -> bool {
    page.pd_flags() & PD_PAGE_FULL != 0
}

/// `PageSetFull` (bufpage.h).
pub fn PageSetFull(page: &mut PageMut<'_>) {
    let flags = page.pd_flags() | PD_PAGE_FULL;
    page.set_pd_flags(flags);
}

/// `PageClearFull` (bufpage.h).
pub fn PageClearFull(page: &mut PageMut<'_>) {
    let flags = page.pd_flags() & !PD_PAGE_FULL;
    page.set_pd_flags(flags);
}

/// `PageIsAllVisible` (bufpage.h).
pub fn PageIsAllVisible(page: &PageRef<'_>) -> bool {
    page.pd_flags() & PD_ALL_VISIBLE != 0
}

/// `PageSetAllVisible` (bufpage.h).
pub fn PageSetAllVisible(page: &mut PageMut<'_>) {
    let flags = page.pd_flags() | PD_ALL_VISIBLE;
    page.set_pd_flags(flags);
}

/// `PageClearAllVisible` (bufpage.h).
pub fn PageClearAllVisible(page: &mut PageMut<'_>) {
    let flags = page.pd_flags() & !PD_ALL_VISIBLE;
    page.set_pd_flags(flags);
}

/// `PageClearPrunable` (bufpage.h).
pub fn PageClearPrunable(page: &mut PageMut<'_>) {
    page.set_pd_prune_xid(0);
}

/// `FirstNormalTransactionId` (access/transam.h).
const FirstNormalTransactionId: TransactionId = 3;

/// `TransactionIdIsNormal` (access/transam.h): `(xid) >= FirstNormalTransactionId`.
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    xid >= FirstNormalTransactionId
}

/// `TransactionIdPrecedes` (transam.c): is `id1` logically `< id2`?
///
/// If either ID is a permanent (non-normal) XID then we do plain unsigned
/// comparison; if both are normal we do a modulo-2^32 comparison via the
/// signed difference, matching the C `(int32) (id1 - id2) < 0` test so that
/// the comparison stays correct across XID wraparound.
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `PageSetPrunable` (bufpage.h).
pub fn PageSetPrunable(page: &mut PageMut<'_>, xid: TransactionId) {
    let prune = page.pd_prune_xid();
    if prune == 0 || TransactionIdPrecedes(xid, prune) {
        page.set_pd_prune_xid(xid);
    }
}

// ---------------------------------------------------------------------------
// bufpage.c — page support functions
// ---------------------------------------------------------------------------

/// `PageInit` (bufpage.c): initialize the contents of a page.
pub fn PageInit(page: &mut [u8], pageSize: Size, specialSize: Size) -> PgResult<()> {
    let mut page = PageMut::new(page)?;
    let specialSize = maxalign(specialSize);

    // C: Assert(pageSize == BLCKSZ); Assert(pageSize > specialSize + SizeOfPageHeaderData);
    if pageSize != BLCKSZ {
        return Err(PgError::error("PageInit requires BLCKSZ page size"));
    }
    if pageSize <= specialSize + SizeOfPageHeaderData {
        return Err(PgError::error("PageInit special space is too large"));
    }

    // MemSet(p, 0, pageSize)
    page.as_mut_bytes()[..pageSize].fill(0);

    page.set_pd_flags(0);
    page.set_pd_lower(SizeOfPageHeaderData as LocationIndex);
    page.set_pd_upper((pageSize - specialSize) as LocationIndex);
    page.set_pd_special((pageSize - specialSize) as LocationIndex);
    PageSetPageSizeAndVersion(&mut page, pageSize, PG_PAGE_LAYOUT_VERSION);
    // pd_prune_xid = InvalidTransactionId already done by the MemSet.
    Ok(())
}

/// `PageIsVerified` (bufpage.c): check that the page header and checksum (if
/// any) appear valid.
///
/// Returns `(verified, checksum_failure)`, where `checksum_failure` is C's
/// `*checksum_failure_p` out-parameter.
pub fn PageIsVerified(page: &PageRef<'_>, blkno: BlockNumber, flags: i32) -> PgResult<(bool, bool)> {
    let mut checksum_failure = false;
    let mut header_sane = false;
    let mut checksum = 0_u16;
    let pd_checksum = page.pd_checksum();

    // Don't verify page data unless the page passes basic non-zero test.
    if !PageIsNew(page) {
        if backend_access_transam_xlog_seams::data_checksums_enabled::call() {
            checksum = checksum_page(page.bytes, blkno);
            if checksum != pd_checksum {
                checksum_failure = true;
            }
        }

        let p_flags = page.pd_flags();
        let p_lower = page.pd_lower();
        let p_upper = page.pd_upper();
        let p_special = page.pd_special();
        if (p_flags & !PD_VALID_FLAG_BITS) == 0
            && p_lower <= p_upper
            && p_upper <= p_special
            && p_special as usize <= BLCKSZ
            && p_special as usize == maxalign(p_special as usize)
        {
            header_sane = true;
        }

        if header_sane && !checksum_failure {
            return Ok((true, false));
        }
    }

    // Check all-zeroes case.
    if page.bytes.iter().all(|byte| *byte == 0) {
        return Ok((true, checksum_failure));
    }

    // Throw a WARNING/LOG, as instructed by PIV_LOG_*, on checksum failure, but
    // only after we've checked for the all-zeroes case.
    if checksum_failure {
        if flags & (PIV_LOG_WARNING | PIV_LOG_LOG) != 0 {
            let level = if flags & PIV_LOG_WARNING != 0 {
                WARNING
            } else {
                LOG
            };
            ereport(level)
                .errcode(ERRCODE_DATA_CORRUPTED)
                .errmsg(format!(
                    "page verification failed, calculated checksum {} but expected {}",
                    checksum, pd_checksum
                ))
                .finish(here("PageIsVerified"))?;
        }

        if header_sane && flags & PIV_IGNORE_CHECKSUM_FAILURE != 0 {
            return Ok((true, true));
        }
    }

    Ok((false, checksum_failure))
}

/// `PageAddItemExtended` (bufpage.c): add an item to a page.
///
/// Returns the offset at which the item was inserted, or `InvalidOffsetNumber`
/// if it was not inserted (a `WARNING` indicates why). `EREPORT(ERROR)` is
/// disallowed here; corrupted pointers raise `PANIC`.
pub fn PageAddItemExtended(
    page: &mut PageMut<'_>,
    item: &[u8],
    offsetNumber: OffsetNumber,
    flags: i32,
) -> PgResult<OffsetNumber> {
    let size = item.len();

    // Be wary about corrupted page pointers (note: no pd_special MAXALIGN check
    // here, matching bufpage.c). On failure -> PANIC.
    check_page_pointers(&page.as_ref(), "PageAddItemExtended", true, false)?;

    // Select offsetNumber to place the new item at.
    let limit = PageGetMaxOffsetNumber(&page.as_ref()) + 1; // OffsetNumberNext
    let mut offset_number = offsetNumber;
    let mut needshuffle = false;

    if OffsetNumberIsValid(offset_number) {
        // was offsetNumber passed in? yes, check it.
        if flags & PAI_OVERWRITE != 0 {
            if offset_number < limit {
                let item_id = page.item_id(offset_number).ok_or_else(|| {
                    PgError::error("PageAddItemExtended: line pointer below limit does not exist")
                })?;
                if item_id.is_used() || item_id.has_storage() {
                    elog(WARNING, "will not overwrite a used ItemId")?;
                    return Ok(INVALID_OFFSET_NUMBER);
                }
            }
        } else if offset_number < limit {
            needshuffle = true; // need to move existing linp's
        }
    } else if PageHasFreeLinePointers(&page.as_ref()) {
        // offsetNumber not passed in: scan for a "recyclable" (unused) ItemId.
        let mut found = INVALID_OFFSET_NUMBER;
        for off in FIRST_OFFSET_NUMBER..limit {
            let item_id = page.item_id(off).ok_or_else(|| {
                PgError::error("PageAddItemExtended: line pointer below limit does not exist")
            })?;
            // Unused items should never have storage; the C code asserts this.
            if !item_id.is_used() && !item_id.has_storage() {
                found = off;
                break;
            }
        }
        if found != INVALID_OFFSET_NUMBER {
            offset_number = found;
        } else {
            // The hint is wrong, so reset it; put item at limit.
            PageClearHasFreeLinePointers(page);
            offset_number = limit;
        }
    } else {
        // Don't bother searching if hint says there's no free slot.
        offset_number = limit;
    }

    // Reject placing items beyond the first unused line pointer.
    if offset_number > limit {
        elog(WARNING, "specified item offset is too large")?;
        return Ok(INVALID_OFFSET_NUMBER);
    }

    // Reject placing items beyond heap boundary, if heap.
    if flags & PAI_IS_HEAP != 0 && offset_number as usize > MaxHeapTuplesPerPage {
        elog(WARNING, "can't put more than MaxHeapTuplesPerPage items in a heap page")?;
        return Ok(INVALID_OFFSET_NUMBER);
    }

    // Compute new lower and upper pointers for page, see if it'll fit. Signed
    // arithmetic, to avoid mistakes if alignedSize > pd_upper.
    let lower = if offset_number == limit || needshuffle {
        page.pd_lower() as isize + ITEM_ID_SIZE as isize
    } else {
        page.pd_lower() as isize
    };
    let aligned_size = maxalign(size);
    let upper = page.pd_upper() as isize - aligned_size as isize;
    if lower > upper {
        return Ok(INVALID_OFFSET_NUMBER);
    }
    let upper = upper as usize;

    // OK to insert. First, shuffle the existing pointers if needed.
    if needshuffle {
        let start = SizeOfPageHeaderData + item_index(offset_number)? * ITEM_ID_SIZE;
        let nbytes = (limit - offset_number) as usize * ITEM_ID_SIZE;
        page.bytes
            .copy_within(start..start + nbytes, start + ITEM_ID_SIZE);
    }

    // Set the line pointer.
    let mut item_id = ItemIdData::default();
    item_id.set_normal(upper as ItemOffset, size as ItemLength);
    page.set_item_id(offset_number, item_id)?;

    // Copy the item's data onto the page.
    page.bytes[upper..upper + size].copy_from_slice(item);

    // Adjust page header.
    page.set_pd_lower(lower as LocationIndex);
    page.set_pd_upper(upper as LocationIndex);

    Ok(offset_number)
}

/// `PageGetTempPage` (bufpage.c): an uninitialized temporary page in local
/// memory. The idiomatic equivalent allocates an owned [`PageTemp`].
pub fn PageGetTempPage(page: &PageRef<'_>) -> PgResult<PageTemp> {
    PageTemp::new(PageGetPageSize(page))
}

/// `PageGetTempPageCopy` (bufpage.c): a temporary page initialized by copying
/// the given page.
pub fn PageGetTempPageCopy(page: &PageRef<'_>) -> PgResult<PageTemp> {
    let page_size = PageGetPageSize(page);
    let mut temp = PageTemp::new(page_size)?;
    temp.as_mut_bytes()[..page_size].copy_from_slice(&page.bytes[..page_size]);
    Ok(temp)
}

/// `PageGetTempPageCopySpecial` (bufpage.c): a temporary page `PageInit`'d with
/// the same special-space size, with the special space copied across.
pub fn PageGetTempPageCopySpecial(page: &PageRef<'_>) -> PgResult<PageTemp> {
    let page_size = PageGetPageSize(page);
    let mut temp = PageTemp::new(page_size)?;
    PageInit(temp.as_mut_bytes(), page_size, PageGetSpecialSize(page) as Size)?;
    let special = page.pd_special() as usize;
    temp.as_mut_bytes()[special..page_size].copy_from_slice(&page.bytes[special..page_size]);
    Ok(temp)
}

/// `PageRestoreTempPage` (bufpage.c): copy temporary page back to the permanent
/// page and release the temporary page (here, by consuming it).
pub fn PageRestoreTempPage(tempPage: PageTemp, oldPage: &mut PageMut<'_>) -> PgResult<()> {
    let temp_ref = PageRef::new(tempPage.as_bytes())?;
    let page_size = PageGetPageSize(&temp_ref);
    oldPage.as_mut_bytes()[..page_size].copy_from_slice(&tempPage.as_bytes()[..page_size]);
    Ok(())
}

/// `compactify_tuples` (bufpage.c): after removing or marking some line
/// pointers unused, move the tuples to remove the gaps and reorder them back
/// into reverse line-pointer order.
///
/// The C routine has hot-path optimizations (a presorted `memmove`-only path
/// and a scratch-buffer path) that produce the *same* result as the
/// straightforward "copy each kept tuple to the top of the page in itemidbase
/// order" loop. We implement that single faithful core: `itemidbase` is in the
/// order the tuples should end up (kept order = increasing line-pointer index),
/// and we pack each captured tuple down from `pd_special`. Because we stage the
/// moved tuple bytes in `itemidbase`'s own captured copies, no not-yet-moved
/// tuple can be clobbered (exactly what C's scratch buffer guards against).
fn compactify_tuples(page: &mut PageMut<'_>, itemidbase: &[ItemIdCompact<'_>]) {
    let mut upper = page.pd_special() as usize;
    for entry in itemidbase {
        upper -= entry.data.len();
        page.bytes[upper..upper + entry.data.len()].copy_from_slice(&entry.data);
        // Update the line pointer to reference the new offset; lp->lp_off = upper.
        let mut lp = page
            .item_id(entry.offsetindex + 1)
            .expect("compactify line pointer exists");
        lp.set_storage(upper as ItemOffset, lp.lp_len());
        page.set_item_id(entry.offsetindex + 1, lp)
            .expect("compactify line pointer in range");
    }
    page.set_pd_upper(upper as LocationIndex);
}

/// `PageRepairFragmentation` (bufpage.c): free fragmented space on a heap page
/// following pruning, and truncate trailing unused line pointers.
///
/// The crate OWNS a per-call [`MemoryContext`] (the accounting context for C's
/// compaction scratch space, built in `CurrentMemoryContext`). The worker's
/// `itemidbase` working buffer is charged to it; the context (and so the
/// buffer) drops when the function returns — invisible to callers.
pub fn PageRepairFragmentation(page: &mut PageMut<'_>) -> PgResult<()> {
    let ctx = MemoryContext::new("PageRepairFragmentation");
    repair_fragmentation(&ctx, page)
}

fn repair_fragmentation(ctx: &MemoryContext, page: &mut PageMut<'_>) -> PgResult<()> {
    let pd_lower = page.pd_lower() as usize;
    let pd_upper = page.pd_upper() as usize;
    let pd_special = page.pd_special() as usize;

    // Be more paranoid here than most places (full pd_special MAXALIGN check).
    // On failure -> ERROR.
    check_page_pointers(&page.as_ref(), "PageRepairFragmentation", false, true)?;

    let nline = PageGetMaxOffsetNumber(&page.as_ref());
    // At most `nline` (<= MaxOffsetNumber) tuples are captured; reserve against
    // that validated bound so the spine push never reallocates (C's compaction
    // scratch is a single bounded palloc).
    let mut itemidbase: PgVec<'_, ItemIdCompact<'_>> =
        vec_with_capacity_in(ctx.mcx(), nline as usize)?;
    let mut nunused: i32 = 0;
    let mut totallen: usize = 0;
    let mut finalusedlp = INVALID_OFFSET_NUMBER;

    for i in FIRST_OFFSET_NUMBER..=nline {
        let lp = page
            .item_id(i)
            .ok_or_else(|| PgError::error("PageRepairFragmentation: line pointer does not exist"))?;
        if lp.is_used() {
            if lp.has_storage() {
                let itemoff = lp.lp_off() as usize;
                if itemoff < pd_upper || itemoff >= pd_special {
                    return Err(errcode_loc(
                        format!("corrupted line pointer: {itemoff}"),
                        "PageRepairFragmentation",
                    ));
                }
                let alignedlen = maxalign(lp.lp_len() as usize);
                totallen += alignedlen;
                push_compact(
                    ctx,
                    &mut itemidbase,
                    i - 1,
                    &page.bytes[itemoff..itemoff + alignedlen],
                )?;
            }
            finalusedlp = i; // Could be the final non-LP_UNUSED item.
        } else {
            // Unused entries should have lp_len = 0; make sure.
            let mut lp = lp;
            lp.set_unused();
            page.set_item_id(i, lp)?;
            nunused += 1;
        }
    }

    if itemidbase.is_empty() {
        // Page is completely empty, so just reset it quickly.
        page.set_pd_upper(pd_special as LocationIndex);
    } else {
        // Need to compact the page the hard way.
        if totallen > pd_special - pd_lower {
            return Err(errcode_loc(
                format!(
                    "corrupted item lengths: total {}, available space {}",
                    totallen,
                    pd_special - pd_lower
                ),
                "PageRepairFragmentation",
            ));
        }
        compactify_tuples(page, &itemidbase);
    }

    if finalusedlp != nline {
        // The last line pointer is not the last used line pointer.
        let nunusedend = nline - finalusedlp;
        nunused -= nunusedend as i32;
        // Truncate the line pointer array.
        let new_lower = page.pd_lower() - nunusedend * ITEM_ID_SIZE as LocationIndex;
        page.set_pd_lower(new_lower);
    }

    // Set hint bit for PageAddItemExtended.
    if nunused > 0 {
        PageSetHasFreeLinePointers(page);
    } else {
        PageClearHasFreeLinePointers(page);
    }
    Ok(())
}

/// `PageTruncateLinePointerArray` (bufpage.c): remove unused line pointers at
/// the end of the line pointer array, never truncating to zero items.
pub fn PageTruncateLinePointerArray(page: &mut PageMut<'_>) {
    let nline = PageGetMaxOffsetNumber(&page.as_ref()) as i32;
    let mut countdone = false;
    let mut sethint = false;
    let mut nunusedend = 0_i32;

    // Scan line pointer array back-to-front.
    for i in (FIRST_OFFSET_NUMBER as i32..=nline).rev() {
        let lp = page.item_id(i as OffsetNumber).expect("line pointer exists");
        if !countdone && i > FIRST_OFFSET_NUMBER as i32 {
            if !lp.is_used() {
                nunusedend += 1;
            } else {
                countdone = true;
            }
        } else if !lp.is_used() {
            // An unused line pointer we won't truncate away: at least one.
            sethint = true;
            break;
        }
    }

    if nunusedend > 0 {
        let bytes = ITEM_ID_SIZE as LocationIndex * nunusedend as LocationIndex;
        let new_lower = page.pd_lower() - bytes;
        page.set_pd_lower(new_lower);

        // C: #ifdef CLOBBER_FREED_MEMORY -- debug-only poison of the truncated
        // line-pointer slots, compiled out by default like the C `#ifdef`.
        #[cfg(feature = "clobber_freed_memory")]
        {
            let lo = new_lower as usize;
            let hi = lo + bytes as usize;
            page.as_mut_bytes()[lo..hi].fill(0x7F);
        }
    }

    // Set hint bit for PageAddItemExtended.
    if sethint {
        PageSetHasFreeLinePointers(page);
    } else {
        PageClearHasFreeLinePointers(page);
    }
}

/// `PageGetFreeSpace` (bufpage.c): free space reduced by one new line pointer.
pub fn PageGetFreeSpace(page: &PageRef<'_>) -> Size {
    let space = page.pd_upper() as isize - page.pd_lower() as isize;
    if space < ITEM_ID_SIZE as isize {
        0
    } else {
        (space - ITEM_ID_SIZE as isize) as Size
    }
}

/// `PageGetFreeSpaceForMultipleTuples` (bufpage.c).
pub fn PageGetFreeSpaceForMultipleTuples(page: &PageRef<'_>, ntups: i32) -> Size {
    let space = page.pd_upper() as isize - page.pd_lower() as isize;
    let needed = ntups as isize * ITEM_ID_SIZE as isize;
    if space < needed {
        0
    } else {
        (space - needed) as Size
    }
}

/// `PageGetExactFreeSpace` (bufpage.c).
pub fn PageGetExactFreeSpace(page: &PageRef<'_>) -> Size {
    let space = page.pd_upper() as isize - page.pd_lower() as isize;
    if space < 0 {
        0
    } else {
        space as Size
    }
}

/// `PageGetHeapFreeSpace` (bufpage.c): like `PageGetFreeSpace`, but returns zero
/// if there are already `MaxHeapTuplesPerPage` line pointers and none are free.
pub fn PageGetHeapFreeSpace(page: &PageRef<'_>) -> Size {
    let mut space = PageGetFreeSpace(page);
    if space > 0 {
        let nline = PageGetMaxOffsetNumber(page);
        if nline as usize >= MaxHeapTuplesPerPage {
            if PageHasFreeLinePointers(page) {
                // Since this is just a hint, confirm there is a free line ptr.
                let mut offnum = FIRST_OFFSET_NUMBER;
                while offnum <= nline {
                    let lp = page.item_id(offnum).expect("line pointer exists");
                    if !lp.is_used() {
                        break;
                    }
                    offnum += 1;
                }
                if offnum > nline {
                    // The hint is wrong, but we can't clear it here.
                    space = 0;
                }
            } else {
                // The hint might be wrong, but PageAddItem believes it, so we
                // must too.
                space = 0;
            }
        }
    }
    space
}

/// `PageIndexTupleDelete` (bufpage.c): remove a tuple from an index page,
/// compacting out its line pointer.
pub fn PageIndexTupleDelete(page: &mut PageMut<'_>, offnum: OffsetNumber) -> PgResult<()> {
    check_page_pointers(&page.as_ref(), "PageIndexTupleDelete", false, true)?;

    let nline = PageGetMaxOffsetNumber(&page.as_ref());
    if offnum == 0 || offnum > nline {
        return elog_error(format!("invalid index offnum: {offnum}"));
    }

    let offidx = (offnum - 1) as usize;
    let tup = page
        .item_id(offnum)
        .ok_or_else(|| PgError::error("PageIndexTupleDelete: line pointer does not exist"))?;
    let size = tup.lp_len() as usize;
    let offset = tup.lp_off() as usize;

    let pd_upper = page.pd_upper() as usize;
    let pd_special = page.pd_special() as usize;
    if offset < pd_upper || offset + size > pd_special || offset != maxalign(offset) {
        return Err(errcode_loc(
            format!("corrupted line pointer: offset = {offset}, size = {size}"),
            "PageIndexTupleDelete",
        ));
    }

    // Amount of space to actually be deleted.
    let size = maxalign(size);

    // Get rid of the pd_linp entry for the index tuple: shift subsequent linp's
    // back one slot. nbytes = pd_lower - offset_of(pd_linp[offidx + 1]).
    let linp_offidx1 = SizeOfPageHeaderData + (offidx + 1) * ITEM_ID_SIZE;
    let nbytes = page.pd_lower() as isize - linp_offidx1 as isize;
    if nbytes > 0 {
        let dst = SizeOfPageHeaderData + offidx * ITEM_ID_SIZE;
        page.bytes
            .copy_within(linp_offidx1..linp_offidx1 + nbytes as usize, dst);
    }

    // Move tuple data between old pd_upper and the deleted tuple forward.
    if offset > pd_upper {
        page.bytes.copy_within(pd_upper..offset, pd_upper + size);
    }

    // Adjust free space boundary pointers.
    page.set_pd_upper((pd_upper + size) as LocationIndex);
    page.set_pd_lower(page.pd_lower() - ITEM_ID_SIZE as LocationIndex);

    // Adjust the linp entries that remain: anything before the deleted tuple's
    // data was moved forward by `size`.
    if !PageIsEmpty(&page.as_ref()) {
        let nline = PageGetMaxOffsetNumber(&page.as_ref()); // one less now
        for i in FIRST_OFFSET_NUMBER..=nline {
            let mut ii = page
                .item_id(i)
                .ok_or_else(|| PgError::error("PageIndexTupleDelete: line pointer does not exist"))?;
            if (ii.lp_off() as usize) <= offset {
                ii.set_storage(ii.lp_off() + size as ItemOffset, ii.lp_len());
                page.set_item_id(i, ii)?;
            }
        }
    }
    Ok(())
}

/// `PageIndexMultiDelete` (bufpage.c): delete multiple index tuples at once.
/// The caller *must* supply `itemnos` in increasing item-number order.
///
/// The crate OWNS a per-call [`MemoryContext`] charging the two working buffers
/// (`itemidbase` compaction scratch + the `newitemids` line-pointer copy, C's
/// fixed `newitemids[MaxIndexTuplesPerPage]` stack array). The context (and so
/// both buffers) drops when the function returns.
pub fn PageIndexMultiDelete(page: &mut PageMut<'_>, itemnos: &[OffsetNumber]) -> PgResult<()> {
    let nitems = itemnos.len();

    // If there aren't very many items to delete, retail PageIndexTupleDelete is
    // best. Delete in reverse order so item numbers don't need adjusting. (No
    // working buffers on this path, so no context is needed.)
    if nitems <= 2 {
        for &offnum in itemnos.iter().rev() {
            PageIndexTupleDelete(page, offnum)?;
        }
        return Ok(());
    }

    let ctx = MemoryContext::new("PageIndexMultiDelete");
    index_multi_delete(&ctx, page, itemnos)
}

fn index_multi_delete(
    ctx: &MemoryContext,
    page: &mut PageMut<'_>,
    itemnos: &[OffsetNumber],
) -> PgResult<()> {
    let nitems = itemnos.len();
    let pd_lower = page.pd_lower() as usize;
    let pd_upper = page.pd_upper() as usize;
    let pd_special = page.pd_special() as usize;

    check_page_pointers(&page.as_ref(), "PageIndexMultiDelete", false, true)?;

    // Scan the line pointer array, building the list of those we keep. Don't
    // modify the page yet, since we are still validity-checking.
    let nline = PageGetMaxOffsetNumber(&page.as_ref());
    // C uses a fixed `ItemIdData newitemids[MaxIndexTuplesPerPage]` stack array;
    // at most `nline` (<= MaxOffsetNumber line pointers per page) items are kept,
    // so reserve both working buffers against that validated bound rather than
    // growing unbounded.
    let mut itemidbase: PgVec<'_, ItemIdCompact<'_>> =
        vec_with_capacity_in(ctx.mcx(), nline as usize)?;
    let mut newitemids: PgVec<'_, ItemIdData> = vec_with_capacity_in(ctx.mcx(), nline as usize)?;
    let mut totallen: usize = 0;
    let mut nused = 0_usize;
    let mut nextitm = 0_usize;

    for offnum in FIRST_OFFSET_NUMBER..=nline {
        let lp = page
            .item_id(offnum)
            .ok_or_else(|| PgError::error("PageIndexMultiDelete: line pointer does not exist"))?;
        let size = lp.lp_len() as usize;
        let offset = lp.lp_off() as usize;
        if offset < pd_upper || offset + size > pd_special || offset != maxalign(offset) {
            return Err(errcode_loc(
                format!("corrupted line pointer: offset = {offset}, size = {size}"),
                "PageIndexMultiDelete",
            ));
        }

        if nextitm < nitems && offnum == itemnos[nextitm] {
            // Skip item to be deleted.
            nextitm += 1;
        } else {
            let alignedlen = maxalign(size);
            totallen += alignedlen;
            push_compact(
                ctx,
                &mut itemidbase,
                nused as OffsetNumber,
                &page.bytes[offset..offset + alignedlen],
            )?;
            // Spine pre-reserved above against `nline`; push cannot grow further.
            newitemids.push(lp);
            nused += 1;
        }
    }

    // This catches invalid or out-of-order itemnos[].
    if nextitm != nitems {
        return elog_error("incorrect index offsets supplied");
    }

    if totallen > pd_special - pd_lower {
        return Err(errcode_loc(
            format!(
                "corrupted item lengths: total {}, available space {}",
                totallen,
                pd_special - pd_lower
            ),
            "PageIndexMultiDelete",
        ));
    }

    // Overwrite the line pointers with the copy, from which we've removed all
    // the unused items.
    for (index, item) in newitemids.iter().enumerate() {
        page.set_item_id(index as OffsetNumber + 1, *item)?;
    }
    page.set_pd_lower((SizeOfPageHeaderData + nused * ITEM_ID_SIZE) as LocationIndex);

    // And compactify the tuple data.
    if nused > 0 {
        compactify_tuples(page, &itemidbase);
    } else {
        page.set_pd_upper(pd_special as LocationIndex);
    }
    Ok(())
}

/// `PageIndexTupleDeleteNoCompact` (bufpage.c): set the tuple's line pointer to
/// "unused" instead of compacting it out (or zap it if it's the last one).
pub fn PageIndexTupleDeleteNoCompact(page: &mut PageMut<'_>, offnum: OffsetNumber) -> PgResult<()> {
    check_page_pointers(&page.as_ref(), "PageIndexTupleDeleteNoCompact", false, true)?;

    let mut nline = PageGetMaxOffsetNumber(&page.as_ref());
    if offnum == 0 || offnum > nline {
        return elog_error(format!("invalid index offnum: {offnum}"));
    }

    let tup = page.item_id(offnum).ok_or_else(|| {
        PgError::error("PageIndexTupleDeleteNoCompact: line pointer does not exist")
    })?;
    let size = tup.lp_len() as usize;
    let offset = tup.lp_off() as usize;

    let pd_upper = page.pd_upper() as usize;
    let pd_special = page.pd_special() as usize;
    if offset < pd_upper || offset + size > pd_special || offset != maxalign(offset) {
        return Err(errcode_loc(
            format!("corrupted line pointer: offset = {offset}, size = {size}"),
            "PageIndexTupleDeleteNoCompact",
        ));
    }

    // Amount of space to actually be deleted.
    let size = maxalign(size);

    // Either set the line pointer to "unused", or zap it if it's the last one.
    if offnum < nline {
        let mut tup = tup;
        tup.set_unused();
        page.set_item_id(offnum, tup)?;
    } else {
        page.set_pd_lower(page.pd_lower() - ITEM_ID_SIZE as LocationIndex);
        nline -= 1; // one less than when we started
    }

    // Move tuple data between old pd_upper and the deleted tuple forward.
    if offset > pd_upper {
        page.bytes.copy_within(pd_upper..offset, pd_upper + size);
    }

    // Adjust free space boundary pointer.
    page.set_pd_upper((pd_upper + size) as LocationIndex);

    // Adjust the linp entries that remain.
    if !PageIsEmpty(&page.as_ref()) {
        for i in FIRST_OFFSET_NUMBER..=nline {
            let mut ii = page.item_id(i).ok_or_else(|| {
                PgError::error("PageIndexTupleDeleteNoCompact: line pointer does not exist")
            })?;
            if ii.has_storage() && (ii.lp_off() as usize) <= offset {
                ii.set_storage(ii.lp_off() + size as ItemOffset, ii.lp_len());
                page.set_item_id(i, ii)?;
            }
        }
    }
    Ok(())
}

/// `PageIndexTupleOverwrite` (bufpage.c): replace a tuple on an index page in
/// place, shifting other tuple data as needed. Returns `false` on insufficient
/// space; other problems are data corruption and `elog(ERROR)`.
pub fn PageIndexTupleOverwrite(
    page: &mut PageMut<'_>,
    offnum: OffsetNumber,
    newtup: &[u8],
) -> PgResult<bool> {
    let newsize = newtup.len();
    check_page_pointers(&page.as_ref(), "PageIndexTupleOverwrite", false, true)?;

    let itemcount = PageGetMaxOffsetNumber(&page.as_ref());
    if offnum == 0 || offnum > itemcount {
        return elog_error(format!("invalid index offnum: {offnum}"));
    }

    let tupid = page
        .item_id(offnum)
        .ok_or_else(|| PgError::error("PageIndexTupleOverwrite: line pointer does not exist"))?;
    let oldsize = tupid.lp_len() as usize;
    let offset = tupid.lp_off() as usize;

    let pd_upper = page.pd_upper() as usize;
    let pd_lower = page.pd_lower() as usize;
    let pd_special = page.pd_special() as usize;
    if offset < pd_upper || offset + oldsize > pd_special || offset != maxalign(offset) {
        return Err(errcode_loc(
            format!("corrupted line pointer: offset = {offset}, size = {oldsize}"),
            "PageIndexTupleOverwrite",
        ));
    }

    // Determine actual change in space requirement, check for page overflow.
    let oldsize = maxalign(oldsize);
    let alignednewsize = maxalign(newsize);
    if alignednewsize > oldsize + (pd_upper - pd_lower) {
        return Ok(false);
    }

    // size_diff = oldsize - alignednewsize: the amount the tuple shrinks, i.e.
    // the delta added to pd_upper and affected line pointers.
    let size_diff = oldsize as isize - alignednewsize as isize;
    if size_diff != 0 {
        // Relocate all tuple data before the target tuple.
        let new_upper = (pd_upper as isize + size_diff) as usize;
        if offset > pd_upper {
            page.bytes.copy_within(pd_upper..offset, new_upper);
        }
        page.set_pd_upper(new_upper as LocationIndex);

        // Adjust affected line pointers; allow items without storage (BRIN).
        for i in FIRST_OFFSET_NUMBER..=itemcount {
            let mut ii = page.item_id(i).ok_or_else(|| {
                PgError::error("PageIndexTupleOverwrite: line pointer does not exist")
            })?;
            if ii.has_storage() && (ii.lp_off() as usize) <= offset {
                let new_off = (ii.lp_off() as isize + size_diff) as usize;
                ii.set_storage(new_off as ItemOffset, ii.lp_len());
                page.set_item_id(i, ii)?;
            }
        }
    }

    // Update the item's tuple length without changing its lp_flags field:
    // tupid->lp_off = offset + size_diff; tupid->lp_len = newsize.
    let new_offset = (offset as isize + size_diff) as usize;
    let mut tupid = page
        .item_id(offnum)
        .ok_or_else(|| PgError::error("PageIndexTupleOverwrite: line pointer does not exist"))?;
    tupid.set_storage(new_offset as ItemOffset, newsize as ItemLength);
    page.set_item_id(offnum, tupid)?;

    // Copy new tuple data onto page.
    page.bytes[new_offset..new_offset + newsize].copy_from_slice(newtup);
    Ok(true)
}

/// `PageSetChecksumCopy` (bufpage.c): return a checksummed copy of the page.
///
/// In C this writes the checksum into a process-static scratch buffer and
/// returns it; here it returns an owned [`PageTemp`] copy. If checksums are off
/// or the page is new, the copy carries the page unchanged.
pub fn PageSetChecksumCopy(page: &PageRef<'_>, blkno: BlockNumber) -> PgResult<PageTemp> {
    let mut temp = PageGetTempPageCopy(page)?;
    if !PageIsNew(page) && backend_access_transam_xlog_seams::data_checksums_enabled::call() {
        let checksum = checksum_page(temp.as_bytes(), blkno);
        let mut temp_page = PageMut::new(temp.as_mut_bytes())?;
        temp_page.set_pd_checksum(checksum);
    }
    Ok(temp)
}

/// `PageSetChecksumInplace` (bufpage.c): set the checksum in private memory.
/// Must only be used when no other process can be modifying the buffer.
pub fn PageSetChecksumInplace(page: &mut PageMut<'_>, blkno: BlockNumber) {
    if PageIsNew(&page.as_ref()) || !backend_access_transam_xlog_seams::data_checksums_enabled::call() {
        return;
    }
    let checksum = checksum_page(page.as_bytes(), blkno);
    page.set_pd_checksum(checksum);
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Captured copy of a kept tuple plus where it belongs, the idiomatic owned
/// counterpart of C's `itemIdCompactData` (`offsetindex`, `itemoff`,
/// `alignedlen`). We carry the tuple bytes directly so `compactify_tuples` can
/// pack them without clobbering not-yet-moved tuples. The bytes are charged to
/// the crate's per-call compaction [`MemoryContext`].
struct ItemIdCompact<'mcx> {
    /// `offsetindex` — line-pointer array index (0-based).
    offsetindex: OffsetNumber,
    /// `MAXALIGN(item data len)` bytes of the tuple, as captured from the page.
    data: PgVec<'mcx, u8>,
}

/// Push a captured tuple onto the compaction list, charging the captured bytes
/// and the list spine to `ctx` (the crate's per-call compaction context). The
/// data is page-derived and bounded by `BLCKSZ`, but we still grow fallibly so
/// the charge is accounted exactly.
fn push_compact<'mcx>(
    ctx: &'mcx MemoryContext,
    base: &mut PgVec<'mcx, ItemIdCompact<'mcx>>,
    offsetindex: OffsetNumber,
    data: &[u8],
) -> PgResult<()> {
    let owned = mcx::slice_in(ctx.mcx(), data)?;
    base.push(ItemIdCompact { offsetindex, data: owned });
    Ok(())
}

fn checksum_page(bytes: &[u8], blkno: BlockNumber) -> u16 {
    let mut page = [0_u8; BLCKSZ];
    page.copy_from_slice(&bytes[..BLCKSZ]);
    pg_checksum_page(&mut page, blkno)
}

/// The shared "corrupted page pointers" paranoia check used by
/// `PageAddItemExtended` and the index/repair routines.
///
/// `panic == true` (only `PageAddItemExtended`) reports `PANIC`, because that
/// caller is in an `EREPORT(ERROR) IS DISALLOWED` context (bufpage.c) and also
/// skips the `pd_special == MAXALIGN(pd_special)` clause. The index/repair
/// callers pass `panic == false` and report `ERROR` with the extra MAXALIGN
/// clause.
fn check_page_pointers(
    page: &PageRef<'_>,
    function: &str,
    panic: bool,
    check_special_maxalign: bool,
) -> PgResult<()> {
    let lower = page.pd_lower();
    let upper = page.pd_upper();
    let special = page.pd_special();
    let corrupt = (lower as Size) < SizeOfPageHeaderData
        || lower > upper
        || upper > special
        || special as usize > BLCKSZ
        || (check_special_maxalign && special as usize != maxalign(special as usize));
    if !corrupt {
        return Ok(());
    }

    let message =
        format!("corrupted page pointers: lower = {lower}, upper = {upper}, special = {special}");
    if panic {
        return ereport(PANIC)
            .errcode(ERRCODE_DATA_CORRUPTED)
            .errmsg(message)
            .finish(here(function));
    }
    Err(errcode_loc(message, function))
}

/// Build a data-corruption `PgError` (sqlstate `ERRCODE_DATA_CORRUPTED`) tagged
/// with the source function — the `Err(...)`-returning counterpart of C's
/// `ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED), errmsg(...)))`.
fn errcode_loc(message: impl Into<String>, function: &str) -> PgError {
    PgError::error(message)
        .with_sqlstate(ERRCODE_DATA_CORRUPTED)
        .with_error_location(here(function))
}

fn elog_error<T>(message: impl Into<String>) -> PgResult<T> {
    match elog(ERROR, message) {
        Err(error) => Err(error),
        Ok(()) => unreachable!("ERROR level elog must return an error"),
    }
}

fn here(funcname: &str) -> ErrorLocation {
    ErrorLocation {
        filename: None,
        lineno: 0,
        funcname: Some(funcname.into()),
    }
}

/// `OffsetNumberIsValid` (off.h).
fn OffsetNumberIsValid(offsetNumber: OffsetNumber) -> bool {
    offsetNumber != INVALID_OFFSET_NUMBER && offsetNumber <= MaxOffsetNumber
}

/// 1-based offset number -> 0-based line-pointer index.
fn item_index(offset: OffsetNumber) -> PgResult<usize> {
    if !OffsetNumberIsValid(offset) {
        return Err(PgError::error("invalid offset number"));
    }
    Ok(offset as usize - 1)
}

/// `MAXALIGN` (c.h): round up to the 8-byte maximum alignment boundary.
fn maxalign(size: Size) -> Size {
    (size + 7) & !7
}

/// This crate owns no inward seams (no consumer calls a `bufpage.c` /
/// `itemptr.c` function across a cycle on the current frontier — the page code
/// is self-contained arithmetic), so `init_seams()` is a no-op. Its one
/// external, the `data_checksums_enabled` GUC, is owned by `xlog` and called
/// through `backend-access-transam-xlog-seams`.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
