//! On-disk / ABI structures and constants for the BRIN access method.
//!
//! These mirror `src/include/access/brin_page.h`, `brin_tuple.h`,
//! `brin_revmap.h` and `brin.h` from PostgreSQL 18.3.  The on-disk page
//! structs (`BrinSpecialSpace`, `BrinMetaPageData`, `BrinTuple`) live here as
//! `#[repr(C)]` with compile-time layout assertions.  The in-memory working
//! structs (`BrinMemTuple`, `BrinValues`, `BrinRevmap`) cross the crate
//! boundary as opaque ABI handles and are described here too; their full
//! field-by-field runtime semantics live inside `backend-access-brin`.

use core::ffi::c_void;

use crate::storage::{ItemIdData, SizeOfPageHeaderData};
use crate::{uint16, uint32, uint8, AttrNumber, BlockNumber, Datum, ItemPointerData, BLCKSZ};

/// `MAXALIGN_OF` mirroring the platform `MAXIMUM_ALIGNOF`.
const MAXIMUM_ALIGNOF: usize = 8;

const fn maxalign(size: usize) -> usize {
    (size + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

const fn maxalign_down(size: usize) -> usize {
    size & !(MAXIMUM_ALIGNOF - 1)
}

/// `offsetof(RevmapContents, rm_tids)` -- the `RevmapContents` struct begins
/// directly with the `rm_tids[1]` array, so this offset is 0.
pub const REVMAP_RM_TIDS_OFFSET: usize = 0;

/// `REVMAP_CONTENT_SIZE` (brin_page.h): bytes of a revmap page available for
/// the `rm_tids` `ItemPointerData` array.
pub const REVMAP_CONTENT_SIZE: usize = BLCKSZ
    - maxalign(SizeOfPageHeaderData)
    - REVMAP_RM_TIDS_OFFSET
    - maxalign(core::mem::size_of::<BrinSpecialSpace>());

/// `REVMAP_PAGE_MAXITEMS` (brin_page.h): max number of `ItemPointerData`
/// entries per revmap page.
pub const REVMAP_PAGE_MAXITEMS: usize =
    REVMAP_CONTENT_SIZE / core::mem::size_of::<ItemPointerData>();

/// `BrinMaxItemSize` (brin_pageops.c): largest item that fits in a regular
/// BRIN page (BRIN tolerates a single item per page).
pub const BRIN_MAX_ITEM_SIZE: usize = maxalign_down(
    BLCKSZ
        - (maxalign(SizeOfPageHeaderData + core::mem::size_of::<ItemIdData>())
            + maxalign(core::mem::size_of::<BrinSpecialSpace>())),
);

/// `BRIN_CURRENT_VERSION` -- on-disk format version (brin_page.h).
pub const BRIN_CURRENT_VERSION: uint32 = 1;
/// `BRIN_META_MAGIC` -- magic in the metapage (brin_page.h).
pub const BRIN_META_MAGIC: uint32 = 0xA8109CFA;
/// `BRIN_METAPAGE_BLKNO` -- metapage block number (brin_page.h).
pub const BRIN_METAPAGE_BLKNO: BlockNumber = 0;

// Tuple `bt_info` bit layout (brin_tuple.h).
//
//   7th (high) bit: has nulls
//   6th bit:        is placeholder tuple
//   5th bit:        range is empty
//   4-0 bit:        offset of data
pub const BRIN_OFFSET_MASK: uint8 = 0x1F;
pub const BRIN_EMPTY_RANGE_MASK: uint8 = 0x20; // range is empty
pub const BRIN_PLACEHOLDER_MASK: uint8 = 0x40; // is placeholder tuple
pub const BRIN_NULLS_MASK: uint8 = 0x80; // has nulls

// Special-space page-type identifiers (brin_page.h).
/// `BRIN_PAGETYPE_META`.
pub const BRIN_PAGETYPE_META: uint16 = 0xF091;
/// `BRIN_PAGETYPE_REVMAP`.
pub const BRIN_PAGETYPE_REVMAP: uint16 = 0xF092;
/// `BRIN_PAGETYPE_REGULAR`.
pub const BRIN_PAGETYPE_REGULAR: uint16 = 0xF093;

/// `BRIN_EVACUATE_PAGE` -- flag in `BrinSpecialSpace` (brin_page.h).
pub const BRIN_EVACUATE_PAGE: uint16 = 1 << 0;

/// `BRIN_IS_META_PAGE(page)` predicate over a page-type half-word.
#[inline]
pub const fn BRIN_IS_META_PAGE_TYPE(ty: uint16) -> bool {
    ty == BRIN_PAGETYPE_META
}

/// `BRIN_IS_REVMAP_PAGE(page)` predicate over a page-type half-word.
#[inline]
pub const fn BRIN_IS_REVMAP_PAGE_TYPE(ty: uint16) -> bool {
    ty == BRIN_PAGETYPE_REVMAP
}

/// `BRIN_IS_REGULAR_PAGE(page)` predicate over a page-type half-word.
#[inline]
pub const fn BRIN_IS_REGULAR_PAGE_TYPE(ty: uint16) -> bool {
    ty == BRIN_PAGETYPE_REGULAR
}

/// `SizeOfBrinTuple` -- `offsetof(BrinTuple, bt_info) + sizeof(uint8)`.
pub const SIZE_OF_BRIN_TUPLE: usize = 5;

/// `BrinSpecialSpace` -- BRIN page special area (brin_page.h).
///
/// `uint16 vector[MAXALIGN(1) / sizeof(uint16)]`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrinSpecialSpace {
    pub vector: [uint16; maxalign(1) / core::mem::size_of::<uint16>()],
}

/// `BrinMetaPageData` -- the contents of the BRIN metapage (brin_page.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrinMetaPageData {
    pub brinMagic: uint32,
    pub brinVersion: uint32,
    pub pagesPerRange: BlockNumber,
    pub lastRevmapPage: BlockNumber,
}

/// `BrinTuple` -- the on-disk BRIN index tuple header (brin_tuple.h).
///
/// `bt_info` packs flags + the offset of the data area; the data (null bitmaps
/// and per-column values) follows the header on disk.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrinTuple {
    /// heap block number that the tuple is for.
    pub bt_blkno: BlockNumber,
    /// flags + data offset (see `BRIN_*_MASK`).
    pub bt_info: uint8,
}

/// `BrinValues` -- per-column accumulated values within a `BrinMemTuple`
/// (brin_tuple.h).  This is an in-memory ABI struct, not on disk; the trailing
/// callback pointer and memory context are opaque to the FFI layer.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BrinValues {
    /// index attribute number.
    pub bv_attno: AttrNumber,
    /// are there any nulls in the page range?
    pub bv_hasnulls: bool,
    /// are all values nulls in the page range?
    pub bv_allnulls: bool,
    /// current accumulated values (`Datum *`).
    pub bv_values: *mut Datum,
    /// expanded accumulated values.
    pub bv_mem_value: Datum,
    /// memory context (`MemoryContext`, opaque).
    pub bv_context: *mut c_void,
    /// serialize callback (`brin_serialize_callback_type`, opaque).
    pub bv_serialize: *mut c_void,
}

/// `BrinMemTuple` -- the in-memory (deformed) BRIN tuple (brin_tuple.h).
///
/// In-memory ABI struct: `bt_columns` is a flexible array member modelled as a
/// zero-length array; the pointer members are opaque to the FFI layer.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BrinMemTuple {
    /// this is a placeholder tuple.
    pub bt_placeholder: bool,
    /// range represents no tuples.
    pub bt_empty_range: bool,
    /// heap blkno that the tuple is for.
    pub bt_blkno: BlockNumber,
    /// memcxt holding the `bt_columns` values (`MemoryContext`, opaque).
    pub bt_context: *mut c_void,
    /// values array (`Datum *`).
    pub bt_values: *mut Datum,
    /// allnulls array (`bool *`).
    pub bt_allnulls: *mut bool,
    /// hasnulls array (`bool *`).
    pub bt_hasnulls: *mut bool,
    /// per-column output array (flexible array member); must be last.
    pub bt_columns: [BrinValues; 0],
}

// ---------------------------------------------------------------------------
// WAL record definitions (brin_xlog.h).
// ---------------------------------------------------------------------------

/// `XLOG_BRIN_*` info-byte op codes (brin_xlog.h).
pub const XLOG_BRIN_CREATE_INDEX: uint8 = 0x00;
pub const XLOG_BRIN_INSERT: uint8 = 0x10;
pub const XLOG_BRIN_UPDATE: uint8 = 0x20;
pub const XLOG_BRIN_SAMEPAGE_UPDATE: uint8 = 0x30;
pub const XLOG_BRIN_REVMAP_EXTEND: uint8 = 0x40;
pub const XLOG_BRIN_DESUMMARIZE: uint8 = 0x50;
pub const XLOG_BRIN_OPMASK: uint8 = 0x70;
/// `XLOG_BRIN_INIT_PAGE` -- restore the entire page in redo (brin_xlog.h).
pub const XLOG_BRIN_INIT_PAGE: uint8 = 0x80;

/// `xl_brin_insert` (brin_xlog.h): heapBlk, pagesPerRange, offnum.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_brin_insert {
    pub heapBlk: BlockNumber,
    pub pagesPerRange: BlockNumber,
    pub offnum: uint16,
}

/// `SizeOfBrinInsert` = `offsetof(xl_brin_insert, offnum) + sizeof(OffsetNumber)`.
pub const SizeOfBrinInsert: usize =
    core::mem::offset_of!(xl_brin_insert, offnum) + core::mem::size_of::<uint16>();

impl xl_brin_insert {
    /// Serialize the leading `SizeOfBrinInsert` bytes for `XLogRegisterData`.
    pub fn to_bytes(&self) -> [u8; SizeOfBrinInsert] {
        let mut b = [0u8; SizeOfBrinInsert];
        b[0..4].copy_from_slice(&self.heapBlk.to_ne_bytes());
        b[4..8].copy_from_slice(&self.pagesPerRange.to_ne_bytes());
        b[8..10].copy_from_slice(&self.offnum.to_ne_bytes());
        b
    }
}

/// `xl_brin_update` (brin_xlog.h): oldOffnum + embedded `xl_brin_insert`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_brin_update {
    pub oldOffnum: uint16,
    pub insert: xl_brin_insert,
}

/// `SizeOfBrinUpdate` = `offsetof(xl_brin_update, insert) + SizeOfBrinInsert`.
pub const SizeOfBrinUpdate: usize =
    core::mem::offset_of!(xl_brin_update, insert) + SizeOfBrinInsert;

impl xl_brin_update {
    pub fn to_bytes(&self) -> [u8; SizeOfBrinUpdate] {
        let mut b = [0u8; SizeOfBrinUpdate];
        b[0..2].copy_from_slice(&self.oldOffnum.to_ne_bytes());
        let off = core::mem::offset_of!(xl_brin_update, insert);
        b[off..off + SizeOfBrinInsert].copy_from_slice(&self.insert.to_bytes());
        b
    }
}

/// `xl_brin_samepage_update` (brin_xlog.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_brin_samepage_update {
    pub offnum: uint16,
}

/// `SizeOfBrinSamepageUpdate` = `sizeof(OffsetNumber)`.
pub const SizeOfBrinSamepageUpdate: usize = core::mem::size_of::<uint16>();

impl xl_brin_samepage_update {
    pub fn to_bytes(&self) -> [u8; SizeOfBrinSamepageUpdate] {
        self.offnum.to_ne_bytes()
    }
}

/// `xl_brin_revmap_extend` (brin_xlog.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_brin_revmap_extend {
    pub targetBlk: BlockNumber,
}

/// `SizeOfBrinRevmapExtend`.
pub const SizeOfBrinRevmapExtend: usize =
    core::mem::offset_of!(xl_brin_revmap_extend, targetBlk) + core::mem::size_of::<BlockNumber>();

impl xl_brin_revmap_extend {
    pub fn to_bytes(&self) -> [u8; SizeOfBrinRevmapExtend] {
        self.targetBlk.to_ne_bytes()
    }
}

/// `xl_brin_desummarize` (brin_xlog.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct xl_brin_desummarize {
    pub pagesPerRange: BlockNumber,
    pub heapBlk: BlockNumber,
    pub regOffset: uint16,
}

/// `SizeOfBrinDesummarize`.
pub const SizeOfBrinDesummarize: usize =
    core::mem::offset_of!(xl_brin_desummarize, regOffset) + core::mem::size_of::<uint16>();

impl xl_brin_desummarize {
    pub fn to_bytes(&self) -> [u8; SizeOfBrinDesummarize] {
        let mut b = [0u8; SizeOfBrinDesummarize];
        b[0..4].copy_from_slice(&self.pagesPerRange.to_ne_bytes());
        b[4..8].copy_from_slice(&self.heapBlk.to_ne_bytes());
        b[8..10].copy_from_slice(&self.regOffset.to_ne_bytes());
        b
    }
}

/// `BrinRevmap` -- the in-memory revmap-access state (brin_revmap.c, opaque
/// struct).  Carried across the crate boundary as an ABI handle; the buffer and
/// cached metapage fields live in `backend-access-brin`.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BrinRevmap {
    /// index relation (`Relation`, opaque).
    pub rm_irel: *mut c_void,
    /// pages per range.
    pub rm_pagesPerRange: BlockNumber,
    /// last revmap page, cached from the metapage.
    pub rm_lastRevmapPage: BlockNumber,
    /// metapage buffer (`Buffer` is an `int`).
    pub rm_metaBuf: i32,
    /// current revmap buffer (`Buffer`).
    pub rm_currBuf: i32,
}

// ---------------------------------------------------------------------------
// Compile-time layout assertions.
// ---------------------------------------------------------------------------

const _: () = {
    // BrinSpecialSpace: MAXALIGN(1) bytes of uint16 storage = 8 / 2 = 4 u16s.
    assert!(core::mem::size_of::<BrinSpecialSpace>() == maxalign(1));
    assert!(core::mem::align_of::<BrinSpecialSpace>() == 2);

    // BrinMetaPageData: 4 * uint32-sized fields = 16.
    assert!(core::mem::size_of::<BrinMetaPageData>() == 16);
    assert!(core::mem::align_of::<BrinMetaPageData>() == 4);

    // BrinTuple: BlockNumber (4) + uint8 (1), padded to align 4 = 8.
    assert!(core::mem::offset_of!(BrinTuple, bt_info) == 4);
    assert!(core::mem::align_of::<BrinTuple>() == 4);
};
