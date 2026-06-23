//! On-disk / ABI structures and constants for the GIN access method.
//!
//! These mirror `src/include/access/ginblock.h` and `gin_private.h` from
//! PostgreSQL 18.3.  Only the types that genuinely cross the on-disk /
//! page-layout boundary live here as `#[repr(C)]` with compile-time layout
//! assertions; the purely internal runtime structures of the access method
//! (`GinState`, scan opaques, build state, ...) live inside `backend-access-gin`
//! itself as idiomatic Rust.

use crate::{uint16, uint32, BlockIdData, BlockNumber, ItemPointerData, OffsetNumber};

/// `int32` from c.h.
pub type int32 = i32;
/// `int64` from c.h.
pub type int64 = i64;

/// `GinNullCategory` -- placeholder/null discriminator (ginblock.h: `signed char`).
pub type GinNullCategory = i8;

pub const GIN_CAT_NORM_KEY: GinNullCategory = 0; // normal, non-null key value
pub const GIN_CAT_NULL_KEY: GinNullCategory = 1; // null key value
pub const GIN_CAT_EMPTY_ITEM: GinNullCategory = 2; // placeholder for zero-key item
pub const GIN_CAT_NULL_ITEM: GinNullCategory = 3; // placeholder for null item
pub const GIN_CAT_EMPTY_QUERY: GinNullCategory = -1; // placeholder for full-scan query

/// `GIN_CURRENT_VERSION` -- on-disk version stored in the metapage.
pub const GIN_CURRENT_VERSION: int32 = 2;

/// `GinPageOpaqueData` -- the special area of every GIN page (ginblock.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GinPageOpaqueData {
    /// next page if any.
    pub rightlink: BlockNumber,
    /// number of `PostingItem`s on GIN_DATA & ~GIN_LEAF page; on GIN_LIST page,
    /// number of heap tuples.
    pub maxoff: OffsetNumber,
    /// see bit definitions in `backend-access-gin`.
    pub flags: uint16,
}

/// `GinMetaPageData` -- contents of the GIN metapage (ginblock.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GinMetaPageData {
    /// head of pending list.
    pub head: BlockNumber,
    /// tail of pending list.
    pub tail: BlockNumber,
    /// free space (bytes) in the pending list's tail page.
    pub tailFreeSize: uint32,
    /// number of pages in the pending list.
    pub nPendingPages: BlockNumber,
    /// number of heap tuples in the pending list.
    pub nPendingHeapTuples: int64,
    /// total number of pages (planner stat).
    pub nTotalPages: BlockNumber,
    /// number of entry-tree pages (planner stat).
    pub nEntryPages: BlockNumber,
    /// number of data-tree pages (planner stat).
    pub nDataPages: BlockNumber,
    /// number of entries (planner stat).
    pub nEntries: int64,
    /// GIN version number.  (Must stay last; see header note.)
    pub ginVersion: int32,
}

/// `PostingItem` -- a posting item in a non-leaf posting-tree page (ginblock.h).
///
/// Uses `BlockIdData` (not `BlockNumber`) to avoid padding-space wastage.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PostingItem {
    /// child block number, stored unaligned as a `BlockIdData`.
    pub child_blkno: BlockIdData,
    /// the highest TID in the child subtree.
    pub key: ItemPointerData,
}

/// `GinPostingList` -- a compressed posting list (ginblock.h).
///
/// The `bytes` member is a C flexible-array member of varbyte-encoded items;
/// modelled here as a zero-length array so the header has the exact on-disk
/// layout.  Requires 2-byte alignment.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct GinPostingList {
    /// first item in this posting list (unpacked).
    pub first: ItemPointerData,
    /// number of bytes that follow.
    pub nbytes: uint16,
    /// varbyte-encoded items (flexible array member).
    pub bytes: [u8; 0],
}

/// `GinTuple` -- data for one GIN index key, serialized for the parallel-build
/// tuplesort (gin_tuple.h).
///
/// This is the on-the-wire layout `_gin_build_tuple` writes (and
/// `_gin_parse_tuple_*` reads): a header of scalar fields, then a `data`
/// flexible-array member holding the key value (SHORTALIGN-padded) followed by
/// the compressed posting-list segments.  The header is `#[repr(C)]` so the
/// `data` offset (16) and the trailing posting-list start match C exactly.
#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GinTuple {
    /// length of the whole tuple.
    pub tuplen: int32,
    /// attnum of index key.
    pub attrnum: OffsetNumber,
    /// bytes in `data` for the key value.
    pub keylen: uint16,
    /// typlen for the key type.
    pub typlen: i16,
    /// typbyval for the key type.
    pub typbyval: bool,
    /// category: normal or NULL? (`GinNullCategory`).
    pub category: i8,
    /// number of TIDs in the data.
    pub nitems: int32,
    /// key value then compressed posting list (flexible array member).
    pub data: [u8; 0],
}

// ---------------------------------------------------------------------------
// Compile-time layout assertions.
// ---------------------------------------------------------------------------

const _: () = {
    // GinPageOpaqueData: BlockNumber (4) + OffsetNumber (2) + uint16 (2) = 8.
    assert!(core::mem::size_of::<GinPageOpaqueData>() == 8);
    assert!(core::mem::align_of::<GinPageOpaqueData>() == 4);

    // PostingItem: BlockIdData (2*uint16 = 4) + ItemPointerData (6) = 10.
    assert!(core::mem::size_of::<PostingItem>() == 10);
    assert!(core::mem::align_of::<PostingItem>() == 2);

    // GinPostingList header: ItemPointerData (6) + uint16 (2) = 8.
    assert!(core::mem::offset_of!(GinPostingList, bytes) == 8);
    assert!(core::mem::align_of::<GinPostingList>() == 2);

    // GinMetaPageData: int64 members force 8-byte alignment.
    assert!(core::mem::align_of::<GinMetaPageData>() == 8);

    // GinTuple header: int(4) + OffsetNumber(2) + uint16(2) + int16(2) +
    // bool(1) + signed char(1) + int(4) = 16, data follows aligned to 4.
    assert!(core::mem::offset_of!(GinTuple, nitems) == 12);
    assert!(core::mem::offset_of!(GinTuple, data) == 16);
    assert!(core::mem::align_of::<GinTuple>() == 4);
};
