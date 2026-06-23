//! GIN rmgr WAL record bodies (`access/ginxlog.h`) and the posting-list
//! pieces the records embed (`access/ginblock.h`), trimmed to the fields
//! ports consume so far.

use crate::bytes::{block_id_at, bool_at, i32_at, item_pointer_at, u16_at, u32_at};
use ::types_core::OffsetNumber;
use ::types_tuple::{BlockIdData, ItemPointerData};

/// `ginxlogInsert`: `{uint16 flags;}` — the common head of all GIN insertion
/// records. If the target is not a leaf, `BlockIdData[2]` children follow;
/// then a [`ginxlogInsertEntry`] or `ginxlogRecompressDataLeaf`.
#[derive(Clone, Copy, Debug)]
pub struct ginxlogInsert {
    pub flags: u16,
}

/// `sizeof(ginxlogInsert)`.
pub const SIZEOF_GINXLOG_INSERT: usize = 2;
/// `sizeof(BlockIdData)` — `{uint16 bi_hi; uint16 bi_lo;}`.
pub const SIZEOF_BLOCK_ID_DATA: usize = 4;

impl ginxlogInsert {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { flags: u16_at(rec, 0) }
    }

    /// The `BlockIdData[2]` left/right children following the struct on a
    /// non-leaf insertion.
    pub fn children(rec: &[u8]) -> (BlockIdData, BlockIdData) {
        (
            block_id_at(rec, SIZEOF_GINXLOG_INSERT),
            block_id_at(rec, SIZEOF_GINXLOG_INSERT + SIZEOF_BLOCK_ID_DATA),
        )
    }
}

/// `ginxlogCreatePostingTree` (`access/ginxlog.h`): `{uint32 size;}` — a
/// compressed posting list (the leaf's segments) follows the struct.
#[derive(Clone, Copy, Debug)]
pub struct ginxlogCreatePostingTree {
    pub size: u32,
}

/// `sizeof(ginxlogCreatePostingTree)`.
pub const SIZEOF_GINXLOG_CREATE_POSTING_TREE: usize = 4;

impl ginxlogCreatePostingTree {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { size: u32_at(rec, 0) }
    }
}

/// `ginxlogInsertEntry`: `{OffsetNumber offset; bool isDelete;
/// IndexTupleData tuple;}` — trimmed of the variable-length tuple.
#[derive(Clone, Copy, Debug)]
pub struct ginxlogInsertEntry {
    pub offset: OffsetNumber,
    pub isDelete: bool,
}

impl ginxlogInsertEntry {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offset: u16_at(rec, 0),
            isDelete: bool_at(rec, 2),
        }
    }
}

/// `ginxlogRecompressDataLeaf`: `{uint16 nactions;}` — a variable number of
/// segment actions follow (segno byte, action byte, action-specific data).
#[derive(Clone, Copy, Debug)]
pub struct ginxlogRecompressDataLeaf {
    pub nactions: u16,
}

/// `sizeof(ginxlogRecompressDataLeaf)`.
pub const SIZEOF_GINXLOG_RECOMPRESS_DATA_LEAF: usize = 2;

impl ginxlogRecompressDataLeaf {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { nactions: u16_at(rec, 0) }
    }
}

/// `PostingItem` (`access/ginblock.h`): `{BlockIdData child_blkno;
/// ItemPointerData key;}` — 2-aligned, 10 bytes.
#[derive(Clone, Copy, Debug)]
pub struct PostingItem {
    pub child_blkno: BlockIdData,
    pub key: ItemPointerData,
}

/// `sizeof(PostingItem)` — `child_blkno` (4) + `key` (6), 2-aligned, 10 bytes.
pub const SIZEOF_POSTING_ITEM: usize = 10;

impl PostingItem {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            child_blkno: block_id_at(rec, 0),
            key: item_pointer_at(rec, 4),
        }
    }
}

/// `ginxlogInsertDataInternal`: `{OffsetNumber offset; PostingItem newitem;}`
/// — `newitem` 2-aligned at 2.
#[derive(Clone, Copy, Debug)]
pub struct ginxlogInsertDataInternal {
    pub offset: OffsetNumber,
    pub newitem: PostingItem,
}

impl ginxlogInsertDataInternal {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            offset: u16_at(rec, 0),
            newitem: PostingItem::from_bytes(&rec[2..]),
        }
    }
}

/// `ginxlogSplit`: trimmed to `flags`; the locator/links live at
/// `{RelFileLocator locator /*0*/; BlockNumber rrlink /*12*/;
/// BlockNumber leftChildBlkno /*16*/; BlockNumber rightChildBlkno /*20*/;
/// uint16 flags /*24*/;}`.
#[derive(Clone, Copy, Debug)]
pub struct ginxlogSplit {
    pub flags: u16,
}

impl ginxlogSplit {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { flags: u16_at(rec, 24) }
    }
}

/// `GinPostingList` (`access/ginblock.h`): `{ItemPointerData first;
/// uint16 nbytes; unsigned char bytes[FLEXIBLE_ARRAY_MEMBER];}` — trimmed to
/// `nbytes`, which sizes the segment.
#[derive(Clone, Copy, Debug)]
pub struct GinPostingList {
    pub nbytes: u16,
}

/// `offsetof(GinPostingList, bytes)` — `first` (6 bytes) + `nbytes` (2).
pub const OFFSETOF_GIN_POSTING_LIST_BYTES: usize = 8;

/// `SHORTALIGN`.
pub const fn shortalign(n: usize) -> usize {
    (n + 1) & !1
}

impl GinPostingList {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self { nbytes: u16_at(rec, 6) }
    }

    /// `SizeOfGinPostingList(plist)` —
    /// `offsetof(GinPostingList, bytes) + SHORTALIGN(nbytes)`.
    pub const fn size(&self) -> usize {
        OFFSETOF_GIN_POSTING_LIST_BYTES + shortalign(self.nbytes as usize)
    }
}

/// `sizeof(ItemPointerData)` (6 bytes, 2-aligned).
pub const SIZEOF_ITEM_POINTER_DATA: usize = 6;

/// `sizeof(GinMetaPageData)` (`access/ginblock.h`; natural layout, 8-aligned:
/// five `BlockNumber`/`uint32` words, two 8-aligned `int64`, `int32`
/// `ginVersion`, tail padding to 56).
pub const SIZEOF_GIN_META_PAGE_DATA: usize = 56;

/// `ginxlogDeleteListPages`: `{GinMetaPageData metadata; int32 ndeleted;}` —
/// trimmed to `ndeleted` (at `sizeof(GinMetaPageData)`).
#[derive(Clone, Copy, Debug)]
pub struct ginxlogDeleteListPages {
    pub ndeleted: i32,
}

impl ginxlogDeleteListPages {
    pub fn from_bytes(rec: &[u8]) -> Self {
        Self {
            ndeleted: i32_at(rec, SIZEOF_GIN_META_PAGE_DATA),
        }
    }
}
