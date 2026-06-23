//! Runtime page / scan vocabulary for the hash access method
//! (`src/include/access/hash.h`, PostgreSQL 18.3): the page-special and metapage
//! structs, the bucket math constants, and the scan-private state the hash AM
//! threads through `scan->opaque`.
//!
//! Mirrors how `types_nbtree` models `BTScanOpaqueData` / `BTScanPos` for the
//! nbtree port: plain owned Rust structs the hash crates manipulate, with the
//! `HashScanPos*` invalidate/pinned/valid predicates as free functions.

use ::types_core::primitive::{
    BlockNumber, OffsetNumber, RegProcedure, BLCKSZ, InvalidBlockNumber,
};
use types_core::{uint16, uint32};
use ::types_core::primitive::BufferIsValid;
use ::types_storage::storage::{Buffer, InvalidBuffer};
use ::types_tuple::heaptuple::ItemPointerData;

use alloc::vec::Vec;

// ===========================================================================
// Bucket
// ===========================================================================

/// `Bucket` (`access/hash.h`) — `typedef uint32 Bucket`.
pub type Bucket = uint32;

/// `InvalidBucket` (`access/hash.h`): `(Bucket) 0xFFFFFFFF`.
pub const InvalidBucket: Bucket = 0xFFFF_FFFF;

// ===========================================================================
// Page flag bits (hasho_flag).
// ===========================================================================

/// `LH_UNUSED_PAGE` (hash.h).
pub const LH_UNUSED_PAGE: uint16 = 0;
/// `LH_OVERFLOW_PAGE` (hash.h).
pub const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
/// `LH_BUCKET_PAGE` (hash.h).
pub const LH_BUCKET_PAGE: uint16 = 1 << 1;
/// `LH_BITMAP_PAGE` (hash.h).
pub const LH_BITMAP_PAGE: uint16 = 1 << 2;
/// `LH_META_PAGE` (hash.h).
pub const LH_META_PAGE: uint16 = 1 << 3;
/// `LH_BUCKET_BEING_POPULATED` (hash.h).
pub const LH_BUCKET_BEING_POPULATED: uint16 = 1 << 4;
/// `LH_BUCKET_BEING_SPLIT` (hash.h).
pub const LH_BUCKET_BEING_SPLIT: uint16 = 1 << 5;
/// `LH_BUCKET_NEEDS_SPLIT_CLEANUP` (hash.h).
pub const LH_BUCKET_NEEDS_SPLIT_CLEANUP: uint16 = 1 << 6;
/// `LH_PAGE_HAS_DEAD_TUPLES` (hash.h).
pub const LH_PAGE_HAS_DEAD_TUPLES: uint16 = 1 << 7;

/// `LH_PAGE_TYPE` (hash.h) — the page-type bits.
pub const LH_PAGE_TYPE: uint16 =
    LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

/// `HASHO_PAGE_ID` (hash.h) — identifies hash index pages.
pub const HASHO_PAGE_ID: uint16 = 0xFF80;

// ===========================================================================
// HashPageOpaqueData (the page special area).
// ===========================================================================

/// `HashPageOpaqueData` (`access/hash.h`) — the hash index page special space.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HashPageOpaqueData {
    /// `hasho_prevblkno` — previous page in the bucket chain (or the
    /// `hashm_maxbucket` value for a bucket page).
    pub hasho_prevblkno: BlockNumber,
    /// `hasho_nextblkno` — next page in the bucket chain.
    pub hasho_nextblkno: BlockNumber,
    /// `hasho_bucket` — bucket number this page belongs to.
    pub hasho_bucket: Bucket,
    /// `hasho_flag` — page type code + flag bits.
    pub hasho_flag: uint16,
    /// `hasho_page_id` — `HASHO_PAGE_ID`.
    pub hasho_page_id: uint16,
}

/// `H_NEEDS_SPLIT_CLEANUP(opaque)`.
#[inline]
pub fn H_NEEDS_SPLIT_CLEANUP(flag: uint16) -> bool {
    (flag & LH_BUCKET_NEEDS_SPLIT_CLEANUP) != 0
}
/// `H_BUCKET_BEING_SPLIT(opaque)`.
#[inline]
pub fn H_BUCKET_BEING_SPLIT(flag: uint16) -> bool {
    (flag & LH_BUCKET_BEING_SPLIT) != 0
}
/// `H_BUCKET_BEING_POPULATED(opaque)`.
#[inline]
pub fn H_BUCKET_BEING_POPULATED(flag: uint16) -> bool {
    (flag & LH_BUCKET_BEING_POPULATED) != 0
}
/// `H_HAS_DEAD_TUPLES(opaque)`.
#[inline]
pub fn H_HAS_DEAD_TUPLES(flag: uint16) -> bool {
    (flag & LH_PAGE_HAS_DEAD_TUPLES) != 0
}

// ===========================================================================
// Metapage constants + HashMetaPageData.
// ===========================================================================

/// `HASH_METAPAGE` (hash.h) — metapage is always block 0.
pub const HASH_METAPAGE: BlockNumber = 0;

/// `HASH_MAGIC` (hash.h).
pub const HASH_MAGIC: uint32 = 0x6440640;
/// `HASH_VERSION` (hash.h).
pub const HASH_VERSION: uint32 = 4;

/// `HASH_MAX_BITMAPS` (hash.h): `Min(BLCKSZ / 8, 1024)`.
pub const HASH_MAX_BITMAPS: usize = {
    let a = BLCKSZ / 8;
    if a < 1024 {
        a
    } else {
        1024
    }
};

/// `HASH_SPLITPOINT_PHASE_BITS` (hash.h).
pub const HASH_SPLITPOINT_PHASE_BITS: uint32 = 2;
/// `HASH_SPLITPOINT_PHASES_PER_GRP` (hash.h).
pub const HASH_SPLITPOINT_PHASES_PER_GRP: uint32 = 1 << HASH_SPLITPOINT_PHASE_BITS;
/// `HASH_SPLITPOINT_PHASE_MASK` (hash.h).
pub const HASH_SPLITPOINT_PHASE_MASK: uint32 = HASH_SPLITPOINT_PHASES_PER_GRP - 1;
/// `HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE` (hash.h).
pub const HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE: uint32 = 10;

/// `HASH_MAX_SPLITPOINT_GROUP` (hash.h).
pub const HASH_MAX_SPLITPOINT_GROUP: uint32 = 32;

/// `HASH_MAX_SPLITPOINTS` (hash.h).
pub const HASH_MAX_SPLITPOINTS: usize = (((HASH_MAX_SPLITPOINT_GROUP
    - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
    * HASH_SPLITPOINT_PHASES_PER_GRP)
    + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) as usize;

/// `HashMetaPageData` (`access/hash.h`) — the hash index metapage payload.
#[derive(Clone, Debug)]
pub struct HashMetaPageData {
    /// `hashm_magic`.
    pub hashm_magic: uint32,
    /// `hashm_version`.
    pub hashm_version: uint32,
    /// `hashm_ntuples`.
    pub hashm_ntuples: f64,
    /// `hashm_ffactor`.
    pub hashm_ffactor: uint16,
    /// `hashm_bsize`.
    pub hashm_bsize: uint16,
    /// `hashm_bmsize`.
    pub hashm_bmsize: uint16,
    /// `hashm_bmshift`.
    pub hashm_bmshift: uint16,
    /// `hashm_maxbucket`.
    pub hashm_maxbucket: uint32,
    /// `hashm_highmask`.
    pub hashm_highmask: uint32,
    /// `hashm_lowmask`.
    pub hashm_lowmask: uint32,
    /// `hashm_ovflpoint`.
    pub hashm_ovflpoint: uint32,
    /// `hashm_firstfree`.
    pub hashm_firstfree: uint32,
    /// `hashm_nmaps`.
    pub hashm_nmaps: uint32,
    /// `hashm_procid`.
    pub hashm_procid: RegProcedure,
    /// `hashm_spares[HASH_MAX_SPLITPOINTS]`.
    pub hashm_spares: [uint32; HASH_MAX_SPLITPOINTS],
    /// `hashm_mapp[HASH_MAX_BITMAPS]`.
    pub hashm_mapp: [BlockNumber; HASH_MAX_BITMAPS],
}

impl Default for HashMetaPageData {
    fn default() -> Self {
        HashMetaPageData {
            hashm_magic: 0,
            hashm_version: 0,
            hashm_ntuples: 0.0,
            hashm_ffactor: 0,
            hashm_bsize: 0,
            hashm_bmsize: 0,
            hashm_bmshift: 0,
            hashm_maxbucket: 0,
            hashm_highmask: 0,
            hashm_lowmask: 0,
            hashm_ovflpoint: 0,
            hashm_firstfree: 0,
            hashm_nmaps: 0,
            hashm_procid: 0,
            hashm_spares: [0; HASH_MAX_SPLITPOINTS],
            hashm_mapp: [0; HASH_MAX_BITMAPS],
        }
    }
}

// ===========================================================================
// Misc constants (hash.h).
// ===========================================================================

/// `HASH_READ` (hash.h) = `BUFFER_LOCK_SHARE`.
pub const HASH_READ: i32 = 1;
/// `HASH_WRITE` (hash.h) = `BUFFER_LOCK_EXCLUSIVE`.
pub const HASH_WRITE: i32 = 2;
/// `HASH_NOLOCK` (hash.h) = `-1`.
pub const HASH_NOLOCK: i32 = -1;

/// `HASH_MIN_FILLFACTOR` (hash.h).
pub const HASH_MIN_FILLFACTOR: i32 = 10;
/// `HASH_DEFAULT_FILLFACTOR` (hash.h).
pub const HASH_DEFAULT_FILLFACTOR: i32 = 75;

/// `BYTE_TO_BIT` (hash.h) — 2^3 bits/byte.
pub const BYTE_TO_BIT: uint32 = 3;
/// `ALL_SET` (hash.h).
pub const ALL_SET: uint32 = u32::MAX;
/// `BITS_PER_MAP` (hash.h) — number of bits in a uint32 bitmap word.
pub const BITS_PER_MAP: uint32 = 32;

/// `INDEX_MOVED_BY_SPLIT_MASK` (hash.h) = `INDEX_AM_RESERVED_BIT` (itup.h)
/// = `0x2000`.
pub const INDEX_MOVED_BY_SPLIT_MASK: uint16 = 0x2000;

// ===========================================================================
// HashScanPosItem / HashScanPosData (the per-page match buffer).
// ===========================================================================

/// `MaxIndexTuplesPerPage` (`access/itup.h`).
pub const MaxIndexTuplesPerPage: usize = (BLCKSZ - 24) / (16 + 4);

/// `HashScanPosItem` (`access/hash.h`) — one remembered match.
#[derive(Clone, Copy, Debug, Default)]
pub struct HashScanPosItem {
    /// `heapTid` — TID of the referenced heap item.
    pub heapTid: ItemPointerData,
    /// `indexOffset` — the index item's location within its page.
    pub indexOffset: OffsetNumber,
}

/// `HashScanPosData` (`access/hash.h`) — matches found on one page.
#[derive(Clone, Debug)]
pub struct HashScanPosData {
    /// `buf` — if valid, the pinned buffer.
    pub buf: Buffer,
    /// `currPage` — current hash index page.
    pub currPage: BlockNumber,
    /// `nextPage` — next overflow page.
    pub nextPage: BlockNumber,
    /// `prevPage` — prev overflow or bucket page.
    pub prevPage: BlockNumber,
    /// `firstItem` — first valid index in `items`.
    pub firstItem: i32,
    /// `lastItem` — last valid index in `items`.
    pub lastItem: i32,
    /// `itemIndex` — cursor: entry last returned to caller.
    pub itemIndex: i32,
    /// `items[MaxIndexTuplesPerPage]`.
    pub items: [HashScanPosItem; MaxIndexTuplesPerPage],
}

impl Default for HashScanPosData {
    fn default() -> Self {
        HashScanPosData {
            buf: InvalidBuffer,
            currPage: InvalidBlockNumber,
            nextPage: InvalidBlockNumber,
            prevPage: InvalidBlockNumber,
            firstItem: 0,
            lastItem: 0,
            itemIndex: 0,
            items: [HashScanPosItem::default(); MaxIndexTuplesPerPage],
        }
    }
}

/// `HashScanPosIsPinned(scanpos)`.
#[inline]
pub fn HashScanPosIsPinned(scanpos: &HashScanPosData) -> bool {
    BufferIsValid(scanpos.buf)
}

/// `HashScanPosIsValid(scanpos)`.
#[inline]
pub fn HashScanPosIsValid(scanpos: &HashScanPosData) -> bool {
    scanpos.currPage != InvalidBlockNumber
}

/// `HashScanPosInvalidate(scanpos)`.
#[inline]
pub fn HashScanPosInvalidate(scanpos: &mut HashScanPosData) {
    scanpos.buf = InvalidBuffer;
    scanpos.currPage = InvalidBlockNumber;
    scanpos.nextPage = InvalidBlockNumber;
    scanpos.prevPage = InvalidBlockNumber;
    scanpos.firstItem = 0;
    scanpos.lastItem = 0;
    scanpos.itemIndex = 0;
}

// ===========================================================================
// HashScanOpaqueData (scan->opaque).
// ===========================================================================

/// `HashScanOpaqueData` (`access/hash.h`) — hash index scan private state.
#[derive(Clone, Debug)]
pub struct HashScanOpaqueData {
    /// `hashso_sk_hash` — hash value of the scan key sought.
    pub hashso_sk_hash: uint32,
    /// `hashso_bucket_buf` — buffer of the primary bucket page.
    pub hashso_bucket_buf: Buffer,
    /// `hashso_split_bucket_buf` — primary bucket page of a bucket being split.
    pub hashso_split_bucket_buf: Buffer,
    /// `hashso_buc_populated` — scan starts on a bucket being populated.
    pub hashso_buc_populated: bool,
    /// `hashso_buc_split` — scanning a bucket being split.
    pub hashso_buc_split: bool,
    /// `killedItems` — `currPos.items` indexes of killed items (empty if
    /// never used; the C `NULL` sentinel).
    pub killedItems: Vec<i32>,
    /// `numKilled` — number of currently stored killed items.
    pub numKilled: i32,
    /// `currPos` — current position data.
    pub currPos: HashScanPosData,
}

impl Default for HashScanOpaqueData {
    fn default() -> Self {
        HashScanOpaqueData {
            hashso_sk_hash: 0,
            hashso_bucket_buf: InvalidBuffer,
            hashso_split_bucket_buf: InvalidBuffer,
            hashso_buc_populated: false,
            hashso_buc_split: false,
            killedItems: Vec::new(),
            numKilled: 0,
            currPos: HashScanPosData::default(),
        }
    }
}
