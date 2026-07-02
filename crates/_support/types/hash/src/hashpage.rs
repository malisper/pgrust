use ::mcx::{Mcx, PgVec};
use ::types_core::{
    uint16, uint32, BlockNumber, Buffer, BufferIsValid, InvalidBlockNumber, InvalidBuffer,
    OffsetNumber, RegProcedure, BLCKSZ,
};
use ::types_tuple::itemptr::ItemPointerData;

pub type Bucket = uint32;

pub const InvalidBucket: Bucket = 0xFFFF_FFFF;

pub const LH_UNUSED_PAGE: uint16 = 0;
pub const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
pub const LH_BUCKET_PAGE: uint16 = 1 << 1;
pub const LH_BITMAP_PAGE: uint16 = 1 << 2;
pub const LH_META_PAGE: uint16 = 1 << 3;
pub const LH_BUCKET_BEING_POPULATED: uint16 = 1 << 4;
pub const LH_BUCKET_BEING_SPLIT: uint16 = 1 << 5;
pub const LH_BUCKET_NEEDS_SPLIT_CLEANUP: uint16 = 1 << 6;
pub const LH_PAGE_HAS_DEAD_TUPLES: uint16 = 1 << 7;

pub const LH_PAGE_TYPE: uint16 =
    LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

pub const HASHO_PAGE_ID: uint16 = 0xFF80;

// hasho_prevblkno also carries hashm_maxbucket on a primary bucket page.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HashPageOpaqueData {
    pub hasho_prevblkno: BlockNumber,
    pub hasho_nextblkno: BlockNumber,
    pub hasho_bucket: Bucket,
    pub hasho_flag: uint16,
    pub hasho_page_id: uint16,
}

#[inline]
pub fn H_NEEDS_SPLIT_CLEANUP(flag: uint16) -> bool {
    (flag & LH_BUCKET_NEEDS_SPLIT_CLEANUP) != 0
}
#[inline]
pub fn H_BUCKET_BEING_SPLIT(flag: uint16) -> bool {
    (flag & LH_BUCKET_BEING_SPLIT) != 0
}
#[inline]
pub fn H_BUCKET_BEING_POPULATED(flag: uint16) -> bool {
    (flag & LH_BUCKET_BEING_POPULATED) != 0
}
#[inline]
pub fn H_HAS_DEAD_TUPLES(flag: uint16) -> bool {
    (flag & LH_PAGE_HAS_DEAD_TUPLES) != 0
}

pub const HASH_METAPAGE: BlockNumber = 0;

pub const HASH_MAGIC: uint32 = 0x6440640;
pub const HASH_VERSION: uint32 = 4;

pub const HASH_MAX_BITMAPS: usize = {
    let a = BLCKSZ / 8;
    if a < 1024 {
        a
    } else {
        1024
    }
};

pub const HASH_SPLITPOINT_PHASE_BITS: uint32 = 2;
pub const HASH_SPLITPOINT_PHASES_PER_GRP: uint32 = 1 << HASH_SPLITPOINT_PHASE_BITS;
pub const HASH_SPLITPOINT_PHASE_MASK: uint32 = HASH_SPLITPOINT_PHASES_PER_GRP - 1;
pub const HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE: uint32 = 10;

pub const HASH_MAX_SPLITPOINT_GROUP: uint32 = 32;

pub const HASH_MAX_SPLITPOINTS: usize = (((HASH_MAX_SPLITPOINT_GROUP
    - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
    * HASH_SPLITPOINT_PHASES_PER_GRP)
    + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) as usize;

#[derive(Clone, Debug)]
pub struct HashMetaPageData {
    pub hashm_magic: uint32,
    pub hashm_version: uint32,
    pub hashm_ntuples: f64,
    pub hashm_ffactor: uint16,
    pub hashm_bsize: uint16,
    pub hashm_bmsize: uint16,
    pub hashm_bmshift: uint16,
    pub hashm_maxbucket: uint32,
    pub hashm_highmask: uint32,
    pub hashm_lowmask: uint32,
    pub hashm_ovflpoint: uint32,
    pub hashm_firstfree: uint32,
    pub hashm_nmaps: uint32,
    pub hashm_procid: RegProcedure,
    pub hashm_spares: [uint32; HASH_MAX_SPLITPOINTS],
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

pub const HASH_READ: i32 = 1;
pub const HASH_WRITE: i32 = 2;
pub const HASH_NOLOCK: i32 = -1;

pub const HASH_MIN_FILLFACTOR: i32 = 10;
pub const HASH_DEFAULT_FILLFACTOR: i32 = 75;

pub const BYTE_TO_BIT: uint32 = 3;
pub const ALL_SET: uint32 = u32::MAX;
pub const BITS_PER_MAP: uint32 = 32;

// INDEX_AM_RESERVED_BIT (itup.h).
pub const INDEX_MOVED_BY_SPLIT_MASK: uint16 = 0x2000;

pub const HASHSTANDARD_PROC: uint16 = 1;
pub const HASHEXTENDED_PROC: uint16 = 2;
pub const HASHOPTIONS_PROC: uint16 = 3;
pub const HASHNProcs: uint16 = 3;

// itup.h: (BLCKSZ - SizeOfPageHeaderData) /
// (MAXALIGN(sizeof(IndexTupleData) + 1) + sizeof(ItemIdData)).
pub const MaxIndexTuplesPerPage: usize = (BLCKSZ - 24) / (16 + 4);

#[derive(Clone, Copy, Debug, Default)]
pub struct HashScanPosItem {
    pub heapTid: ItemPointerData,
    pub indexOffset: OffsetNumber,
}

#[derive(Clone, Debug)]
pub struct HashScanPosData {
    pub buf: Buffer,
    pub currPage: BlockNumber,
    pub nextPage: BlockNumber,
    pub prevPage: BlockNumber,
    pub firstItem: i32,
    pub lastItem: i32,
    pub itemIndex: i32,
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

#[inline]
pub fn HashScanPosIsPinned(scanpos: &HashScanPosData) -> bool {
    BufferIsValid(scanpos.buf)
}

#[inline]
pub fn HashScanPosIsValid(scanpos: &HashScanPosData) -> bool {
    scanpos.currPage != InvalidBlockNumber
}

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

#[derive(Debug)]
pub struct HashScanOpaqueData<'mcx> {
    pub hashso_sk_hash: uint32,
    pub hashso_bucket_buf: Buffer,
    pub hashso_split_bucket_buf: Buffer,
    pub hashso_buc_populated: bool,
    pub hashso_buc_split: bool,
    // Empty is C's NULL sentinel (killedItems is lazily allocated in C).
    pub killedItems: PgVec<'mcx, i32>,
    pub numKilled: i32,
    pub currPos: HashScanPosData,
}

impl<'mcx> HashScanOpaqueData<'mcx> {
    pub fn new_in(mcx: Mcx<'mcx>) -> Self {
        HashScanOpaqueData {
            hashso_sk_hash: 0,
            hashso_bucket_buf: InvalidBuffer,
            hashso_split_bucket_buf: InvalidBuffer,
            hashso_buc_populated: false,
            hashso_buc_split: false,
            killedItems: PgVec::new_in(mcx),
            numKilled: 0,
            currPos: HashScanPosData::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derived_constants_match_c() {
        assert_eq!(HASH_MAX_BITMAPS, 1024);
        assert_eq!(HASH_MAX_SPLITPOINTS, 98);
        assert_eq!(HASH_SPLITPOINT_PHASES_PER_GRP, 4);
        assert_eq!(HASH_SPLITPOINT_PHASE_MASK, 3);
        assert_eq!(LH_PAGE_TYPE, 0x000F);
        assert_eq!(MaxIndexTuplesPerPage, 408);
    }

    #[test]
    fn scanpos_invalidate_matches_default() {
        let mut pos = HashScanPosData::default();
        pos.buf = 7;
        pos.currPage = 3;
        pos.itemIndex = 5;
        assert!(HashScanPosIsPinned(&pos));
        assert!(HashScanPosIsValid(&pos));
        HashScanPosInvalidate(&mut pos);
        assert!(!HashScanPosIsPinned(&pos));
        assert!(!HashScanPosIsValid(&pos));
        assert_eq!(pos.itemIndex, 0);
    }
}
