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
#[repr(C)]
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

#[repr(C)]
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

// items live as MaybeUninit: only [firstItem, lastItem] is ever written
// before a read (a zeroing Default would memset 3.3KB per scan).
pub struct HashScanPosData {
    pub buf: Buffer,
    pub currPage: BlockNumber,
    pub nextPage: BlockNumber,
    pub prevPage: BlockNumber,
    pub firstItem: i32,
    pub lastItem: i32,
    pub itemIndex: i32,
    pub items: [core::mem::MaybeUninit<HashScanPosItem>; MaxIndexTuplesPerPage],
}

impl HashScanPosData {
    /// # Safety
    /// `i` must have been written by `set_item` since the last page load.
    #[inline]
    pub unsafe fn item(&self, i: usize) -> HashScanPosItem {
        unsafe { self.items[i].assume_init() }
    }

    #[inline]
    pub fn set_item(&mut self, i: usize, item: HashScanPosItem) {
        self.items[i] = core::mem::MaybeUninit::new(item);
    }
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
            items: [core::mem::MaybeUninit::uninit(); MaxIndexTuplesPerPage],
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
    // hashbeginscan's palloc + assignments, in place (a by-value Self would
    // memcpy ~3.3KB through the stack); currPos.items stays uninit.
    pub fn alloc_in(mcx: Mcx<'mcx>) -> ::types_error::PgResult<::mcx::PgBox<'mcx, Self>> {
        use ::mcx::Allocator;
        let layout = core::alloc::Layout::new::<Self>();
        let ptr = Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(layout.size()))?;
        let p = ptr.as_ptr() as *mut Self;
        // SAFETY: fresh allocation of `layout`; every field except
        // currPos.items (MaybeUninit by type) is written exactly once.
        unsafe {
            (&raw mut (*p).hashso_sk_hash).write(0);
            (&raw mut (*p).hashso_bucket_buf).write(InvalidBuffer);
            (&raw mut (*p).hashso_split_bucket_buf).write(InvalidBuffer);
            (&raw mut (*p).hashso_buc_populated).write(false);
            (&raw mut (*p).hashso_buc_split).write(false);
            (&raw mut (*p).killedItems).write(PgVec::new_in(mcx));
            (&raw mut (*p).numKilled).write(0);
            let cp = &raw mut (*p).currPos;
            (&raw mut (*cp).buf).write(InvalidBuffer);
            (&raw mut (*cp).currPage).write(InvalidBlockNumber);
            (&raw mut (*cp).nextPage).write(InvalidBlockNumber);
            (&raw mut (*cp).prevPage).write(InvalidBlockNumber);
            (&raw mut (*cp).firstItem).write(0);
            (&raw mut (*cp).lastItem).write(0);
            (&raw mut (*cp).itemIndex).write(0);
            Ok(::mcx::PgBox::from_raw_in(p, mcx))
        }
    }
}

pub const XLOG_HASH_INIT_META_PAGE: u8 = 0x00;
pub const XLOG_HASH_INIT_BITMAP_PAGE: u8 = 0x10;
pub const XLOG_HASH_INSERT: u8 = 0x20;
pub const XLOG_HASH_ADD_OVFL_PAGE: u8 = 0x30;
pub const XLOG_HASH_SPLIT_ALLOCATE_PAGE: u8 = 0x40;
pub const XLOG_HASH_SPLIT_PAGE: u8 = 0x50;
pub const XLOG_HASH_SPLIT_COMPLETE: u8 = 0x60;
pub const XLOG_HASH_MOVE_PAGE_CONTENTS: u8 = 0x70;
pub const XLOG_HASH_SQUEEZE_PAGE: u8 = 0x80;
pub const XLOG_HASH_DELETE: u8 = 0x90;
pub const XLOG_HASH_SPLIT_CLEANUP: u8 = 0xA0;
pub const XLOG_HASH_UPDATE_META_PAGE: u8 = 0xB0;
pub const XLOG_HASH_VACUUM_ONE_PAGE: u8 = 0xC0;

pub const XLH_SPLIT_META_UPDATE_MASKS: u8 = 1 << 0;
pub const XLH_SPLIT_META_UPDATE_SPLITPOINT: u8 = 1 << 1;

pub const HASH_XLOG_FREE_OVFL_BUFS: usize = 6;

#[inline]
pub fn _hash_hashkey2bucket(hashkey: uint32, maxbucket: uint32, highmask: uint32, lowmask: uint32) -> Bucket {
    let mut bucket = hashkey & highmask;
    if bucket > maxbucket {
        bucket &= lowmask;
    }
    bucket
}

#[inline]
pub fn pg_ceil_log2_32(num: uint32) -> uint32 {
    if num < 2 {
        return 0;
    }
    32 - (num - 1).leading_zeros()
}

pub fn _hash_spareindex(num_bucket: uint32) -> uint32 {
    let splitpoint_group = pg_ceil_log2_32(num_bucket);
    if splitpoint_group < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
        return splitpoint_group;
    }
    let mut splitpoint_phases = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;
    splitpoint_phases +=
        (splitpoint_group - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) << HASH_SPLITPOINT_PHASE_BITS;
    splitpoint_phases += ((num_bucket - 1) >> (splitpoint_group - (HASH_SPLITPOINT_PHASE_BITS + 1)))
        & HASH_SPLITPOINT_PHASE_MASK;
    splitpoint_phases
}

pub fn _hash_get_totalbuckets(splitpoint_phase: uint32) -> uint32 {
    if splitpoint_phase < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
        return 1 << splitpoint_phase;
    }
    let splitpoint_group = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE
        + ((splitpoint_phase - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) >> HASH_SPLITPOINT_PHASE_BITS);
    let mut total_buckets = 1 << (splitpoint_group - 1);
    let phases_within_splitpoint_group =
        ((splitpoint_phase - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) & HASH_SPLITPOINT_PHASE_MASK) + 1;
    total_buckets +=
        ((1 << (splitpoint_group - 1)) >> HASH_SPLITPOINT_PHASE_BITS) * phases_within_splitpoint_group;
    total_buckets
}

impl HashMetaPageData {
    /// BUCKET_TO_BLKNO.
    #[inline]
    pub fn bucket_to_blkno(&self, bucket: Bucket) -> BlockNumber {
        let spare = if bucket != 0 {
            self.hashm_spares[(_hash_spareindex(bucket + 1) - 1) as usize]
        } else {
            0
        };
        bucket + spare + 1
    }
}

const _: () = assert!(core::mem::size_of::<HashPageOpaqueData>() == 16);
const _: () = assert!(core::mem::size_of::<HashMetaPageData>() == 4544);
const _: () = assert!(core::mem::offset_of!(HashMetaPageData, hashm_ntuples) == 8);
const _: () = assert!(core::mem::offset_of!(HashMetaPageData, hashm_maxbucket) == 24);
const _: () = assert!(core::mem::offset_of!(HashMetaPageData, hashm_procid) == 48);
const _: () = assert!(core::mem::offset_of!(HashMetaPageData, hashm_spares) == 52);
const _: () = assert!(core::mem::offset_of!(HashMetaPageData, hashm_mapp) == 444);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitpoint_functions_match_c() {
        assert_eq!(_hash_spareindex(1), 0);
        assert_eq!(_hash_spareindex(2), 1);
        assert_eq!(_hash_spareindex(3), 2);
        assert_eq!(_hash_spareindex(4), 2);
        assert_eq!(_hash_spareindex(512), 9);
        assert_eq!(_hash_spareindex(513), 10);
        assert_eq!(_hash_spareindex(1024), 13);
        for phase in 0..30 {
            let total = _hash_get_totalbuckets(phase);
            assert_eq!(_hash_spareindex(total), phase);
            if total > 1 {
                assert_eq!(_hash_spareindex(total + 1), phase + 1);
            }
        }
    }

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
