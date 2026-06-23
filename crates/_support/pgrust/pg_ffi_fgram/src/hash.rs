//! On-disk / ABI structures and constants for the hash access method.
//!
//! These mirror `src/include/access/hash.h` from PostgreSQL 18.3.  Only the
//! types that genuinely cross the on-disk / page-layout boundary live here as
//! `#[repr(C)]` with compile-time layout assertions; the purely internal
//! runtime structures of the access method live inside `backend-access-hash`
//! itself as idiomatic Rust.

use crate::{
    uint16, uint32, uint8, BlockNumber, OffsetNumber, RegProcedure, TransactionId, BLCKSZ,
};

/// `Bucket` -- a hash bucket number (hash.h).
pub type Bucket = uint32;

// ---------------------------------------------------------------------------
// Strategy numbers (`access/hash.h` via `access/stratnum.h`) and the
// hashhandler vacuum-option / amkeytype constants.
// ---------------------------------------------------------------------------

/// `HTEqualStrategyNumber` -- the hash AM's only strategy (`=`).
pub const HTEqualStrategyNumber: crate::StrategyNumber = 1;
/// `HTMaxStrategyNumber` -- one strategy in total.
pub const HTMaxStrategyNumber: crate::StrategyNumber = 1;
/// `VACUUM_OPTION_PARALLEL_BULKDEL` (vacuum.h) -- the hash AM's only parallel
/// vacuum option.
pub const VACUUM_OPTION_PARALLEL_BULKDEL: uint8 = 1 << 0;

/// `InvalidBucket` -- sentinel value (hash.h).
pub const InvalidBucket: Bucket = 0xFFFF_FFFF;

/// `HASHO_PAGE_ID` -- the page-id stored in every hash page's opaque area.
pub const HASHO_PAGE_ID: uint16 = 0xFF80;

// ---------------------------------------------------------------------------
// Special-space page-type / transient flag bits (`hasho_flag`), hash.h.
// ---------------------------------------------------------------------------

/// `LH_UNUSED_PAGE`.
pub const LH_UNUSED_PAGE: uint16 = 0;
/// `LH_OVERFLOW_PAGE`.
pub const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
/// `LH_BUCKET_PAGE`.
pub const LH_BUCKET_PAGE: uint16 = 1 << 1;
/// `LH_BITMAP_PAGE`.
pub const LH_BITMAP_PAGE: uint16 = 1 << 2;
/// `LH_META_PAGE`.
pub const LH_META_PAGE: uint16 = 1 << 3;
/// `LH_BUCKET_BEING_POPULATED`.
pub const LH_BUCKET_BEING_POPULATED: uint16 = 1 << 4;
/// `LH_BUCKET_BEING_SPLIT`.
pub const LH_BUCKET_BEING_SPLIT: uint16 = 1 << 5;
/// `LH_BUCKET_NEEDS_SPLIT_CLEANUP`.
pub const LH_BUCKET_NEEDS_SPLIT_CLEANUP: uint16 = 1 << 6;
/// `LH_PAGE_HAS_DEAD_TUPLES`.
pub const LH_PAGE_HAS_DEAD_TUPLES: uint16 = 1 << 7;

/// `LH_PAGE_TYPE` -- the OR of the four mutually-exclusive page-type bits.
pub const LH_PAGE_TYPE: uint16 = LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

// ---------------------------------------------------------------------------
// Page-level locking modes (hash.h); mirror bufmgr's BUFFER_LOCK_* values.
// ---------------------------------------------------------------------------

/// `HASH_READ` == `BUFFER_LOCK_SHARE`.
pub const HASH_READ: i32 = 1;
/// `HASH_WRITE` == `BUFFER_LOCK_EXCLUSIVE`.
pub const HASH_WRITE: i32 = 2;
/// `HASH_NOLOCK` == `-1`.
pub const HASH_NOLOCK: i32 = -1;

// ---------------------------------------------------------------------------
// Support-function numbers (hash.h).
// ---------------------------------------------------------------------------

/// `HASHSTANDARD_PROC` -- the standard (32-bit) hash function.
pub const HASHSTANDARD_PROC: uint16 = 1;
/// `HASHEXTENDED_PROC` -- the optional extended (64-bit, salted) hash function.
pub const HASHEXTENDED_PROC: uint16 = 2;
/// `HASHOPTIONS_PROC`.
pub const HASHOPTIONS_PROC: uint16 = 3;
/// `HASHNProcs`.
pub const HASHNProcs: uint16 = 3;

// ---------------------------------------------------------------------------
// Fillfactor / item-size / bitmap constants (hash.h).
// ---------------------------------------------------------------------------

/// `HASH_MIN_FILLFACTOR`.
pub const HASH_MIN_FILLFACTOR: i32 = 10;
/// `HASH_DEFAULT_FILLFACTOR`.
pub const HASH_DEFAULT_FILLFACTOR: i32 = 75;

/// `BYTE_TO_BIT` -- 2^3 bits/byte.
pub const BYTE_TO_BIT: uint32 = 3;
/// `ALL_SET` -- a fully-set bitmap word.
pub const ALL_SET: uint32 = u32::MAX;
/// `BITS_PER_MAP` -- number of bits in a `uint32` bitmap word.
pub const BITS_PER_MAP: uint32 = 32;

/// `HashOptions` -- the parsed reloptions blob for a hash index (hash.h).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HashOptions {
    /// varlena header (do not touch directly!).
    pub varlena_header_: i32,
    /// page fill factor in percent (0..100).
    pub fillfactor: i32,
}

/// `HASH_METAPAGE` -- the metapage is always block 0.
pub const HASH_METAPAGE: BlockNumber = 0;

/// `HASH_MAGIC` -- magic number for hash tables.
pub const HASH_MAGIC: uint32 = 0x6440_640;

/// `HASH_VERSION` -- version ID.
pub const HASH_VERSION: uint32 = 4;

/// `HASH_MAX_BITMAPS` -- `Min(BLCKSZ / 8, 1024)`.
pub const HASH_MAX_BITMAPS: usize = {
    let by_block = (BLCKSZ as usize) / 8;
    if by_block < 1024 {
        by_block
    } else {
        1024
    }
};

pub const HASH_SPLITPOINT_PHASE_BITS: usize = 2;
/// `HASH_SPLITPOINT_PHASES_PER_GRP` -- `(1 << HASH_SPLITPOINT_PHASE_BITS)` (hash.h:233).
pub const HASH_SPLITPOINT_PHASES_PER_GRP: usize = 1 << HASH_SPLITPOINT_PHASE_BITS;
pub const HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE: usize = 10;
/// `HASH_MAX_SPLITPOINT_GROUP` -- max number of splitpoint phases (hash.h:238).
pub const HASH_MAX_SPLITPOINT_GROUP: usize = 32;

/// `HASH_MAX_SPLITPOINTS` -- maximum number of splitpoints (hash.h:239-242).
///
/// `(((HASH_MAX_SPLITPOINT_GROUP - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) *
///    HASH_SPLITPOINT_PHASES_PER_GRP) + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)`
/// = `(((32 - 10) * 4) + 10)` = 98 with the 18.3 phase parameters.
pub const HASH_MAX_SPLITPOINTS: usize = ((HASH_MAX_SPLITPOINT_GROUP
    - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
    * HASH_SPLITPOINT_PHASES_PER_GRP)
    + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;
const _: () = assert!(HASH_MAX_SPLITPOINTS == 98);

/// `HashPageOpaqueData` -- stored in the special area at the end of every hash
/// page (`src/include/access/hash.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HashPageOpaqueData {
    /// previous block in the bucket chain (or special sentinel during split).
    pub hasho_prevblkno: BlockNumber,
    /// next block in the bucket chain, or `InvalidBlockNumber`.
    pub hasho_nextblkno: BlockNumber,
    /// bucket number this page belongs to.
    pub hasho_bucket: Bucket,
    /// page type code + flag bits.
    pub hasho_flag: uint16,
    /// for identification of hash indexes.
    pub hasho_page_id: uint16,
}

/// `HashMetaPageData` -- the contents of the hash metapage
/// (`src/include/access/hash.h`).
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HashMetaPageData {
    /// magic no. for hash tables.
    pub hashm_magic: uint32,
    /// version ID.
    pub hashm_version: uint32,
    /// number of tuples stored in the table.
    pub hashm_ntuples: f64,
    /// target fill factor (tuples/bucket).
    pub hashm_ffactor: uint16,
    /// index page size (bytes).
    pub hashm_bsize: uint16,
    /// bitmap array size (bytes) -- must be a power of 2.
    pub hashm_bmsize: uint16,
    /// `log2(bitmap array size in BITS)`.
    pub hashm_bmshift: uint16,
    /// ID of maximum bucket in use.
    pub hashm_maxbucket: uint32,
    /// mask to modulo into entire table.
    pub hashm_highmask: uint32,
    /// mask to modulo into lower half of table.
    pub hashm_lowmask: uint32,
    /// splitpoint from which ovflpage being allocated.
    pub hashm_ovflpoint: uint32,
    /// lowest-number free ovflpage (bit#).
    pub hashm_firstfree: uint32,
    /// number of bitmap pages.
    pub hashm_nmaps: uint32,
    /// hash function id from pg_proc.
    pub hashm_procid: RegProcedure,
    /// spare pages before each splitpoint.
    pub hashm_spares: [uint32; HASH_MAX_SPLITPOINTS],
    /// blknos of ovfl bitmaps.
    pub hashm_mapp: [BlockNumber; HASH_MAX_BITMAPS],
}

// ===========================================================================
// WAL records / opcodes (`src/include/access/hash_xlog.h`).
//
// These are the on-the-wire fixed bodies of every hash WAL record (emitted by
// the engine, replayed by `hash_redo`).  Each is `#[repr(C)]` with the exact C
// field order; the `SizeOf*` constants reproduce the C `offsetof(...) +
// sizeof(last_field)` (which strips trailing struct padding so the wire image
// is byte-for-byte identical).
// ===========================================================================

/// Number of buffers required for `XLOG_HASH_SQUEEZE_PAGE`.
pub const HASH_XLOG_FREE_OVFL_BUFS: usize = 6;

// -- XLOG record opcode info bytes (`XLogRecGetInfo(record) & ~XLR_INFO_MASK`) --

/// `XLOG_HASH_INIT_META_PAGE` -- initialize the meta page.
pub const XLOG_HASH_INIT_META_PAGE: uint8 = 0x00;
/// `XLOG_HASH_INIT_BITMAP_PAGE` -- initialize the bitmap page.
pub const XLOG_HASH_INIT_BITMAP_PAGE: uint8 = 0x10;
/// `XLOG_HASH_INSERT` -- add index tuple without split.
pub const XLOG_HASH_INSERT: uint8 = 0x20;
/// `XLOG_HASH_ADD_OVFL_PAGE` -- add overflow page.
pub const XLOG_HASH_ADD_OVFL_PAGE: uint8 = 0x30;
/// `XLOG_HASH_SPLIT_ALLOCATE_PAGE` -- allocate new page for split.
pub const XLOG_HASH_SPLIT_ALLOCATE_PAGE: uint8 = 0x40;
/// `XLOG_HASH_SPLIT_PAGE` -- split page.
pub const XLOG_HASH_SPLIT_PAGE: uint8 = 0x50;
/// `XLOG_HASH_SPLIT_COMPLETE` -- completion of split operation.
pub const XLOG_HASH_SPLIT_COMPLETE: uint8 = 0x60;
/// `XLOG_HASH_MOVE_PAGE_CONTENTS` -- remove tuples from one page, add to another.
pub const XLOG_HASH_MOVE_PAGE_CONTENTS: uint8 = 0x70;
/// `XLOG_HASH_SQUEEZE_PAGE` -- add tuples to a previous page and free ovfl page.
pub const XLOG_HASH_SQUEEZE_PAGE: uint8 = 0x80;
/// `XLOG_HASH_DELETE` -- delete index tuples from a page.
pub const XLOG_HASH_DELETE: uint8 = 0x90;
/// `XLOG_HASH_SPLIT_CLEANUP` -- clear split-cleanup flag in primary bucket page.
pub const XLOG_HASH_SPLIT_CLEANUP: uint8 = 0xA0;
/// `XLOG_HASH_UPDATE_META_PAGE` -- update meta page after vacuum.
pub const XLOG_HASH_UPDATE_META_PAGE: uint8 = 0xB0;
/// `XLOG_HASH_VACUUM_ONE_PAGE` -- remove dead tuples from index page.
pub const XLOG_HASH_VACUUM_ONE_PAGE: uint8 = 0xC0;

// -- xl_hash_split_allocate_page flag bits --

/// `XLH_SPLIT_META_UPDATE_MASKS`.
pub const XLH_SPLIT_META_UPDATE_MASKS: uint8 = 1 << 0;
/// `XLH_SPLIT_META_UPDATE_SPLITPOINT`.
pub const XLH_SPLIT_META_UPDATE_SPLITPOINT: uint8 = 1 << 1;

const fn maxalign_usize(v: usize) -> usize {
    (v + 8 - 1) & !(8 - 1)
}

/// `xl_hash_insert` (`XLOG_HASH_INSERT`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_insert {
    /// Offset number at which the tuple was inserted.
    pub offnum: OffsetNumber,
}
/// `SizeOfHashInsert == offsetof(xl_hash_insert, offnum) + sizeof(OffsetNumber)`.
pub const SizeOfHashInsert: usize = core::mem::size_of::<xl_hash_insert>();

/// `xl_hash_add_ovfl_page` (`XLOG_HASH_ADD_OVFL_PAGE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_add_ovfl_page {
    pub bmsize: uint16,
    pub bmpage_found: bool,
}
/// `SizeOfHashAddOvflPage == offsetof(bmpage_found) + sizeof(bool)`.
pub const SizeOfHashAddOvflPage: usize =
    core::mem::offset_of!(xl_hash_add_ovfl_page, bmpage_found) + 1;

/// `xl_hash_split_allocate_page` (`XLOG_HASH_SPLIT_ALLOCATE_PAGE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_split_allocate_page {
    pub new_bucket: uint32,
    pub old_bucket_flag: uint16,
    pub new_bucket_flag: uint16,
    pub flags: uint8,
}
/// `SizeOfHashSplitAllocPage == offsetof(flags) + sizeof(uint8)`.
pub const SizeOfHashSplitAllocPage: usize =
    core::mem::offset_of!(xl_hash_split_allocate_page, flags) + 1;

/// `xl_hash_split_complete` (`XLOG_HASH_SPLIT_COMPLETE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_split_complete {
    pub old_bucket_flag: uint16,
    pub new_bucket_flag: uint16,
}
/// `SizeOfHashSplitComplete == offsetof(new_bucket_flag) + sizeof(uint16)`.
pub const SizeOfHashSplitComplete: usize =
    core::mem::offset_of!(xl_hash_split_complete, new_bucket_flag) + 2;

/// `xl_hash_move_page_contents` (`XLOG_HASH_MOVE_PAGE_CONTENTS`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_move_page_contents {
    pub ntups: uint16,
    /// true if the page tuples are moved to is the primary bucket page.
    pub is_prim_bucket_same_wrt: bool,
}
/// `SizeOfHashMovePageContents == offsetof(is_prim_bucket_same_wrt) + sizeof(bool)`.
pub const SizeOfHashMovePageContents: usize =
    core::mem::offset_of!(xl_hash_move_page_contents, is_prim_bucket_same_wrt) + 1;

/// `xl_hash_squeeze_page` (`XLOG_HASH_SQUEEZE_PAGE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_squeeze_page {
    pub prevblkno: BlockNumber,
    pub nextblkno: BlockNumber,
    pub ntups: uint16,
    /// true if the page tuples are moved to is the primary bucket page.
    pub is_prim_bucket_same_wrt: bool,
    /// true if the page tuples are moved to is the page previous to the freed
    /// overflow page.
    pub is_prev_bucket_same_wrt: bool,
}
/// `SizeOfHashSqueezePage == offsetof(is_prev_bucket_same_wrt) + sizeof(bool)`.
pub const SizeOfHashSqueezePage: usize =
    core::mem::offset_of!(xl_hash_squeeze_page, is_prev_bucket_same_wrt) + 1;

/// `xl_hash_delete` (`XLOG_HASH_DELETE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_delete {
    /// true if this operation clears `LH_PAGE_HAS_DEAD_TUPLES`.
    pub clear_dead_marking: bool,
    /// true if the operation is for the primary bucket page.
    pub is_primary_bucket_page: bool,
}
/// `SizeOfHashDelete == offsetof(is_primary_bucket_page) + sizeof(bool)`.
pub const SizeOfHashDelete: usize =
    core::mem::offset_of!(xl_hash_delete, is_primary_bucket_page) + 1;

/// `xl_hash_update_meta_page` (`XLOG_HASH_UPDATE_META_PAGE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_update_meta_page {
    pub ntuples: f64,
}
/// `SizeOfHashUpdateMetaPage == offsetof(ntuples) + sizeof(double)`.
pub const SizeOfHashUpdateMetaPage: usize = core::mem::size_of::<xl_hash_update_meta_page>();

/// `xl_hash_init_meta_page` (`XLOG_HASH_INIT_META_PAGE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_init_meta_page {
    pub num_tuples: f64,
    pub procid: RegProcedure,
    pub ffactor: uint16,
}
/// `SizeOfHashInitMetaPage == offsetof(ffactor) + sizeof(uint16)`.
pub const SizeOfHashInitMetaPage: usize =
    core::mem::offset_of!(xl_hash_init_meta_page, ffactor) + 2;

/// `xl_hash_init_bitmap_page` (`XLOG_HASH_INIT_BITMAP_PAGE`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_init_bitmap_page {
    pub bmsize: uint16,
}
/// `SizeOfHashInitBitmapPage == offsetof(bmsize) + sizeof(uint16)`.
pub const SizeOfHashInitBitmapPage: usize = core::mem::size_of::<xl_hash_init_bitmap_page>();

/// `xl_hash_vacuum_one_page` fixed header (`XLOG_HASH_VACUUM_ONE_PAGE`); the
/// flexible `OffsetNumber offsets[]` array follows and is registered/replayed
/// separately.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub struct xl_hash_vacuum_one_page {
    pub snapshotConflictHorizon: TransactionId,
    pub ntuples: uint16,
    /// to handle recovery conflict during logical decoding on standby.
    pub isCatalogRel: bool,
    // OffsetNumber offsets[FLEXIBLE_ARRAY_MEMBER];
}
/// `SizeOfHashVacuumOnePage == offsetof(xl_hash_vacuum_one_page, offsets)`.
///
/// The flexible `offsets[]` (an `OffsetNumber`, align 2) follows the 7-byte
/// fixed prefix, so `offsetof(offsets)` is the prefix rounded up to align 2 ==
/// 8.
pub const SizeOfHashVacuumOnePage: usize = {
    let fixed = core::mem::offset_of!(xl_hash_vacuum_one_page, isCatalogRel) + 1;
    let align = core::mem::align_of::<OffsetNumber>();
    (fixed + align - 1) & !(align - 1)
};

// ---------------------------------------------------------------------------
// Compile-time layout assertions.
// ---------------------------------------------------------------------------

const _: () = {
    // HashPageOpaqueData: 2*BlockNumber (8) + Bucket (4) + 2*uint16 (4) = 16.
    assert!(core::mem::size_of::<HashPageOpaqueData>() == 16);
    assert!(core::mem::align_of::<HashPageOpaqueData>() == 4);

    // HashMetaPageData fixed prefix must align as in C: the `double` forces an
    // 8-byte alignment on the whole struct.
    assert!(core::mem::align_of::<HashMetaPageData>() == 8);

    // WAL record SizeOf* parity with the C offsetof()-based macros.
    assert!(SizeOfHashInsert == 2);
    assert!(SizeOfHashAddOvflPage == 3);
    assert!(SizeOfHashSplitAllocPage == 9);
    assert!(SizeOfHashSplitComplete == 4);
    assert!(SizeOfHashMovePageContents == 3);
    assert!(SizeOfHashSqueezePage == 12);
    assert!(SizeOfHashDelete == 2);
    assert!(SizeOfHashUpdateMetaPage == 8);
    assert!(SizeOfHashInitMetaPage == 14);
    assert!(SizeOfHashInitBitmapPage == 2);
    assert!(SizeOfHashVacuumOnePage == 8);
    // silence unused on the maxalign helper if the layout math changes.
    assert!(maxalign_usize(7) == 8);
};
