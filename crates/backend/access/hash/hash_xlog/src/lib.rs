//! `backend-access-hash-xlog` — an owned-tree Rust port of
//! `src/backend/access/hash/hash_xlog.c` (PostgreSQL 18.3): the WAL redo
//! (`hash_redo`) and consistency-mask (`hash_mask`) resource-manager callbacks
//! for hash indexes.
//!
//! `hash_redo` dispatches a decoded WAL record to its per-op handler
//! (`hash_xlog_init_meta_page` / `hash_xlog_init_bitmap_page` /
//! `hash_xlog_insert` / `hash_xlog_add_ovfl_page` /
//! `hash_xlog_split_allocate_page` / `hash_xlog_split_page` /
//! `hash_xlog_split_complete` / `hash_xlog_move_page_contents` /
//! `hash_xlog_squeeze_page` / `hash_xlog_delete` / `hash_xlog_split_cleanup` /
//! `hash_xlog_update_meta_page` / `hash_xlog_vacuum_one_page`), reading the
//! record through the xlogreader value-typed accessors, fetching the redo
//! buffers via xlogutils' `XLogReadBufferForRedo` / `XLogInitBufferForRedo`,
//! and applying the page edits with the bufpage primitives (`PageInit`,
//! `PageAddItemExtended`, `PageIndexMultiDelete`).
//!
//! ## What is grounded in-crate vs. what is seamed
//!
//! There is no ported sibling `hashpage` / `hashovfl` / `hashutil` crate yet
//! (`backend-access-hash-core` and friends are still `todo`), so the hash
//! page-byte primitives the redo path needs —
//! `_hash_pageinit` / `_hash_initbuf` / `_hash_initbitmapbuffer` /
//! `_hash_init_metabuffer` (hashpage.c / hashovfl.c, whose bodies pull in
//! hashutil.c's splitpoint bit math `_hash_spareindex` /
//! `_hash_get_totalbuckets`), the `HashPageOpaqueData` (`hasho_*`) and
//! `HashMetaPageData` (`hashm_*`) field accessors, and the bitmap `SETBIT` /
//! `CLRBIT` arithmetic — are transcribed 1:1 here against the `BLCKSZ` page
//! bytes, exactly as the freshly-landed `backend-access-brin-xlog` port does
//! for its brin_page/brin_pageops primitives. They move to the hash-core crate
//! once it lands.
//!
//! The genuinely-external WAL-recovery substrate crosses seams / owner crates:
//! `XLogReadBufferForRedo[Extended]` / `XLogInitBufferForRedo` (xlogutils), the
//! `XLogReaderState` block-tag accessor (xlogreader), the buffer manager
//! (`with_buffer_page` / `BufferGetBlockNumber` / `MarkBufferDirty` /
//! `UnlockReleaseBuffer` / `FlushOneBuffer`), the page-masking helpers
//! (bufmask), and the Hot-Standby recovery-conflict resolver (standby). The
//! decoded-record main data / per-block data / info byte are read off
//! `record.record` (the decoded payload owned by xlogreader).
//!
//! No raw pointers, no `extern "C"`, no `unsafe`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use ::mcx::MemoryContext;
use ::types_core::primitive::{
    BlockNumber, ForkNumber, InvalidBlockNumber, OffsetNumber, RegProcedure, BLCKSZ,
};
use ::types_core::XLogRecPtr;
use types_error::{PgError, PgResult, PANIC};
use ::types_storage::storage::{Buffer, InvalidBuffer, ReadBufferMode};
use ::types_storage::RelFileLocator;
use ::wal::rmgr::XLogReaderState;
use ::wal::xlogutils::in_hot_standby;
use ::wal::XLogRedoAction;
use ::xlog_records::hash_xlog::{
    xl_hash_add_ovfl_page, xl_hash_delete, xl_hash_init_bitmap_page, xl_hash_init_meta_page,
    xl_hash_insert, xl_hash_move_page_contents, xl_hash_split_allocate_page,
    xl_hash_split_complete, xl_hash_squeeze_page, xl_hash_update_meta_page,
    xl_hash_vacuum_one_page,
};

use page::{PageAddItemExtended, PageIndexMultiDelete, PageInit, PageMut};

use bufmask_seams as bufmask;
use ::xlogreader_seams::xlog_rec_get_block_tag_extended;
use xlogutils_seams as xlogutils;
use bufmgr_seams as bufmgr;
use standby_seams as standby;

#[cfg(test)]
mod tests;

// ===========================================================================
// hash_xlog.h opcodes (access/hash_xlog.h). The opcode lives in the high
// nibble; `XLR_INFO_MASK` (xlogrecord.h) masks OFF the low four framework bits.
// ===========================================================================

/// `XLR_INFO_MASK` (xlogrecord.h) = `0x0F`.
const XLR_INFO_MASK: u8 = 0x0F;

const XLOG_HASH_INIT_META_PAGE: u8 = 0x00;
const XLOG_HASH_INIT_BITMAP_PAGE: u8 = 0x10;
const XLOG_HASH_INSERT: u8 = 0x20;
const XLOG_HASH_ADD_OVFL_PAGE: u8 = 0x30;
const XLOG_HASH_SPLIT_ALLOCATE_PAGE: u8 = 0x40;
const XLOG_HASH_SPLIT_PAGE: u8 = 0x50;
const XLOG_HASH_SPLIT_COMPLETE: u8 = 0x60;
const XLOG_HASH_MOVE_PAGE_CONTENTS: u8 = 0x70;
const XLOG_HASH_SQUEEZE_PAGE: u8 = 0x80;
const XLOG_HASH_DELETE: u8 = 0x90;
const XLOG_HASH_SPLIT_CLEANUP: u8 = 0xA0;
const XLOG_HASH_UPDATE_META_PAGE: u8 = 0xB0;
const XLOG_HASH_VACUUM_ONE_PAGE: u8 = 0xC0;

/// `XLH_SPLIT_META_UPDATE_MASKS` (hash_xlog.h).
const XLH_SPLIT_META_UPDATE_MASKS: u8 = 1 << 0;
/// `XLH_SPLIT_META_UPDATE_SPLITPOINT` (hash_xlog.h).
const XLH_SPLIT_META_UPDATE_SPLITPOINT: u8 = 1 << 1;

// ===========================================================================
// hash.h constants.
// ===========================================================================

/// `InvalidBucket` (hash.h): `(Bucket) 0xFFFFFFFF`.
const InvalidBucket: u32 = 0xFFFF_FFFF;

/// `HASHO_PAGE_ID` (hash.h) — identifies hash index pages.
const HASHO_PAGE_ID: u16 = 0xFF80;

// `hasho_flag` page-type / flag bits.
const LH_UNUSED_PAGE: u16 = 0;
const LH_OVERFLOW_PAGE: u16 = 1 << 0;
const LH_BUCKET_PAGE: u16 = 1 << 1;
const LH_BITMAP_PAGE: u16 = 1 << 2;
const LH_META_PAGE: u16 = 1 << 3;
const LH_BUCKET_NEEDS_SPLIT_CLEANUP: u16 = 1 << 6;
const LH_PAGE_HAS_DEAD_TUPLES: u16 = 1 << 7;

/// `LH_PAGE_TYPE` (hash.h): the page-type bits.
const LH_PAGE_TYPE: u16 =
    LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

/// `HASH_MAGIC` (hash.h).
const HASH_MAGIC: u32 = 0x6440640;
/// `HASH_VERSION` (hash.h).
const HASH_VERSION: u32 = 4;

/// `BYTE_TO_BIT` (hash.h): 2^3 bits/byte.
const BYTE_TO_BIT: u32 = 3;
/// `BITS_PER_MAP` (hash.h): bits in an ovflpage bitmap word.
const BITS_PER_MAP: u32 = 32;

/// `HASH_SPLITPOINT_PHASE_BITS` (hash.h).
const HASH_SPLITPOINT_PHASE_BITS: u32 = 2;
/// `HASH_SPLITPOINT_PHASES_PER_GRP` (hash.h) = `1 << HASH_SPLITPOINT_PHASE_BITS`.
const HASH_SPLITPOINT_PHASES_PER_GRP: u32 = 1 << HASH_SPLITPOINT_PHASE_BITS;
/// `HASH_SPLITPOINT_PHASE_MASK` (hash.h).
const HASH_SPLITPOINT_PHASE_MASK: u32 = HASH_SPLITPOINT_PHASES_PER_GRP - 1;
/// `HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE` (hash.h).
const HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE: u32 = 10;

/// `HASH_MAX_BITMAPS` (hash.h): `Min(BLCKSZ / 8, 1024)`.
const HASH_MAX_BITMAPS: usize = {
    let a = BLCKSZ / 8;
    if a < 1024 {
        a
    } else {
        1024
    }
};

/// `HASH_MAX_SPLITPOINT_GROUP` (hash.h): `32` (number of bits in `Bucket`).
const HASH_MAX_SPLITPOINT_GROUP: u32 = 32;

/// `HASH_MAX_SPLITPOINTS` (hash.h):
/// `((HASH_MAX_SPLITPOINT_GROUP - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) *
///   HASH_SPLITPOINT_PHASES_PER_GRP) + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE`.
const HASH_MAX_SPLITPOINTS: usize = (((HASH_MAX_SPLITPOINT_GROUP
    - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
    * HASH_SPLITPOINT_PHASES_PER_GRP)
    + HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) as usize;

// ===========================================================================
// Byte-level page layout helpers (transcribed from bufpage.h / hash.h).
// ===========================================================================

/// `MAXALIGN(x)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// `SizeOfPageHeaderData` (bufpage.h).
const SizeOfPageHeaderData: usize = 24;

/// `PageGetContents(page)` offset (bufpage.h): the area after the MAXALIGN'd
/// page header. `HashPageGetMeta` and `HashPageGetBitmap` both begin here.
const CONTENTS_OFFSET: usize = maxalign(SizeOfPageHeaderData);

/// `sizeof(HashPageOpaqueData)` (hash.h): `{BlockNumber hasho_prevblkno;
/// BlockNumber hasho_nextblkno; Bucket hasho_bucket; uint16 hasho_flag;
/// uint16 hasho_page_id;}` = 4 + 4 + 4 + 2 + 2 = 16 bytes (no trailing pad).
const SIZEOF_HASH_PAGE_OPAQUE_DATA: usize = 16;

/// Byte offset of `pd_lower` within `PageHeaderData` (the uint16 at offset 12).
const OFF_PD_LOWER: usize = 12;

/// `((PageHeader) page)->pd_lower = value`.
fn set_pd_lower(page: &mut [u8], value: u16) {
    page[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

// --- HashPageOpaqueData accessors over the page special area ----------------
//
// struct HashPageOpaqueData {
//   BlockNumber hasho_prevblkno;  // off 0
//   BlockNumber hasho_nextblkno;  // off 4
//   Bucket      hasho_bucket;     // off 8
//   uint16      hasho_flag;       // off 12
//   uint16      hasho_page_id;    // off 14
// }

const HOP_OFF_PREVBLKNO: usize = 0;
const HOP_OFF_NEXTBLKNO: usize = 4;
const HOP_OFF_BUCKET: usize = 8;
const HOP_OFF_FLAG: usize = 12;
const HOP_OFF_PAGE_ID: usize = 14;

/// Byte offset of the `HashPageOpaqueData` special area
/// (`PageGetSpecialPointer(page)`): read `pd_special` directly.
fn special_offset(page: &[u8]) -> usize {
    // pd_special is the uint16 at PageHeaderData offset 16.
    u16::from_ne_bytes([page[16], page[16 + 1]]) as usize
}

fn hasho_flag(page: &[u8]) -> u16 {
    let s = special_offset(page);
    u16::from_ne_bytes([page[s + HOP_OFF_FLAG], page[s + HOP_OFF_FLAG + 1]])
}

fn set_hasho_flag(page: &mut [u8], flag: u16) {
    let s = special_offset(page);
    page[s + HOP_OFF_FLAG..s + HOP_OFF_FLAG + 2].copy_from_slice(&flag.to_ne_bytes());
}

fn set_hasho_prevblkno(page: &mut [u8], blk: BlockNumber) {
    let s = special_offset(page);
    page[s + HOP_OFF_PREVBLKNO..s + HOP_OFF_PREVBLKNO + 4].copy_from_slice(&blk.to_ne_bytes());
}

fn set_hasho_nextblkno(page: &mut [u8], blk: BlockNumber) {
    let s = special_offset(page);
    page[s + HOP_OFF_NEXTBLKNO..s + HOP_OFF_NEXTBLKNO + 4].copy_from_slice(&blk.to_ne_bytes());
}

fn set_hasho_bucket(page: &mut [u8], bucket: u32) {
    let s = special_offset(page);
    page[s + HOP_OFF_BUCKET..s + HOP_OFF_BUCKET + 4].copy_from_slice(&bucket.to_ne_bytes());
}

fn set_hasho_page_id(page: &mut [u8], id: u16) {
    let s = special_offset(page);
    page[s + HOP_OFF_PAGE_ID..s + HOP_OFF_PAGE_ID + 2].copy_from_slice(&id.to_ne_bytes());
}

// --- HashMetaPageData accessors over the page contents area -----------------
//
// struct HashMetaPageData {
//   uint32 hashm_magic;        // off 0
//   uint32 hashm_version;      // off 4
//   double hashm_ntuples;      // off 8   (8-byte aligned)
//   uint16 hashm_ffactor;      // off 16
//   uint16 hashm_bsize;        // off 18
//   uint16 hashm_bmsize;       // off 20
//   uint16 hashm_bmshift;      // off 22
//   uint32 hashm_maxbucket;    // off 24
//   uint32 hashm_highmask;     // off 28
//   uint32 hashm_lowmask;      // off 32
//   uint32 hashm_ovflpoint;    // off 36
//   uint32 hashm_firstfree;    // off 40
//   uint32 hashm_nmaps;        // off 44
//   RegProcedure hashm_procid; // off 48 (uint32)
//   uint32 hashm_spares[HASH_MAX_SPLITPOINTS];   // off 52
//   BlockNumber hashm_mapp[HASH_MAX_BITMAPS];    // after spares
// }
//
// All field byte offsets are relative to PageGetContents (CONTENTS_OFFSET).

const META_OFF_MAGIC: usize = 0;
const META_OFF_VERSION: usize = 4;
const META_OFF_NTUPLES: usize = 8;
const META_OFF_FFACTOR: usize = 16;
const META_OFF_BSIZE: usize = 18;
const META_OFF_BMSIZE: usize = 20;
const META_OFF_BMSHIFT: usize = 22;
const META_OFF_MAXBUCKET: usize = 24;
const META_OFF_HIGHMASK: usize = 28;
const META_OFF_LOWMASK: usize = 32;
const META_OFF_OVFLPOINT: usize = 36;
const META_OFF_FIRSTFREE: usize = 40;
const META_OFF_NMAPS: usize = 44;
const META_OFF_PROCID: usize = 48;
const META_OFF_SPARES: usize = 52;
const META_OFF_MAPP: usize = META_OFF_SPARES + HASH_MAX_SPLITPOINTS * 4;

/// `sizeof(HashMetaPageData)`.
const SIZEOF_HASH_META_PAGE_DATA: usize = META_OFF_MAPP + HASH_MAX_BITMAPS * 4;

/// Absolute page byte offset of a `HashMetaPageData` field.
#[inline]
fn meta(off: usize) -> usize {
    CONTENTS_OFFSET + off
}

fn meta_get_u32(page: &[u8], field_off: usize) -> u32 {
    let o = meta(field_off);
    u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]])
}

fn meta_set_u32(page: &mut [u8], field_off: usize, value: u32) {
    let o = meta(field_off);
    page[o..o + 4].copy_from_slice(&value.to_ne_bytes());
}

fn meta_get_f64(page: &[u8], field_off: usize) -> f64 {
    let o = meta(field_off);
    let mut a = [0u8; 8];
    a.copy_from_slice(&page[o..o + 8]);
    f64::from_ne_bytes(a)
}

fn meta_set_f64(page: &mut [u8], field_off: usize, value: f64) {
    let o = meta(field_off);
    page[o..o + 8].copy_from_slice(&value.to_ne_bytes());
}

fn meta_set_u16(page: &mut [u8], field_off: usize, value: u16) {
    let o = meta(field_off);
    page[o..o + 2].copy_from_slice(&value.to_ne_bytes());
}

/// `metap->hashm_spares[i]`.
fn meta_get_spare(page: &[u8], i: usize) -> u32 {
    meta_get_u32(page, META_OFF_SPARES + i * 4)
}

fn meta_set_spare(page: &mut [u8], i: usize, value: u32) {
    meta_set_u32(page, META_OFF_SPARES + i * 4, value);
}

/// `metap->hashm_mapp[i] = blk`.
fn meta_set_mapp(page: &mut [u8], i: usize, blk: BlockNumber) {
    meta_set_u32(page, META_OFF_MAPP + i * 4, blk);
}

// --- HashPageGetBitmap (uint32 array at PageGetContents) ---------------------

/// `SETBIT(freep, n)` over the bitmap page's `HashPageGetBitmap` array, which
/// begins at `PageGetContents` (`CONTENTS_OFFSET`).
fn bitmap_setbit(page: &mut [u8], n: u32) {
    let word = (n / BITS_PER_MAP) as usize;
    let bit = n % BITS_PER_MAP;
    let o = CONTENTS_OFFSET + word * 4;
    let mut v = u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]]);
    v |= 1 << bit;
    page[o..o + 4].copy_from_slice(&v.to_ne_bytes());
}

/// `CLRBIT(freep, n)`.
fn bitmap_clrbit(page: &mut [u8], n: u32) {
    let word = (n / BITS_PER_MAP) as usize;
    let bit = n % BITS_PER_MAP;
    let o = CONTENTS_OFFSET + word * 4;
    let mut v = u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]]);
    v &= !(1 << bit);
    page[o..o + 4].copy_from_slice(&v.to_ne_bytes());
}

// ===========================================================================
// hashutil.c splitpoint bit math (used by _hash_init_metabuffer).
// ===========================================================================

/// `pg_ceil_log2_32(num)` (pg_bitutils.h): `num <= 1 ? 0 :
/// pg_leftmost_one_pos32(num - 1) + 1`.
fn pg_ceil_log2_32(num: u32) -> u32 {
    if num <= 1 {
        0
    } else {
        (31 - (num - 1).leading_zeros()) + 1
    }
}

/// `pg_leftmost_one_pos32(word)` (pg_bitutils.h): position of the most
/// significant set bit (0-based), `word != 0`.
fn pg_leftmost_one_pos32(word: u32) -> u32 {
    debug_assert!(word != 0);
    31 - word.leading_zeros()
}

/// `pg_nextpower2_32(num)` (pg_bitutils.h): smallest power of 2 >= num
/// (`num >= 1`).
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num >= 1);
    if num <= 1 {
        1
    } else {
        1u32 << (pg_leftmost_one_pos32(num - 1) + 1)
    }
}

/// `_hash_spareindex(num_bucket)` (hashutil.c).
fn _hash_spareindex(num_bucket: u32) -> u32 {
    let splitpoint_group = pg_ceil_log2_32(num_bucket);

    if splitpoint_group < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
        return splitpoint_group;
    }

    let mut splitpoint_phases = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;
    splitpoint_phases += (splitpoint_group - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
        << HASH_SPLITPOINT_PHASE_BITS;
    splitpoint_phases += ((num_bucket - 1)
        >> (splitpoint_group - (HASH_SPLITPOINT_PHASE_BITS + 1)))
        & HASH_SPLITPOINT_PHASE_MASK;

    splitpoint_phases
}

/// `_hash_get_totalbuckets(splitpoint_phase)` (hashutil.c).
fn _hash_get_totalbuckets(splitpoint_phase: u32) -> u32 {
    if splitpoint_phase < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
        return 1 << splitpoint_phase;
    }

    let mut splitpoint_group = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;
    splitpoint_group += (splitpoint_phase - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
        >> HASH_SPLITPOINT_PHASE_BITS;

    let mut total_buckets = 1u32 << (splitpoint_group - 1);

    let phases_within_splitpoint_group = ((splitpoint_phase
        - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
        & HASH_SPLITPOINT_PHASE_MASK)
        + 1;
    total_buckets += ((1u32 << (splitpoint_group - 1)) >> HASH_SPLITPOINT_PHASE_BITS)
        * phases_within_splitpoint_group;

    total_buckets
}

// ===========================================================================
// _hash_pageinit / _hash_initbuf / _hash_initbitmapbuffer /
// _hash_init_metabuffer (hashpage.c / hashovfl.c), transcribed 1:1 against the
// page bytes. No ported hash-core sibling crate yet.
// ===========================================================================

/// `_hash_pageinit(page, size)` (hashpage.c): `PageInit(page, size,
/// sizeof(HashPageOpaqueData))`.
fn _hash_pageinit(page: &mut [u8], size: usize) -> PgResult<()> {
    PageInit(page, size, SIZEOF_HASH_PAGE_OPAQUE_DATA)
}

/// `_hash_initbuf(buf, max_bucket, num_bucket, flag, initpage)` (hashpage.c),
/// applied directly to the page bytes.
fn _hash_initbuf(
    page: &mut [u8],
    max_bucket: u32,
    num_bucket: u32,
    flag: u32,
    initpage: bool,
) -> PgResult<()> {
    if initpage {
        _hash_pageinit(page, page.len())?;
    }

    // hasho_prevblkno carries the current hashm_maxbucket (used to validate
    // cached HashMetaPageData).
    set_hasho_prevblkno(page, max_bucket);
    set_hasho_nextblkno(page, InvalidBlockNumber);
    set_hasho_bucket(page, num_bucket);
    set_hasho_flag(page, flag as u16);
    set_hasho_page_id(page, HASHO_PAGE_ID);
    Ok(())
}

/// `_hash_initbitmapbuffer(buf, bmsize, initpage)` (hashovfl.c), applied
/// directly to the page bytes.
fn _hash_initbitmapbuffer(page: &mut [u8], bmsize: u16, initpage: bool) -> PgResult<()> {
    if initpage {
        _hash_pageinit(page, page.len())?;
    }

    set_hasho_prevblkno(page, InvalidBlockNumber);
    set_hasho_nextblkno(page, InvalidBlockNumber);
    set_hasho_bucket(page, InvalidBucket);
    set_hasho_flag(page, LH_BITMAP_PAGE);
    set_hasho_page_id(page, HASHO_PAGE_ID);

    // freep = HashPageGetBitmap(pg); memset(freep, 0xFF, bmsize);
    let bm = bmsize as usize;
    for b in &mut page[CONTENTS_OFFSET..CONTENTS_OFFSET + bm] {
        *b = 0xFF;
    }

    // ((PageHeader) pg)->pd_lower = ((char *) freep + bmsize) - (char *) pg;
    set_pd_lower(page, (CONTENTS_OFFSET + bm) as u16);
    Ok(())
}

/// `_hash_init_metabuffer(buf, num_tuples, procid, ffactor, initpage)`
/// (hashpage.c), applied directly to the page bytes.
fn _hash_init_metabuffer(
    page: &mut [u8],
    num_tuples: f64,
    procid: RegProcedure,
    ffactor: u16,
    initpage: bool,
) -> PgResult<()> {
    // Choose the number of initial bucket pages.
    let dnumbuckets = num_tuples / (ffactor as f64);
    let num_buckets: u32 = if dnumbuckets <= 2.0 {
        2
    } else if dnumbuckets >= (0x4000_0000u32 as f64) {
        0x4000_0000
    } else {
        _hash_get_totalbuckets(_hash_spareindex(dnumbuckets as u32))
    };

    let spare_index = _hash_spareindex(num_buckets);
    debug_assert!((spare_index as usize) < HASH_MAX_SPLITPOINTS);

    if initpage {
        _hash_pageinit(page, page.len())?;
    }

    set_hasho_prevblkno(page, InvalidBlockNumber);
    set_hasho_nextblkno(page, InvalidBlockNumber);
    set_hasho_bucket(page, InvalidBucket);
    set_hasho_flag(page, LH_META_PAGE);
    set_hasho_page_id(page, HASHO_PAGE_ID);

    meta_set_u32(page, META_OFF_MAGIC, HASH_MAGIC);
    meta_set_u32(page, META_OFF_VERSION, HASH_VERSION);
    meta_set_f64(page, META_OFF_NTUPLES, 0.0);
    meta_set_u32(page, META_OFF_NMAPS, 0);
    meta_set_u16(page, META_OFF_FFACTOR, ffactor);

    // metap->hashm_bsize = HashGetMaxBitmapSize(page) =
    //   PageGetPageSize(page) - (MAXALIGN(SizeOfPageHeaderData) +
    //                            MAXALIGN(sizeof(HashPageOpaqueData)))
    let hashm_bsize = (page.len()
        - (maxalign(SizeOfPageHeaderData) + maxalign(SIZEOF_HASH_PAGE_OPAQUE_DATA)))
        as u16;
    meta_set_u16(page, META_OFF_BSIZE, hashm_bsize);

    // find largest bitmap array size that will fit in page size
    let lshift = pg_leftmost_one_pos32(hashm_bsize as u32);
    debug_assert!(lshift > 0);
    let hashm_bmsize = 1u16 << lshift;
    meta_set_u16(page, META_OFF_BMSIZE, hashm_bmsize);
    meta_set_u16(page, META_OFF_BMSHIFT, (lshift + BYTE_TO_BIT) as u16);

    meta_set_u32(page, META_OFF_PROCID, procid);

    // We initialize the index with N buckets, 0 .. N-1.
    meta_set_u32(page, META_OFF_MAXBUCKET, num_buckets - 1);

    let highmask = pg_nextpower2_32(num_buckets + 1) - 1;
    meta_set_u32(page, META_OFF_HIGHMASK, highmask);
    meta_set_u32(page, META_OFF_LOWMASK, highmask >> 1);

    // MemSet(hashm_spares, 0, ...); MemSet(hashm_mapp, 0, ...);
    for b in &mut page[meta(META_OFF_SPARES)..meta(META_OFF_SPARES) + HASH_MAX_SPLITPOINTS * 4] {
        *b = 0;
    }
    for b in &mut page[meta(META_OFF_MAPP)..meta(META_OFF_MAPP) + HASH_MAX_BITMAPS * 4] {
        *b = 0;
    }

    // Set up mapping for one spare page after the initial splitpoints.
    meta_set_spare(page, spare_index as usize, 1);
    meta_set_u32(page, META_OFF_OVFLPOINT, spare_index);
    meta_set_u32(page, META_OFF_FIRSTFREE, 0);

    // ((PageHeader) page)->pd_lower = ((char *) metap + sizeof(HashMetaPageData))
    //                                 - (char *) page;
    set_pd_lower(page, (CONTENTS_OFFSET + SIZEOF_HASH_META_PAGE_DATA) as u16);
    Ok(())
}

// ===========================================================================
// IndexTuple size (access/itup.h).
// ===========================================================================

/// `INDEX_SIZE_MASK` (itup.h): the size bits of `IndexTupleData.t_info`.
const INDEX_SIZE_MASK: u16 = 0x1FFF;

/// `IndexTupleSize(itup)` (itup.h): `t_info & INDEX_SIZE_MASK`. The
/// `IndexTupleData` header is 8 bytes; `t_info` is the uint16 at offset 6.
fn index_tuple_size(itup: &[u8]) -> usize {
    let t_info = u16::from_ne_bytes([itup[6], itup[7]]);
    (t_info & INDEX_SIZE_MASK) as usize
}

// ===========================================================================
// Decoded-record accessors (read off `record.record`, owned by xlogreader).
// ===========================================================================

/// `XLogRecGetData(record)` — the record's main data.
fn record_get_data<'a>(record: &'a XLogReaderState<'_>) -> &'a [u8] {
    record.record.as_ref().map(|r| r.data()).unwrap_or(&[])
}

/// `XLogRecGetInfo(record)` — the raw `xl_info` byte.
fn record_get_info(record: &XLogReaderState<'_>) -> u8 {
    record.record.as_ref().map(|r| r.info()).unwrap_or(0)
}

/// `XLogRecGetBlockData(record, block_id, &len)` — per-block data.
fn record_get_block_data<'a>(record: &'a XLogReaderState<'_>, block_id: u8) -> &'a [u8] {
    record
        .record
        .as_ref()
        .and_then(|r| r.block_data(block_id as usize))
        .unwrap_or(&[])
}

/// `XLogRecHasBlockRef(record, block_id)`.
fn record_has_block_ref(record: &XLogReaderState<'_>, block_id: u8) -> bool {
    record
        .record
        .as_ref()
        .map(|r| r.has_block_ref(block_id as usize))
        .unwrap_or(false)
}

/// `XLogRecGetBlockTag(record, block_id, NULL, &forknum, NULL)` — the fork.
fn block_tag_forknum(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<ForkNumber> {
    Ok(xlog_rec_get_block_tag_extended::call(record, block_id)?
        .map(|t| t.forknum)
        .unwrap_or(ForkNumber::MAIN_FORKNUM))
}

/// `XLogRecGetBlockTag(record, block_id, &rlocator, ...)` — the relation.
fn block_tag_rlocator(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<RelFileLocator> {
    Ok(xlog_rec_get_block_tag_extended::call(record, block_id)?
        .map(|t| t.rlocator)
        .unwrap_or_default())
}

/// `XLogRecGetBlockTag(record, block_id, NULL, NULL, &blk)` — block number.
fn block_tag_blknum(record: &XLogReaderState<'_>, block_id: u8) -> PgResult<BlockNumber> {
    Ok(xlog_rec_get_block_tag_extended::call(record, block_id)?
        .map(|t| t.blkno)
        .unwrap_or(InvalidBlockNumber))
}

// ===========================================================================
// Buffer helpers (storage/bufmgr.h).
// ===========================================================================

/// `BufferIsValid(buf)`.
#[inline]
fn buffer_is_valid(buf: Buffer) -> bool {
    buf != InvalidBuffer
}

/// Apply `f` to the page bytes of a redo buffer, then stamp the record LSN
/// (`PageSetLSN`) and `MarkBufferDirty`, mirroring the C
/// `PageSetLSN(page, lsn); MarkBufferDirty(buf);` sequence.
fn buffer_modify_page(
    buf: Buffer,
    lsn: XLogRecPtr,
    f: &mut dyn FnMut(&mut [u8]) -> PgResult<()>,
) -> PgResult<()> {
    bufmgr::with_buffer_page::call(buf, f)?;
    bufmgr::page_set_lsn::call(buf, lsn)?;
    bufmgr::mark_buffer_dirty::call(buf);
    Ok(())
}

/// Like [`buffer_modify_page`], but `f` returns whether the page was modified;
/// the LSN is stamped and the buffer marked dirty only when it was (mirrors the
/// squeeze handler's `mod_wbuf` gating).
fn buffer_modify_page_conditional(
    buf: Buffer,
    lsn: XLogRecPtr,
    f: &mut dyn FnMut(&mut [u8]) -> PgResult<bool>,
) -> PgResult<()> {
    let mut modified = false;
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        modified = f(page)?;
        Ok(())
    })?;
    if modified {
        bufmgr::page_set_lsn::call(buf, lsn)?;
        bufmgr::mark_buffer_dirty::call(buf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_init_meta_page (hash_xlog.c:26)
// ===========================================================================

fn hash_xlog_init_meta_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = xl_hash_init_meta_page::from_bytes(record_get_data(record));

    // create the index' metapage
    let metabuf = xlogutils::xlog_init_buffer_for_redo::call(record, 0)?;
    debug_assert!(buffer_is_valid(metabuf));
    buffer_modify_page(metabuf, lsn, &mut |page| {
        _hash_init_metabuffer(page, xlrec.num_tuples, xlrec.procid, xlrec.ffactor, true)
    })?;

    // Force the on-disk state of init forks to always be in sync with shared
    // buffers (create index doesn't log a full page image of the metapage).
    if block_tag_forknum(record, 0)? == ForkNumber::INIT_FORKNUM {
        bufmgr::flush_one_buffer::call(metabuf)?;
    }

    bufmgr::unlock_release_buffer::call(metabuf);
    Ok(())
}

// ===========================================================================
// hash_xlog_init_bitmap_page (hash_xlog.c:62)
// ===========================================================================

fn hash_xlog_init_bitmap_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = xl_hash_init_bitmap_page::from_bytes(record_get_data(record));

    // Initialize bitmap page.
    let bitmapbuf = xlogutils::xlog_init_buffer_for_redo::call(record, 0)?;
    buffer_modify_page(bitmapbuf, lsn, &mut |page| {
        _hash_initbitmapbuffer(page, xlrec.bmsize, true)
    })?;

    if block_tag_forknum(record, 0)? == ForkNumber::INIT_FORKNUM {
        bufmgr::flush_one_buffer::call(bitmapbuf)?;
    }
    bufmgr::unlock_release_buffer::call(bitmapbuf);

    // Add the new bitmap page to the metapage's list of bitmaps.
    let (action, metabuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(metabuf, lsn, &mut |page| {
            let num_buckets = meta_get_u32(page, META_OFF_MAXBUCKET) + 1;
            let nmaps = meta_get_u32(page, META_OFF_NMAPS);
            meta_set_mapp(page, nmaps as usize, num_buckets + 1);
            meta_set_u32(page, META_OFF_NMAPS, nmaps + 1);
            Ok(())
        })?;

        if block_tag_forknum(record, 1)? == ForkNumber::INIT_FORKNUM {
            bufmgr::flush_one_buffer::call(metabuf)?;
        }
    }
    if buffer_is_valid(metabuf) {
        bufmgr::unlock_release_buffer::call(metabuf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_insert (hash_xlog.c:124)
// ===========================================================================

fn hash_xlog_insert(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = xl_hash_insert::from_bytes(record_get_data(record));

    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let datapos = record_get_block_data(record, 0);
        let datalen = datapos.len();
        let offnum = xlrec.offnum;
        buffer_modify_page(buffer, lsn, &mut |page| {
            let placed = {
                let mut pmut = PageMut::new(page)?;
                PageAddItemExtended(&mut pmut, &datapos[..datalen], offnum, 0)?
            };
            if placed == InvalidOffsetNumber {
                return Err(PgError::new(PANIC, "hash_xlog_insert: failed to add item"));
            }
            Ok(())
        })?;
    }
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(buffer, lsn, &mut |page| {
            let n = meta_get_f64(page, META_OFF_NTUPLES);
            meta_set_f64(page, META_OFF_NTUPLES, n + 1.0);
            Ok(())
        })?;
    }
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_add_ovfl_page (hash_xlog.c:172)
// ===========================================================================

fn hash_xlog_add_ovfl_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = xl_hash_add_ovfl_page::from_bytes(record_get_data(record));

    let rightblk = block_tag_blknum(record, 0)?;
    let leftblk = block_tag_blknum(record, 1)?;
    let mut newmapblk: BlockNumber = InvalidBlockNumber;
    let mut new_bmpage = false;

    let ovflbuf = xlogutils::xlog_init_buffer_for_redo::call(record, 0)?;
    debug_assert!(buffer_is_valid(ovflbuf));

    let data = record_get_block_data(record, 0);
    debug_assert_eq!(data.len(), 4); // sizeof(uint32)
    let num_bucket = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
    buffer_modify_page(ovflbuf, lsn, &mut |page| {
        _hash_initbuf(page, InvalidBlockNumber, num_bucket, LH_OVERFLOW_PAGE as u32, true)?;
        // update backlink
        set_hasho_prevblkno(page, leftblk);
        Ok(())
    })?;

    let (action, leftbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(leftbuf, lsn, &mut |page| {
            set_hasho_nextblkno(page, rightblk);
            Ok(())
        })?;
    }

    if buffer_is_valid(leftbuf) {
        bufmgr::unlock_release_buffer::call(leftbuf);
    }
    bufmgr::unlock_release_buffer::call(ovflbuf);

    // Bitmap page: set the bit for the new ovfl page.
    if record_has_block_ref(record, 2) {
        let (action, mapbuffer) = xlogutils::xlog_read_buffer_for_redo::call(record, 2)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            let bdata = record_get_block_data(record, 2);
            let bit = u32::from_ne_bytes([bdata[0], bdata[1], bdata[2], bdata[3]]);
            buffer_modify_page(mapbuffer, lsn, &mut |page| {
                bitmap_setbit(page, bit);
                Ok(())
            })?;
        }
        if buffer_is_valid(mapbuffer) {
            bufmgr::unlock_release_buffer::call(mapbuffer);
        }
    }

    // New bitmap page.
    if record_has_block_ref(record, 3) {
        let newmapbuf = xlogutils::xlog_init_buffer_for_redo::call(record, 3)?;
        // Note: C marks dirty then sets LSN; ordering is irrelevant for
        // correctness.
        bufmgr::with_buffer_page::call(newmapbuf, &mut |page: &mut [u8]| {
            _hash_initbitmapbuffer(page, xlrec.bmsize, true)
        })?;
        new_bmpage = true;
        newmapblk = bufmgr::buffer_get_block_number::call(newmapbuf);
        bufmgr::mark_buffer_dirty::call(newmapbuf);
        bufmgr::page_set_lsn::call(newmapbuf, lsn)?;
        bufmgr::unlock_release_buffer::call(newmapbuf);
    }

    // Metapage.
    let (action, metabuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 4)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let mdata = record_get_block_data(record, 4);
        let firstfree_ovflpage = u32::from_ne_bytes([mdata[0], mdata[1], mdata[2], mdata[3]]);
        buffer_modify_page(metabuf, lsn, &mut |page| {
            meta_set_u32(page, META_OFF_FIRSTFREE, firstfree_ovflpage);

            if !xlrec.bmpage_found {
                let ovflpoint = meta_get_u32(page, META_OFF_OVFLPOINT) as usize;
                let spare = meta_get_spare(page, ovflpoint);
                meta_set_spare(page, ovflpoint, spare + 1);

                if new_bmpage {
                    debug_assert!(newmapblk != InvalidBlockNumber);

                    let nmaps = meta_get_u32(page, META_OFF_NMAPS);
                    meta_set_mapp(page, nmaps as usize, newmapblk);
                    meta_set_u32(page, META_OFF_NMAPS, nmaps + 1);
                    let spare = meta_get_spare(page, ovflpoint);
                    meta_set_spare(page, ovflpoint, spare + 1);
                }
            }
            Ok(())
        })?;
    }
    if buffer_is_valid(metabuf) {
        bufmgr::unlock_release_buffer::call(metabuf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_split_allocate_page (hash_xlog.c:310)
// ===========================================================================

fn hash_xlog_split_allocate_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = xl_hash_split_allocate_page::from_bytes(record_get_data(record));

    // Replay the record for the old bucket (cleanup lock).
    let (action, oldbuf) =
        xlogutils::xlog_read_buffer_for_redo_extended::call(record, 0, ReadBufferMode::Normal, true)?;

    // We still update the page even if it was restored from a full page image,
    // because the special space is not included in the image.
    if action == XLogRedoAction::BlkNeedsRedo || action == XLogRedoAction::BlkRestored {
        buffer_modify_page(oldbuf, lsn, &mut |page| {
            set_hasho_flag(page, xlrec.old_bucket_flag);
            set_hasho_prevblkno(page, xlrec.new_bucket);
            Ok(())
        })?;
    }

    // Replay the record for the new bucket.
    let (_action, newbuf) = xlogutils::xlog_read_buffer_for_redo_extended::call(
        record,
        1,
        ReadBufferMode::ZeroAndCleanupLock,
        true,
    )?;
    buffer_modify_page(newbuf, lsn, &mut |page| {
        _hash_initbuf(
            page,
            xlrec.new_bucket,
            xlrec.new_bucket,
            xlrec.new_bucket_flag as u32,
            true,
        )
    })?;

    if buffer_is_valid(oldbuf) {
        bufmgr::unlock_release_buffer::call(oldbuf);
    }
    if buffer_is_valid(newbuf) {
        bufmgr::unlock_release_buffer::call(newbuf);
    }

    // Replay the record for metapage changes.
    let (action, metabuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 2)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let data = record_get_block_data(record, 2);
        buffer_modify_page(metabuf, lsn, &mut |page| {
            meta_set_u32(page, META_OFF_MAXBUCKET, xlrec.new_bucket);

            let mut off = 0usize;
            if xlrec.flags & XLH_SPLIT_META_UPDATE_MASKS != 0 {
                let lowmask = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
                let highmask = u32::from_ne_bytes([data[4], data[5], data[6], data[7]]);
                meta_set_u32(page, META_OFF_LOWMASK, lowmask);
                meta_set_u32(page, META_OFF_HIGHMASK, highmask);
                off += 8; // sizeof(uint32) * 2
            }

            if xlrec.flags & XLH_SPLIT_META_UPDATE_SPLITPOINT != 0 {
                let ovflpoint =
                    u32::from_ne_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]]);
                let ovflpages = u32::from_ne_bytes([
                    data[off + 4],
                    data[off + 5],
                    data[off + 6],
                    data[off + 7],
                ]);
                meta_set_spare(page, ovflpoint as usize, ovflpages);
                meta_set_u32(page, META_OFF_OVFLPOINT, ovflpoint);
            }
            Ok(())
        })?;
    }

    if buffer_is_valid(metabuf) {
        bufmgr::unlock_release_buffer::call(metabuf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_split_page (hash_xlog.c:428)
// ===========================================================================

fn hash_xlog_split_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let (action, buf) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action != XLogRedoAction::BlkRestored {
        return Err(PgError::error(
            "Hash split record did not contain a full-page image",
        ));
    }
    bufmgr::unlock_release_buffer::call(buf);
    Ok(())
}

// ===========================================================================
// hash_xlog_split_complete (hash_xlog.c:442)
// ===========================================================================

fn hash_xlog_split_complete(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xlrec = xl_hash_split_complete::from_bytes(record_get_data(record));

    // Replay the record for the old bucket.
    let (action, oldbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo || action == XLogRedoAction::BlkRestored {
        buffer_modify_page(oldbuf, lsn, &mut |page| {
            set_hasho_flag(page, xlrec.old_bucket_flag);
            Ok(())
        })?;
    }
    if buffer_is_valid(oldbuf) {
        bufmgr::unlock_release_buffer::call(oldbuf);
    }

    // Replay the record for the new bucket.
    let (action, newbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
    if action == XLogRedoAction::BlkNeedsRedo || action == XLogRedoAction::BlkRestored {
        buffer_modify_page(newbuf, lsn, &mut |page| {
            set_hasho_flag(page, xlrec.new_bucket_flag);
            Ok(())
        })?;
    }
    if buffer_is_valid(newbuf) {
        bufmgr::unlock_release_buffer::call(newbuf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_move_page_contents (hash_xlog.c:501)
// ===========================================================================

fn hash_xlog_move_page_contents(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = xl_hash_move_page_contents::from_bytes(record_get_data(record));

    let mut bucketbuf = InvalidBuffer;
    let writebuf;
    let action;

    // Ensure a cleanup lock on the primary bucket page before replay.
    if xldata.is_prim_bucket_same_wrt {
        let (a, b) = xlogutils::xlog_read_buffer_for_redo_extended::call(
            record,
            1,
            ReadBufferMode::Normal,
            true,
        )?;
        action = a;
        writebuf = b;
    } else {
        let (_a, b) = xlogutils::xlog_read_buffer_for_redo_extended::call(
            record,
            0,
            ReadBufferMode::Normal,
            true,
        )?;
        bucketbuf = b;

        let (a, wb) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
        action = a;
        writebuf = wb;
    }

    // Replay adding entries to the write (overflow) buffer.
    if action == XLogRedoAction::BlkNeedsRedo {
        let block = record_get_block_data(record, 1);
        let ntups = xldata.ntups as usize;
        buffer_modify_page(writebuf, lsn, &mut |page| {
            replay_add_tuples(page, block, ntups, "hash_xlog_move_page_contents")
        })?;
    }

    // Replay deleting entries from the overflow buffer.
    let (action, deletebuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 2)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let ptr = record_get_block_data(record, 2);
        buffer_modify_page(deletebuf, lsn, &mut |page| {
            if !ptr.is_empty() {
                let unused = decode_offsets(ptr);
                if !unused.is_empty() {
                    let mut pmut = PageMut::new(page)?;
                    PageIndexMultiDelete(&mut pmut, &unused)?;
                }
            }
            Ok(())
        })?;
    }

    // Release the buffers (locks held till end of replay).
    if buffer_is_valid(deletebuf) {
        bufmgr::unlock_release_buffer::call(deletebuf);
    }
    if buffer_is_valid(writebuf) {
        bufmgr::unlock_release_buffer::call(writebuf);
    }
    if buffer_is_valid(bucketbuf) {
        bufmgr::unlock_release_buffer::call(bucketbuf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_squeeze_page (hash_xlog.c:627)
// ===========================================================================

fn hash_xlog_squeeze_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = xl_hash_squeeze_page::from_bytes(record_get_data(record));

    let mut bucketbuf = InvalidBuffer;
    let mut writebuf = InvalidBuffer;
    let action;

    if xldata.is_prim_bucket_same_wrt {
        let (a, b) = xlogutils::xlog_read_buffer_for_redo_extended::call(
            record,
            1,
            ReadBufferMode::Normal,
            true,
        )?;
        action = a;
        writebuf = b;
    } else {
        let (_a, b) = xlogutils::xlog_read_buffer_for_redo_extended::call(
            record,
            0,
            ReadBufferMode::Normal,
            true,
        )?;
        bucketbuf = b;

        if xldata.ntups > 0 || xldata.is_prev_bucket_same_wrt {
            let (a, wb) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
            action = a;
            writebuf = wb;
        } else {
            action = XLogRedoAction::BlkNotFound;
        }
    }

    // Replay adding entries to the write buffer.
    if action == XLogRedoAction::BlkNeedsRedo {
        let block = record_get_block_data(record, 1);
        let ntups = xldata.ntups as usize;
        let nextblkno = xldata.nextblkno;
        let is_prev_same = xldata.is_prev_bucket_same_wrt;
        buffer_modify_page_conditional(writebuf, lsn, &mut |page| {
            let mut mod_wbuf = false;
            if ntups > 0 {
                replay_add_tuples(page, block, ntups, "hash_xlog_squeeze_page")?;
                mod_wbuf = true;
            }
            // if the write page is the page previous to the freed overflow
            // page, update its nextblkno.
            if is_prev_same {
                set_hasho_nextblkno(page, nextblkno);
                mod_wbuf = true;
            }
            Ok(mod_wbuf)
        })?;
    }

    // Replay initializing the freed overflow buffer.
    let (action, ovflbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 2)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(ovflbuf, lsn, &mut |page| {
            _hash_pageinit(page, page.len())?;
            set_hasho_prevblkno(page, InvalidBlockNumber);
            set_hasho_nextblkno(page, InvalidBlockNumber);
            set_hasho_bucket(page, InvalidBucket);
            set_hasho_flag(page, LH_UNUSED_PAGE);
            set_hasho_page_id(page, HASHO_PAGE_ID);
            Ok(())
        })?;
    }
    if buffer_is_valid(ovflbuf) {
        bufmgr::unlock_release_buffer::call(ovflbuf);
    }

    // Replay the page previous to the freed overflow page.
    if !xldata.is_prev_bucket_same_wrt {
        let (action, prevbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 3)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            let nextblkno = xldata.nextblkno;
            buffer_modify_page(prevbuf, lsn, &mut |page| {
                set_hasho_nextblkno(page, nextblkno);
                Ok(())
            })?;
        }
        if buffer_is_valid(prevbuf) {
            bufmgr::unlock_release_buffer::call(prevbuf);
        }
    }

    // Replay the page next to the freed overflow page.
    if record_has_block_ref(record, 4) {
        let (action, nextbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 4)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            let prevblkno = xldata.prevblkno;
            buffer_modify_page(nextbuf, lsn, &mut |page| {
                set_hasho_prevblkno(page, prevblkno);
                Ok(())
            })?;
        }
        if buffer_is_valid(nextbuf) {
            bufmgr::unlock_release_buffer::call(nextbuf);
        }
    }

    if buffer_is_valid(writebuf) {
        bufmgr::unlock_release_buffer::call(writebuf);
    }
    if buffer_is_valid(bucketbuf) {
        bufmgr::unlock_release_buffer::call(bucketbuf);
    }

    // Replay the bitmap page (clear the bit for the freed ovfl page).
    let (action, mapbuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 5)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let data = record_get_block_data(record, 5);
        let bit = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
        buffer_modify_page(mapbuf, lsn, &mut |page| {
            bitmap_clrbit(page, bit);
            Ok(())
        })?;
    }
    if buffer_is_valid(mapbuf) {
        bufmgr::unlock_release_buffer::call(mapbuf);
    }

    // Replay the meta page.
    if record_has_block_ref(record, 6) {
        let (action, metabuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 6)?;
        if action == XLogRedoAction::BlkNeedsRedo {
            let data = record_get_block_data(record, 6);
            let firstfree_ovflpage = u32::from_ne_bytes([data[0], data[1], data[2], data[3]]);
            buffer_modify_page(metabuf, lsn, &mut |page| {
                meta_set_u32(page, META_OFF_FIRSTFREE, firstfree_ovflpage);
                Ok(())
            })?;
        }
        if buffer_is_valid(metabuf) {
            bufmgr::unlock_release_buffer::call(metabuf);
        }
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_delete (hash_xlog.c:861)
// ===========================================================================

fn hash_xlog_delete(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = xl_hash_delete::from_bytes(record_get_data(record));

    let mut bucketbuf = InvalidBuffer;
    let deletebuf;
    let action;

    if xldata.is_primary_bucket_page {
        let (a, b) = xlogutils::xlog_read_buffer_for_redo_extended::call(
            record,
            1,
            ReadBufferMode::Normal,
            true,
        )?;
        action = a;
        deletebuf = b;
    } else {
        let (_a, b) = xlogutils::xlog_read_buffer_for_redo_extended::call(
            record,
            0,
            ReadBufferMode::Normal,
            true,
        )?;
        bucketbuf = b;

        let (a, db) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
        action = a;
        deletebuf = db;
    }

    // Replay deleting entries in the bucket page.
    if action == XLogRedoAction::BlkNeedsRedo {
        let ptr = record_get_block_data(record, 1);
        let clear_dead_marking = xldata.clear_dead_marking;
        buffer_modify_page(deletebuf, lsn, &mut |page| {
            if !ptr.is_empty() {
                let unused = decode_offsets(ptr);
                if !unused.is_empty() {
                    let mut pmut = PageMut::new(page)?;
                    PageIndexMultiDelete(&mut pmut, &unused)?;
                }
            }

            // Mark the page as not containing LP_DEAD items if requested.
            if clear_dead_marking {
                let f = hasho_flag(page);
                set_hasho_flag(page, f & !LH_PAGE_HAS_DEAD_TUPLES);
            }
            Ok(())
        })?;
    }
    if buffer_is_valid(deletebuf) {
        bufmgr::unlock_release_buffer::call(deletebuf);
    }
    if buffer_is_valid(bucketbuf) {
        bufmgr::unlock_release_buffer::call(bucketbuf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_split_cleanup (hash_xlog.c:939)
// ===========================================================================

fn hash_xlog_split_cleanup(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;

    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(buffer, lsn, &mut |page| {
            let f = hasho_flag(page);
            set_hasho_flag(page, f & !LH_BUCKET_NEEDS_SPLIT_CLEANUP);
            Ok(())
        })?;
    }
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_update_meta_page (hash_xlog.c:964)
// ===========================================================================

fn hash_xlog_update_meta_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let xldata = xl_hash_update_meta_page::from_bytes(record_get_data(record));

    let (action, metabuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 0)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(metabuf, lsn, &mut |page| {
            meta_set_f64(page, META_OFF_NTUPLES, xldata.ntuples);
            Ok(())
        })?;
    }
    if buffer_is_valid(metabuf) {
        bufmgr::unlock_release_buffer::call(metabuf);
    }
    Ok(())
}

// ===========================================================================
// hash_xlog_vacuum_one_page (hash_xlog.c:991)
// ===========================================================================

fn hash_xlog_vacuum_one_page(record: &XLogReaderState<'_>) -> PgResult<()> {
    let lsn = record.EndRecPtr;
    let data = record_get_data(record);
    let xldata = xl_hash_vacuum_one_page::from_bytes(data);

    // toDelete = xldata->offsets; offsets[] follows the 8-byte header
    // (offsetof(xl_hash_vacuum_one_page, offsets) == 8).
    const SIZE_OF_HASH_VACUUM_ONE_PAGE: usize = 8;
    let ntuples = xldata.ntuples as usize;
    let mut to_delete: Vec<OffsetNumber> = Vec::new();
    let _ = to_delete.try_reserve(ntuples);
    for i in 0..ntuples {
        let o = SIZE_OF_HASH_VACUUM_ONE_PAGE + i * 2;
        to_delete.push(u16::from_ne_bytes([data[o], data[o + 1]]));
    }

    // Conflict processing must happen before we update the page.
    if in_hot_standby(xlogutils::standby_state::call()) {
        let rlocator = block_tag_rlocator(record, 0)?;
        let ctx = MemoryContext::new("hash_redo conflict resolution");
        standby::resolve_recovery_conflict_with_snapshot::call(
            ctx.mcx(),
            xldata.snapshotConflictHorizon,
            xldata.isCatalogRel,
            rlocator,
        )?;
    }

    let (action, buffer) = xlogutils::xlog_read_buffer_for_redo_extended::call(
        record,
        0,
        ReadBufferMode::Normal,
        true,
    )?;
    if action == XLogRedoAction::BlkNeedsRedo {
        buffer_modify_page(buffer, lsn, &mut |page| {
            {
                let mut pmut = PageMut::new(page)?;
                PageIndexMultiDelete(&mut pmut, &to_delete)?;
            }
            // Mark the page as not containing any LP_DEAD items.
            let f = hasho_flag(page);
            set_hasho_flag(page, f & !LH_PAGE_HAS_DEAD_TUPLES);
            Ok(())
        })?;
    }
    if buffer_is_valid(buffer) {
        bufmgr::unlock_release_buffer::call(buffer);
    }

    let (action, metabuf) = xlogutils::xlog_read_buffer_for_redo::call(record, 1)?;
    if action == XLogRedoAction::BlkNeedsRedo {
        let ntuples_f = xldata.ntuples as f64;
        buffer_modify_page(metabuf, lsn, &mut |page| {
            let n = meta_get_f64(page, META_OFF_NTUPLES);
            meta_set_f64(page, META_OFF_NTUPLES, n - ntuples_f);
            Ok(())
        })?;
    }
    if buffer_is_valid(metabuf) {
        bufmgr::unlock_release_buffer::call(metabuf);
    }
    Ok(())
}

// ===========================================================================
// Shared replay helpers.
// ===========================================================================

/// Decode a contiguous array of `OffsetNumber` (uint16) from a byte slice.
fn decode_offsets(ptr: &[u8]) -> Vec<OffsetNumber> {
    let n = ptr.len() / 2;
    let mut v: Vec<OffsetNumber> = Vec::new();
    let _ = v.try_reserve(n);
    for i in 0..n {
        v.push(u16::from_ne_bytes([ptr[i * 2], ptr[i * 2 + 1]]));
    }
    v
}

/// Replay the "add `ntups` tuples to a page" body shared by
/// `hash_xlog_move_page_contents` / `hash_xlog_squeeze_page`. `block` is the
/// per-block data: an array of `ntups` target `OffsetNumber`s followed by the
/// `MAXALIGN`'d on-page `IndexTuple`s.
fn replay_add_tuples(
    page: &mut [u8],
    block: &[u8],
    ntups: usize,
    who: &'static str,
) -> PgResult<()> {
    if ntups == 0 {
        return Ok(());
    }
    let towrite = &block[..ntups * 2];
    let mut data_off = ntups * 2; // sizeof(OffsetNumber) * ntups
    let datalen = block.len();
    let mut ninserted = 0usize;

    while data_off < datalen {
        let itup = &block[data_off..];
        let itemsz = maxalign(index_tuple_size(itup));

        let target = u16::from_ne_bytes([towrite[ninserted * 2], towrite[ninserted * 2 + 1]]);
        let placed = {
            let mut pmut = PageMut::new(page)?;
            PageAddItemExtended(&mut pmut, &block[data_off..data_off + itemsz], target, 0)?
        };
        if placed == InvalidOffsetNumber {
            return Err(PgError::new(
                PANIC,
                format!("{who}: failed to add item to hash index page, size {itemsz} bytes"),
            ));
        }

        data_off += itemsz;
        ninserted += 1;
    }

    debug_assert_eq!(ninserted, ntups);
    Ok(())
}

/// `InvalidOffsetNumber` (storage/off.h).
const InvalidOffsetNumber: OffsetNumber = 0;

// ===========================================================================
// hash_redo (hash_xlog.c:1066)
// ===========================================================================

/// `hash_redo(record)` — the hash rmgr redo dispatcher (`rm_redo` slot).
pub fn hash_redo(record: &mut XLogReaderState<'_>) -> PgResult<()> {
    let info = record_get_info(record) & !XLR_INFO_MASK;

    match info {
        XLOG_HASH_INIT_META_PAGE => hash_xlog_init_meta_page(record),
        XLOG_HASH_INIT_BITMAP_PAGE => hash_xlog_init_bitmap_page(record),
        XLOG_HASH_INSERT => hash_xlog_insert(record),
        XLOG_HASH_ADD_OVFL_PAGE => hash_xlog_add_ovfl_page(record),
        XLOG_HASH_SPLIT_ALLOCATE_PAGE => hash_xlog_split_allocate_page(record),
        XLOG_HASH_SPLIT_PAGE => hash_xlog_split_page(record),
        XLOG_HASH_SPLIT_COMPLETE => hash_xlog_split_complete(record),
        XLOG_HASH_MOVE_PAGE_CONTENTS => hash_xlog_move_page_contents(record),
        XLOG_HASH_SQUEEZE_PAGE => hash_xlog_squeeze_page(record),
        XLOG_HASH_DELETE => hash_xlog_delete(record),
        XLOG_HASH_SPLIT_CLEANUP => hash_xlog_split_cleanup(record),
        XLOG_HASH_UPDATE_META_PAGE => hash_xlog_update_meta_page(record),
        XLOG_HASH_VACUUM_ONE_PAGE => hash_xlog_vacuum_one_page(record),
        _ => Err(PgError::new(
            PANIC,
            format!("hash_redo: unknown op code {info}"),
        )),
    }
}

// ===========================================================================
// hash_mask (hash_xlog.c:1120)
// ===========================================================================

/// `hash_mask(pagedata, blkno)` — mask a hash page before WAL consistency
/// checking (`rm_mask` slot).
pub fn hash_mask(pagedata: &mut [u8], _blkno: BlockNumber) -> PgResult<()> {
    bufmask::mask_page_lsn_and_checksum::call(pagedata);
    bufmask::mask_page_hint_bits::call(pagedata);
    bufmask::mask_unused_space::call(pagedata)?;

    let pagetype = hasho_flag(pagedata) & LH_PAGE_TYPE;
    if pagetype == LH_UNUSED_PAGE {
        // Mask everything on a UNUSED page.
        bufmask::mask_page_content::call(pagedata);
    } else if pagetype == LH_BUCKET_PAGE || pagetype == LH_OVERFLOW_PAGE {
        // LP_FLAGS can change without WAL on bucket / overflow pages.
        bufmask::mask_lp_flags::call(pagedata);
    }

    // The LH_PAGE_HAS_DEAD_TUPLES hint may remain unlogged; mask it.
    let f = hasho_flag(pagedata);
    set_hasho_flag(pagedata, f & !LH_PAGE_HAS_DEAD_TUPLES);
    Ok(())
}

// ===========================================================================
// Seam installation.
// ===========================================================================

/// Install the rmgr-table callbacks this unit owns (`hash_redo` / `hash_mask`).
pub fn init_seams() {
    hash_xlog_seams::hash_redo::set(hash_redo);
    hash_xlog_seams::hash_mask::set(hash_mask);
}
