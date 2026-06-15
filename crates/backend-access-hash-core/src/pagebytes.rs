//! Byte-level hash page primitives (`access/hash.h` / `bufpage.h` layout),
//! transcribed 1:1 against the `BLCKSZ` page bytes. These are the canonical
//! versions the hash-core crate owns (the `backend-access-hash-xlog` redo crate
//! keeps its own private copies); see the CATALOG note.
//!
//! Covers: `_hash_pageinit` / `_hash_initbuf` / `_hash_initbitmapbuffer` /
//! `_hash_init_metabuffer` page-init bodies (here as `page`-byte helpers), the
//! `HashPageOpaqueData` (`hasho_*`) and `HashMetaPageData` (`hashm_*`) field
//! accessors, `SETBIT`/`CLRBIT`, and the `IndexTupleData` size/TID readers.
//!
//! No raw pointers, no `extern "C"`, no `unsafe`.

use types_core::primitive::{BlockNumber, InvalidBlockNumber, RegProcedure, BLCKSZ};
use types_error::PgResult;
use types_hash::hashpage::{
    HashMetaPageData, BYTE_TO_BIT, HASHO_PAGE_ID, HASH_MAGIC, HASH_MAX_BITMAPS,
    HASH_MAX_SPLITPOINTS, HASH_VERSION, InvalidBucket, LH_BITMAP_PAGE, LH_META_PAGE,
};
use types_tuple::heaptuple::{BlockIdData, ItemPointerData};

use backend_storage_page::PageInit;

use crate::hashutil::{_hash_get_totalbuckets, _hash_spareindex, pg_leftmost_one_pos32, pg_nextpower2_32};

// ===========================================================================
// MAXALIGN + page layout constants.
// ===========================================================================

/// `MAXALIGN(x)` (c.h): round up to `MAXIMUM_ALIGNOF` (8).
pub(crate) const fn maxalign(x: usize) -> usize {
    (x + 7) & !7
}

/// `SizeOfPageHeaderData` (bufpage.h).
pub(crate) const SIZEOF_PAGE_HEADER_DATA: usize = 24;

/// `PageGetContents` offset (bufpage.h): after the MAXALIGN'd page header.
pub(crate) const CONTENTS_OFFSET: usize = maxalign(SIZEOF_PAGE_HEADER_DATA);

/// `sizeof(HashPageOpaqueData)` (hash.h) = 4+4+4+2+2 = 16 (no trailing pad).
pub(crate) const SIZEOF_HASH_PAGE_OPAQUE_DATA: usize = 16;

/// `sizeof(IndexTupleData)` = `t_tid` 6 + `t_info` 2 = 8.
const SIZEOF_INDEX_TUPLE_DATA: usize = 8;

/// `IndexInfoFindDataOffset(t_info)` with no nulls: `MAXALIGN(sizeof(IndexTupleData))`.
pub(crate) const INDEX_INFO_HEADER_SIZE: usize = maxalign(SIZEOF_INDEX_TUPLE_DATA);

/// `INDEX_SIZE_MASK` (itup.h).
const INDEX_SIZE_MASK: u16 = 0x1FFF;

// --- PageHeaderData field offsets ------------------------------------------

const OFF_PD_LOWER: usize = 12;
const OFF_PD_SPECIAL: usize = 16;

/// `((PageHeader) page)->pd_lower = value`.
pub(crate) fn set_pd_lower(page: &mut [u8], value: u16) {
    page[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

fn special_offset(page: &[u8]) -> usize {
    u16::from_ne_bytes([page[OFF_PD_SPECIAL], page[OFF_PD_SPECIAL + 1]]) as usize
}

// ===========================================================================
// HashPageOpaqueData accessors over the page special area.
//   hasho_prevblkno @0, hasho_nextblkno @4, hasho_bucket @8, hasho_flag @12,
//   hasho_page_id @14.
// ===========================================================================

const HOP_OFF_PREVBLKNO: usize = 0;
const HOP_OFF_NEXTBLKNO: usize = 4;
const HOP_OFF_BUCKET: usize = 8;
const HOP_OFF_FLAG: usize = 12;
const HOP_OFF_PAGE_ID: usize = 14;

pub(crate) fn hasho_prevblkno(page: &[u8]) -> BlockNumber {
    let s = special_offset(page);
    u32::from_ne_bytes([
        page[s + HOP_OFF_PREVBLKNO],
        page[s + HOP_OFF_PREVBLKNO + 1],
        page[s + HOP_OFF_PREVBLKNO + 2],
        page[s + HOP_OFF_PREVBLKNO + 3],
    ])
}

pub(crate) fn hasho_nextblkno(page: &[u8]) -> BlockNumber {
    let s = special_offset(page);
    u32::from_ne_bytes([
        page[s + HOP_OFF_NEXTBLKNO],
        page[s + HOP_OFF_NEXTBLKNO + 1],
        page[s + HOP_OFF_NEXTBLKNO + 2],
        page[s + HOP_OFF_NEXTBLKNO + 3],
    ])
}

pub(crate) fn hasho_bucket(page: &[u8]) -> u32 {
    let s = special_offset(page);
    u32::from_ne_bytes([
        page[s + HOP_OFF_BUCKET],
        page[s + HOP_OFF_BUCKET + 1],
        page[s + HOP_OFF_BUCKET + 2],
        page[s + HOP_OFF_BUCKET + 3],
    ])
}

pub(crate) fn hasho_flag(page: &[u8]) -> u16 {
    let s = special_offset(page);
    u16::from_ne_bytes([page[s + HOP_OFF_FLAG], page[s + HOP_OFF_FLAG + 1]])
}

pub(crate) fn set_hasho_prevblkno(page: &mut [u8], blk: BlockNumber) {
    let s = special_offset(page);
    page[s + HOP_OFF_PREVBLKNO..s + HOP_OFF_PREVBLKNO + 4].copy_from_slice(&blk.to_ne_bytes());
}

pub(crate) fn set_hasho_nextblkno(page: &mut [u8], blk: BlockNumber) {
    let s = special_offset(page);
    page[s + HOP_OFF_NEXTBLKNO..s + HOP_OFF_NEXTBLKNO + 4].copy_from_slice(&blk.to_ne_bytes());
}

pub(crate) fn set_hasho_bucket(page: &mut [u8], bucket: u32) {
    let s = special_offset(page);
    page[s + HOP_OFF_BUCKET..s + HOP_OFF_BUCKET + 4].copy_from_slice(&bucket.to_ne_bytes());
}

pub(crate) fn set_hasho_flag(page: &mut [u8], flag: u16) {
    let s = special_offset(page);
    page[s + HOP_OFF_FLAG..s + HOP_OFF_FLAG + 2].copy_from_slice(&flag.to_ne_bytes());
}

pub(crate) fn set_hasho_page_id(page: &mut [u8], id: u16) {
    let s = special_offset(page);
    page[s + HOP_OFF_PAGE_ID..s + HOP_OFF_PAGE_ID + 2].copy_from_slice(&id.to_ne_bytes());
}

// ===========================================================================
// HashMetaPageData accessors over the page contents area (offsets relative to
// PageGetContents).
// ===========================================================================

pub(crate) const META_OFF_MAGIC: usize = 0;
pub(crate) const META_OFF_VERSION: usize = 4;
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
pub(crate) const SIZEOF_HASH_META_PAGE_DATA: usize = META_OFF_MAPP + HASH_MAX_BITMAPS * 4;

#[inline]
fn meta(off: usize) -> usize {
    CONTENTS_OFFSET + off
}

pub(crate) fn meta_get_u32(page: &[u8], field_off: usize) -> u32 {
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

fn meta_get_u16(page: &[u8], field_off: usize) -> u16 {
    let o = meta(field_off);
    u16::from_ne_bytes([page[o], page[o + 1]])
}

fn meta_set_u16(page: &mut [u8], field_off: usize, value: u16) {
    let o = meta(field_off);
    page[o..o + 2].copy_from_slice(&value.to_ne_bytes());
}

fn meta_get_spare(page: &[u8], i: usize) -> u32 {
    meta_get_u32(page, META_OFF_SPARES + i * 4)
}

fn meta_set_spare(page: &mut [u8], i: usize, value: u32) {
    meta_set_u32(page, META_OFF_SPARES + i * 4, value);
}

fn meta_get_mapp(page: &[u8], i: usize) -> BlockNumber {
    meta_get_u32(page, META_OFF_MAPP + i * 4)
}

fn meta_set_mapp(page: &mut [u8], i: usize, blk: BlockNumber) {
    meta_set_u32(page, META_OFF_MAPP + i * 4, blk);
}

/// Decode the whole `HashMetaPageData` out of the metapage's contents area.
pub(crate) fn read_hash_meta(page: &[u8]) -> HashMetaPageData {
    let mut m = HashMetaPageData {
        hashm_magic: meta_get_u32(page, META_OFF_MAGIC),
        hashm_version: meta_get_u32(page, META_OFF_VERSION),
        hashm_ntuples: meta_get_f64(page, META_OFF_NTUPLES),
        hashm_ffactor: meta_get_u16(page, META_OFF_FFACTOR),
        hashm_bsize: meta_get_u16(page, META_OFF_BSIZE),
        hashm_bmsize: meta_get_u16(page, META_OFF_BMSIZE),
        hashm_bmshift: meta_get_u16(page, META_OFF_BMSHIFT),
        hashm_maxbucket: meta_get_u32(page, META_OFF_MAXBUCKET),
        hashm_highmask: meta_get_u32(page, META_OFF_HIGHMASK),
        hashm_lowmask: meta_get_u32(page, META_OFF_LOWMASK),
        hashm_ovflpoint: meta_get_u32(page, META_OFF_OVFLPOINT),
        hashm_firstfree: meta_get_u32(page, META_OFF_FIRSTFREE),
        hashm_nmaps: meta_get_u32(page, META_OFF_NMAPS),
        hashm_procid: meta_get_u32(page, META_OFF_PROCID),
        hashm_spares: [0; HASH_MAX_SPLITPOINTS],
        hashm_mapp: [0; HASH_MAX_BITMAPS],
    };
    for i in 0..HASH_MAX_SPLITPOINTS {
        m.hashm_spares[i] = meta_get_spare(page, i);
    }
    for i in 0..HASH_MAX_BITMAPS {
        m.hashm_mapp[i] = meta_get_mapp(page, i);
    }
    m
}

// --- individual metapage field mutators (used under crit sections) ----------

pub(crate) fn meta_ntuples(page: &[u8]) -> f64 {
    meta_get_f64(page, META_OFF_NTUPLES)
}
pub(crate) fn set_meta_ntuples(page: &mut [u8], v: f64) {
    meta_set_f64(page, META_OFF_NTUPLES, v);
}
pub(crate) fn meta_ffactor(page: &[u8]) -> u16 {
    meta_get_u16(page, META_OFF_FFACTOR)
}
pub(crate) fn meta_maxbucket(page: &[u8]) -> u32 {
    meta_get_u32(page, META_OFF_MAXBUCKET)
}
pub(crate) fn set_meta_maxbucket(page: &mut [u8], v: u32) {
    meta_set_u32(page, META_OFF_MAXBUCKET, v);
}
pub(crate) fn meta_highmask(page: &[u8]) -> u32 {
    meta_get_u32(page, META_OFF_HIGHMASK)
}
pub(crate) fn set_meta_highmask(page: &mut [u8], v: u32) {
    meta_set_u32(page, META_OFF_HIGHMASK, v);
}
pub(crate) fn meta_lowmask(page: &[u8]) -> u32 {
    meta_get_u32(page, META_OFF_LOWMASK)
}
pub(crate) fn set_meta_lowmask(page: &mut [u8], v: u32) {
    meta_set_u32(page, META_OFF_LOWMASK, v);
}
pub(crate) fn meta_ovflpoint(page: &[u8]) -> u32 {
    meta_get_u32(page, META_OFF_OVFLPOINT)
}
pub(crate) fn set_meta_ovflpoint(page: &mut [u8], v: u32) {
    meta_set_u32(page, META_OFF_OVFLPOINT, v);
}
pub(crate) fn meta_firstfree(page: &[u8]) -> u32 {
    meta_get_u32(page, META_OFF_FIRSTFREE)
}
pub(crate) fn set_meta_firstfree(page: &mut [u8], v: u32) {
    meta_set_u32(page, META_OFF_FIRSTFREE, v);
}
pub(crate) fn meta_nmaps(page: &[u8]) -> u32 {
    meta_get_u32(page, META_OFF_NMAPS)
}
pub(crate) fn set_meta_nmaps(page: &mut [u8], v: u32) {
    meta_set_u32(page, META_OFF_NMAPS, v);
}
pub(crate) fn meta_bmsize(page: &[u8]) -> u16 {
    meta_get_u16(page, META_OFF_BMSIZE)
}
pub(crate) fn meta_bmshift(page: &[u8]) -> u16 {
    meta_get_u16(page, META_OFF_BMSHIFT)
}
pub(crate) fn page_meta_spare(page: &[u8], i: usize) -> u32 {
    meta_get_spare(page, i)
}
pub(crate) fn set_page_meta_spare(page: &mut [u8], i: usize, v: u32) {
    meta_set_spare(page, i, v);
}
pub(crate) fn page_meta_mapp(page: &[u8], i: usize) -> BlockNumber {
    meta_get_mapp(page, i)
}
pub(crate) fn set_page_meta_mapp(page: &mut [u8], i: usize, v: BlockNumber) {
    meta_set_mapp(page, i, v);
}

// ===========================================================================
// HashPageGetBitmap (uint32 array at PageGetContents) — SETBIT/CLRBIT/ISSET.
// ===========================================================================

/// `BITS_PER_MAP` (hash.h).
const BITS_PER_MAP: u32 = 32;

/// `BITS_PER_MAP` exposed for the overflow-page free-bit scan arithmetic.
pub(crate) const BITS_PER_MAP_U32: u32 = BITS_PER_MAP;

/// `SETBIT(freep, n)`.
pub(crate) fn bitmap_setbit(page: &mut [u8], n: u32) {
    let word = (n / BITS_PER_MAP) as usize;
    let bit = n % BITS_PER_MAP;
    let o = CONTENTS_OFFSET + word * 4;
    let mut v = u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]]);
    v |= 1 << bit;
    page[o..o + 4].copy_from_slice(&v.to_ne_bytes());
}

/// `CLRBIT(freep, n)`.
pub(crate) fn bitmap_clrbit(page: &mut [u8], n: u32) {
    let word = (n / BITS_PER_MAP) as usize;
    let bit = n % BITS_PER_MAP;
    let o = CONTENTS_OFFSET + word * 4;
    let mut v = u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]]);
    v &= !(1 << bit);
    page[o..o + 4].copy_from_slice(&v.to_ne_bytes());
}

/// `ISSET(freep, n)`.
pub(crate) fn bitmap_isset(page: &[u8], n: u32) -> bool {
    let word = (n / BITS_PER_MAP) as usize;
    let bit = n % BITS_PER_MAP;
    let o = CONTENTS_OFFSET + word * 4;
    let v = u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]]);
    (v & (1 << bit)) != 0
}

/// `freep[j]` — read the `j`th 32-bit bitmap word.
pub(crate) fn bitmap_word(page: &[u8], j: usize) -> u32 {
    let o = CONTENTS_OFFSET + j * 4;
    u32::from_ne_bytes([page[o], page[o + 1], page[o + 2], page[o + 3]])
}

// ===========================================================================
// IndexTuple readers (the on-page item bytes).
// ===========================================================================

/// `IndexTupleSize(itup)` (itup.h): `t_info & INDEX_SIZE_MASK`. `t_info` is the
/// uint16 at offset 6.
pub(crate) fn index_tuple_size(itup: &[u8]) -> usize {
    let t_info = u16::from_ne_bytes([itup[6], itup[7]]);
    (t_info & INDEX_SIZE_MASK) as usize
}

/// `itup->t_info` (the raw 16-bit info word).
pub(crate) fn index_tuple_t_info(itup: &[u8]) -> u16 {
    u16::from_ne_bytes([itup[6], itup[7]])
}

/// `itup->t_tid` — the `ItemPointerData` heap TID (`ip_blkid` 4 bytes + posid 2).
pub(crate) fn index_tuple_tid(itup: &[u8]) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([itup[0], itup[1]]),
            bi_lo: u16::from_ne_bytes([itup[2], itup[3]]),
        },
        ip_posid: u16::from_ne_bytes([itup[4], itup[5]]),
    }
}

// ===========================================================================
// _hash_pageinit / _hash_initbuf / _hash_initbitmapbuffer /
// _hash_init_metabuffer — page-byte bodies (hashpage.c / hashovfl.c).
// ===========================================================================

/// `_hash_pageinit(page, size)` — `PageInit(page, size, sizeof(HashPageOpaqueData))`.
pub(crate) fn _hash_pageinit_bytes(page: &mut [u8], size: usize) -> PgResult<()> {
    PageInit(page, size, SIZEOF_HASH_PAGE_OPAQUE_DATA)
}

/// `_hash_initbuf` body, applied to the page bytes.
pub(crate) fn _hash_initbuf_bytes(
    page: &mut [u8],
    max_bucket: u32,
    num_bucket: u32,
    flag: u32,
    initpage: bool,
) -> PgResult<()> {
    if initpage {
        _hash_pageinit_bytes(page, page.len())?;
    }
    set_hasho_prevblkno(page, max_bucket);
    set_hasho_nextblkno(page, InvalidBlockNumber);
    set_hasho_bucket(page, num_bucket);
    set_hasho_flag(page, flag as u16);
    set_hasho_page_id(page, HASHO_PAGE_ID);
    Ok(())
}

/// `_hash_initbitmapbuffer` body, applied to the page bytes.
pub(crate) fn _hash_initbitmapbuffer_bytes(page: &mut [u8], bmsize: u16, initpage: bool) -> PgResult<()> {
    if initpage {
        _hash_pageinit_bytes(page, page.len())?;
    }
    set_hasho_prevblkno(page, InvalidBlockNumber);
    set_hasho_nextblkno(page, InvalidBlockNumber);
    set_hasho_bucket(page, InvalidBucket);
    set_hasho_flag(page, LH_BITMAP_PAGE);
    set_hasho_page_id(page, HASHO_PAGE_ID);

    // memset(freep, 0xFF, bmsize)
    let bm = bmsize as usize;
    for b in &mut page[CONTENTS_OFFSET..CONTENTS_OFFSET + bm] {
        *b = 0xFF;
    }
    // pd_lower = ((char *) freep + bmsize) - (char *) pg
    set_pd_lower(page, (CONTENTS_OFFSET + bm) as u16);
    Ok(())
}

/// `_hash_init_metabuffer` body, applied to the page bytes.
pub(crate) fn _hash_init_metabuffer_bytes(
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
        _hash_pageinit_bytes(page, page.len())?;
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

    // hashm_bsize = HashGetMaxBitmapSize(page)
    let hashm_bsize = (page.len()
        - (maxalign(SIZEOF_PAGE_HEADER_DATA) + maxalign(SIZEOF_HASH_PAGE_OPAQUE_DATA)))
        as u16;
    meta_set_u16(page, META_OFF_BSIZE, hashm_bsize);

    // largest bitmap array size that fits in page size
    let lshift = pg_leftmost_one_pos32(hashm_bsize as u32);
    debug_assert!(lshift > 0);
    let hashm_bmsize = 1u16 << lshift;
    meta_set_u16(page, META_OFF_BMSIZE, hashm_bmsize);
    meta_set_u16(page, META_OFF_BMSHIFT, (lshift + BYTE_TO_BIT) as u16);

    meta_set_u32(page, META_OFF_PROCID, procid);

    meta_set_u32(page, META_OFF_MAXBUCKET, num_buckets - 1);

    let highmask = pg_nextpower2_32(num_buckets + 1) - 1;
    meta_set_u32(page, META_OFF_HIGHMASK, highmask);
    meta_set_u32(page, META_OFF_LOWMASK, highmask >> 1);

    // MemSet spares + mapp to 0.
    for b in &mut page[meta(META_OFF_SPARES)..meta(META_OFF_SPARES) + HASH_MAX_SPLITPOINTS * 4] {
        *b = 0;
    }
    for b in &mut page[meta(META_OFF_MAPP)..meta(META_OFF_MAPP) + HASH_MAX_BITMAPS * 4] {
        *b = 0;
    }

    meta_set_spare(page, spare_index as usize, 1);
    meta_set_u32(page, META_OFF_OVFLPOINT, spare_index);
    meta_set_u32(page, META_OFF_FIRSTFREE, 0);

    // pd_lower = ((char *) metap + sizeof(HashMetaPageData)) - (char *) page
    set_pd_lower(page, (CONTENTS_OFFSET + SIZEOF_HASH_META_PAGE_DATA) as u16);
    Ok(())
}

// ===========================================================================
// BMPG_SHIFT / BMPG_MASK / BMPGSZ_BIT (hash.h) over a HashMetaPageData.
// ===========================================================================

/// `BMPG_SHIFT(metap)` = `metap->hashm_bmshift`.
pub(crate) fn bmpg_shift(metap: &HashMetaPageData) -> u32 {
    metap.hashm_bmshift as u32
}
/// `BMPG_MASK(metap)` = `(1 << BMPG_SHIFT(metap)) - 1`.
pub(crate) fn bmpg_mask(metap: &HashMetaPageData) -> u32 {
    (1u32 << bmpg_shift(metap)) - 1
}
/// `BMPGSZ_BIT(metap)` = `metap->hashm_bmsize << BYTE_TO_BIT`.
pub(crate) fn bmpgsz_bit(metap: &HashMetaPageData) -> u32 {
    (metap.hashm_bmsize as u32) << BYTE_TO_BIT
}

// ===========================================================================
// HashGetTargetPageUsage / HashMaxItemSize / HashGetMaxBitmapSize (hash.h).
// ===========================================================================

/// `HashMaxItemSize(page)` (hash.h):
/// `MAXALIGN_DOWN(PageGetPageSize(page) - SizeOfPageHeaderData -
///   sizeof(ItemIdData) - MAXALIGN(sizeof(HashPageOpaqueData)))`.
pub(crate) fn hash_max_item_size(page_size: usize) -> usize {
    let v = page_size - SIZEOF_PAGE_HEADER_DATA - 4 /* sizeof(ItemIdData) */
        - maxalign(SIZEOF_HASH_PAGE_OPAQUE_DATA);
    v & !7 // MAXALIGN_DOWN
}

/// `RelationGetTargetPageUsage(rel, HASH_DEFAULT_FILLFACTOR)` for hash uses
/// `BLCKSZ * fillfactor / 100`. The hash AM applies its own fillfactor reloption
/// via `HashGetTargetPageUsage`, which is `RelationGetTargetPageUsage(rel,
/// HASH_DEFAULT_FILLFACTOR)`.
pub(crate) fn hash_get_target_page_usage(fillfactor: i32) -> i32 {
    (BLCKSZ as i32) * fillfactor / 100
}

/// `MAXALIGN(sizeof(IndexTupleData))` — for the `_hash_init` ffactor math.
pub(crate) fn maxalign_sizeof_index_tuple_data() -> usize {
    maxalign(SIZEOF_INDEX_TUPLE_DATA)
}

/// `MAXALIGN(data_width)` — the `_hash_init` ffactor item-width term.
pub(crate) fn maxalign_data_width(data_width: i32) -> i32 {
    maxalign(data_width as usize) as i32
}

/// `sizeof(ItemIdData)`.
pub(crate) const SIZEOF_ITEM_ID_DATA: usize = 4;

