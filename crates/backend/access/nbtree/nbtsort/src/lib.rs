//! Port of `src/backend/access/nbtree/nbtsort.c` (PostgreSQL 18.3) — build a
//! new btree index from scratch by sorting the heap and loading leaf pages
//! bottom-up.
//!
//! This crate **grounds the page-format + suffix-truncation + deduplication +
//! leaf-load half** of `nbtsort.c` (the part driven by a *pull-based* sorted
//! spool over owned page buffers) AND the **serial heap build scan** (`btbuild`
//! / `_bt_spools_heapscan` / `_bt_build_callback` / `_bt_spool`, which feed a
//! per-tuple callback closure into the spool through `table_index_build_scan`,
//! exactly as hash `hashbuild` and brin `brinbuild` do). It **honestly panics
//! the parallel-build half** (genuinely blocked on the DSM/parallel-context
//! machinery) plus the one irreducible serial residual: the AM `ambuild`
//! callback's type-erased `amapi::IndexInfo` exposes no `ii_Unique` /
//! `ii_NullsNotDistinct` (see [`deferred`]). The deferred panics are *not*
//! silent stubs — this is sanctioned mirror-and-panic, never a placeholder
//! stub-panic.
//!
//! ## Grounded here (1:1 with C, original names preserved)
//! - [`_bt_leafbuild`], [`_bt_load`], [`_bt_buildadd`], [`_bt_pagestate`],
//!   [`_bt_blnewpage`], [`_bt_blwritepage`], [`_bt_slideleft`],
//!   [`_bt_sortaddtup`], [`_bt_sort_dedup_finish_pending`],
//!   [`_bt_uppershutdown`], [`_bt_spool`], [`_bt_spooldestroy`].
//! - the in-crate page-format / tuple-format / dedup byte codecs re-ported from
//!   the sibling `nbtree.h` / `nbtpage.c` / `nbtdedup.c` modules
//!   (`BTPageGetOpaque`, the `BTreeTuple*` accessors, `_bt_pageinit`,
//!   `_bt_initmetapage`, `_bt_form_posting`, the dedup pending-list codec) —
//!   pure page/byte logic with no external deps. (`_bt_pageinit` is the trivial
//!   bufpage `PageInit`; re-implemented locally as the dedup sibling does,
//!   rather than seamed.)
//!
//! ## Seams (cross-module callees / unported substrate, panic-until-owner)
//! - sorted spool: `tuplesort_begin_index_btree` / `tuplesort_performsort` /
//!   `tuplesort_getindextuple` / `tuplesort_end`
//!   (`backend-utils-sort-tuplesort-seams`).
//! - bulk page writer: `smgr_bulk_start_rel` / `smgr_bulk_get_buf` /
//!   `smgr_bulk_write` / `smgr_bulk_finish`
//!   (`backend-storage-smgr-bulkwrite-seams`).
//! - nbtree-core helpers: `_bt_mkscankey` / `_bt_allequalimage` /
//!   `_bt_keep_natts_fast` (`backend-access-nbtree-core-seams`); `_bt_truncate`
//!   / `_bt_check_third_page` / the merge comparison
//!   (`backend-access-nbtree-build-seams`).
//! - progress reporting: `pgstat_progress_update_param`
//!   (`backend-utils-activity-small`, already ported).
//!
//! ## Serial build (see [`deferred`])
//! [`deferred::btbuild`], `_bt_spools_heapscan`, `_bt_build_callback`,
//! `_bt_spool` — ported; the heap scan crosses `table_index_build_scan` (heap
//! AM provider) and the per-tuple closure feeds the spool. The only panic in
//! this path is the type-erased `amapi::IndexInfo` flag read.
//!
//! ## Deferred behind a loud panic (see [`deferred`])
//! The entire parallel build coordination (`_bt_begin_parallel`,
//! `_bt_end_parallel`, `_bt_parallel_*`).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::boxed::Box;

use ::mcx::{alloc_in, vec_with_capacity_in, Mcx, PgBox, PgVec};
use ::types_core::primitive::{BlockNumber, ForkNumber, OffsetNumber, Size, BLCKSZ};
use ::types_error::{PgError, PgResult};
use ::types_nbtree::{
    BTMetaPageData, BTPageOpaqueData, BTScanInsert, BTMaxItemSize, BTP_LEAF, BTP_META, BTP_ROOT,
    BTREE_DEFAULT_FILLFACTOR, BTREE_MAGIC, BTREE_METAPAGE, BTREE_NONLEAF_FILLFACTOR, BTREE_VERSION,
    BT_IS_POSTING, BT_OFFSET_MASK, BT_PIVOT_HEAP_TID_ATTR, INDEX_ALT_TID_MASK, P_FIRSTKEY, P_HIKEY,
    P_NONE,
};
use ::nodes::Tuplesortstate;
use ::rel::Relation;
use ::types_tuple::heaptuple::{
    IndexTupleData, IndexTupleSize, ItemPointerData, INDEX_SIZE_MASK, INVALID_OFFSET_NUMBER,
};

use ::page::{
    PageAddItemExtended, PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetSpecialPointer, PageIndexTupleOverwrite, PageInit, PageMut, PageRef,
};

use build_seams as buildhelp;
use nbtree_core_seams as nbtcore;
use bulkwrite_seams as bulk;
use ::bulkwrite_seams::BulkWriteState;
use ::activity_small::backend_progress::pgstat_progress_update_param;
use tuplesort_seams as tuplesort;

pub mod deferred;

// ===========================================================================
// init_seams — nbtsort owns the `btbuild` build-dispatch seam.
//
// `btbuild` (the btree AM `ambuild`) lives here, ABOVE the AM-vtable crate
// (`backend-access-nbtree-nbtree`) in the dep graph, so the vtable's `ambuild`
// adapter (`btbuild_am`) cannot call it directly. The cross-crate edge is
// bridged through the `backend-access-nbtree-build-seams::btbuild` seam, which
// nbtsort installs here: the adapter passes the `IndexInfoCarrier` (#342)
// through, and this installer downcasts it back to the real
// `::nodes::execnodes::IndexInfo<'mcx>` before invoking the serial build.
// ===========================================================================

/// Install this crate's inward seams.
pub fn init_seams() {
    buildhelp::btbuild::set(|mcx, heap, index, index_info| {
        // The dispatch layer (index.c) wraps the caller's owned
        // `&mut IndexInfo<'mcx>` in the carrier; recover the concrete struct
        // (tag-checked downcast — a NULL/wrong-type carrier is the C
        // NULL-pointer programming error).
        let info = index_info
            .downcast_mut::<::nodes::execnodes::IndexInfo<'_>>()
            .unwrap_or_else(|| {
                panic!("btbuild: IndexInfoCarrier did not carry the expected IndexInfo")
            });
        deferred::btbuild(mcx, heap, index, info)
    });
}

// ===========================================================================
// Page-layout constants and helpers (re-ported from nbtree.h / off.h).
// Pure page/byte logic owned by the build, identical to the sibling nbtdedup
// module's 1:1 ports.
// ===========================================================================

const MAXIMUM_ALIGNOF: usize = 8;
const SIZE_OF_ITEM_ID_DATA: usize = 4; // sizeof(ItemIdData)
const SIZE_OF_INDEX_TUPLE_DATA: usize = 8; // sizeof(IndexTupleData) = ItemPointerData(6) + uint16(2), MAXALIGN'd reads as 8
const SIZE_OF_ITEM_POINTER_DATA: usize = 6; // sizeof(ItemPointerData)
/// `SizeOfPageHeaderData` — the unaligned 24-byte page header.
const SIZE_OF_PAGE_HEADER_DATA: usize = 24;
/// byte offset of `pd_lower` within the page header.
const OFF_PD_LOWER: usize = 12;
/// byte offset of `pd_special` within the page header.
const OFF_PD_SPECIAL: usize = 16;

/// `MAXALIGN(len)`.
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `MAXALIGN_DOWN(LEN)` (`c.h`).
#[inline]
const fn maxalign_down(len: usize) -> usize {
    len & !(MAXIMUM_ALIGNOF - 1)
}

/// `OffsetNumberNext(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberNext(off: OffsetNumber) -> OffsetNumber {
    off + 1
}
/// `OffsetNumberPrev(offsetNumber)` (`storage/off.h`).
#[inline]
const fn OffsetNumberPrev(off: OffsetNumber) -> OffsetNumber {
    off - 1
}

/// `P_LEFTMOST(opaque)`.
#[inline]
fn P_LEFTMOST(opaque: &BTPageOpaqueData) -> bool {
    opaque.btpo_prev == P_NONE
}

// --- raw page-header / line-pointer byte helpers ---------------------------
//
// The page crate exposes the line-pointer array read-only (`PageGetItemId`
// returns a copy). nbtsort's page builder must (a) bump `pd_lower` to make a
// line pointer appear allocated, and (b) physically copy/clear individual line
// pointers when it turns a data item into a high key or slides the array left.
// The page header and line-pointer array have a fixed byte layout, so these are
// done directly on the page bytes — exactly the bytes the C port wrote through
// `((PageHeader) page)->pd_lower` / `PageGetItemId`.

#[inline]
fn read_pd_lower(buf: &[u8]) -> u16 {
    u16::from_ne_bytes([buf[OFF_PD_LOWER], buf[OFF_PD_LOWER + 1]])
}

#[inline]
fn write_pd_lower(buf: &mut [u8], value: u16) {
    buf[OFF_PD_LOWER..OFF_PD_LOWER + 2].copy_from_slice(&value.to_ne_bytes());
}

#[inline]
fn read_pd_special(buf: &[u8]) -> u16 {
    u16::from_ne_bytes([buf[OFF_PD_SPECIAL], buf[OFF_PD_SPECIAL + 1]])
}

/// Byte offset of the line pointer at `off` (1-based) within the page.
#[inline]
fn line_pointer_offset(off: OffsetNumber) -> usize {
    SIZE_OF_PAGE_HEADER_DATA + (off as usize - 1) * SIZE_OF_ITEM_ID_DATA
}

/// Read the 4 raw bytes of the line pointer at `off`.
#[inline]
fn read_line_pointer(buf: &[u8], off: OffsetNumber) -> [u8; 4] {
    let p = line_pointer_offset(off);
    [buf[p], buf[p + 1], buf[p + 2], buf[p + 3]]
}

/// Write the 4 raw bytes of the line pointer at `off`.
#[inline]
fn write_line_pointer(buf: &mut [u8], off: OffsetNumber, raw: [u8; 4]) {
    let p = line_pointer_offset(off);
    buf[p..p + 4].copy_from_slice(&raw);
}

// ---------------------------------------------------------------------------
// BTreeTupleData inline helpers (nbtree.h "Notes on B-Tree tuple format").
// `IndexTupleData` and `BTreeTupleData` are layout-identical { t_tid, t_info };
// these operate on the on-page bytes' leading tuple header.
// ---------------------------------------------------------------------------

/// Interpret the leading bytes of a page item as an [`IndexTupleData`] header.
#[inline]
fn index_tuple_header(tuple: &[u8]) -> IndexTupleData {
    let t_tid = read_item_pointer(&tuple[0..SIZE_OF_ITEM_POINTER_DATA]);
    let t_info = u16::from_ne_bytes([tuple[6], tuple[7]]);
    IndexTupleData { t_tid, t_info }
}

/// Write an [`IndexTupleData`] header back into the leading bytes of a buffer.
#[inline]
fn write_tuple_header(tuple: &mut [u8], hdr: &IndexTupleData) {
    write_item_pointer(&mut tuple[0..SIZE_OF_ITEM_POINTER_DATA], &hdr.t_tid);
    tuple[6..8].copy_from_slice(&hdr.t_info.to_ne_bytes());
}

/// `ItemPointerData` byte read (6 bytes: blkid hi/lo u16 + posid u16).
#[inline]
fn read_item_pointer(b: &[u8]) -> ItemPointerData {
    let bi_hi = u16::from_ne_bytes([b[0], b[1]]);
    let bi_lo = u16::from_ne_bytes([b[2], b[3]]);
    let posid = u16::from_ne_bytes([b[4], b[5]]);
    let blkno = ((bi_hi as u32) << 16) | bi_lo as u32;
    ItemPointerData::new(blkno, posid)
}

/// `ItemPointerData` byte write (mirror of [`read_item_pointer`]).
#[inline]
fn write_item_pointer(b: &mut [u8], ptr: &ItemPointerData) {
    let blkno = ItemPointerGetBlockNumberNoCheck(ptr);
    let bi_hi = ((blkno >> 16) & 0xFFFF) as u16;
    let bi_lo = (blkno & 0xFFFF) as u16;
    b[0..2].copy_from_slice(&bi_hi.to_ne_bytes());
    b[2..4].copy_from_slice(&bi_lo.to_ne_bytes());
    b[4..6].copy_from_slice(&ptr.ip_posid.to_ne_bytes());
}

#[inline]
fn ItemPointerGetBlockNumberNoCheck(ptr: &ItemPointerData) -> BlockNumber {
    ptr.ip_blkid.block_number()
}

#[inline]
fn ItemPointerGetOffsetNumberNoCheck(ptr: &ItemPointerData) -> OffsetNumber {
    ptr.ip_posid
}

#[inline]
fn ItemPointerSetBlockNumber(ptr: &mut ItemPointerData, blkno: BlockNumber) {
    ptr.ip_blkid.set_block_number(blkno);
}

#[inline]
fn ItemPointerSetOffsetNumber(ptr: &mut ItemPointerData, off: OffsetNumber) {
    ptr.ip_posid = off;
}

/// `ItemPointerCompare(arg1, arg2)`.
fn ItemPointerCompare(arg1: &ItemPointerData, arg2: &ItemPointerData) -> i32 {
    let b1 = ItemPointerGetBlockNumberNoCheck(arg1);
    let b2 = ItemPointerGetBlockNumberNoCheck(arg2);
    if b1 < b2 {
        -1
    } else if b1 > b2 {
        1
    } else {
        let p1 = ItemPointerGetOffsetNumberNoCheck(arg1);
        let p2 = ItemPointerGetOffsetNumberNoCheck(arg2);
        if p1 < p2 {
            -1
        } else if p1 > p2 {
            1
        } else {
            0
        }
    }
}

/// `BTreeTupleIsPivot(itup)`.
#[inline]
fn BTreeTupleIsPivot(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_IS_POSTING) == 0
}

/// `BTreeTupleIsPosting(itup)`.
#[inline]
fn BTreeTupleIsPosting(itup: &IndexTupleData) -> bool {
    if (itup.t_info & INDEX_ALT_TID_MASK) == 0 {
        return false;
    }
    (ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_IS_POSTING) != 0
}

/// `BTreeTupleSetPosting(itup, nhtids, postingoffset)`.
#[inline]
fn BTreeTupleSetPosting(itup: &mut IndexTupleData, nhtids: u16, postingoffset: i32) {
    debug_assert!(nhtids > 1);
    debug_assert!((nhtids & 0xF000) == 0);
    debug_assert!(postingoffset as usize == maxalign(postingoffset as usize));
    debug_assert!(!BTreeTupleIsPivot(itup));
    itup.t_info |= INDEX_ALT_TID_MASK;
    ItemPointerSetOffsetNumber(&mut itup.t_tid, nhtids | BT_IS_POSTING);
    ItemPointerSetBlockNumber(&mut itup.t_tid, postingoffset as BlockNumber);
}

/// `BTreeTupleGetNPosting(posting)`.
#[inline]
fn BTreeTupleGetNPosting(posting: &IndexTupleData) -> u16 {
    debug_assert!(BTreeTupleIsPosting(posting));
    ItemPointerGetOffsetNumberNoCheck(&posting.t_tid) & BT_OFFSET_MASK
}

/// `BTreeTupleGetPostingOffset(posting)`.
#[inline]
fn BTreeTupleGetPostingOffset(posting: &IndexTupleData) -> u32 {
    debug_assert!(BTreeTupleIsPosting(posting));
    ItemPointerGetBlockNumberNoCheck(&posting.t_tid)
}

/// `BTreeTupleSetDownLink(pivot, blkno)`.
#[inline]
fn BTreeTupleSetDownLink(pivot: &mut IndexTupleData, blkno: BlockNumber) {
    ItemPointerSetBlockNumber(&mut pivot.t_tid, blkno);
}

/// `BTreeTupleGetNAtts(itup, indnatts)`.
#[inline]
fn BTreeTupleGetNAtts(itup: &IndexTupleData, indnatts: u16) -> u16 {
    if BTreeTupleIsPivot(itup) {
        ItemPointerGetOffsetNumberNoCheck(&itup.t_tid) & BT_OFFSET_MASK
    } else {
        indnatts
    }
}

/// `BTreeTupleSetNAtts(itup, nkeyatts, heaptid)`.
#[inline]
fn BTreeTupleSetNAtts(itup: &mut IndexTupleData, nkeyatts: u16, heaptid: bool) {
    debug_assert!(!BTreeTupleIsPivot(itup) || nkeyatts == 0);
    itup.t_info |= INDEX_ALT_TID_MASK;
    let mut nkeyatts = nkeyatts;
    if heaptid {
        nkeyatts |= BT_PIVOT_HEAP_TID_ATTR;
    }
    ItemPointerSetOffsetNumber(&mut itup.t_tid, nkeyatts);
    debug_assert!(BTreeTupleIsPivot(itup));
}

/// `BTreeTupleGetPostingN(posting, n)` — the `n`th heap TID of a posting list.
fn posting_list_n(tuple: &[u8], n: usize) -> ItemPointerData {
    let off = BTreeTupleGetPostingOffset(&index_tuple_header(tuple)) as usize;
    let base = off + n * SIZE_OF_ITEM_POINTER_DATA;
    read_item_pointer(&tuple[base..base + SIZE_OF_ITEM_POINTER_DATA])
}

// ===========================================================================
// BTPageOpaqueData on-disk codec + page-init (re-ported from nbtree.h/nbtpage).
// ===========================================================================

/// `BTPageGetOpaque(page)` — read the special-area opaque (16 bytes).
fn BTPageGetOpaque(page: &PageRef<'_>) -> PgResult<BTPageOpaqueData> {
    let special = PageGetSpecialPointer(page)?;
    let rd_u32 = |off: usize| {
        u32::from_ne_bytes([
            special[off],
            special[off + 1],
            special[off + 2],
            special[off + 3],
        ])
    };
    let rd_u16 = |off: usize| u16::from_ne_bytes([special[off], special[off + 1]]);
    Ok(BTPageOpaqueData {
        btpo_prev: rd_u32(0),
        btpo_next: rd_u32(4),
        btpo_level: rd_u32(8),
        btpo_flags: rd_u16(12),
        btpo_cycleid: rd_u16(14),
    })
}

/// Write a [`BTPageOpaqueData`] back into a mutable page's special area.
fn write_opaque(page: &mut PageMut<'_>, opaque: &BTPageOpaqueData) {
    let special = (read_pd_special(page.as_bytes()) as usize).min(BLCKSZ - 16);
    let bytes = page.as_mut_bytes();
    bytes[special..special + 4].copy_from_slice(&opaque.btpo_prev.to_ne_bytes());
    bytes[special + 4..special + 8].copy_from_slice(&opaque.btpo_next.to_ne_bytes());
    bytes[special + 8..special + 12].copy_from_slice(&opaque.btpo_level.to_ne_bytes());
    bytes[special + 12..special + 14].copy_from_slice(&opaque.btpo_flags.to_ne_bytes());
    bytes[special + 14..special + 16].copy_from_slice(&opaque.btpo_cycleid.to_ne_bytes());
}

/// `_bt_pageinit(page, size)` — zero + standard header with the nbtree special
/// area sized to `sizeof(BTPageOpaqueData)` (16). This is the trivial bufpage
/// inline (`PageInit` + special-size setup); re-implemented locally as the
/// dedup sibling does, rather than seamed.
fn _bt_pageinit(page: &mut [u8], size: usize) -> PgResult<()> {
    PageInit(page, size, 16)
}

/// `_bt_initmetapage(page, rootbknum, level, allequalimage)`.
fn _bt_initmetapage(
    page: &mut PageMut<'_>,
    rootbknum: BlockNumber,
    level: u32,
    allequalimage: bool,
) -> PgResult<()> {
    _bt_pageinit(page.as_mut_bytes(), BLCKSZ)?;

    let metad = BTMetaPageData {
        btm_magic: BTREE_MAGIC,
        btm_version: BTREE_VERSION,
        btm_root: rootbknum,
        btm_level: level,
        btm_fastroot: rootbknum,
        btm_fastlevel: level,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: allequalimage,
    };
    write_meta(page, &metad);

    let mut metaopaque = BTPageGetOpaque(&page.as_ref())?;
    metaopaque.btpo_flags = BTP_META;
    write_opaque(page, &metaopaque);

    // Set pd_lower just past the end of the metadata (essential so xlog.c page
    // compression does not lose it).
    let lower = (maxalign(SIZE_OF_PAGE_HEADER_DATA) + size_of_bt_meta_page_data()) as u16;
    write_pd_lower(page.as_mut_bytes(), lower);
    Ok(())
}

/// `sizeof(BTMetaPageData)`: 7*u32 = 28 -> pad to 32 for the f64; +8 = 40; +1
/// bool = 41 -> MAXALIGN 48.
#[inline]
const fn size_of_bt_meta_page_data() -> usize {
    48
}

/// Write the [`BTMetaPageData`] into the page contents area (MAXALIGN'd start),
/// field by field in the C struct order.
fn write_meta(page: &mut PageMut<'_>, metad: &BTMetaPageData) {
    let off = maxalign(SIZE_OF_PAGE_HEADER_DATA);
    let b = page.as_mut_bytes();
    b[off..off + 4].copy_from_slice(&metad.btm_magic.to_ne_bytes());
    b[off + 4..off + 8].copy_from_slice(&metad.btm_version.to_ne_bytes());
    b[off + 8..off + 12].copy_from_slice(&metad.btm_root.to_ne_bytes());
    b[off + 12..off + 16].copy_from_slice(&metad.btm_level.to_ne_bytes());
    b[off + 16..off + 20].copy_from_slice(&metad.btm_fastroot.to_ne_bytes());
    b[off + 20..off + 24].copy_from_slice(&metad.btm_fastlevel.to_ne_bytes());
    b[off + 24..off + 28].copy_from_slice(&metad.btm_last_cleanup_num_delpages.to_ne_bytes());
    // f64 at the 8-aligned offset 32.
    b[off + 32..off + 40].copy_from_slice(&metad.btm_last_cleanup_num_heap_tuples.to_ne_bytes());
    b[off + 40] = metad.btm_allequalimage as u8;
}

// ===========================================================================
// Deduplication byte codecs (re-ported from nbtdedup.c — pure tuple/byte logic).
// ===========================================================================

const PG_UINT16_MAX: i32 = 0xFFFF;

/// `_bt_form_posting(base, htids, nhtids)` — build a posting-list (or plain)
/// tuple from `base`'s key bytes and the supplied heap TIDs.
fn _bt_form_posting<'mcx>(
    mcx: Mcx<'mcx>,
    base: &[u8],
    htids: &[ItemPointerData],
    nhtids: i32,
) -> PgResult<PgVec<'mcx, u8>> {
    let basehdr = index_tuple_header(base);
    let keysize: u32 = if BTreeTupleIsPosting(&basehdr) {
        BTreeTupleGetPostingOffset(&basehdr)
    } else {
        IndexTupleSize(&basehdr) as u32
    };

    debug_assert!(!BTreeTupleIsPivot(&basehdr));
    debug_assert!(nhtids > 0 && nhtids <= PG_UINT16_MAX);
    debug_assert!(keysize as usize == maxalign(keysize as usize));

    let newsize: u32 = if nhtids > 1 {
        maxalign(keysize as usize + nhtids as usize * SIZE_OF_ITEM_POINTER_DATA) as u32
    } else {
        keysize
    };

    debug_assert!(newsize <= INDEX_SIZE_MASK as u32);
    debug_assert!(newsize as usize == maxalign(newsize as usize));

    let mut itup_bytes = alloc_zeroed(mcx, newsize as usize)?;
    itup_bytes[..keysize as usize].copy_from_slice(&base[..keysize as usize]);
    {
        let mut itup = index_tuple_header(&itup_bytes);
        itup.t_info &= !INDEX_SIZE_MASK;
        itup.t_info |= newsize as u16;
        write_tuple_header(&mut itup_bytes, &itup);
    }
    if nhtids > 1 {
        {
            let mut itup = index_tuple_header(&itup_bytes);
            BTreeTupleSetPosting(&mut itup, nhtids as u16, keysize as i32);
            write_tuple_header(&mut itup_bytes, &itup);
        }
        write_posting(&mut itup_bytes, keysize as usize, &htids[..nhtids as usize]);
    } else {
        let mut itup = index_tuple_header(&itup_bytes);
        itup.t_info &= !INDEX_ALT_TID_MASK;
        itup.t_tid = htids[0];
        write_tuple_header(&mut itup_bytes, &itup);
    }

    Ok(itup_bytes)
}

/// Write a posting list (array of `ItemPointerData`) into `bytes` at `off`.
fn write_posting(bytes: &mut [u8], off: usize, htids: &[ItemPointerData]) {
    for (i, h) in htids.iter().enumerate() {
        let base = off + i * SIZE_OF_ITEM_POINTER_DATA;
        write_item_pointer(&mut bytes[base..base + SIZE_OF_ITEM_POINTER_DATA], h);
    }
}

/// `BTDedupStateData` (`access/nbtree.h`) — working area for deduplication, the
/// subset `_bt_load`'s deduplicate path needs. `base`/`htids` are owned copies
/// of the relevant tuple bytes / heap-TID array (charged to `mcx`).
pub struct BTDedupState<'mcx> {
    /// Limit on size of final tuple
    pub maxpostingsize: Size,
    /// Number of max-sized tuples so far
    pub nmaxitems: i32,
    /// base tuple bytes (used to form new posting list)
    pub base: PgVec<'mcx, u8>,
    /// base size without original posting list
    pub basetupsize: Size,
    /// Heap TIDs in pending posting list
    pub htids: PgVec<'mcx, ItemPointerData>,
    /// Number of existing tuples/line pointers
    pub nitems: i32,
    /// Includes line pointer overhead
    pub phystupsize: Size,
}

/// `_bt_dedup_start_pending(state, base, baseoff)` — begin a pending posting
/// list with `base` as its first tuple. (`baseoff` is `InvalidOffsetNumber`
/// during a build, so it is dropped — the build never records page offsets.)
fn _bt_dedup_start_pending<'mcx>(
    mcx: Mcx<'mcx>,
    state: &mut BTDedupState<'mcx>,
    base: &[u8],
) -> PgResult<()> {
    debug_assert!(state.htids.is_empty());
    debug_assert!(state.nitems == 0);
    let basehdr = index_tuple_header(base);
    debug_assert!(!BTreeTupleIsPivot(&basehdr));

    state.htids.clear();
    if !BTreeTupleIsPosting(&basehdr) {
        state.htids.push(basehdr.t_tid);
        state.basetupsize = IndexTupleSize(&basehdr);
    } else {
        let nposting = BTreeTupleGetNPosting(&basehdr) as usize;
        for i in 0..nposting {
            state.htids.push(posting_list_n(base, i));
        }
        state.basetupsize = BTreeTupleGetPostingOffset(&basehdr) as Size;
    }

    state.nitems = 1;
    let mut base_copy: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, base.len())?;
    base_copy.extend_from_slice(base);
    state.base = base_copy;
    state.phystupsize = maxalign(IndexTupleSize(&basehdr)) + SIZE_OF_ITEM_ID_DATA;
    Ok(())
}

/// `_bt_dedup_save_htid(state, itup)` — returns whether the merge happened.
fn _bt_dedup_save_htid(state: &mut BTDedupState<'_>, itup: &[u8]) -> bool {
    let ihdr = index_tuple_header(itup);
    debug_assert!(!BTreeTupleIsPivot(&ihdr));

    let nhtids: i32;
    let mut new_htids: alloc::vec::Vec<ItemPointerData> = alloc::vec::Vec::new();
    if !BTreeTupleIsPosting(&ihdr) {
        nhtids = 1;
        new_htids.push(ihdr.t_tid);
    } else {
        let n = BTreeTupleGetNPosting(&ihdr) as i32;
        for i in 0..n as usize {
            new_htids.push(posting_list_n(itup, i));
        }
        nhtids = n;
    }

    let mergedtupsz = maxalign(
        state.basetupsize
            + (state.htids.len() as i32 + nhtids) as usize * SIZE_OF_ITEM_POINTER_DATA,
    );

    if mergedtupsz > state.maxpostingsize {
        if state.htids.len() as i32 > 50 {
            state.nmaxitems += 1;
        }
        return false;
    }

    state.nitems += 1;
    for h in new_htids {
        state.htids.push(h);
    }
    state.phystupsize += maxalign(IndexTupleSize(&ihdr)) + SIZE_OF_ITEM_ID_DATA;
    true
}

// ===========================================================================
// Runtime structs (nbtsort.c) — idiomatic Rust over real Relation handles.
// These are nbtsort.c-private (not shared nbtree vocabulary), so they live in
// the crate, mirroring the src-idiomatic port.
// ===========================================================================

/// `BTSpool` — status record for the spooling/sorting phase. The sort state is
/// the type-erased [`Tuplesortstate`] the tuplesort seams interpret.
pub struct BTSpool<'mcx> {
    /// `Tuplesortstate *sortstate` — state data for tuplesort.c
    pub sortstate: Tuplesortstate<'mcx>,
    /// `Relation heap`
    pub heap: Relation<'mcx>,
    /// `Relation index`
    pub index: Relation<'mcx>,
    pub isunique: bool,
    pub nulls_not_distinct: bool,
}

/// `BTPageState` — status record for a btree page being built (one per active
/// tree level). `btps_buf` is the owned page-building workspace (a
/// `BulkWriteBuffer` in C); `None` once the page has been handed to the writer.
pub struct BTPageState<'mcx> {
    /// workspace for page building (`BLCKSZ` bytes), or `None` after write-out
    pub btps_buf: Option<PgVec<'mcx, u8>>,
    /// block # to write this page at
    pub btps_blkno: BlockNumber,
    /// page's strict lower bound pivot tuple (owned), or `None`
    pub btps_lowkey: Option<PgVec<'mcx, u8>>,
    /// last item offset loaded
    pub btps_lastoff: OffsetNumber,
    /// last item's extra posting list space
    pub btps_lastextra: Size,
    /// tree level (0 = leaf)
    pub btps_level: u32,
    /// "full" if less than this much free space
    pub btps_full: Size,
    /// link to parent level, if any
    pub btps_next: Option<Box<BTPageState<'mcx>>>,
}

/// `BTWriteState` — overall status record for the index writing phase.
pub struct BTWriteState<'mcx> {
    /// `Relation heap`
    pub heap: Relation<'mcx>,
    /// `Relation index`
    pub index: Relation<'mcx>,
    /// index relation's `indnkeyatts`.
    pub keysz: i32,
    /// index relation's `indnatts` (`RelationGetNumberOfAttributes`).
    pub natts: i32,
    /// `BTScanInsert inskey` — the build insertion scankey (`_bt_mkscankey`).
    pub inskey: BTScanInsert<'mcx>,
    /// bulk write state, set in `_bt_load`. `None` before the load begins.
    pub bulkstate: Option<BulkWriteState<'mcx>>,
    /// # pages allocated
    pub btws_pages_alloced: BlockNumber,
}

// ===========================================================================
// _bt_spool / _bt_spooldestroy (the grounded spool accessors)
// ===========================================================================

/// `_bt_spool(btspool, self_tid, values, isnull)` — spool one index tuple for
/// the build sort, via `tuplesort_putindextuplevalues`. The real serial-build
/// accessor lives in [`deferred`] (it is driven by the build-scan callback);
/// re-exported here under its C name.
pub use deferred::_bt_spool;

/// `btbuild()` — the AM `ambuild`; the serial build entry point. Re-exported
/// from [`deferred`] (where it lives alongside the build-scan callback).
pub use deferred::btbuild;

/// `_bt_spooldestroy(btspool)` — clean up a spool structure and its sort state.
///
/// `mcx` is threaded in for the boxed-handle the `tuplesort_end` seam consumes
/// (C frees in `CurrentMemoryContext`; the owned model passes the context).
pub fn _bt_spooldestroy<'mcx>(mcx: Mcx<'mcx>, btspool: BTSpool<'mcx>) -> PgResult<()> {
    // C: tuplesort_end(btspool->sortstate); pfree(btspool);
    let BTSpool { sortstate, .. } = btspool;
    let boxed: PgBox<'mcx, Tuplesortstate<'mcx>> = alloc_in(mcx, sortstate)?;
    tuplesort::tuplesort_end::call(boxed)
}

// ===========================================================================
// _bt_leafbuild()
// ===========================================================================

// progress-reporting constants (commands/progress.h)
const PROGRESS_CREATEIDX_SUBPHASE: i32 = 10;
const PROGRESS_CREATEIDX_TUPLES_DONE: i32 = 12;
const PROGRESS_BTREE_PHASE_PERFORMSORT_1: i64 = 3;
const PROGRESS_BTREE_PHASE_PERFORMSORT_2: i64 = 4;
const PROGRESS_BTREE_PHASE_LEAF_LOAD: i64 = 5;

/// `RelationGetNumberOfAttributes(index)` (`utils/rel.h`).
#[inline]
fn rel_natts(index: &Relation<'_>) -> i32 {
    index.rd_att.natts
}

/// `_bt_leafbuild()` — given a spool loaded by successive `_bt_spool` calls,
/// create an entire btree.
pub fn _bt_leafbuild<'mcx>(
    mcx: Mcx<'mcx>,
    btspool: &mut BTSpool<'mcx>,
    btspool2: Option<&mut BTSpool<'mcx>>,
) -> PgResult<()> {
    // Execute the sort
    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_BTREE_PHASE_PERFORMSORT_1);
    tuplesort::tuplesort_performsort::call(&mut btspool.sortstate)?;
    let mut btspool2 = btspool2;
    if let Some(ref mut sp2) = btspool2 {
        pgstat_progress_update_param(
            PROGRESS_CREATEIDX_SUBPHASE,
            PROGRESS_BTREE_PHASE_PERFORMSORT_2,
        );
        tuplesort::tuplesort_performsort::call(&mut sp2.sortstate)?;
    }

    // _bt_mkscankey(index, NULL): the build insertion scankey.
    let mut inskey = nbtcore::bt_mkscankey::call(&btspool.index, None)?;
    // _bt_mkscankey() won't set allequalimage without metapage
    if let Some(key) = inskey.as_mut() {
        key.allequalimage = nbtcore::bt_allequalimage_dbg::call(&btspool.index, true)?;
    }

    let keysz = btspool.index.indnkeyatts();
    let natts = rel_natts(&btspool.index);

    let mut wstate = BTWriteState {
        heap: btspool.heap.alias(),
        index: btspool.index.alias(),
        keysz,
        natts,
        inskey,
        bulkstate: None,
        // reserve the metapage
        btws_pages_alloced: BTREE_METAPAGE + 1,
    };

    pgstat_progress_update_param(PROGRESS_CREATEIDX_SUBPHASE, PROGRESS_BTREE_PHASE_LEAF_LOAD);
    _bt_load(mcx, &mut wstate, btspool, btspool2)
}

// ===========================================================================
// Page building
// ===========================================================================

/// `BTGetTargetPageFreeSpace(rel)` = `BLCKSZ * (100 - fillfactor) / 100`.
#[inline]
fn bt_target_page_free_space(index: &Relation<'_>) -> usize {
    let fillfactor = index.get_fillfactor(BTREE_DEFAULT_FILLFACTOR);
    BLCKSZ * (100 - fillfactor as usize) / 100
}

/// `BTGetDeduplicateItems(rel)` — read the index's `deduplicate_items`
/// reloption. The btree-specific `BtOptions` reloption struct is not yet
/// modeled in this repo, so this returns the C default (`true`) when no
/// reloptions are set, which is the behaviour for every index that does not
/// explicitly set `deduplicate_items = off`.
#[inline]
fn bt_get_deduplicate_items(_index: &Relation<'_>) -> bool {
    true
}

/// `_bt_blnewpage()` — fresh, clean btree page workspace (not linked to siblings).
fn _bt_blnewpage<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'mcx>,
    level: u32,
) -> PgResult<PgVec<'mcx, u8>> {
    let bulkstate = wstate
        .bulkstate
        .as_mut()
        .ok_or_else(|| PgError::error("_bt_blnewpage: bulkstate present"))?;
    let mut buf = bulk::smgr_bulk_get_buf::call(mcx, bulkstate)?;

    // Zero the page and set up standard page header info
    _bt_pageinit(&mut buf, BLCKSZ)?;

    {
        let mut page = PageMut::new(&mut buf)?;
        let mut opaque = BTPageGetOpaque(&page.as_ref())?;
        opaque.btpo_prev = P_NONE;
        opaque.btpo_next = P_NONE;
        opaque.btpo_level = level;
        opaque.btpo_flags = if level > 0 { 0 } else { BTP_LEAF };
        opaque.btpo_cycleid = 0;
        write_opaque(&mut page, &opaque);

        // Make the P_HIKEY line pointer appear allocated.
        let lower = read_pd_lower(page.as_bytes()) + SIZE_OF_ITEM_ID_DATA as u16;
        write_pd_lower(page.as_mut_bytes(), lower);
    }

    Ok(buf)
}

/// `_bt_blwritepage()` — emit a completed btree page (the bulk writer takes
/// ownership of `buf`).
fn _bt_blwritepage<'mcx>(
    wstate: &mut BTWriteState<'mcx>,
    buf: PgVec<'mcx, u8>,
    blkno: BlockNumber,
) -> PgResult<()> {
    let bulkstate = wstate
        .bulkstate
        .as_mut()
        .ok_or_else(|| PgError::error("_bt_blwritepage: bulkstate present"))?;
    // C: smgr_bulk_write(wstate->bulkstate, blkno, buf, true);
    bulk::smgr_bulk_write::call(bulkstate, blkno, buf, true)
}

/// `_bt_pagestate()` — allocate and initialise a new [`BTPageState`].
fn _bt_pagestate<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'mcx>,
    level: u32,
) -> PgResult<BTPageState<'mcx>> {
    let buf = _bt_blnewpage(mcx, wstate, level)?;

    let blkno = wstate.btws_pages_alloced;
    wstate.btws_pages_alloced += 1;

    let full = if level > 0 {
        BLCKSZ * (100 - BTREE_NONLEAF_FILLFACTOR as usize) / 100
    } else {
        bt_target_page_free_space(&wstate.index)
    };

    Ok(BTPageState {
        btps_buf: Some(buf),
        btps_blkno: blkno,
        btps_lowkey: None,
        btps_lastoff: P_HIKEY,
        btps_lastextra: 0,
        btps_level: level,
        btps_full: full,
        btps_next: None,
    })
}

/// `_bt_slideleft()` — slide the array of ItemIds back one slot (from
/// P_FIRSTKEY to P_HIKEY, overwriting the unneeded empty P_HIKEY slot on a
/// rightmost page).
fn _bt_slideleft(buf: &mut [u8]) -> PgResult<()> {
    let maxoff = {
        let page = PageRef::new(buf)?;
        PageGetMaxOffsetNumber(&page)
    };
    debug_assert!(maxoff >= P_FIRSTKEY);

    let mut off = P_FIRSTKEY;
    while off <= maxoff {
        let thisii = read_line_pointer(buf, off);
        write_line_pointer(buf, OffsetNumberPrev(off), thisii);
        off = OffsetNumberNext(off);
    }
    let lower = read_pd_lower(buf) - SIZE_OF_ITEM_ID_DATA as u16;
    write_pd_lower(buf, lower);
    Ok(())
}

/// `_bt_sortaddtup()` — add an item to a page being built.
///
/// `newfirstdataitem` truncates the item to a 0-attribute minus-infinity pivot
/// (internal pages store only the downlink in their first data item).
fn _bt_sortaddtup(
    page: &mut PageMut<'_>,
    itemsize: Size,
    itup: &[u8],
    itup_off: OffsetNumber,
    newfirstdataitem: bool,
) -> PgResult<()> {
    let trunctuple: alloc::vec::Vec<u8>;
    let item: &[u8] = if newfirstdataitem {
        let mut t = itup[..SIZE_OF_INDEX_TUPLE_DATA].to_vec();
        let mut hdr = index_tuple_header(&t);
        hdr.t_info = SIZE_OF_INDEX_TUPLE_DATA as u16;
        write_tuple_header(&mut t, &hdr);
        {
            let mut hdr2 = index_tuple_header(&t);
            BTreeTupleSetNAtts(&mut hdr2, 0, false);
            write_tuple_header(&mut t, &hdr2);
        }
        trunctuple = t;
        &trunctuple[..]
    } else {
        &itup[..itemsize]
    };

    if PageAddItemExtended(page, item, itup_off, 0)? == INVALID_OFFSET_NUMBER {
        return Err(PgError::error("failed to add item to the index page"));
    }
    Ok(())
}

/// `_bt_buildadd()` — add an item to a disk page from the sort output (or add a
/// posting list item formed from the sort output).
fn _bt_buildadd<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'mcx>,
    state: &mut BTPageState<'mcx>,
    itup: &[u8],
    truncextra: Size,
) -> PgResult<()> {
    let mut nbuf = state
        .btps_buf
        .take()
        .ok_or_else(|| PgError::error("_bt_buildadd: btps_buf present"))?;
    let mut nblkno = state.btps_blkno;
    let mut last_off = state.btps_lastoff;
    let last_truncextra = state.btps_lastextra;
    state.btps_lastextra = truncextra;

    let pgspc = {
        let page = PageRef::new(&nbuf)?;
        PageGetFreeSpace(&page)
    };
    let mut itupsz = IndexTupleSize(&index_tuple_header(itup));
    itupsz = maxalign(itupsz);
    let isleaf = state.btps_level == 0;

    if itupsz > BTMaxItemSize {
        // C: _bt_check_third_page(wstate->index, wstate->heap, isleaf,
        //    npage, itup) (nbtsort.c:833).
        buildhelp::bt_check_third_page::call(
            &wstate.index,
            &wstate.heap,
            isleaf,
            &nbuf,
            itup,
        )?;
    }

    debug_assert!(last_truncextra == 0 || isleaf);
    if pgspc < itupsz + (if isleaf { maxalign(SIZE_OF_ITEM_POINTER_DATA) } else { 0 })
        || (pgspc + last_truncextra < state.btps_full && last_off > P_FIRSTKEY)
    {
        // Finish off the page and write it out.
        let mut obuf = nbuf; // old page buffer (current page being closed)
        let oblkno = nblkno;

        nbuf = _bt_blnewpage(mcx, wstate, state.btps_level)?;
        nblkno = wstate.btws_pages_alloced;
        wstate.btws_pages_alloced += 1;

        debug_assert!(last_off > P_FIRSTKEY);

        // Read the last item (oitup) out of the old page, copy it onto the new
        // page at P_FIRSTKEY.
        let (oitup_bytes, ii_len) = {
            let opage = PageRef::new(&obuf)?;
            let ii = PageGetItemId(&opage, last_off)?;
            let oitup = PageGetItem(&opage, &ii)?;
            (oitup.to_vec(), ii.lp_len())
        };
        {
            let mut npage = PageMut::new(&mut nbuf)?;
            _bt_sortaddtup(&mut npage, ii_len as Size, &oitup_bytes, P_FIRSTKEY, !isleaf)?;
        }

        // Move 'last' into the high key position on opage.
        {
            let ii_copy = read_line_pointer(&obuf, last_off);
            write_line_pointer(&mut obuf, P_HIKEY, ii_copy);
            write_line_pointer(&mut obuf, last_off, [0u8; 4]);
            let lower = read_pd_lower(&obuf) - SIZE_OF_ITEM_ID_DATA as u16;
            write_pd_lower(&mut obuf, lower);
        }

        let oitup_hikey: alloc::vec::Vec<u8>;

        if isleaf {
            // Truncate away any unneeded attributes from high key on leaf level.
            let (lastleft, oitup): (alloc::vec::Vec<u8>, alloc::vec::Vec<u8>) = {
                let opage = PageRef::new(&obuf)?;
                let ii = PageGetItemId(&opage, OffsetNumberPrev(last_off))?;
                let lastleft = PageGetItem(&opage, &ii)?.to_vec();
                let hii = PageGetItemId(&opage, P_HIKEY)?;
                let oitup = PageGetItem(&opage, &hii)?.to_vec();
                (lastleft, oitup)
            };

            debug_assert!(IndexTupleSize(&index_tuple_header(&oitup)) > last_truncextra);
            let truncated =
                buildhelp::bt_truncate::call(mcx, &wstate.index, &lastleft, &oitup, &wstate.inskey)?;
            {
                let trunc_sz = IndexTupleSize(&index_tuple_header(&truncated));
                let mut opage = PageMut::new(&mut obuf)?;
                if !PageIndexTupleOverwrite(&mut opage, P_HIKEY, &truncated[..trunc_sz])? {
                    return Err(PgError::error("failed to add high key to the index page"));
                }
            }

            let opage = PageRef::new(&obuf)?;
            let hii = PageGetItemId(&opage, P_HIKEY)?;
            oitup_hikey = PageGetItem(&opage, &hii)?.to_vec();
        } else {
            oitup_hikey = oitup_bytes.clone();
        }

        // Link the old page into its parent (creating a level if needed).
        if state.btps_next.is_none() {
            let parent = _bt_pagestate(mcx, wstate, state.btps_level + 1)?;
            state.btps_next = Some(Box::new(parent));
        }

        {
            let mut lowkey = state
                .btps_lowkey
                .take()
                .ok_or_else(|| PgError::error("_bt_buildadd: btps_lowkey present"))?;
            debug_assert!({
                let opage = PageRef::new(&obuf)?;
                let oopaque = BTPageGetOpaque(&opage)?;
                let lk = index_tuple_header(&lowkey);
                (BTreeTupleGetNAtts(&lk, wstate.natts as u16) <= wstate.keysz as u16
                    && BTreeTupleGetNAtts(&lk, wstate.natts as u16) > 0)
                    || P_LEFTMOST(&oopaque)
            });
            debug_assert!({
                let opage = PageRef::new(&obuf)?;
                let oopaque = BTPageGetOpaque(&opage)?;
                let lk = index_tuple_header(&lowkey);
                BTreeTupleGetNAtts(&lk, wstate.natts as u16) == 0 || !P_LEFTMOST(&oopaque)
            });
            {
                let mut hdr = index_tuple_header(&lowkey);
                BTreeTupleSetDownLink(&mut hdr, oblkno);
                write_tuple_header(&mut lowkey, &hdr);
            }
            let mut parent = state
                .btps_next
                .take()
                .ok_or_else(|| PgError::error("_bt_buildadd: btps_next is NULL"))?;
            _bt_buildadd(mcx, wstate, &mut parent, &lowkey, 0)?;
            state.btps_next = Some(parent);
        }

        // Save a copy of the high key from the old page (= low key for the new).
        let lowkey_sz = IndexTupleSize(&index_tuple_header(&oitup_hikey));
        let mut lk: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, lowkey_sz)?;
        lk.extend_from_slice(&oitup_hikey[..lowkey_sz]);
        state.btps_lowkey = Some(lk);

        // Set the sibling links for both pages.
        {
            let mut opage = PageMut::new(&mut obuf)?;
            let mut oopaque = BTPageGetOpaque(&opage.as_ref())?;
            oopaque.btpo_next = nblkno;
            write_opaque(&mut opage, &oopaque);
        }
        {
            let mut npage = PageMut::new(&mut nbuf)?;
            let mut nopaque = BTPageGetOpaque(&npage.as_ref())?;
            nopaque.btpo_prev = oblkno;
            nopaque.btpo_next = P_NONE;
            write_opaque(&mut npage, &nopaque);
        }

        _bt_blwritepage(wstate, obuf, oblkno)?;

        last_off = P_FIRSTKEY;
    }

    // If the new item is the first for its page, generate a minus infinity lowkey.
    if last_off == P_HIKEY {
        debug_assert!(state.btps_lowkey.is_none());
        let mut lowkey = alloc_zeroed(mcx, SIZE_OF_INDEX_TUPLE_DATA)?;
        {
            let mut hdr = index_tuple_header(&lowkey);
            hdr.t_info = SIZE_OF_INDEX_TUPLE_DATA as u16;
            write_tuple_header(&mut lowkey, &hdr);
        }
        {
            let mut hdr = index_tuple_header(&lowkey);
            BTreeTupleSetNAtts(&mut hdr, 0, false);
            write_tuple_header(&mut lowkey, &hdr);
        }
        state.btps_lowkey = Some(lowkey);
    }

    // Add the new item into the current page.
    last_off = OffsetNumberNext(last_off);
    {
        let mut npage = PageMut::new(&mut nbuf)?;
        _bt_sortaddtup(
            &mut npage,
            itupsz,
            itup,
            last_off,
            !isleaf && last_off == P_FIRSTKEY,
        )?;
    }

    state.btps_buf = Some(nbuf);
    state.btps_blkno = nblkno;
    state.btps_lastoff = last_off;
    Ok(())
}

/// `_bt_sort_dedup_finish_pending()` — finalize the pending posting list and add
/// it via `_bt_buildadd`.
fn _bt_sort_dedup_finish_pending<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'mcx>,
    state: &mut BTPageState<'mcx>,
    dstate: &mut BTDedupState<'mcx>,
) -> PgResult<()> {
    debug_assert!(dstate.nitems > 0);

    if dstate.nitems == 1 {
        let base = dstate.base.to_vec();
        _bt_buildadd(mcx, wstate, state, &base, 0)?;
    } else {
        let htids = dstate.htids.to_vec();
        let postingtuple = _bt_form_posting(mcx, &dstate.base, &htids, htids.len() as i32)?;
        let truncextra = IndexTupleSize(&index_tuple_header(&postingtuple))
            - BTreeTupleGetPostingOffset(&index_tuple_header(&postingtuple)) as usize;
        _bt_buildadd(mcx, wstate, state, &postingtuple, truncextra)?;
    }

    dstate.nmaxitems = 0;
    dstate.htids.clear();
    dstate.nitems = 0;
    dstate.phystupsize = 0;
    Ok(())
}

/// `_bt_uppershutdown()` — finish writing out the completed btree.
fn _bt_uppershutdown<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'mcx>,
    state: Option<Box<BTPageState<'mcx>>>,
) -> PgResult<()> {
    let mut rootblkno = P_NONE;
    let mut rootlevel: u32 = 0;

    let mut cur = state;
    while let Some(mut s) = cur {
        let next = s.btps_next.take();
        let blkno = s.btps_blkno;
        let mut buf = s
            .btps_buf
            .take()
            .ok_or_else(|| PgError::error("_bt_uppershutdown: btps_buf present"))?;

        let next_after: Option<Box<BTPageState<'mcx>>>;

        if next.is_none() {
            let mut page = PageMut::new(&mut buf)?;
            let mut opaque = BTPageGetOpaque(&page.as_ref())?;
            opaque.btpo_flags |= BTP_ROOT;
            write_opaque(&mut page, &opaque);
            rootblkno = blkno;
            rootlevel = s.btps_level;
            next_after = None;
        } else {
            let mut parent =
                next.ok_or_else(|| PgError::error("_bt_uppershutdown: btps_next is NULL"))?;
            let mut lowkey = s
                .btps_lowkey
                .take()
                .ok_or_else(|| PgError::error("_bt_uppershutdown: btps_lowkey present"))?;
            debug_assert!({
                let page = PageRef::new(&buf)?;
                let opaque = BTPageGetOpaque(&page)?;
                let lk = index_tuple_header(&lowkey);
                (BTreeTupleGetNAtts(&lk, wstate.natts as u16) <= wstate.keysz as u16
                    && BTreeTupleGetNAtts(&lk, wstate.natts as u16) > 0)
                    || P_LEFTMOST(&opaque)
            });
            debug_assert!({
                let page = PageRef::new(&buf)?;
                let opaque = BTPageGetOpaque(&page)?;
                let lk = index_tuple_header(&lowkey);
                BTreeTupleGetNAtts(&lk, wstate.natts as u16) == 0 || !P_LEFTMOST(&opaque)
            });
            {
                let mut hdr = index_tuple_header(&lowkey);
                BTreeTupleSetDownLink(&mut hdr, blkno);
                write_tuple_header(&mut lowkey, &hdr);
            }
            _bt_buildadd(mcx, wstate, &mut parent, &lowkey, 0)?;
            next_after = Some(parent);
        }

        // Rightmost page: slide the ItemId array back one slot, then dump it out.
        _bt_slideleft(&mut buf)?;
        _bt_blwritepage(wstate, buf, s.btps_blkno)?;

        cur = next_after;
    }

    // Construct the metapage pointing at the new root (P_NONE for an empty tree).
    let bulkstate = wstate
        .bulkstate
        .as_mut()
        .ok_or_else(|| PgError::error("_bt_uppershutdown: bulkstate present"))?;
    let mut metabuf = bulk::smgr_bulk_get_buf::call(mcx, bulkstate)?;
    {
        let mut metapage = PageMut::new(&mut metabuf)?;
        let allequalimage = inskey_allequalimage(&wstate.inskey);
        _bt_initmetapage(&mut metapage, rootblkno, rootlevel, allequalimage)?;
    }
    _bt_blwritepage(wstate, metabuf, BTREE_METAPAGE)?;
    Ok(())
}

/// `_bt_load()` — read tuples in correct sort order from tuplesort and load
/// them into btree leaves.
fn _bt_load<'mcx>(
    mcx: Mcx<'mcx>,
    wstate: &mut BTWriteState<'mcx>,
    btspool: &mut BTSpool<'mcx>,
    btspool2: Option<&mut BTSpool<'mcx>>,
) -> PgResult<()> {
    let mut state: Option<Box<BTPageState<'mcx>>> = None;
    let merge = btspool2.is_some();
    let keysz = wstate.keysz;
    let mut tuples_done: i64 = 0;

    // C: wstate->bulkstate = smgr_bulk_start_rel(wstate->index, MAIN_FORKNUM);
    wstate.bulkstate = Some(bulk::smgr_bulk_start_rel::call(
        mcx,
        &wstate.index,
        ForkNumber::MAIN_FORKNUM,
    )?);

    let allequalimage = inskey_allequalimage(&wstate.inskey);
    let deduplicate =
        allequalimage && !btspool.isunique && bt_get_deduplicate_items(&wstate.index);

    if merge {
        let btspool2 = btspool2.ok_or_else(|| PgError::error("_bt_load: btspool2 is NULL"))?;
        // Merge btspool and btspool2 (the unique-index dead-tuple spool).
        let mut itup =
            tuplesort::tuplesort_getindextuple::call(&mut btspool.sortstate, true)?;
        let mut itup2 =
            tuplesort::tuplesort_getindextuple::call(&mut btspool2.sortstate, true)?;

        loop {
            let mut load1 = true; // load BTSpool next ?
            if itup2.is_none() {
                if itup.is_none() {
                    break;
                }
            } else if itup.is_some() {
                let it = itup.as_ref().unwrap();
                let it2 = itup2.as_ref().unwrap();
                // C inlines index_getattr + ApplySortComparator per key, then
                // ItemPointerCompare on heap TID; the build's SortSupport
                // substrate is unported, so the whole comparison is one
                // owned-by-nbtree-core seam (panic-until-owner).
                let mut compare =
                    buildhelp::bt_load_compare_index_tuples::call(&wstate.index, &wstate.inskey, it, it2)?;

                // Equal key values -> sort on heap TID (the implicit last attr).
                if compare == 0 {
                    compare = ItemPointerCompare(
                        &index_tuple_header(it).t_tid,
                        &index_tuple_header(it2).t_tid,
                    );
                    debug_assert!(compare != 0);
                    if compare > 0 {
                        load1 = false;
                    }
                } else if compare > 0 {
                    load1 = false;
                }
            } else {
                load1 = false;
            }

            if state.is_none() {
                state = Some(Box::new(_bt_pagestate(mcx, wstate, 0)?));
            }

            if load1 {
                let it = itup.take().unwrap();
                let st = state.as_mut().unwrap();
                _bt_buildadd(mcx, wstate, st, &it, 0)?;
                itup =
                    tuplesort::tuplesort_getindextuple::call(&mut btspool.sortstate, true)?;
            } else {
                let it2 = itup2.take().unwrap();
                let st = state.as_mut().unwrap();
                _bt_buildadd(mcx, wstate, st, &it2, 0)?;
                itup2 =
                    tuplesort::tuplesort_getindextuple::call(&mut btspool2.sortstate, true)?;
            }

            tuples_done += 1;
            pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, tuples_done);
        }
    } else if deduplicate {
        // merge unnecessary; deduplicate into posting lists.
        let mut dstate = new_load_dedup_state(mcx)?;

        while let Some(itup) =
            tuplesort::tuplesort_getindextuple::call(&mut btspool.sortstate, true)?
        {
            if state.is_none() {
                state = Some(Box::new(_bt_pagestate(mcx, wstate, 0)?));

                // Limit posting-list tuple size to 1/10 of the page (plus the
                // final item's line pointer).
                dstate.maxpostingsize = maxalign_down(BLCKSZ * 10 / 100) - SIZE_OF_ITEM_ID_DATA;
                debug_assert!(
                    dstate.maxpostingsize <= BTMaxItemSize
                        && dstate.maxpostingsize <= INDEX_SIZE_MASK as usize
                );

                _bt_dedup_start_pending(mcx, &mut dstate, &itup)?;
            } else if nbtcore::bt_keep_natts_fast::call(&wstate.index, &dstate.base, &itup)? > keysz
                && _bt_dedup_save_htid(&mut dstate, &itup)
            {
                // itup merged into the pending posting list.
            } else {
                {
                    let st = state.as_mut().unwrap();
                    _bt_sort_dedup_finish_pending(mcx, wstate, st, &mut dstate)?;
                }
                _bt_dedup_start_pending(mcx, &mut dstate, &itup)?;
            }

            tuples_done += 1;
            pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, tuples_done);
        }

        if state.is_some() {
            let st = state.as_mut().unwrap();
            _bt_sort_dedup_finish_pending(mcx, wstate, st, &mut dstate)?;
        }
    } else {
        // merging and deduplication both unnecessary.
        while let Some(itup) =
            tuplesort::tuplesort_getindextuple::call(&mut btspool.sortstate, true)?
        {
            if state.is_none() {
                state = Some(Box::new(_bt_pagestate(mcx, wstate, 0)?));
            }
            {
                let st = state.as_mut().unwrap();
                _bt_buildadd(mcx, wstate, st, &itup, 0)?;
            }

            tuples_done += 1;
            pgstat_progress_update_param(PROGRESS_CREATEIDX_TUPLES_DONE, tuples_done);
        }
    }

    _bt_uppershutdown(mcx, wstate, state)?;

    // C: smgr_bulk_finish(wstate->bulkstate);
    let bulkstate = wstate
        .bulkstate
        .take()
        .ok_or_else(|| PgError::error("_bt_load: bulkstate present"))?;
    bulk::smgr_bulk_finish::call(bulkstate)?;
    Ok(())
}

/// Build the `BTDedupState` used by `_bt_load`'s deduplicate path.
fn new_load_dedup_state<'mcx>(mcx: Mcx<'mcx>) -> PgResult<BTDedupState<'mcx>> {
    Ok(BTDedupState {
        maxpostingsize: 0,
        nmaxitems: 0,
        base: vec_with_capacity_in(mcx, 0)?,
        basetupsize: 0,
        htids: vec_with_capacity_in(mcx, 0)?,
        nitems: 0,
        phystupsize: 0,
    })
}

// --- BTScanInsert read-only field accessors --------------------------------

/// `itup_key->allequalimage`.
#[inline]
fn inskey_allequalimage(inskey: &BTScanInsert<'_>) -> bool {
    inskey.as_ref().map(|k| k.allequalimage).unwrap_or(false)
}

/// OOM-safe zero-filled byte buffer in `mcx` (the `palloc0(len)` paths).
fn alloc_zeroed<'mcx>(mcx: Mcx<'mcx>, len: usize) -> PgResult<PgVec<'mcx, u8>> {
    let mut v: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    v.resize(len, 0);
    Ok(v)
}

#[cfg(test)]
mod tests;
