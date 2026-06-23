//! Owned-tree Rust port of `src/backend/access/gin/ginentrypage.c`
//! (PostgreSQL 18.3) — the routines for handling GIN **entry tree** pages, plus
//! the entry-tree `GinBtreeData` method table that the shared descent spine
//! (`ginbtree.c`, the landed [`ginbtree`] crate) dispatches
//! through.
//!
//! The C functions ported here, 1:1:
//!
//!   * `GinFormTuple`            — form a leaf entry tuple (key + posting list)
//!   * `ginReadTuple`            — decode a leaf entry tuple's item pointers
//!   * `GinFormInteriorTuple`    — form a non-leaf entry tuple from a key tuple
//!   * `getRightMostTuple`       — the rightmost tuple on an entry page
//!   * `entryIsMoveRight`        — should the descent follow the right link?
//!   * `entryLocateEntry`        — binary-search a non-leaf entry page
//!   * `entryLocateLeafEntry`    — binary-search a leaf entry page
//!   * `entryFindChildPtr`       — find the offset of a child's downlink
//!   * `entryGetLeftMostPage`    — leftmost child of a non-leaf entry page
//!   * `entryIsEnoughSpace`      — does the new tuple fit?
//!   * `entryPreparePage`        — delete/relink before placing a tuple
//!   * `entryBeginPlaceToPage` / `entryExecPlaceToPage`
//!   * `entrySplitPage`          — split an entry page
//!   * `entryPrepareDownlink`    — build the downlink for a split child
//!   * `ginEntryFillRoot`        — fill a new entry root page
//!   * `ginPrepareEntryScan`     — set up a [`GinBtreeData`] for entry access
//!
//! ## Model
//!
//! GIN manipulates `IndexTuple`s at the byte level (`GinGet*`/`GinSet*` macros);
//! the on-disk image is carried as an owned `Vec<u8>` (`GinBtreeEntryInsertData.
//! entry`, the C contiguous `palloc`'d chunk). Page bytes are reached through the
//! bufmgr seam (`BufferGetPage(buf)` via [`bufmgr::with_buffer_page`]); the
//! GIN-page-flag readers (`GinPageIsLeaf` / `GinPageIsData` / `GinPageRightMost`),
//! the index-tuple `t_tid`/`t_info` math, and the page sizing constants come from
//! the [`gindatapage`] byte substrate. The null-category
//! comparison (`ginCompareAttEntries`), the key/attnum deform
//! (`gintuple_get_key` / `gintuple_get_attrnum`), `index_form_tuple`, and
//! `GinInitPage` are the [`ginutil`] owner's logic (catalog /
//! fmgr substrate reached through its seams).
//!
//! ## WAL deferral
//!
//! The WAL record emission inside `ginPlaceToPage` is done by the spine; the
//! entry-tree `execPlaceToPage` registers the slot-0 buffer and the
//! `ginxlogInsertEntry` byte-data through the `xloginsert.c` seams, exactly as
//! the C `entryExecPlaceToPage` does. No XLog API is called outside those seams.
//!
//! No raw pointers, no `extern "C"`.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
// `PgError` is large, so the un-boxed `PgResult` `Err` is large; project-wide
// error contract.
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;
use alloc::vec::Vec;

use ::mcx::Mcx;

use bufmgr_seams as bufmgr;
use page::{
    PageAddItemExtended, PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageIndexTupleDelete, PageMut, PageRef,
};
use utils_error::{ereport, PgResult};
use ::types_error::error::{ERROR, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

use indextuple::{index_form_tuple, FormedIndexTuple};
use gindatapage as gdp;
use ginutil as ginutil;

use ::types_core::primitive::{BlockNumber, OffsetNumber, BLCKSZ};
use ::types_core::InvalidBlockNumber;
use gin::{
    BeginPlaceToPageResult, GinBtreeData, GinBtreeEntryInsertData, GinBtreeStack, GinInsertPayload,
    GinNullCategory, GinPlaceToPageRC, GinState, PtpWorkspace, GinMaxItemSize, GIN_CAT_NORM_KEY,
    GIN_ROOT_BLKNO,
};
use ::types_storage::storage::Buffer;
use ::types_tuple::heaptuple::Datum;
use ::types_tuple::heaptuple::{
    BlockIdData, IndexTupleData, ItemPointerData, FIRST_OFFSET_NUMBER as FirstOffsetNumber,
    INDEX_NULL_MASK, INDEX_SIZE_MASK, INVALID_OFFSET_NUMBER as InvalidOffsetNumber,
};

#[cfg(test)]
mod tests;

// ===========================================================================
// init_seams — this crate owns no inward seams.
// ===========================================================================

/// `ginentrypage` owns no inward seams of its own; it consumes the bufmgr /
/// xloginsert / ginutil / indextuple seams installed by their real owners, and
/// it *installs* the entry-tree page callbacks into a [`GinBtreeData`] vtable
/// (via [`ginPrepareEntryScan`]) rather than into a global seam registry. The
/// conventional hook is therefore empty.
pub fn init_seams() {}

// ===========================================================================
// Alignment helpers (c.h MAXALIGN / SHORTALIGN) and fixed sizes.
// ===========================================================================

/// `MAXALIGN(LEN)` — round up to `MAXIMUM_ALIGNOF` (8).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + (8 - 1)) & !(8 - 1)
}

/// `SHORTALIGN(LEN)` — round up to a multiple of `ALIGNOF_SHORT` (2).
#[inline]
const fn shortalign(len: usize) -> usize {
    (len + (2 - 1)) & !(2 - 1)
}

/// `sizeof(ItemIdData)` == 4.
const SIZE_OF_ITEM_ID: usize = 4;

/// `sizeof(ItemPointerData)` on disk == 6.
const SIZE_OF_ITEM_POINTER: usize = 6;

// ===========================================================================
// On-disk GIN IndexTuple byte helpers (ginblock.h leaf/non-leaf macros).
//
// We model a GIN tuple as an owned `Vec<u8>` holding the exact on-disk
// `IndexTupleData` image (`t_tid: ItemPointerData (6) | t_info: u16 (2) |
// data`), the same model the `gindatapage` byte substrate uses. Decoding the
// `IndexTupleData` header out of the first 8 bytes lets us reuse the substrate
// `Gin*` accessors which take `&IndexTupleData`.
// ===========================================================================

/// `GIN_ITUP_COMPRESSED` (ginblock.h) — high bit of the posting-offset field.
const GIN_ITUP_COMPRESSED: u32 = 1u32 << 31;

/// Read a 6-byte on-disk `ItemPointerData` at the start of `buf`.
#[inline]
fn read_item_pointer(buf: &[u8]) -> ItemPointerData {
    ItemPointerData {
        ip_blkid: BlockIdData {
            bi_hi: u16::from_ne_bytes([buf[0], buf[1]]),
            bi_lo: u16::from_ne_bytes([buf[2], buf[3]]),
        },
        ip_posid: u16::from_ne_bytes([buf[4], buf[5]]),
    }
}

/// Write a 6-byte on-disk `ItemPointerData` at the start of `buf`.
#[inline]
fn write_item_pointer(buf: &mut [u8], iptr: &ItemPointerData) {
    buf[0..2].copy_from_slice(&iptr.ip_blkid.bi_hi.to_ne_bytes());
    buf[2..4].copy_from_slice(&iptr.ip_blkid.bi_lo.to_ne_bytes());
    buf[4..6].copy_from_slice(&iptr.ip_posid.to_ne_bytes());
}

/// Read `itup->t_info`.
#[inline]
fn read_t_info(tup: &[u8]) -> u16 {
    u16::from_ne_bytes([tup[6], tup[7]])
}

/// Write `itup->t_info`.
#[inline]
fn write_t_info(tup: &mut [u8], info: u16) {
    tup[6..8].copy_from_slice(&info.to_ne_bytes());
}

/// Decode the 8-byte `IndexTupleData` header out of an on-disk tuple image.
#[inline]
fn header_of(tup: &[u8]) -> IndexTupleData {
    IndexTupleData {
        t_tid: read_item_pointer(tup),
        t_info: read_t_info(tup),
    }
}

/// `IndexTupleSize(itup)` — `t_info & INDEX_SIZE_MASK`.
#[inline]
fn index_tuple_size(tup: &[u8]) -> usize {
    (read_t_info(tup) & INDEX_SIZE_MASK) as usize
}

/// `IndexTupleHasNulls(itup)` — `t_info & INDEX_NULL_MASK`.
#[inline]
fn index_tuple_has_nulls(tup: &[u8]) -> bool {
    read_t_info(tup) & INDEX_NULL_MASK != 0
}

/// `GinGetNPosting(itup)`.
#[inline]
fn gin_get_n_posting(tup: &[u8]) -> OffsetNumber {
    gdp::GinGetNPosting(&header_of(tup))
}

/// `GinSetNPosting(itup, n)`.
#[inline]
fn gin_set_n_posting(tup: &mut [u8], n: OffsetNumber) {
    // ItemPointerSetOffsetNumber(&(itup)->t_tid, n): write ip_posid (byte 4..6).
    tup[4..6].copy_from_slice(&n.to_ne_bytes());
}

/// `GinIsPostingTree(itup)`.
#[inline]
fn gin_is_posting_tree(tup: &[u8]) -> bool {
    gdp::GinIsPostingTree(&header_of(tup))
}

/// `GinSetPostingOffset(itup, n)` — store `n | GIN_ITUP_COMPRESSED` as the t_tid
/// block number.
#[inline]
fn gin_set_posting_offset(tup: &mut [u8], n: u32) {
    let mut tid = read_item_pointer(tup);
    tid.ip_blkid.set_block_number(n | GIN_ITUP_COMPRESSED);
    write_item_pointer(tup, &tid);
}

/// `GinItupIsCompressed(itup)`.
#[inline]
fn gin_itup_is_compressed(tup: &[u8]) -> bool {
    gdp::GinItupIsCompressed(&header_of(tup))
}

/// `GinGetPosting(itup)` — the byte offset of the posting area within the tuple.
#[inline]
fn gin_get_posting_data_offset(tup: &[u8]) -> usize {
    gdp::GinGetPosting(&header_of(tup))
}

/// `GinGetDownlink(itup)` — the child block number in a non-leaf entry tuple.
#[inline]
fn gin_get_downlink(tup: &[u8]) -> BlockNumber {
    gdp::GinGetDownlink(&header_of(tup))
}

/// `GinSetDownlink(itup, blkno)` — set `t_tid = (blkno, InvalidOffsetNumber)`.
#[inline]
fn gin_set_downlink(tup: &mut [u8], blkno: BlockNumber) {
    let tid = ItemPointerData::new(blkno, InvalidOffsetNumber);
    write_item_pointer(tup, &tid);
}

/// `GinCategoryOffset(itup, ginstate)`.
#[inline]
fn gin_category_offset(tup: &[u8], one_col: bool) -> usize {
    gdp::GinCategoryOffset(&header_of(tup), one_col)
}

/// `GinSetNullCategory(itup, ginstate, c)`.
#[inline]
fn gin_set_null_category(tup: &mut [u8], one_col: bool, c: GinNullCategory) {
    let off = gin_category_offset(tup, one_col);
    tup[off] = c as u8;
}

// ===========================================================================
// GIN page-flag predicates over a page byte image (gindatapage substrate).
// ===========================================================================

/// `GinPageIsLeaf(page)`.
#[inline]
fn page_is_leaf(page: &[u8]) -> bool {
    gdp::GinPageIsLeaf(page)
}

/// `GinPageIsData(page)`.
#[inline]
fn page_is_data(page: &[u8]) -> bool {
    gdp::GinPageIsData(page)
}

/// `GinPageRightMost(page)`.
#[inline]
fn page_right_most(page: &[u8]) -> bool {
    gdp::GinPageRightMost(page)
}

/// `GinPageGetOpaque(page)->flags`.
#[inline]
fn page_get_flags(page: &[u8]) -> u16 {
    gdp::gin_page_get_flags(page)
}

// ===========================================================================
// bufmgr page access helpers.
// ===========================================================================

/// `BufferGetPage(buf)` copied out as an owned image (for the read-only entry
/// callbacks, which only need the page bytes). The owner holds the content lock
/// across the closure; the copy is a faithful snapshot for the duration of the
/// callback (the C reads the live `Page`, but the callbacks never mutate while
/// reading).
fn page_bytes(buffer: Buffer) -> PgResult<Vec<u8>> {
    let mut out = Vec::new();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        out = page.to_vec();
        Ok(())
    })?;
    Ok(out)
}

// ===========================================================================
// ginstate helpers — the entry-tree btree carries `GinState` in `btree.ginstate`.
// ===========================================================================

/// `btree->ginstate` — the entry-tree callbacks always run with a valid
/// `GinState` (set by `ginPrepareEntryScan`); mirror the C unconditional deref.
#[inline]
fn ginstate<'a, 'mcx>(btree: &'a GinBtreeData<'mcx>) -> &'a GinState<'mcx> {
    btree
        .ginstate
        .as_ref()
        .expect("entry-tree GinBtreeData has a ginstate (set by ginPrepareEntryScan)")
}

/// Deform a GIN entry tuple into `(attnum, key, category)` for comparison
/// (`gintuple_get_attrnum` + `gintuple_get_key`, ginutil.c).
fn deform_tuple<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    itup: &[u8],
) -> PgResult<(OffsetNumber, Datum<'mcx>, GinNullCategory)> {
    let attnum = ginutil::gintuple_get_attrnum(ginstate, itup, mcx)?;
    let (key, category) = ginutil::gintuple_get_key(ginstate, itup, mcx)?;
    Ok((attnum, key, category))
}

// ===========================================================================
// GinFormTuple (ginentrypage.c:43)
// ===========================================================================

/// `GinFormTuple(ginstate, attnum, key, category, data, dataSize, nipd,
/// errorTooBig)` (ginentrypage.c:43): form a leaf-level entry tuple containing a
/// posting list of `nipd` items.
///
/// `index_form_tuple` against the GIN per-column tuple descriptor is the catalog
/// substrate ([`ginutil`]'s indextuple owner); the GIN
/// post-processing — the posting offset, the resize, the category byte — is done
/// here on the formed tuple bytes. Returns the on-disk tuple image, or `None`
/// when it is too big and `errorTooBig` is false (the C `NULL` return).
pub fn GinFormTuple<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    attnum: OffsetNumber,
    key: Datum<'mcx>,
    category: GinNullCategory,
    data: Option<&[u8]>,
    dataSize: usize,
    nipd: i32,
    errorTooBig: bool,
) -> PgResult<Option<Vec<u8>>> {
    // Build the basic tuple: optional column number, plus key datum.
    let (datums, isnull): (Vec<Datum<'mcx>>, Vec<bool>) = if ginstate.oneCol {
        (
            alloc::vec![key],
            alloc::vec![category != GIN_CAT_NORM_KEY],
        )
    } else {
        (
            alloc::vec![Datum::from_u16(attnum), key],
            alloc::vec![false, category != GIN_CAT_NORM_KEY],
        )
    };

    let tupdesc = ginstate
        .tupdesc
        .get((attnum - 1) as usize)
        .and_then(|d| d.as_ref())
        .expect("GinFormTuple: ginstate has a tuple descriptor for the attribute");

    let formed: FormedIndexTuple<'mcx> = index_form_tuple(mcx, tupdesc, &datums, &isnull)?;
    let mut itup: Vec<u8> = formed.on_disk_image(mcx)?.to_vec();

    // Determine and store offset to the posting list (room for category byte).
    let mut newsize = index_tuple_size(&itup) as u32;

    if index_tuple_has_nulls(&itup) {
        debug_assert!(category != GIN_CAT_NORM_KEY);
        let minsize = gin_category_offset(&itup, ginstate.oneCol) as u32 + 1;
        newsize = newsize.max(minsize);
    }

    newsize = shortalign(newsize as usize) as u32;

    gin_set_posting_offset(&mut itup, newsize);
    gin_set_n_posting(&mut itup, nipd as OffsetNumber);

    // Add space for the posting list, check the size limit.
    newsize += dataSize as u32;
    newsize = maxalign(newsize as usize) as u32;

    if newsize as usize > GinMaxItemSize {
        if errorTooBig {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
                .errmsg(format!(
                    "index row size {} exceeds maximum {} for index with OID {}",
                    newsize, GinMaxItemSize, ginstate.index,
                ))
                .into_error());
        }
        return Ok(None);
    }

    // Resize the tuple if needed (C: repalloc + the implicit zero of new space).
    if newsize as usize != index_tuple_size(&itup) {
        itup.resize(newsize as usize, 0);
        // set new size in tuple header
        let info = (read_t_info(&itup) & !INDEX_SIZE_MASK) | (newsize as u16 & INDEX_SIZE_MASK);
        write_t_info(&mut itup, info);
    }

    // Copy in the posting list, if provided.
    if let Some(data) = data {
        let ptr = gin_get_posting_data_offset(&itup);
        itup[ptr..ptr + dataSize].copy_from_slice(&data[..dataSize]);
    }

    // Insert category byte, if needed.
    if category != GIN_CAT_NORM_KEY {
        debug_assert!(index_tuple_has_nulls(&itup));
        gin_set_null_category(&mut itup, ginstate.oneCol, category);
    }

    Ok(Some(itup))
}

// ===========================================================================
// ginReadTuple (ginentrypage.c:161)
// ===========================================================================

/// `ginReadTuple(ginstate, attnum, itup, &nitems)`: decode the item pointers
/// stored in a leaf entry tuple. Returns the decoded item pointers (the count is
/// the `Vec`'s length).
pub fn ginReadTuple(itup: &[u8]) -> PgResult<Vec<ItemPointerData>> {
    let nipd = gin_get_n_posting(itup) as i32;
    let ptr = gin_get_posting_data_offset(itup);

    let ipd = if gin_itup_is_compressed(itup) {
        if nipd > 0 {
            let mut ndecoded: i32 = 0;
            let decoded = gin_posting_list_decode(&itup[ptr..], &mut ndecoded);
            if nipd != ndecoded {
                return Err(ereport(ERROR)
                    .errmsg(format!(
                        "number of items mismatch in GIN entry tuple, {} in tuple header, {} decoded",
                        nipd, ndecoded
                    ))
                    .into_error());
            }
            decoded
        } else {
            Vec::new()
        }
    } else {
        let mut ipd: Vec<ItemPointerData> = Vec::with_capacity(nipd as usize);
        for i in 0..nipd as usize {
            let o = ptr + i * SIZE_OF_ITEM_POINTER;
            ipd.push(read_item_pointer(&itup[o..]));
        }
        ipd
    };

    Ok(ipd)
}

/// `ginPostingListDecode(plist, &ndecoded)` — the posting-list codec owned by the
/// audited [`core_probe`] lane. Decode a compressed posting
/// list image into item pointers.
fn gin_posting_list_decode(plist: &[u8], ndecoded: &mut i32) -> Vec<ItemPointerData> {
    core_probe::ginpostinglist::ginPostingListDecode(plist, Some(ndecoded))
}

// ===========================================================================
// GinFormInteriorTuple (ginentrypage.c:200)
// ===========================================================================

/// `GinFormInteriorTuple(itup, page, childblk)`: form a non-leaf entry tuple by
/// copying the key data of `itup`, inserting `childblk` as the downlink.
fn GinFormInteriorTuple(itup: &[u8], page: &[u8], childblk: BlockNumber) -> Vec<u8> {
    let mut nitup = if page_is_leaf(page) && !gin_is_posting_tree(itup) {
        // Tuple contains a posting list, copy stuff before that.
        let mut origsize = gin_get_posting_data_offset(itup);
        origsize = maxalign(origsize);
        let mut nitup = itup[..origsize].to_vec();
        // fix the size header field
        let info = (read_t_info(&nitup) & !INDEX_SIZE_MASK) | (origsize as u16 & INDEX_SIZE_MASK);
        write_t_info(&mut nitup, info);
        nitup
    } else {
        // Copy the tuple as-is.
        itup[..index_tuple_size(itup)].to_vec()
    };

    gin_set_downlink(&mut nitup, childblk);
    nitup
}

// ===========================================================================
// getRightMostTuple (ginentrypage.c:234)
// ===========================================================================

/// `getRightMostTuple(page)`: the rightmost (highest-offset) tuple bytes on an
/// entry page.
fn getRightMostTuple(page: &[u8]) -> PgResult<Vec<u8>> {
    let pr = PageRef::new(page)?;
    let maxoff = PageGetMaxOffsetNumber(&pr);
    let iid = PageGetItemId(&pr, maxoff)?;
    Ok(PageGetItem(&pr, &iid)?.to_vec())
}

// ===========================================================================
// entryIsMoveRight (ginentrypage.c:242)
// ===========================================================================

/// `entryIsMoveRight(btree, page)`: should the descent follow the right link?
fn entryIsMoveRight<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    buffer: Buffer,
) -> PgResult<bool> {
    let page = page_bytes(buffer)?;
    if page_right_most(&page) {
        return Ok(false);
    }

    let itup = getRightMostTuple(&page)?;
    let mcx = btree_mcx(btree);
    let e_attnum = btree.entryAttnum;
    let e_key = btree.entryKey.clone();
    let e_cat = btree.entryCategory;
    let gst = ginstate(btree);
    let (attnum, key, category) = deform_tuple(gst, mcx, &itup)?;

    let cmp = ginutil::ginCompareAttEntries(
        gst, e_attnum, e_key, e_cat, attnum, key, category,
    )?;
    Ok(cmp > 0)
}

// ===========================================================================
// entryLocateEntry (ginentrypage.c:269)
// ===========================================================================

/// `entryLocateEntry(btree, stack)`: binary-search a non-leaf entry page; sets
/// `stack.off`, returns the child block number.
fn entryLocateEntry<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    stack: &mut GinBtreeStack,
) -> PgResult<BlockNumber> {
    let page = page_bytes(stack.buffer)?;
    debug_assert!(!page_is_leaf(&page));
    debug_assert!(!page_is_data(&page));

    let mcx = btree_mcx(btree);
    let e_attnum = btree.entryAttnum;
    let e_key = btree.entryKey.clone();
    let e_cat = btree.entryCategory;
    let pr = PageRef::new(&page)?;

    if btree.fullScan {
        stack.off = FirstOffsetNumber;
        stack.predictNumber *= PageGetMaxOffsetNumber(&pr) as u32;
        return Ok(entryGetLeftMostPage(&page)?);
    }

    let mut low = FirstOffsetNumber;
    let maxoff = PageGetMaxOffsetNumber(&pr);
    let mut high = maxoff;
    debug_assert!(high >= low);

    high += 1;

    let mut found_itup: Option<Vec<u8>> = None;
    while high > low {
        let mid = low + (high - low) / 2;

        let result;
        let mut this_itup: Option<Vec<u8>> = None;
        if mid == maxoff && page_right_most(&page) {
            // Right infinity
            result = -1;
        } else {
            let itup = get_item(&pr, mid)?;
            let gst = ginstate(btree);
            let (attnum, key, category) = deform_tuple(gst, mcx, &itup)?;
            result = ginutil::ginCompareAttEntries(
                gst, e_attnum, e_key.clone(), e_cat, attnum, key, category,
            )?;
            this_itup = Some(itup);
        }

        if result == 0 {
            // Found
            stack.off = mid;
            let itup = this_itup.unwrap();
            let downlink = gin_get_downlink(&itup);
            debug_assert!(downlink != GIN_ROOT_BLKNO);
            return Ok(downlink);
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
            found_itup = this_itup;
        }
    }

    debug_assert!(high >= FirstOffsetNumber && high <= maxoff);

    stack.off = high;
    let itup = match found_itup {
        Some(t) => t,
        None => get_item(&pr, high)?,
    };
    let downlink = gin_get_downlink(&itup);
    debug_assert!(downlink != GIN_ROOT_BLKNO);
    Ok(downlink)
}

// ===========================================================================
// entryLocateLeafEntry (ginentrypage.c:345)
// ===========================================================================

/// `entryLocateLeafEntry(btree, stack)`: binary-search a leaf entry page; sets
/// `stack.off`, returns whether the search value is present.
fn entryLocateLeafEntry<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    stack: &mut GinBtreeStack,
) -> PgResult<bool> {
    let page = page_bytes(stack.buffer)?;
    debug_assert!(page_is_leaf(&page));
    debug_assert!(!page_is_data(&page));

    let mcx = btree_mcx(btree);
    let e_attnum = btree.entryAttnum;
    let e_key = btree.entryKey.clone();
    let e_cat = btree.entryCategory;
    let pr = PageRef::new(&page)?;

    if btree.fullScan {
        stack.off = FirstOffsetNumber;
        return Ok(true);
    }

    let mut low = FirstOffsetNumber;
    let mut high = PageGetMaxOffsetNumber(&pr);

    if high < low {
        stack.off = FirstOffsetNumber;
        return Ok(false);
    }

    high += 1;

    while high > low {
        let mid = low + (high - low) / 2;
        let itup = get_item(&pr, mid)?;
        let gst = ginstate(btree);
        let (attnum, key, category) = deform_tuple(gst, mcx, &itup)?;
        let result = ginutil::ginCompareAttEntries(
            gst, e_attnum, e_key.clone(), e_cat, attnum, key, category,
        )?;
        if result == 0 {
            // Found
            stack.off = mid;
            return Ok(true);
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }

    stack.off = high;
    Ok(false)
}

// ===========================================================================
// entryFindChildPtr (ginentrypage.c:404)
// ===========================================================================

/// `entryFindChildPtr(btree, page, blkno, storedOff)`: find the offset of the
/// downlink to `blkno`.
fn entryFindChildPtr<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    buffer: Buffer,
    blkno: BlockNumber,
    storedOff: OffsetNumber,
) -> PgResult<OffsetNumber> {
    let page = page_bytes(buffer)?;
    let pr = PageRef::new(&page)?;
    let mut maxoff = PageGetMaxOffsetNumber(&pr);
    debug_assert!(!page_is_leaf(&page));
    debug_assert!(!page_is_data(&page));

    // if page isn't changed, we return storedOff
    if storedOff >= FirstOffsetNumber && storedOff <= maxoff {
        let itup = get_item(&pr, storedOff)?;
        if gin_get_downlink(&itup) == blkno {
            return Ok(storedOff);
        }

        // we hope that the needed pointer goes to the right; true if there
        // wasn't a deletion
        let mut i = storedOff + 1;
        while i <= maxoff {
            let itup = get_item(&pr, i)?;
            if gin_get_downlink(&itup) == blkno {
                return Ok(i);
            }
            i += 1;
        }
        maxoff = storedOff - 1;
    }

    // last chance
    let mut i = FirstOffsetNumber;
    while i <= maxoff {
        let itup = get_item(&pr, i)?;
        if gin_get_downlink(&itup) == blkno {
            return Ok(i);
        }
        i += 1;
    }

    Ok(InvalidOffsetNumber)
}

// ===========================================================================
// entryGetLeftMostPage (ginentrypage.c:445)
// ===========================================================================

/// `entryGetLeftMostPage(btree, page)`: block number of the leftmost child.
fn entryGetLeftMostPage(page: &[u8]) -> PgResult<BlockNumber> {
    debug_assert!(!page_is_leaf(page));
    debug_assert!(!page_is_data(page));
    let pr = PageRef::new(page)?;
    debug_assert!(PageGetMaxOffsetNumber(&pr) >= FirstOffsetNumber);

    let itup = get_item(&pr, FirstOffsetNumber)?;
    Ok(gin_get_downlink(&itup))
}

/// `getLeftMostChild` vtable adapter (takes the buffer, reads its page).
fn entryGetLeftMostChild<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    buffer: Buffer,
) -> PgResult<BlockNumber> {
    let page = page_bytes(buffer)?;
    entryGetLeftMostPage(&page)
}

// ===========================================================================
// entryIsEnoughSpace (ginentrypage.c:458)
// ===========================================================================

/// `entryIsEnoughSpace(btree, buf, off, insertData)`: does the new entry tuple
/// fit on the page (accounting for the released tuple on a delete)?
fn entryIsEnoughSpace(
    page: &[u8],
    off: OffsetNumber,
    insertData: &GinBtreeEntryInsertData<'_>,
) -> PgResult<bool> {
    debug_assert!(!insertData.entry.is_empty());
    debug_assert!(!page_is_data(page));

    let pr = PageRef::new(page)?;

    let mut releasedsz = 0usize;
    if insertData.isDelete {
        let itup = get_item(&pr, off)?;
        releasedsz = maxalign(index_tuple_size(&itup)) + SIZE_OF_ITEM_ID;
    }

    let addedsz = maxalign(insertData.entry.len()) + SIZE_OF_ITEM_ID;

    Ok(PageGetFreeSpace(&pr) + releasedsz >= addedsz)
}

// ===========================================================================
// entryPreparePage (ginentrypage.c:489)
// ===========================================================================

/// `entryPreparePage(btree, page, off, insertData, updateblkno)`: delete the old
/// tuple on a leaf update, and relink an existing downlink on a child split.
fn entryPreparePage(
    page: &mut [u8],
    off: OffsetNumber,
    insertData: &GinBtreeEntryInsertData<'_>,
    updateblkno: BlockNumber,
) -> PgResult<()> {
    debug_assert!(!insertData.entry.is_empty());
    debug_assert!(!page_is_data(page));

    if insertData.isDelete {
        debug_assert!(page_is_leaf(page));
        let mut pm = PageMut::new(page)?;
        PageIndexTupleDelete(&mut pm, off)?;
    }

    if !page_is_leaf(page) && updateblkno != InvalidBlockNumber {
        // GinSetDownlink on the existing tuple at `off`.
        let (item_off, item_len) = {
            let pr = PageRef::new(page)?;
            let iid = PageGetItemId(&pr, off)?;
            (iid.lp_off() as usize, iid.lp_len() as usize)
        };
        let tid = ItemPointerData::new(updateblkno, InvalidOffsetNumber);
        write_item_pointer(&mut page[item_off..item_off + item_len], &tid);
    }

    Ok(())
}

// ===========================================================================
// entryBeginPlaceToPage (ginentrypage.c:526)
// ===========================================================================

/// `entryBeginPlaceToPage(...)`: prepare to insert an entry tuple, computing a
/// split image when it doesn't fit. Returns the result code; on `Split` the two
/// temp page images are returned via the [`BeginPlaceToPageResult`].
fn entryBeginPlaceToPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    _mcx: Mcx<'mcx>,
    buf: Buffer,
    stack: &mut GinBtreeStack,
    insertPayload: &GinInsertPayload<'mcx>,
    updateblkno: BlockNumber,
) -> PgResult<BeginPlaceToPageResult> {
    let insertData = expect_entry(insertPayload);
    let off = stack.off;

    let page = page_bytes(buf)?;

    // If it doesn't fit, deal with split case.
    if !entryIsEnoughSpace(&page, off, insertData)? {
        let (lp, rp) = entrySplitPage(btree, buf, stack, insertData, updateblkno)?;
        return Ok(BeginPlaceToPageResult {
            rc: GinPlaceToPageRC::GPTP_SPLIT,
            ptp_workspace: PtpWorkspace::default(),
            newlpage: Some(lp),
            newrpage: Some(rp),
        });
    }

    // Else, we're ready to proceed with insertion.
    Ok(BeginPlaceToPageResult {
        rc: GinPlaceToPageRC::GPTP_INSERT,
        ptp_workspace: PtpWorkspace::default(),
        newlpage: None,
        newrpage: None,
    })
}

// ===========================================================================
// entryExecPlaceToPage (ginentrypage.c:553)
// ===========================================================================

/// `entryExecPlaceToPage(...)`: place the entry tuple onto the page (in the
/// critical section). Registers the slot-0 buffer + the `ginxlogInsertEntry`
/// byte-data through the xloginsert seams when WAL is needed (the spine has
/// already begun the WAL record); no XLog API is called outside the seams.
fn entryExecPlaceToPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    _mcx: Mcx<'mcx>,
    buf: Buffer,
    stack: &mut GinBtreeStack,
    insertPayload: &GinInsertPayload<'mcx>,
    updateblkno: BlockNumber,
    ws: &mut PtpWorkspace,
) -> PgResult<()> {
    let insertData = expect_entry(insertPayload);
    let off = stack.off;
    let index_oid = btree.index;

    // entryPreparePage(btree, page, off, insertData, updateblkno) then PageAddItem.
    let mut placed = InvalidOffsetNumber;
    bufmgr::with_buffer_page::call(buf, &mut |page: &mut [u8]| {
        entryPreparePage(page, off, insertData, updateblkno)?;
        let mut pm = PageMut::new(page)?;
        placed = PageAddItemExtended(&mut pm, &insertData.entry, off, 0)?;
        Ok(())
    })?;
    if placed != off {
        return Err(ereport(ERROR)
            .errmsg(format!(
                "failed to add item to index page in index with OID {index_oid}"
            ))
            .into_error());
    }

    bufmgr::mark_buffer_dirty::call(buf);

    if ws.want_wal {
        // ginxlogInsertEntry { OffsetNumber offset; bool isDelete; IndexTupleData tuple; }
        // (ginxlog.h:57-62). Field layout: offset@[0..2] (OffsetNumber, 2 bytes),
        // isDelete@[2] (bool, 1 byte), pad@[3], tuple@4 == offsetof(.., tuple).
        // The redo reader reads data->offset then data->isDelete (ginxlog.c:73-86),
        // so the header bytes must match this order exactly.
        let mut data = [0u8; 4];
        data[0..2].copy_from_slice(&off.to_ne_bytes());
        data[2] = insertData.isDelete as u8;

        xlog_register_buffer(0, buf, REGBUF_STANDARD)?;
        xlog_register_buf_data(0, &data)?;
        xlog_register_buf_data(0, &insertData.entry)?;
    }

    Ok(())
}

// ===========================================================================
// entrySplitPage (ginentrypage.c:601)
// ===========================================================================

/// `entrySplitPage(btree, origbuf, stack, insertData, updateblkno, &newlpage,
/// &newrpage)`: split an entry page and insert the new tuple, returning two temp
/// page images. The original buffer is left untouched.
fn entrySplitPage<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    origbuf: Buffer,
    stack: &GinBtreeStack,
    insertData: &GinBtreeEntryInsertData<'mcx>,
    updateblkno: BlockNumber,
) -> PgResult<(Vec<u8>, Vec<u8>)> {
    let off = stack.off;
    let index_oid = btree.index;

    // lpage/rpage are temp page copies of the original (PageGetTempPageCopy).
    let mut lpage = page_bytes(origbuf)?;
    let page_size = lpage.len();
    let mut rpage = alloc::vec![0u8; page_size];

    // entryPreparePage on the left temp page (deletes old tuple / relinks).
    entryPreparePage(&mut lpage, off, insertData, updateblkno)?;

    // Append all existing tuples plus the new one into a workspace.
    let maxoff = {
        let pr = PageRef::new(&lpage)?;
        PageGetMaxOffsetNumber(&pr)
    };
    let mut tupstore: Vec<u8> = Vec::with_capacity(2 * BLCKSZ);
    let mut totalsize = 0usize;

    {
        let pr = PageRef::new(&lpage)?;
        let mut i = FirstOffsetNumber;
        while i <= maxoff {
            if i == off {
                let size = maxalign(insertData.entry.len());
                push_padded(&mut tupstore, &insertData.entry, size);
                totalsize += size + SIZE_OF_ITEM_ID;
            }
            let itup = get_item(&pr, i)?;
            let size = maxalign(itup.len());
            push_padded(&mut tupstore, &itup, size);
            totalsize += size + SIZE_OF_ITEM_ID;
            i += 1;
        }
    }

    if off == maxoff + 1 {
        let size = maxalign(insertData.entry.len());
        push_padded(&mut tupstore, &insertData.entry, size);
        totalsize += size + SIZE_OF_ITEM_ID;
    }

    // Initialize the left and right pages, copy all tuples back.
    let lflags = page_get_flags(&lpage) as u32;
    ginutil::GinInitPage(&mut rpage, lflags, page_size)?;
    ginutil::GinInitPage(&mut lpage, lflags, page_size)?;

    let new_maxoff = maxoff + 1;
    let mut lsize = 0usize;
    let mut separator = InvalidOffsetNumber;
    let mut to_right = false;

    let mut ptr = 0usize;
    let mut i = FirstOffsetNumber;
    while i <= new_maxoff {
        // tuple at `ptr` (its real size from t_info).
        let itup_size = index_tuple_size(&tupstore[ptr..]);
        let itup = tupstore[ptr..ptr + itup_size].to_vec();

        // Decide where to split (equalize data size, not tuple count).
        if lsize > totalsize / 2 {
            if separator == InvalidOffsetNumber {
                separator = i - 1;
            }
            to_right = true;
        } else {
            lsize += maxalign(itup_size) + SIZE_OF_ITEM_ID;
        }

        let target: &mut Vec<u8> = if to_right { &mut rpage } else { &mut lpage };
        let placed = {
            let mut pm = PageMut::new(target)?;
            PageAddItemExtended(&mut pm, &itup, InvalidOffsetNumber, 0)?
        };
        if placed == InvalidOffsetNumber {
            return Err(ereport(ERROR)
                .errmsg(format!(
                    "failed to add item to index page in index with OID {index_oid}"
                ))
                .into_error());
        }

        ptr += maxalign(itup_size);
        i += 1;
    }

    let _ = separator;
    Ok((lpage, rpage))
}

// ===========================================================================
// entryPrepareDownlink (ginentrypage.c:701)
// ===========================================================================

/// `entryPrepareDownlink(btree, lbuf)`: build the insertion payload for the
/// downlink of `lbuf` (an interior tuple from its rightmost key).
fn entryPrepareDownlink<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    _mcx: Mcx<'mcx>,
    lbuf: Buffer,
) -> PgResult<GinInsertPayload<'mcx>> {
    let lblkno = bufmgr::buffer_get_block_number::call(lbuf);

    let lpage = page_bytes(lbuf)?;
    let itup = getRightMostTuple(&lpage)?;

    let entry = GinFormInteriorTuple(&itup, &lpage, lblkno);
    Ok(GinInsertPayload::Entry(GinBtreeEntryInsertData {
        entry,
        isDelete: false,
        _marker: core::marker::PhantomData,
    }))
}

// ===========================================================================
// ginEntryFillRoot (ginentrypage.c:722)
// ===========================================================================

/// `ginEntryFillRoot(btree, root, lblkno, lpage, rblkno, rpage)`: fill a new
/// entry root page with the interior downlinks of the two children's rightmost
/// tuples.
fn ginEntryFillRoot<'mcx>(
    _btree: &mut GinBtreeData<'mcx>,
    root: &mut [u8],
    lblkno: BlockNumber,
    lpage: &[u8],
    rblkno: BlockNumber,
    rpage: &[u8],
) -> PgResult<()> {
    let litup = GinFormInteriorTuple(&getRightMostTuple(lpage)?, lpage, lblkno);
    add_root_item(root, &litup)?;

    let ritup = GinFormInteriorTuple(&getRightMostTuple(rpage)?, rpage, rblkno);
    add_root_item(root, &ritup)?;

    Ok(())
}

fn add_root_item(root: &mut [u8], itup: &[u8]) -> PgResult<()> {
    let mut pm = PageMut::new(root)?;
    let placed = PageAddItemExtended(&mut pm, itup, InvalidOffsetNumber, 0)?;
    if placed == InvalidOffsetNumber {
        return Err(ereport(ERROR)
            .errmsg("failed to add item to index root page")
            .into_error());
    }
    Ok(())
}

// ===========================================================================
// ginPrepareEntryScan (ginentrypage.c:746)
// ===========================================================================

/// `ginPrepareEntryScan(btree, attnum, key, category, ginstate)`: set up a
/// [`GinBtreeData`] for entry-page access, installing the entry-tree method
/// table.
pub fn ginPrepareEntryScan<'mcx>(
    btree: &mut GinBtreeData<'mcx>,
    mcx: Mcx<'mcx>,
    attnum: OffsetNumber,
    key: Datum<'mcx>,
    category: GinNullCategory,
    ginstate: GinState<'mcx>,
) {
    *btree = GinBtreeData::default();

    btree.findChildPage = Some(entryLocateEntry);
    btree.findItem = Some(entryLocateLeafEntry);
    btree.findChildPtr = Some(entryFindChildPtr);
    btree.getLeftMostChild = Some(entryGetLeftMostChild);
    btree.isMoveRight = Some(entryIsMoveRight);
    btree.beginPlaceToPage = Some(entryBeginPlaceToPage);
    btree.execPlaceToPage = Some(entryExecPlaceToPage);
    btree.prepareDownlink = Some(entryPrepareDownlink);
    btree.fillRoot = Some(ginEntryFillRoot);

    btree.isData = false;
    btree.rootBlkno = GIN_ROOT_BLKNO;
    btree.index = ginstate.index;
    btree.fullScan = false;
    btree.isBuild = false;

    btree.entryAttnum = attnum;
    btree.entryKey = key;
    btree.entryCategory = category;
    btree.ginstate = Some(ginstate);
    btree.tmpCtx = Some(mcx);
}

// ===========================================================================
// xloginsert seam pass-throughs.
// ===========================================================================

/// `REGBUF_STANDARD` (xloginsert.h).
const REGBUF_STANDARD: u8 = 0x04;

/// `XLogRegisterBuffer(block_id, buffer, flags)`.
fn xlog_register_buffer(block_id: u8, buffer: Buffer, flags: u8) -> PgResult<()> {
    xloginsert_seams::xlog_register_buffer::call(block_id, buffer, flags)
}

/// `XLogRegisterBufData(block_id, data, len)`.
fn xlog_register_buf_data(block_id: u8, data: &[u8]) -> PgResult<()> {
    xloginsert_seams::xlog_register_buf_data::call(block_id, data)
}

// ===========================================================================
// Local helpers.
// ===========================================================================

/// The memory context the entry-tree read callbacks deform key datums into,
/// stashed in `btree.tmpCtx` by [`ginPrepareEntryScan`] (the C callbacks use
/// `CurrentMemoryContext`; the spine's read-callback dispatch threads no `Mcx`).
#[inline]
fn btree_mcx<'mcx>(btree: &GinBtreeData<'mcx>) -> Mcx<'mcx> {
    btree
        .tmpCtx
        .expect("entry-tree GinBtreeData has a tmpCtx (set by ginPrepareEntryScan)")
}

/// `IndexTuple` from the entry-tree insert payload (`void *insertdata` is always
/// a `GinBtreeEntryInsertData *` for the entry tree).
#[inline]
fn expect_entry<'a, 'mcx>(
    payload: &'a GinInsertPayload<'mcx>,
) -> &'a GinBtreeEntryInsertData<'mcx> {
    match payload {
        GinInsertPayload::Entry(d) => d,
        _ => panic!("entry-tree page callback received a non-entry GinInsertPayload"),
    }
}

/// Read the IndexTuple at offset `off` (1-based) on `page` as owned bytes.
fn get_item(pr: &PageRef<'_>, off: OffsetNumber) -> PgResult<Vec<u8>> {
    let iid = PageGetItemId(pr, off)?;
    Ok(PageGetItem(pr, &iid)?.to_vec())
}

/// Append `tuple` to `dst`, then zero-pad up to `padded_len`.
fn push_padded(dst: &mut Vec<u8>, tuple: &[u8], padded_len: usize) {
    let start = dst.len();
    dst.extend_from_slice(tuple);
    dst.resize(start + padded_len, 0);
}
