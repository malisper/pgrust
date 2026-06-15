#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]

//! Port of `src/backend/access/gin/ginget.c` (PostgreSQL 18.3) — the GIN index
//! `amgetbitmap` read path: fetch heap tuples that match a GIN scan into a
//! [`types_tidbitmap::TIDBitmap`].
//!
//! Every ginget.c function is ported 1:1. The scan key-state machine
//! (`startScan`/`startScanKey`/`startScanEntry`, the per-entry/per-key/AND-of-keys
//! item-pointer stream merge `entryGetItem`/`keyGetItem`/`scanGetItem`, the
//! entry-tree collect `collectMatchBitmap`/`scanPostingTree`, and the
//! pending-list scan `scanPendingInsert`/`collectMatchesForHeapRow`/
//! `matchPartialInPendingList`) is pure control flow over the
//! [`types_gin::GinScanOpaqueData`] runtime model.
//!
//! The GIN entry/posting-tree descent reuses the ported `ginbtree`
//! (`ginFindLeafPage`/`ginStepRight`/`freeGinBtreeStack`), `gindatapage`
//! (`ginScanBeginPostingTree`/`GinDataLeafPageGetItems`/`GinDataLeafPageGetItemsToTbm`
//! + the `ginpage` flag/right-bound helpers), `ginentrypage`
//! (`ginReadTuple`/`ginPrepareEntryScan`), and `ginutil`
//! (`gintuple_get_attrnum`/`gintuple_get_key`/`ginCompareEntries`) crates. The
//! consistent-function dispatch reuses the audited `gin-core-probe` `ginlogic`
//! (`callBoolConsistentFn`/`callTriConsistentFn`). `TIDBitmap` operations and
//! the GIN private-iteration bridge come from the `backend-nodes-core`
//! `tidbitmap` owner. Buffers cross by id through the `bufmgr` seams (pages are
//! read into owned `Vec<u8>` images, the same pattern as `ginbtree`).
//!
//! The opclass `comparePartialFn` fmgr dispatch crosses through the new
//! `gin_compare_partial` seam (declared in `ginutil-seams`, the GIN substrate
//! seam crate; same unported owner as `gin_extract_query`/`gin_extract_value` —
//! the fmgr GIN dispatcher — so it loud-panics until that lands).

extern crate alloc;

use alloc::format;
use alloc::vec;
use alloc::vec::Vec;
use std::rc::Rc;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_page::{PageGetItem, PageGetItemId, PageGetMaxOffsetNumber, PageRef};
use backend_utils_error::{ereport, PgResult};
use types_error::error::{ERROR, ERRCODE_INTERNAL_ERROR};

use mcx::Mcx;
use types_core::primitive::{BlockNumber, OffsetNumber};
use types_core::InvalidBlockNumber;
use types_rel::Relation;
use types_storage::storage::{Buffer, InvalidBuffer};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{BlockIdData, IndexTupleData, ItemPointerData};

use types_gin::{
    GinNullCategory, GinScanEntryData, GinScanOpaqueData, GinState,
    GIN_CAT_NORM_KEY, GIN_CAT_NULL_ITEM, GIN_CAT_EMPTY_QUERY, GIN_SEARCH_MODE_ALL,
    GIN_DELETED, GIN_LIST_FULLROW,
};
use types_gin::{GinBtreeData, GinBtreeStack};

use types_tableam::relscan::IndexScanDescData;

use types_tidbitmap::TIDBitmap;

use backend_access_gin_ginbtree::{freeGinBtreeStack, ginFindLeafPage, ginStepRight};
use backend_access_gin_gindatapage::{
    gin_data_page_get_right_bound, gin_page_get_rightlink, GinDataLeafPageGetItems,
    GinDataLeafPageGetItemsToTbm, ginScanBeginPostingTree, GinGetDownlink, GinGetNPosting,
    GinIsPostingTree,
};
use backend_access_gin_gindatapage::{gin_page_get_flags, GinPageIsLeaf, GinPageRightMost};
use backend_access_gin_core_probe::ginpostinglist::ginCompareItemPointers;
use backend_access_gin_ginentrypage::{ginPrepareEntryScan, ginReadTuple};
use backend_access_gin_ginutil::{gintuple_get_attrnum, gintuple_get_key, ginCompareEntries};
use backend_access_gin_core_probe::ginlogic::{callBoolConsistentFn, callTriConsistentFn};

use backend_access_gin_ginutil_seams as sx;
use backend_nodes_core::tidbitmap as tbm;
use backend_storage_lmgr_predicate_seams as predicate;
use backend_tcop_postgres_seams as postgres;
use backend_utils_activity_pgstat_seams::pgstat_count_index_scan;

#[cfg(test)]
mod tests;

// ===========================================================================
// Constants (gin_private.h / itemptr.h / gin.h / bufmgr.h / ginblock.h)
// ===========================================================================

/// `GIN_UNLOCK` == `BUFFER_LOCK_UNLOCK` (gin_private.h:49).
pub const GIN_UNLOCK: i32 = 0;
/// `GIN_SHARE` == `BUFFER_LOCK_SHARE` (gin_private.h:50).
pub const GIN_SHARE: i32 = 1;

/// `GIN_METAPAGE_BLKNO` (ginblock.h:21): the index metapage block number.
pub const GIN_METAPAGE_BLKNO: BlockNumber = 0;

/// `FirstOffsetNumber` (off.h).
pub const FirstOffsetNumber: OffsetNumber = 1;
/// `InvalidOffsetNumber` (off.h).
pub const InvalidOffsetNumber: OffsetNumber = 0;

// ===========================================================================
// init_seams — install the gingetbitmap AM callback declared in ginutil-seams.
// ===========================================================================

/// Install the GIN `amgetbitmap` callback (`gingetbitmap`) the `ginutil` handler
/// reaches by name through the unified `IndexAmRoutine`. The declaration lives in
/// `ginutil-seams` (the GIN substrate seam crate); `ginget` is the owner that
/// installs it.
pub fn init_seams() {
    sx::gingetbitmap::set(gingetbitmap);
}

// ===========================================================================
// ItemPointer helpers (itemptr.h / gin_private.h:489..510)
// ===========================================================================

/// `ItemPointerGetBlockNumber` / `GinItemPointerGetBlockNumber`.
#[inline]
fn ip_block(p: &ItemPointerData) -> BlockNumber {
    ((p.ip_blkid.bi_hi as u32) << 16) | (p.ip_blkid.bi_lo as u32)
}

/// `ItemPointerGetOffsetNumber` / `GinItemPointerGetOffsetNumber` (no validity
/// check).
#[inline]
fn ip_offset(p: &ItemPointerData) -> OffsetNumber {
    p.ip_posid
}

/// `ItemPointerSet(p, blk, off)` (itemptr.h:135).
#[inline]
fn ip_set(p: &mut ItemPointerData, blk: BlockNumber, off: OffsetNumber) {
    p.ip_blkid = BlockIdData {
        bi_hi: (blk >> 16) as u16,
        bi_lo: (blk & 0xffff) as u16,
    };
    p.ip_posid = off;
}

/// `ItemPointerSetMin(p)` (gin_private.h:498): block 0, offset 0.
#[inline]
fn ip_set_min(p: &mut ItemPointerData) {
    ip_set(p, 0, 0);
}

/// `ItemPointerSetMax(p)` (gin_private.h:502): `InvalidBlockNumber`, `0xffff`.
#[inline]
fn ip_set_max(p: &mut ItemPointerData) {
    ip_set(p, InvalidBlockNumber, 0xffff);
}

/// `ItemPointerIsMin(p)`: block 0 and offset 0.
#[inline]
fn ip_is_min(p: &ItemPointerData) -> bool {
    ip_block(p) == 0 && ip_offset(p) == 0
}

/// `ItemPointerIsValid(p)` (itemptr.h:83): a valid item pointer has a nonzero
/// offset.
#[inline]
fn ip_is_valid(p: &ItemPointerData) -> bool {
    p.ip_posid != InvalidOffsetNumber
}

/// `ItemPointerSetInvalid(p)` (itemptr.h:130).
#[inline]
fn ip_set_invalid(p: &mut ItemPointerData) {
    p.ip_blkid = BlockIdData {
        bi_hi: 0xffff,
        bi_lo: 0xffff,
    };
    p.ip_posid = InvalidOffsetNumber;
}

/// `ItemPointerIsLossyPage(p)` (gin_private.h:506): offset == `0xffff`.
#[inline]
fn ip_is_lossy_page(p: &ItemPointerData) -> bool {
    ip_offset(p) == 0xffff
}

/// `ItemPointerSetLossyPage(p, blk)` (gin_private.h:510).
#[inline]
fn ip_set_lossy_page(p: &mut ItemPointerData, blk: BlockNumber) {
    ip_set(p, blk, 0xffff);
}

/// `ItemPointerEquals(a, b)` (itemptr.c): same block and offset.
#[inline]
fn ip_equals(a: &ItemPointerData, b: &ItemPointerData) -> bool {
    ip_block(a) == ip_block(b) && ip_offset(a) == ip_offset(b)
}

/// `OffsetNumberNext(off)` (off.h).
#[inline]
fn offset_next(off: OffsetNumber) -> OffsetNumber {
    off + 1
}

/// `OffsetNumberPrev(off)` (off.h).
#[inline]
fn offset_prev(off: OffsetNumber) -> OffsetNumber {
    off - 1
}

/// `BlockNumberIsValid(blk)` (block.h).
#[inline]
fn block_is_valid(blk: BlockNumber) -> bool {
    blk != InvalidBlockNumber
}

// ===========================================================================
// Page-byte helpers (over an owned page image returned by the buffer seam).
// ===========================================================================

/// `BufferGetPage(buffer)` copied into an owned image (`with_buffer_page`).
fn read_page(buffer: Buffer) -> PgResult<Vec<u8>> {
    let mut out = Vec::new();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        out = page.to_vec();
        Ok(())
    })?;
    Ok(out)
}

/// `GinPageGetOpaque(page)->flags & GIN_DELETED` (ginblock.h).
#[inline]
fn page_is_deleted(page: &[u8]) -> bool {
    gin_page_get_flags(page) & GIN_DELETED != 0
}

/// `GinPageHasFullRow(page)` (ginblock.h:122).
#[inline]
fn page_has_full_row(page: &[u8]) -> bool {
    gin_page_get_flags(page) & GIN_LIST_FULLROW != 0
}

/// `GinPageGetOpaque(page)->rightlink` (ginblock.h).
#[inline]
fn page_rightlink(page: &[u8]) -> BlockNumber {
    gin_page_get_rightlink(page)
}

/// Read the `IndexTuple` bytes at offset `off` of a page image
/// (`PageGetItem(page, PageGetItemId(page, off))`).
fn page_get_item_bytes(page: &[u8], off: OffsetNumber) -> Vec<u8> {
    let pr = PageRef::new(page).expect("valid GIN page image");
    let iid = PageGetItemId(&pr, off).expect("valid line pointer");
    PageGetItem(&pr, &iid).expect("valid item").to_vec()
}

/// `PageGetMaxOffsetNumber(page)` over a page image.
fn page_max_offset(page: &[u8]) -> OffsetNumber {
    let pr = PageRef::new(page).expect("valid GIN page image");
    PageGetMaxOffsetNumber(&pr)
}

// ===========================================================================
// Buffer-seam thin wrappers (gin uses these names everywhere).
// ===========================================================================

fn read_buffer<'mcx>(index: &Relation<'mcx>, blkno: BlockNumber) -> PgResult<Buffer> {
    bufmgr::read_buffer::call(index, blkno)
}

fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, mode)
}

fn unlock_release_buffer(buffer: Buffer) {
    bufmgr::unlock_release_buffer::call(buffer);
}

fn release_buffer(buffer: Buffer) {
    bufmgr::release_buffer::call(buffer);
}

fn incr_buffer_ref_count(buffer: Buffer) {
    bufmgr::incr_buffer_ref_count::call(buffer);
}

fn buffer_get_block_number(buffer: Buffer) -> BlockNumber {
    bufmgr::buffer_get_block_number::call(buffer)
}

#[inline]
fn buffer_is_valid(buffer: Buffer) -> bool {
    buffer != InvalidBuffer
}

/// `PredicateLockPage(index, blkno, snapshot)`.
fn predicate_lock_page<'mcx>(
    index: &Relation<'mcx>,
    blkno: BlockNumber,
    snapshot: &Option<types_snapshot::SnapshotData>,
) -> PgResult<()> {
    predicate::predicate_lock_page::call(index.alias(), blkno, snapshot.clone().map(Rc::new))
}

/// `gin_rand()` == `pg_prng_double(&pg_global_prng_state)`.
fn gin_rand() -> f64 {
    pg_prng::global_prng(|s| s.next_f64())
}

/// `GinFuzzySearchLimit` GUC.
fn gin_fuzzy_search_limit() -> i32 {
    backend_utils_misc_guc_tables::vars::GinFuzzySearchLimit.read()
}

/// `work_mem` GUC (KB).
fn work_mem() -> i32 {
    backend_utils_init_small_seams::work_mem::call()
}

/// `dropItem(e)`: `gin_rand() > GinFuzzySearchLimit / e->predictNumberResult`.
fn drop_item(predict_number_result: u32) -> bool {
    gin_rand() > (gin_fuzzy_search_limit() as f64) / (predict_number_result as f64)
}

// ===========================================================================
// moveRightIfItNeeded (ginget.c:42)
// ===========================================================================

/// Goes to the next page if current offset is outside of bounds.
fn moveRightIfItNeeded<'mcx>(
    _btree: &GinBtreeData<'mcx>,
    index: &Relation<'mcx>,
    stack: &mut GinBtreeStack,
    snapshot: &Option<types_snapshot::SnapshotData>,
) -> PgResult<bool> {
    let page = read_page(stack.buffer)?;

    if stack.off > page_max_offset(&page) {
        // We scanned the whole page, so we should take right page.
        if GinPageRightMost(&page) {
            return Ok(false); // no more pages
        }

        stack.buffer = ginStepRight(stack.buffer, index, GIN_SHARE)?;
        stack.blkno = buffer_get_block_number(stack.buffer);
        stack.off = FirstOffsetNumber;
        predicate_lock_page(index, stack.blkno, snapshot)?;
    }

    Ok(true)
}

// ===========================================================================
// scanPostingTree (ginget.c:68)
// ===========================================================================

/// Scan all pages of a posting tree and save all its heap ItemPointers in
/// `entry.matchBitmap`.
fn scanPostingTree<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    entry: &mut GinScanEntryData<'mcx>,
    rootPostingTree: BlockNumber,
) -> PgResult<()> {
    let mut btree = GinBtreeData::default();

    // Descend to the leftmost leaf page.
    let stack = ginScanBeginPostingTree(&mut btree, mcx, index, rootPostingTree)?;
    let mut buffer = stack.buffer;

    incr_buffer_ref_count(buffer); // prevent unpin in freeGinBtreeStack
    freeGinBtreeStack(stack);

    // Loop iterates through all leaf pages of posting tree.
    loop {
        let page = read_page(buffer)?;
        if !page_is_deleted(&page) {
            let bm = entry
                .matchBitmap
                .as_mut()
                .expect("scanPostingTree: matchBitmap allocated by collectMatchBitmap");
            let n = GinDataLeafPageGetItemsToTbm(&page, bm)?;
            entry.predictNumberResult += n as u32;
        }

        if GinPageRightMost(&page) {
            break; // no more pages
        }

        buffer = ginStepRight(buffer, index, GIN_SHARE)?;
    }

    unlock_release_buffer(buffer);
    Ok(())
}

// ===========================================================================
// collectMatchBitmap (ginget.c:120)
// ===========================================================================

/// Collects TIDs into `entry.matchBitmap` for all heap tuples that match the
/// search entry. Returns `true` if done, `false` if it's necessary to restart
/// scan from scratch.
fn collectMatchBitmap<'mcx>(
    mcx: Mcx<'mcx>,
    btree: &mut GinBtreeData<'mcx>,
    index: &Relation<'mcx>,
    stack: &mut GinBtreeStack,
    ginstate: &GinState<'mcx>,
    entry: &mut GinScanEntryData<'mcx>,
    snapshot: &Option<types_snapshot::SnapshotData>,
) -> PgResult<bool> {
    // Initialize empty bitmap result.
    entry.matchBitmap = Some(tbm::tbm_create(work_mem() as usize * 1024, None));

    // Null query cannot partial-match anything.
    if entry.isPartialMatch && entry.queryCategory != GIN_CAT_NORM_KEY {
        return Ok(true);
    }

    // Locate tupdesc entry for key column (for attbyval/attlen data).
    let attnum = entry.attnum;
    // `attr = TupleDescCompactAttr(ginstate->origTupdesc, attnum - 1)` is only
    // needed for the `datumCopy(idatum, attr->attbyval, attr->attlen)` below.
    let (attr_attbyval, attr_attlen) = compact_attr(ginstate, attnum);

    // Predicate lock entry leaf page; following pages locked by moveRightIfItNeeded.
    predicate_lock_page(index, buffer_get_block_number(stack.buffer), snapshot)?;

    loop {
        // stack->off points to the interested entry, buffer is already locked.
        if !moveRightIfItNeeded(btree, index, stack, snapshot)? {
            return Ok(true);
        }

        let page = read_page(stack.buffer)?;
        let itup = page_get_item_bytes(&page, stack.off);

        // If tuple stores another attribute then stop scan.
        if gintuple_get_attrnum(ginstate, &itup, mcx)? != attnum {
            return Ok(true);
        }

        // Safe to fetch attribute value.
        let (mut idatum, icategory) = gintuple_get_key(ginstate, &itup, mcx)?;

        // Check for appropriate scan stop conditions.
        if entry.isPartialMatch {
            // In partial match, stop scan at any null (placeholders); partial
            // matches never match nulls.
            if icategory != GIN_CAT_NORM_KEY {
                return Ok(true);
            }

            // cmp == 0 => match; cmp > 0 => not match & finish; cmp < 0 =>
            // not match & continue.
            let cmp = sx::gin_compare_partial::call(
                &ginstate.comparePartialFn[(attnum - 1) as usize],
                ginstate.supportCollation[(attnum - 1) as usize],
                entry.queryKey.clone(),
                idatum.clone(),
                entry.strategy,
                entry.extra_data.as_deref(),
            )?;

            if cmp > 0 {
                return Ok(true);
            } else if cmp < 0 {
                stack.off += 1;
                continue;
            }
        } else if entry.searchMode == GIN_SEARCH_MODE_ALL {
            // In ALL mode, stop at a null-item placeholder (the last entry for a
            // given attnum). NULL_KEY and EMPTY_ITEM entries are included.
            if icategory == GIN_CAT_NULL_ITEM {
                return Ok(true);
            }
        }

        // OK, we want to return the TIDs listed in this entry.
        if GinIsPostingTree(&index_tuple_header(&itup)) {
            let rootPostingTree = GinGetDownlink(&index_tuple_header(&itup));

            // Unlock current page (not unpin) during tree scan to prevent
            // deadlock with vacuum. Save idatum to re-find our tuple.
            if icategory == GIN_CAT_NORM_KEY {
                idatum = datum_copy(&idatum, attr_attbyval, attr_attlen);
            }

            lock_buffer(stack.buffer, GIN_UNLOCK)?;

            // Acquire predicate lock on the posting tree.
            predicate_lock_page(index, rootPostingTree, snapshot)?;

            // Collect all the TIDs in this entry's posting tree.
            scanPostingTree(mcx, index, entry, rootPostingTree)?;

            // Re-lock the entry page and re-find our position.
            lock_buffer(stack.buffer, GIN_SHARE)?;
            let page = read_page(stack.buffer)?;
            if !GinPageIsLeaf(&page) {
                // Root became non-leaf while unlocked. Start again.
                return Ok(false);
            }

            // Search forward to re-find idatum.
            loop {
                if !moveRightIfItNeeded(btree, index, stack, snapshot)? {
                    return Err(ereport(ERROR)
                        .errcode(ERRCODE_INTERNAL_ERROR)
                        .errmsg(format!(
                            "failed to re-find tuple within index \"{}\"",
                            relation_get_relation_name(mcx, index)
                        ))
                        .into_error());
                }

                let page = read_page(stack.buffer)?;
                let itup = page_get_item_bytes(&page, stack.off);

                if gintuple_get_attrnum(ginstate, &itup, mcx)? == attnum {
                    let (newDatum, newCategory) = gintuple_get_key(ginstate, &itup, mcx)?;

                    if ginCompareEntries(
                        ginstate,
                        attnum,
                        newDatum,
                        newCategory,
                        idatum.clone(),
                        icategory,
                    )? == 0
                    {
                        break; // Found!
                    }
                }

                stack.off += 1;
            }

            // (datumCopy/pfree of idatum is handled by ownership in Rust.)
            let _ = (attr_attbyval, attr_attlen);
        } else {
            let ipd = ginReadTuple(&itup)?;
            let nipd = ipd.len();
            let bm = entry
                .matchBitmap
                .as_mut()
                .expect("collectMatchBitmap: matchBitmap allocated above");
            tbm::tbm_add_tuples(bm, &ipd, false)?;
            entry.predictNumberResult += GinGetNPosting(&index_tuple_header(&itup)) as u32;
            let _ = nipd;
        }

        // Done with this entry, go to the next.
        stack.off += 1;
    }
}

// ===========================================================================
// startScanEntry (ginget.c:318)
// ===========================================================================

/// Setup beginning state of one entry's search: finds correct buffer and pins it.
fn startScanEntry<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    ginstate: &GinState<'mcx>,
    entry: &mut GinScanEntryData<'mcx>,
    snapshot: &Option<types_snapshot::SnapshotData>,
) -> PgResult<()> {
    'restart: loop {
        entry.buffer = InvalidBuffer;
        ip_set_min(&mut entry.curItem);
        entry.offset = InvalidOffsetNumber;
        entry.list = Vec::new();
        entry.nlist = 0;
        entry.matchBitmap = None;
        entry.matchNtuples = -1;
        entry.matchResult.blockno = InvalidBlockNumber;
        entry.reduceResult = false;
        entry.predictNumberResult = 0;

        // Find entry, begin scan of posting tree or store posting list in memory.
        let mut btreeEntry = GinBtreeData::default();
        ginPrepareEntryScan(
            &mut btreeEntry,
            mcx,
            entry.attnum,
            entry.queryKey.clone(),
            entry.queryCategory,
            clone_ginstate(mcx, ginstate)?,
        );
        let mut stackEntry = ginFindLeafPage(&mut btreeEntry, mcx, true, false, index)?;
        let page = read_page(stackEntry.buffer)?;

        // ginFindLeafPage() will have already checked snapshot age.
        let mut needUnlock = true;

        entry.isFinished = true;

        if entry.isPartialMatch || entry.queryCategory == GIN_CAT_EMPTY_QUERY {
            // findItem locates the first item >= search key. Scan forward and
            // collect all TIDs needed.
            let find_item = btreeEntry
                .findItem
                .expect("entry-tree btree has findItem (set by ginPrepareEntryScan)");
            find_item(&mut btreeEntry, &mut stackEntry)?;

            if !collectMatchBitmap(
                mcx,
                &mut btreeEntry,
                index,
                &mut stackEntry,
                ginstate,
                entry,
                snapshot,
            )? {
                // GIN tree was seriously restructured; cleanup and rescan.
                if entry.matchBitmap.is_some() {
                    if let Some(it) = entry.matchIterator.take() {
                        tbm::gin_tbm_end_private_iterate(it);
                    }
                    if let Some(bm) = entry.matchBitmap.take() {
                        tbm::tbm_free(bm);
                    }
                }
                lock_buffer(stackEntry.buffer, GIN_UNLOCK)?;
                freeGinBtreeStack(stackEntry);
                continue 'restart;
            }

            if entry
                .matchBitmap
                .as_ref()
                .map(|bm| !tbm::tbm_is_empty(bm).unwrap_or(true))
                .unwrap_or(false)
            {
                let bm = entry.matchBitmap.as_mut().unwrap();
                entry.matchIterator = Some(tbm::gin_tbm_begin_private_iterate(bm)?);
                entry.isFinished = false;
            }
        } else if {
            let find_item = btreeEntry
                .findItem
                .expect("entry-tree btree has findItem (set by ginPrepareEntryScan)");
            find_item(&mut btreeEntry, &mut stackEntry)?
        } {
            let itup = page_get_item_bytes(&page, stackEntry.off);

            if GinIsPostingTree(&index_tuple_header(&itup)) {
                let rootPostingTree = GinGetDownlink(&index_tuple_header(&itup));

                // Equality scan: lock the root of the posting tree.
                predicate_lock_page(index, rootPostingTree, snapshot)?;

                // Unlock entry page before touching posting tree (deadlock).
                lock_buffer(stackEntry.buffer, GIN_UNLOCK)?;
                needUnlock = false;

                let stack = ginScanBeginPostingTree(
                    &mut entry.btree,
                    mcx,
                    index,
                    rootPostingTree,
                )?;
                entry.buffer = stack.buffer;

                // Keep buffer pinned to prevent deletion of page during scan.
                incr_buffer_ref_count(entry.buffer);

                let entrypage = read_page(entry.buffer)?;

                // Load the first page into memory.
                let mut minItem = ItemPointerData::default();
                ip_set_min(&mut minItem);
                entry.list = GinDataLeafPageGetItems(&entrypage, minItem);
                entry.nlist = entry.list.len() as i32;

                entry.predictNumberResult = stack.predictNumber * entry.nlist as u32;

                lock_buffer(entry.buffer, GIN_UNLOCK)?;
                freeGinBtreeStack(stack);
                entry.isFinished = false;
            } else {
                // Lock the entry leaf page.
                predicate_lock_page(
                    index,
                    buffer_get_block_number(stackEntry.buffer),
                    snapshot,
                )?;
                if GinGetNPosting(&index_tuple_header(&itup)) > 0 {
                    entry.list = ginReadTuple(&itup)?;
                    entry.nlist = entry.list.len() as i32;
                    entry.predictNumberResult = entry.nlist as u32;
                    entry.isFinished = false;
                }
            }
        } else {
            // No entry found. Predicate lock the leaf page.
            predicate_lock_page(index, buffer_get_block_number(stackEntry.buffer), snapshot)?;
        }

        if needUnlock {
            lock_buffer(stackEntry.buffer, GIN_UNLOCK)?;
        }
        freeGinBtreeStack(stackEntry);
        return Ok(());
    }
}

// ===========================================================================
// entryIndexByFrequencyCmp / startScanKey (ginget.c:489 / 506)
// ===========================================================================

/// Finish initializing a scan key: divide its entries into required/additional
/// sets, frequency-sorted.
fn startScanKey<'mcx>(so: &mut GinScanOpaqueData<'mcx>, ki: usize) -> PgResult<()> {
    {
        let key = &mut so.keys[ki];
        ip_set_min(&mut key.curItem);
        key.curItemMatches = false;
        key.recheckCurItem = false;
        key.isFinished = false;
    }

    let excludeOnly = so.keys[ki].excludeOnly;
    let nentries = so.keys[ki].nentries as usize;

    if excludeOnly {
        // keyCtx switch is expressed by owned storage.
        let key = &mut so.keys[ki];
        key.nrequired = 0;
        key.nadditional = key.nentries as i32;
        key.additionalEntries = key.scanEntry.clone();
    } else if nentries > 1 {
        // Sort entry indices by predictNumberResult (least frequent first).
        let scan_entry = so.keys[ki].scanEntry.clone();
        let mut entryIndexes: Vec<usize> = (0..nentries).collect();
        entryIndexes.sort_by(|&a, &b| {
            let n1 = so.entries[scan_entry[a] as usize].predictNumberResult;
            let n2 = so.entries[scan_entry[b] as usize].predictNumberResult;
            n1.cmp(&n2)
        });

        for i in 1..nentries {
            so.keys[ki].entryRes[entryIndexes[i]] = types_gin::GIN_MAYBE;
        }
        let mut last_required = nentries - 1;
        for i in 0..nentries - 1 {
            // Pass all entries <= i as FALSE, and the rest as MAYBE.
            so.keys[ki].entryRes[entryIndexes[i]] = types_gin::GIN_FALSE;

            if callTriConsistentFn(&mut so.keys[ki]) == types_gin::GIN_FALSE {
                last_required = i;
                break;
            }

            // Make this loop interruptible.
            postgres::check_for_interrupts::call()?;
        }
        // last_required is now the last required entry.

        let key = &mut so.keys[ki];
        key.nrequired = (last_required + 1) as i32;
        key.nadditional = key.nentries as i32 - key.nrequired;
        let mut j = 0usize;
        let mut required = Vec::with_capacity(key.nrequired as usize);
        for _ in 0..key.nrequired {
            required.push(scan_entry[entryIndexes[j]]);
            j += 1;
        }
        let mut additional = Vec::with_capacity(key.nadditional as usize);
        for _ in 0..key.nadditional {
            additional.push(scan_entry[entryIndexes[j]]);
            j += 1;
        }
        key.requiredEntries = required;
        key.additionalEntries = additional;
        // tempCtx reset (entryIndexes) is the local drop.
    } else {
        let key = &mut so.keys[ki];
        key.nrequired = 1;
        key.nadditional = 0;
        key.requiredEntries = vec![key.scanEntry[0]];
    }

    Ok(())
}

// ===========================================================================
// startScan (ginget.c:604)
// ===========================================================================

/// Set up all entries, apply the `GinFuzzySearchLimit` reduction, and finish
/// initializing the scan keys.
fn startScan<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    so: &mut GinScanOpaqueData<'mcx>,
    snapshot: &Option<types_snapshot::SnapshotData>,
) -> PgResult<()> {
    let totalentries = so.totalentries as usize;

    for i in 0..totalentries {
        // Borrow ginstate immutably while mutating the entry; clone the state
        // (a Copy/Clone snapshot of the per-column support data) to avoid the
        // simultaneous &so.ginstate + &mut so.entries[i] borrow.
        let ginstate = clone_ginstate(mcx, &so.ginstate)?;
        startScanEntry(mcx, index, &ginstate, &mut so.entries[i], snapshot)?;
    }

    let fuzzy = gin_fuzzy_search_limit();
    if fuzzy > 0 {
        let mut reduce = true;
        for i in 0..totalentries {
            if so.entries[i].predictNumberResult <= so.totalentries * fuzzy as u32 {
                reduce = false;
                break;
            }
        }
        if reduce {
            for i in 0..totalentries {
                so.entries[i].predictNumberResult /= so.totalentries;
                so.entries[i].reduceResult = true;
            }
        }
    }

    // Now finish initializing the scan keys.
    let nkeys = so.nkeys as usize;
    for i in 0..nkeys {
        startScanKey(so, i)?;
    }

    Ok(())
}

// ===========================================================================
// entryLoadMoreItems (ginget.c:656)
// ===========================================================================

/// Load the next batch of item pointers from a posting tree.
fn entryLoadMoreItems<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    entry: &mut GinScanEntryData<'mcx>,
    advancePast: ItemPointerData,
) -> PgResult<()> {
    if !buffer_is_valid(entry.buffer) {
        entry.isFinished = true;
        return Ok(());
    }

    // Two strategies: step right, or descend from root.
    let mut stepright;
    if ginCompareItemPointers(&entry.curItem, &advancePast) == 0 {
        stepright = true;
        lock_buffer(entry.buffer, GIN_SHARE)?;
    } else {
        release_buffer(entry.buffer);

        // Set the search key, and find the correct leaf page.
        if ip_is_lossy_page(&advancePast) {
            ip_set(
                &mut entry.btree.itemptr,
                ip_block(&advancePast) + 1,
                FirstOffsetNumber,
            );
        } else {
            ip_set(
                &mut entry.btree.itemptr,
                ip_block(&advancePast),
                offset_next(ip_offset(&advancePast)),
            );
        }
        entry.btree.fullScan = false;
        let stack = ginFindLeafPage(&mut entry.btree, mcx, true, false, index)?;

        // We don't need the stack, just the buffer.
        entry.buffer = stack.buffer;
        incr_buffer_ref_count(entry.buffer);
        freeGinBtreeStack(stack);
        stepright = false;
    }

    let mut page = read_page(entry.buffer)?;
    loop {
        entry.offset = InvalidOffsetNumber;
        if !entry.list.is_empty() {
            entry.list = Vec::new();
            entry.nlist = 0;
        }

        if stepright {
            // We've processed all the entries on this page.
            if GinPageRightMost(&page) {
                unlock_release_buffer(entry.buffer);
                entry.buffer = InvalidBuffer;
                entry.isFinished = true;
                return Ok(());
            }

            // Step to next page, following the right link.
            entry.buffer = ginStepRight(entry.buffer, index, GIN_SHARE)?;
            page = read_page(entry.buffer)?;
        }
        stepright = true;

        if page_is_deleted(&page) {
            continue; // page was deleted by concurrent vacuum
        }

        // Keep following right-links until we re-find the correct page.
        if !GinPageRightMost(&page)
            && ginCompareItemPointers(&advancePast, &gin_data_page_get_right_bound(&page)) >= 0
        {
            continue;
        }

        entry.list = GinDataLeafPageGetItems(&page, advancePast);
        entry.nlist = entry.list.len() as i32;

        for i in 0..entry.nlist as usize {
            if ginCompareItemPointers(&advancePast, &entry.list[i]) < 0 {
                entry.offset = i as OffsetNumber;

                if GinPageRightMost(&page) {
                    // after processing the copied items, we're done.
                    unlock_release_buffer(entry.buffer);
                    entry.buffer = InvalidBuffer;
                } else {
                    lock_buffer(entry.buffer, GIN_UNLOCK)?;
                }
                return Ok(());
            }
        }
    }
}

// ===========================================================================
// entryGetItem (ginget.c:811)
// ===========================================================================

/// Sets `entry.curItem` to next heap item pointer > advancePast for one entry,
/// or sets `entry.isFinished` to true if there are no more.
fn entryGetItem<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    entry: &mut GinScanEntryData<'mcx>,
    mut advancePast: ItemPointerData,
) -> PgResult<()> {
    debug_assert!(!entry.isFinished);
    debug_assert!(
        !ip_is_valid(&entry.curItem) || ginCompareItemPointers(&entry.curItem, &advancePast) <= 0
    );

    if entry.matchBitmap.is_some() {
        // A bitmap result.
        let advancePastBlk = ip_block(&advancePast);
        let advancePastOff = ip_offset(&advancePast);

        loop {
            // If we've exhausted all items on this block, move to next block.
            while !block_is_valid(entry.matchResult.blockno)
                || (!entry.matchResult.lossy && entry.offset as i32 >= entry.matchNtuples)
                || entry.matchResult.blockno < advancePastBlk
                || (ip_is_lossy_page(&advancePast) && entry.matchResult.blockno == advancePastBlk)
            {
                let it = entry
                    .matchIterator
                    .as_mut()
                    .expect("entryGetItem: matchIterator present for bitmap result");
                if !tbm::gin_tbm_private_iterate(it, &mut entry.matchResult) {
                    debug_assert!(!block_is_valid(entry.matchResult.blockno));
                    ip_set_invalid(&mut entry.curItem);
                    if let Some(it) = entry.matchIterator.take() {
                        tbm::gin_tbm_end_private_iterate(it);
                    }
                    entry.isFinished = true;
                    break;
                }

                // Exact pages need their tuple offsets extracted.
                if !entry.matchResult.lossy {
                    entry.matchNtuples =
                        tbm::gin_tbm_extract_page_tuple(&entry.matchResult, &mut entry.matchOffsets);
                }

                // Reset counter to the beginning of matchResult.
                entry.offset = 0;
            }
            if entry.isFinished {
                break;
            }

            // First page after advancePast with items on it.
            if entry.matchResult.lossy {
                ip_set_lossy_page(&mut entry.curItem, entry.matchResult.blockno);
                break;
            }

            debug_assert!(entry.matchNtuples > -1);

            // Skip over any offsets <= advancePast.
            if entry.matchResult.blockno == advancePastBlk {
                debug_assert!(entry.matchNtuples > 0);

                // Quick check against the last offset on the page.
                if entry.matchOffsets[(entry.matchNtuples - 1) as usize] <= advancePastOff {
                    entry.offset = entry.matchNtuples as OffsetNumber;
                    continue;
                }

                // Scan to find the first item > advancePast.
                while entry.matchOffsets[entry.offset as usize] <= advancePastOff {
                    entry.offset += 1;
                }
            }

            ip_set(
                &mut entry.curItem,
                entry.matchResult.blockno,
                entry.matchOffsets[entry.offset as usize],
            );
            entry.offset += 1;

            // Done unless we need to reduce the result.
            if !entry.reduceResult || !drop_item(entry.predictNumberResult) {
                break;
            }
        }
    } else if !buffer_is_valid(entry.buffer) {
        // A posting list from an entry tuple, or last page of a posting tree.
        loop {
            if entry.offset as i32 >= entry.nlist {
                ip_set_invalid(&mut entry.curItem);
                entry.isFinished = true;
                break;
            }

            entry.curItem = entry.list[entry.offset as usize];
            entry.offset += 1;

            // If we're not past advancePast, keep scanning.
            if ginCompareItemPointers(&entry.curItem, &advancePast) <= 0 {
                continue;
            }

            if !entry.reduceResult || !drop_item(entry.predictNumberResult) {
                break;
            }
        }
    } else {
        // A posting tree.
        loop {
            // If we've processed the current batch, load more items.
            while entry.offset as i32 >= entry.nlist {
                entryLoadMoreItems(mcx, index, entry, advancePast)?;

                if entry.isFinished {
                    ip_set_invalid(&mut entry.curItem);
                    return Ok(());
                }
            }

            entry.curItem = entry.list[entry.offset as usize];
            entry.offset += 1;

            // If we're not past advancePast, keep scanning.
            if ginCompareItemPointers(&entry.curItem, &advancePast) <= 0 {
                continue;
            }

            if !entry.reduceResult || !drop_item(entry.predictNumberResult) {
                break;
            }

            // Advance advancePast and keep scanning.
            advancePast = entry.curItem;
        }
    }

    Ok(())
}

// ===========================================================================
// keyGetItem (ginget.c:1004)
// ===========================================================================

/// Identify the "current" item among the input entry streams for this scan key
/// that is greater than advancePast, and test whether it passes the qual.
fn keyGetItem<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    so: &mut GinScanOpaqueData<'mcx>,
    ki: usize,
    mut advancePast: ItemPointerData,
) -> PgResult<()> {
    debug_assert!(!so.keys[ki].isFinished);

    // We might have already tested this item; if so, no need to repeat work.
    if ginCompareItemPointers(&so.keys[ki].curItem, &advancePast) > 0 {
        return Ok(());
    }

    // Find the minimum item > advancePast among the active entry streams.
    let mut minItem = ItemPointerData::default();
    ip_set_max(&mut minItem);
    let mut allFinished = true;

    let nrequired = so.keys[ki].nrequired as usize;
    for i in 0..nrequired {
        let ei = so.keys[ki].requiredEntries[i] as usize;

        if so.entries[ei].isFinished {
            continue;
        }

        // Advance this stream if necessary.
        if ginCompareItemPointers(&so.entries[ei].curItem, &advancePast) <= 0 {
            let entry = &mut so.entries[ei];
            entryGetItem(mcx, index, entry, advancePast)?;
            if so.entries[ei].isFinished {
                continue;
            }
        }

        allFinished = false;
        if ginCompareItemPointers(&so.entries[ei].curItem, &minItem) < 0 {
            minItem = so.entries[ei].curItem;
        }
    }

    let excludeOnly = so.keys[ki].excludeOnly;

    if allFinished && !excludeOnly {
        // all entries are finished
        so.keys[ki].isFinished = true;
        return Ok(());
    }

    if !excludeOnly {
        // For a normal scan key, no matches < minItem.
        if ip_is_lossy_page(&minItem) {
            if ip_block(&advancePast) < ip_block(&minItem) {
                ip_set(&mut advancePast, ip_block(&minItem), InvalidOffsetNumber);
            }
        } else {
            debug_assert!(ip_offset(&minItem) > 0);
            ip_set(
                &mut advancePast,
                ip_block(&minItem),
                offset_prev(ip_offset(&minItem)),
            );
        }
    } else {
        // excludeOnly keys: consider the item just after advancePast.
        debug_assert!(so.keys[ki].nrequired == 0);
        ip_set(
            &mut minItem,
            ip_block(&advancePast),
            offset_next(ip_offset(&advancePast)),
        );
    }

    // Load all the additional entries before calling the consistent function.
    let nadditional = so.keys[ki].nadditional as usize;
    for i in 0..nadditional {
        let ei = so.keys[ki].additionalEntries[i] as usize;

        if so.entries[ei].isFinished {
            continue;
        }

        if ginCompareItemPointers(&so.entries[ei].curItem, &advancePast) <= 0 {
            let entry = &mut so.entries[ei];
            entryGetItem(mcx, index, entry, advancePast)?;
            if so.entries[ei].isFinished {
                continue;
            }
        }

        // Normally none of additionalEntries can have curItem larger than
        // minItem. But if minItem is lossy, there might be exact items.
        if ginCompareItemPointers(&so.entries[ei].curItem, &minItem) < 0 {
            debug_assert!(ip_is_lossy_page(&minItem));
            minItem = so.entries[ei].curItem;
        }
    }

    // Set key->curItem, perform consistentFn test.
    so.keys[ki].curItem = minItem;
    let mut curPageLossy = ItemPointerData::default();
    ip_set_lossy_page(&mut curPageLossy, ip_block(&so.keys[ki].curItem));
    let mut haveLossyEntry = false;
    let nentries = so.keys[ki].nentries as usize;
    let nuserentries = so.keys[ki].nuserentries as usize;
    for i in 0..nentries {
        let ei = so.keys[ki].scanEntry[i] as usize;
        if !so.entries[ei].isFinished
            && ginCompareItemPointers(&so.entries[ei].curItem, &curPageLossy) == 0
        {
            if i < nuserentries {
                so.keys[ki].entryRes[i] = types_gin::GIN_MAYBE;
            } else {
                so.keys[ki].entryRes[i] = types_gin::GIN_TRUE;
            }
            haveLossyEntry = true;
        } else {
            so.keys[ki].entryRes[i] = types_gin::GIN_FALSE;
        }
    }

    // (tempCtx switch / reset is handled by ownership.)
    if haveLossyEntry {
        // Have lossy-page entries, so see if whole page matches.
        let res = callTriConsistentFn(&mut so.keys[ki]);

        if res == types_gin::GIN_TRUE || res == types_gin::GIN_MAYBE {
            // Return lossy pointer for whole page.
            so.keys[ki].curItem = curPageLossy;
            so.keys[ki].curItemMatches = true;
            so.keys[ki].recheckCurItem = true;
            return Ok(());
        }
    }

    // Prepare entryRes array to be passed to consistentFn.
    for i in 0..nentries {
        let ei = so.keys[ki].scanEntry[i] as usize;
        if so.entries[ei].isFinished {
            so.keys[ki].entryRes[i] = types_gin::GIN_FALSE;
        } else if ginCompareItemPointers(&so.entries[ei].curItem, &curPageLossy) == 0 {
            so.keys[ki].entryRes[i] = types_gin::GIN_MAYBE;
        } else if ginCompareItemPointers(&so.entries[ei].curItem, &minItem) == 0 {
            so.keys[ki].entryRes[i] = types_gin::GIN_TRUE;
        } else {
            so.keys[ki].entryRes[i] = types_gin::GIN_FALSE;
        }
    }

    let res = callTriConsistentFn(&mut so.keys[ki]);

    match res {
        types_gin::GIN_TRUE => {
            so.keys[ki].curItemMatches = true;
            // triConsistentFn set recheckCurItem
        }
        types_gin::GIN_FALSE => {
            so.keys[ki].curItemMatches = false;
        }
        types_gin::GIN_MAYBE => {
            so.keys[ki].curItemMatches = true;
            so.keys[ki].recheckCurItem = true;
        }
        _ => {
            // bogus consistent-fn result: the safe result
            so.keys[ki].curItemMatches = true;
            so.keys[ki].recheckCurItem = true;
        }
    }

    Ok(())
}

// ===========================================================================
// scanGetItem (ginget.c:1299)
// ===========================================================================

/// Get next heap item pointer (after advancePast) from scan. Returns
/// `Some((item, recheck))` if anything found, `None` if exhausted.
fn scanGetItem<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    so: &mut GinScanOpaqueData<'mcx>,
    mut advancePast: ItemPointerData,
) -> PgResult<Option<(ItemPointerData, bool)>> {
    let nkeys = so.nkeys as usize;
    let mut item = ItemPointerData::default();

    // Advance the scan keys in lock-step until we find a matching item.
    loop {
        postgres::check_for_interrupts::call()?;

        ip_set_min(&mut item);
        let mut matched = true;
        let mut i = 0;
        while i < nkeys && matched {
            // If we're considering a lossy page, skip excludeOnly keys.
            if ip_is_lossy_page(&item) && so.keys[i].excludeOnly {
                debug_assert!(i > 0);
                i += 1;
                continue;
            }

            // Fetch the next item for this key that is > advancePast.
            keyGetItem(mcx, index, so, i, advancePast)?;

            if so.keys[i].isFinished {
                return Ok(None);
            }

            // If it's not a match, nothing <= this item matches.
            if !so.keys[i].curItemMatches {
                advancePast = so.keys[i].curItem;
                matched = false;
                break;
            }

            // It's a match. Other key streams can skip to this item.
            if ip_is_lossy_page(&so.keys[i].curItem) {
                if ip_block(&advancePast) < ip_block(&so.keys[i].curItem) {
                    ip_set(&mut advancePast, ip_block(&so.keys[i].curItem), InvalidOffsetNumber);
                }
            } else {
                debug_assert!(ip_offset(&so.keys[i].curItem) > 0);
                ip_set(
                    &mut advancePast,
                    ip_block(&so.keys[i].curItem),
                    offset_prev(ip_offset(&so.keys[i].curItem)),
                );
            }

            if i == 0 {
                item = so.keys[i].curItem;
            } else if ip_is_lossy_page(&so.keys[i].curItem) || ip_is_lossy_page(&item) {
                debug_assert!(ip_block(&so.keys[i].curItem) >= ip_block(&item));
                matched = ip_block(&so.keys[i].curItem) == ip_block(&item);
            } else {
                debug_assert!(ginCompareItemPointers(&so.keys[i].curItem, &item) >= 0);
                matched = ginCompareItemPointers(&so.keys[i].curItem, &item) == 0;
            }

            i += 1;
        }

        if matched {
            break;
        }
    }

    debug_assert!(!ip_is_min(&item));

    // recheck = true if any of the keys are marked recheck.
    let mut recheck = false;
    for i in 0..nkeys {
        if so.keys[i].recheckCurItem {
            recheck = true;
            break;
        }
    }

    Ok(Some((item, recheck)))
}

// ===========================================================================
// Pending-list scanning
// ===========================================================================

/// `pendingPosition` (ginget.c:29): cursor over the pending-list pages.
struct PendingPosition {
    pendingBuffer: Buffer,
    firstOffset: OffsetNumber,
    lastOffset: OffsetNumber,
    item: ItemPointerData,
    hasMatchKey: Vec<bool>,
}

// ===========================================================================
// scanGetCandidate (ginget.c:1466)
// ===========================================================================

/// Get ItemPointer of next heap row to be checked from pending list. Returns
/// false if there are no more.
fn scanGetCandidate<'mcx>(
    index: &Relation<'mcx>,
    pos: &mut PendingPosition,
) -> PgResult<bool> {
    ip_set_invalid(&mut pos.item);
    loop {
        let page = read_page(pos.pendingBuffer)?;

        let maxoff = page_max_offset(&page);
        if pos.firstOffset > maxoff {
            let blkno = page_rightlink(&page);

            if blkno == InvalidBlockNumber {
                unlock_release_buffer(pos.pendingBuffer);
                pos.pendingBuffer = InvalidBuffer;
                return Ok(false);
            } else {
                // Lock next page before releasing the current one (prevent
                // deletion by insertcleanup).
                let tmpbuf = read_buffer(index, blkno)?;
                lock_buffer(tmpbuf, GIN_SHARE)?;
                unlock_release_buffer(pos.pendingBuffer);

                pos.pendingBuffer = tmpbuf;
                pos.firstOffset = FirstOffsetNumber;
            }
        } else {
            let itup = page_get_item_bytes(&page, pos.firstOffset);
            pos.item = index_tuple_tid(&itup);
            if page_has_full_row(&page) {
                // find itempointer to the next row
                pos.lastOffset = pos.firstOffset + 1;
                while pos.lastOffset <= maxoff {
                    let itup = page_get_item_bytes(&page, pos.lastOffset);
                    if !ip_equals(&pos.item, &index_tuple_tid(&itup)) {
                        break;
                    }
                    pos.lastOffset += 1;
                }
            } else {
                // All itempointers are the same on this page.
                pos.lastOffset = maxoff + 1;
            }

            break;
        }
    }

    Ok(true)
}

// ===========================================================================
// matchPartialInPendingList (ginget.c:1553)
// ===========================================================================

/// Scan pending-list page from `off` for a partial match.
fn matchPartialInPendingList<'mcx>(
    mcx: Mcx<'mcx>,
    ginstate: &GinState<'mcx>,
    page: &[u8],
    mut off: OffsetNumber,
    maxoff: OffsetNumber,
    entry: &PendingEntryView<'mcx>,
    datum: &mut [Option<Datum<'mcx>>],
    category: &mut [GinNullCategory],
    datumExtracted: &mut [bool],
) -> PgResult<bool> {
    // Partial match to a null is not possible.
    if entry.queryCategory != GIN_CAT_NORM_KEY {
        return Ok(false);
    }

    while off < maxoff {
        let itup = page_get_item_bytes(page, off);

        if gintuple_get_attrnum(ginstate, &itup, mcx)? != entry.attnum {
            return Ok(false);
        }

        let idx = (off - 1) as usize;
        if !datumExtracted[idx] {
            let (d, c) = gintuple_get_key(ginstate, &itup, mcx)?;
            datum[idx] = Some(d);
            category[idx] = c;
            datumExtracted[idx] = true;
        }

        // Once we hit nulls, no further match is possible.
        if category[idx] != GIN_CAT_NORM_KEY {
            return Ok(false);
        }

        // cmp == 0 => match; cmp > 0 => end scan; cmp < 0 => continue.
        let cmp = sx::gin_compare_partial::call(
            &ginstate.comparePartialFn[(entry.attnum - 1) as usize],
            ginstate.supportCollation[(entry.attnum - 1) as usize],
            entry.queryKey.clone(),
            datum[idx].clone().unwrap(),
            entry.strategy,
            entry.extra_data.as_deref(),
        )?;
        if cmp == 0 {
            return Ok(true);
        } else if cmp > 0 {
            return Ok(false);
        }

        off += 1;
    }

    Ok(false)
}

/// The immutable per-entry view `collectMatchesForHeapRow` reads (the fields of
/// a `GinScanEntryData` it needs while the entries pool is borrowed elsewhere).
struct PendingEntryView<'mcx> {
    queryKey: Datum<'mcx>,
    queryCategory: GinNullCategory,
    isPartialMatch: bool,
    extra_data: Option<Vec<u8>>,
    strategy: u16,
    searchMode: i32,
    attnum: OffsetNumber,
}

// ===========================================================================
// collectMatchesForHeapRow (ginget.c:1621)
// ===========================================================================

/// Set up the entryRes array for each key by looking at every entry for the
/// current heap row in the pending list. Returns true if each scan key has at
/// least one entryRes match.
fn collectMatchesForHeapRow<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    so: &mut GinScanOpaqueData<'mcx>,
    pos: &mut PendingPosition,
) -> PgResult<bool> {
    let nkeys = so.nkeys as usize;

    // Reset all entryRes and hasMatchKey flags.
    for i in 0..nkeys {
        let nentries = so.keys[i].nentries as usize;
        for j in 0..nentries {
            so.keys[i].entryRes[j] = types_gin::GIN_FALSE;
        }
    }
    for h in pos.hasMatchKey.iter_mut() {
        *h = false;
    }

    // BLCKSZ / sizeof(IndexTupleData) caching arrays.
    let cache_len = BLCKSZ / SIZE_OF_INDEX_TUPLE_DATA;

    let ginstate = clone_ginstate(mcx, &so.ginstate)?;

    // Outer loop iterates over multiple pending-list pages.
    loop {
        let mut datum: Vec<Option<Datum<'mcx>>> = vec![None; cache_len];
        let mut category: Vec<GinNullCategory> = vec![GIN_CAT_NORM_KEY; cache_len];
        let mut datumExtracted: Vec<bool> = vec![false; cache_len];

        debug_assert!(pos.lastOffset > pos.firstOffset);
        for o in (pos.firstOffset - 1)..(pos.lastOffset - 1) {
            datumExtracted[o as usize] = false;
        }

        let page = read_page(pos.pendingBuffer)?;

        for i in 0..nkeys {
            let nentries = so.keys[i].nentries as usize;
            let key_attnum = so.keys[i].attnum;
            for j in 0..nentries {
                let ei = so.keys[i].scanEntry[j] as usize;
                let entry = PendingEntryView {
                    queryKey: so.entries[ei].queryKey.clone(),
                    queryCategory: so.entries[ei].queryCategory,
                    isPartialMatch: so.entries[ei].isPartialMatch,
                    extra_data: so.entries[ei].extra_data.clone(),
                    strategy: so.entries[ei].strategy,
                    searchMode: so.entries[ei].searchMode,
                    attnum: so.entries[ei].attnum,
                };

                // If already matched on earlier page, do no extra work.
                if so.keys[i].entryRes[j] != types_gin::GIN_FALSE {
                    continue;
                }

                // Binary search over [firstOffset, lastOffset).
                let mut StopLow = pos.firstOffset;
                let mut StopHigh = pos.lastOffset;
                let mut found = false;

                while StopLow < StopHigh {
                    let StopMiddle = StopLow + ((StopHigh - StopLow) >> 1);

                    let itup = page_get_item_bytes(&page, StopMiddle);
                    let attrnum = gintuple_get_attrnum(&ginstate, &itup, mcx)?;

                    if key_attnum < attrnum {
                        StopHigh = StopMiddle;
                        continue;
                    }
                    if key_attnum > attrnum {
                        StopLow = StopMiddle + 1;
                        continue;
                    }

                    let midx = (StopMiddle - 1) as usize;
                    if !datumExtracted[midx] {
                        let (d, c) = gintuple_get_key(&ginstate, &itup, mcx)?;
                        datum[midx] = Some(d);
                        category[midx] = c;
                        datumExtracted[midx] = true;
                    }

                    let res: i32 = if entry.queryCategory == GIN_CAT_EMPTY_QUERY {
                        // special behavior depending on searchMode
                        if entry.searchMode == GIN_SEARCH_MODE_ALL {
                            // match anything except NULL_ITEM
                            if category[midx] == GIN_CAT_NULL_ITEM {
                                -1
                            } else {
                                0
                            }
                        } else {
                            // match everything
                            0
                        }
                    } else {
                        ginCompareEntries(
                            &ginstate,
                            entry.attnum,
                            entry.queryKey.clone(),
                            entry.queryCategory,
                            datum[midx].clone().unwrap(),
                            category[midx],
                        )?
                    };

                    if res == 0 {
                        // Found exact match.
                        if entry.isPartialMatch {
                            so.keys[i].entryRes[j] = bool_to_tri(matchPartialInPendingList(
                                mcx,
                                &ginstate,
                                &page,
                                StopMiddle,
                                pos.lastOffset,
                                &entry,
                                &mut datum,
                                &mut category,
                                &mut datumExtracted,
                            )?);
                        } else {
                            so.keys[i].entryRes[j] = types_gin::GIN_TRUE;
                        }
                        found = true;
                        break;
                    } else if res < 0 {
                        StopHigh = StopMiddle;
                    } else {
                        StopLow = StopMiddle + 1;
                    }
                }

                if !found && StopLow >= StopHigh && entry.isPartialMatch {
                    // No exact match on this page; partial match from StopHigh.
                    so.keys[i].entryRes[j] = bool_to_tri(matchPartialInPendingList(
                        mcx,
                        &ginstate,
                        &page,
                        StopHigh,
                        pos.lastOffset,
                        &entry,
                        &mut datum,
                        &mut category,
                        &mut datumExtracted,
                    )?);
                }

                pos.hasMatchKey[i] = pos.hasMatchKey[i] || (so.keys[i].entryRes[j] != types_gin::GIN_FALSE);
            }
        }

        // Advance firstOffset over the scanned tuples.
        pos.firstOffset = pos.lastOffset;

        if page_has_full_row(&page) {
            // Examined all pending entries for the current heap row.
            break;
        } else {
            // Advance to next page of pending entries for the current heap row.
            let item = pos.item;

            if !scanGetCandidate(index, pos)? || !ip_equals(&pos.item, &item) {
                return Err(ereport(ERROR)
                    .errmsg("could not find additional pending pages for same heap tuple")
                    .into_error());
            }
        }
    }

    // All scan keys except excludeOnly require at least one entry to match.
    for i in 0..nkeys {
        if !pos.hasMatchKey[i] && !so.keys[i].excludeOnly {
            return Ok(false);
        }
    }

    Ok(true)
}

// ===========================================================================
// scanPendingInsert (ginget.c:1836)
// ===========================================================================

/// Collect all matched rows from pending list into bitmap.
fn scanPendingInsert<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    so: &mut GinScanOpaqueData<'mcx>,
    tbm_out: &mut TIDBitmap,
    snapshot: &Option<types_snapshot::SnapshotData>,
) -> PgResult<i64> {
    let mut ntids: i64 = 0;

    let metabuffer = read_buffer(index, GIN_METAPAGE_BLKNO)?;

    // Acquire predicate lock on the metapage, to conflict with fastupdate.
    predicate_lock_page(index, GIN_METAPAGE_BLKNO, snapshot)?;

    lock_buffer(metabuffer, GIN_SHARE)?;
    let page = read_page(metabuffer)?;
    let blkno = gin_page_get_meta_head(&page);

    // Fetch head of list before unlocking metapage.
    if blkno == InvalidBlockNumber {
        // No pending list, so proceed with normal scan.
        unlock_release_buffer(metabuffer);
        return Ok(0);
    }

    let mut pos = PendingPosition {
        pendingBuffer: read_buffer(index, blkno)?,
        firstOffset: FirstOffsetNumber,
        lastOffset: 0,
        item: ItemPointerData::default(),
        hasMatchKey: vec![false; so.nkeys as usize],
    };
    lock_buffer(pos.pendingBuffer, GIN_SHARE)?;
    unlock_release_buffer(metabuffer);

    // Loop for each heap row.
    while scanGetCandidate(index, &mut pos)? {
        if !collectMatchesForHeapRow(mcx, index, so, &mut pos)? {
            continue;
        }

        // Check row using consistent functions.
        let mut recheck = false;
        let mut matched = true;

        let nkeys = so.nkeys as usize;
        for i in 0..nkeys {
            if !callBoolConsistentFn(&mut so.keys[i]) {
                matched = false;
                break;
            }
            recheck = recheck || so.keys[i].recheckCurItem;
        }

        if matched {
            tbm::tbm_add_tuples(tbm_out, &[pos.item], recheck)?;
            ntids += 1;
        }
    }

    Ok(ntids)
}

// ===========================================================================
// gingetbitmap (ginget.c:1930)
// ===========================================================================

/// `gingetbitmap(scan, tbm)` (ginget.c) — the `amgetbitmap` callback; fetch all
/// matching tuples into the bitmap, returning the count. The unified index-AM
/// vtable carries the bitmap erased (`types_tableam::TIDBitmap`); ginget works
/// over the concrete `types_tidbitmap::TIDBitmap`, so downcast it (the same
/// erase/downcast as the BRIN/hash `amgetbitmap` adapters).
pub fn gingetbitmap<'mcx>(
    mcx: Mcx<'mcx>,
    scan: &mut IndexScanDescData<'mcx>,
    tbm_erased: &mut types_tableam::amapi::TIDBitmap,
) -> PgResult<i64> {
    let tbm_out = tbm_erased
        .payload
        .as_mut()
        .and_then(|p| p.downcast_mut::<TIDBitmap>())
        .expect("amgetbitmap TIDBitmap payload is not a types_tidbitmap::TIDBitmap");
    // Set up the scan keys, and check for unsatisfiable query.
    backend_access_gin_ginscan::ginFreeScanKeys(gin_so(scan));
    backend_access_gin_ginscan::ginNewScanKey(scan)?;

    if gin_so(scan).isVoidRes {
        return Ok(0);
    }

    pgstat_count_index_scan::call(scan.index_relation.rd_id);

    let mut ntids: i64 = 0;

    // Take owned copies of the immutable scan inputs so that `so` (which rides
    // `scan.opaque`) can be borrowed mutably without aliasing `scan`.
    let index = scan.index_relation.alias();
    let snapshot = scan.xs_snapshot.clone();

    // First, scan the pending list and collect matching entries into the bitmap.
    {
        let so = gin_so(scan);
        ntids += scanPendingInsert(mcx, &index, so, tbm_out, &snapshot)?;
    }

    // Now scan the main index.
    {
        let so = gin_so(scan);
        startScan(mcx, &index, so, &snapshot)?;
    }

    let mut iptr = ItemPointerData::default();
    ip_set_min(&mut iptr);

    loop {
        let next = {
            let so = gin_so(scan);
            scanGetItem(mcx, &index, so, iptr)?
        };
        let (item, recheck) = match next {
            Some(v) => v,
            None => break,
        };
        iptr = item;

        if ip_is_lossy_page(&iptr) {
            tbm::tbm_add_page(tbm_out, ip_block(&iptr))?;
        } else {
            tbm::tbm_add_tuples(tbm_out, &[iptr], recheck)?;
        }
        ntids += 1;
    }

    Ok(ntids)
}

// ===========================================================================
// Small helpers bridging to the substrate crates.
// ===========================================================================

/// `GinScanOpaque so = (GinScanOpaque) scan->opaque` (the A0 tag-checked
/// downcast).
fn gin_so<'a, 'mcx>(scan: &'a mut IndexScanDescData<'mcx>) -> &'a mut GinScanOpaqueData<'mcx> {
    scan.opaque
        .as_deref_mut()
        .expect("GIN scan descriptor has no opaque (not built by ginbeginscan)")
        .downcast_mut::<GinScanOpaqueData<'mcx>>()
        .expect("GIN scan opaque is not a GinScanOpaqueData")
}

/// `IndexTupleData` header view over the leading bytes of a tuple image
/// (`GinGetNPosting`/`GinIsPostingTree`/`GinGetDownlink` read `t_tid`/`t_info`).
/// Decodes the 8-byte `IndexTupleData` header (`t_tid: ItemPointerData (6) |
/// t_info: u16 (2)`); same layout as `ginentrypage`'s private `header_of`.
fn index_tuple_header(tup: &[u8]) -> IndexTupleData {
    IndexTupleData {
        t_tid: ItemPointerData {
            ip_blkid: BlockIdData {
                bi_hi: u16::from_ne_bytes([tup[0], tup[1]]),
                bi_lo: u16::from_ne_bytes([tup[2], tup[3]]),
            },
            ip_posid: u16::from_ne_bytes([tup[4], tup[5]]),
        },
        t_info: u16::from_ne_bytes([tup[6], tup[7]]),
    }
}

/// `itup->t_tid` of a pending-list tuple.
fn index_tuple_tid(itup: &[u8]) -> ItemPointerData {
    index_tuple_header(itup).t_tid
}

/// `TupleDescCompactAttr(ginstate->origTupdesc, attnum-1)->{attbyval,attlen}`.
fn compact_attr<'mcx>(ginstate: &GinState<'mcx>, attnum: OffsetNumber) -> (bool, i16) {
    let td = ginstate
        .origTupdesc
        .as_ref()
        .expect("GinState.origTupdesc set by initGinState");
    let att = td.attr((attnum - 1) as usize);
    (att.attbyval, att.attlen)
}

/// Deep-clone a [`GinState`] into `mcx`, mirroring C's `GinState *` pointer
/// sharing (the C entry-scan `GinBtreeData` keeps a pointer to the one
/// `so->ginstate`; the repo carrier owns the value by-copy). The opclass
/// `FmgrInfo`/`Oid`/`bool` arrays clone by value; the `TupleDesc`s are deep
/// copied via `CreateTupleDescCopy` (same as `initGinState`).
fn clone_ginstate<'mcx>(mcx: Mcx<'mcx>, src: &GinState<'mcx>) -> PgResult<GinState<'mcx>> {
    let clone_td = |td: &types_tuple::heaptuple::TupleDesc<'mcx>| -> PgResult<
        types_tuple::heaptuple::TupleDesc<'mcx>,
    > {
        match td.as_ref() {
            Some(t) => {
                let copy = backend_access_common_tupdesc::CreateTupleDescCopy(mcx, t)?;
                Ok(Some(mcx::alloc_in(mcx, copy)?))
            }
            None => Ok(None),
        }
    };
    let mut tupdesc = Vec::with_capacity(src.tupdesc.len());
    for td in &src.tupdesc {
        tupdesc.push(clone_td(td)?);
    }
    Ok(GinState {
        index: src.index,
        oneCol: src.oneCol,
        origTupdesc: clone_td(&src.origTupdesc)?,
        tupdesc,
        compareFn: src.compareFn.clone(),
        extractValueFn: src.extractValueFn.clone(),
        extractQueryFn: src.extractQueryFn.clone(),
        consistentFn: src.consistentFn.clone(),
        triConsistentFn: src.triConsistentFn.clone(),
        comparePartialFn: src.comparePartialFn.clone(),
        canPartialMatch: src.canPartialMatch.clone(),
        supportCollation: src.supportCollation.clone(),
    })
}

/// `datumCopy(value, attbyval, attlen)` — owned-value copy (Datum is already an
/// owned value in the repo model; the C deep-copy is the clone).
fn datum_copy<'mcx>(value: &Datum<'mcx>, _attbyval: bool, _attlen: i16) -> Datum<'mcx> {
    value.clone()
}

/// `RelationGetRelationName(index)`.
fn relation_get_relation_name<'mcx>(mcx: Mcx<'mcx>, index: &Relation<'mcx>) -> alloc::string::String {
    sx::gin_relation_get_relation_name::call(mcx, index)
        .map(|s| s.as_str().into())
        .unwrap_or_else(|_| alloc::string::String::from("?"))
}

/// `GinPageGetMeta(page)->head` — the pending-list head block of the metapage.
/// `GinPageGetMeta(p) == (GinMetaPageData *) PageGetContents(p)`; `head` is the
/// first `BlockNumber` (u32) field.
fn gin_page_get_meta_head(page: &[u8]) -> BlockNumber {
    let pr = PageRef::new(page).expect("valid GIN metapage image");
    let contents = backend_storage_page::PageGetContents(&pr).expect("metapage contents");
    BlockNumber::from_ne_bytes([contents[0], contents[1], contents[2], contents[3]])
}

/// Convert a `bool` to a [`types_gin::GinTernaryValue`] flag (the C `key->entryRes[j]
/// = <bool>` assignment, which sets `GIN_TRUE`/`GIN_FALSE`).
fn bool_to_tri(b: bool) -> types_gin::GinTernaryValue {
    if b {
        types_gin::GIN_TRUE
    } else {
        types_gin::GIN_FALSE
    }
}

/// `BLCKSZ` (pg_config.h).
const BLCKSZ: usize = types_core::primitive::BLCKSZ as usize;
/// `sizeof(IndexTupleData)` (itup.h): 8 bytes (t_tid[6] + t_info[2]).
const SIZE_OF_INDEX_TUPLE_DATA: usize = 8;
