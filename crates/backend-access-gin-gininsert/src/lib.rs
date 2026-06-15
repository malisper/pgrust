#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

//! Port of `src/backend/access/gin/gininsert.c` (PostgreSQL 18.3) — **Family 1**:
//! the GIN entry-tree *insert* spine (`ginEntryInsert` and its two leaf-tuple
//! builders `addItemPointersToLeafTuple` / `buildFreshLeafTuple`), the retail
//! single-tuple insert path (`ginHeapTupleInsert` / `gininsert`), and the
//! `ginbuildempty` init-fork builder.
//!
//! Unlike the src-idiomatic copy (which seam-routed the whole descent), this
//! port wires the **real, already-ported GIN B-tree tower**: the entry-tree
//! descent (`ginPrepareEntryScan` / `ginFindLeafPage` / `findItem` /
//! `ginInsertValue` from `ginbtree`), the posting-tree create+insert
//! (`createPostingTree` / `ginInsertItemPointers` from `gindatapage`), the leaf
//! tuple (de)serialization (`GinFormTuple` / `ginReadTuple` from
//! `ginentrypage`), the posting-list codec (`ginCompressPostingList` /
//! `ginMergeItemPointers` from the audited `gin-core-probe`), and the
//! key accessors (`gintuple_get_attrnum` / `gintuple_get_key` /
//! `ginExtractEntries` from `ginutil`). Buffers cross by id through the `bufmgr`
//! seams; serializable-conflict checks through the `predicate` seam. This is the
//! `ginInsertItemPointers` machinery the GIN write path is built on.
//!
//! # Out of scope (separate families — sanctioned panic legs)
//!
//!   * **F2 — ginbulk.c** (`BuildAccumulator`) and the build accumulate-then-dump
//!     orchestration in `ginbuild` / `ginBuildCallback`: `ginbuild` itself drives
//!     `table_index_build_scan` (the A6 heap-scan provider, which is the
//!     sanctioned table-AM panic leg per the GIN stage rules), so the whole
//!     `ginbuild` driver and the bulk-insert callbacks are deferred to F2.
//!   * **F3 — ginfast.c** (the fast-update pending list): `gininsert`'s
//!     fast-update branch (`ginHeapTupleFastCollect` + `ginHeapTupleFastInsert`)
//!     and `ginInsertCleanup`. The fast leg crosses the
//!     [`backend_access_gin_gininsert_seams::gin_fast_insert`] seam (no ginfast
//!     owner yet — loud-panics until F3 lands). The non-fast retail path is
//!     ported fully here.
//!   * The cross-process **parallel build** (`_gin_*` tuplesort serialization /
//!     `GinBuffer` merge / `_gin_begin_parallel`): deferred with F2.

extern crate alloc;

use alloc::vec::Vec;

use backend_storage_buffer_bufmgr_seams as bufmgr;
use backend_storage_lmgr_predicate_seams as predicate;
use backend_storage_page::{PageGetItem, PageGetItemId, PageRef};
use backend_utils_error::PgResult;

use mcx::Mcx;
use types_core::primitive::{BlockNumber, OffsetNumber};
use types_gin::{GinNullCategory, GinState, GinStatsData, GinMaxItemSize, GIN_UNLOCK, GIN_EXCLUSIVE};
use types_rel::Relation;
use types_storage::storage::Buffer;
use types_tableam::amapi::{IndexInfo, IndexUniqueCheck};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{IndexTupleData, ItemPointerData};

// gin-core-probe: the audited posting-list codec + cross-list merge.
use backend_access_gin_core_probe::ginpostinglist::{
    ginCompressPostingList, ginMergeItemPointers,
};
// gindatapage: posting-tree create + bulk-insert + the t_tid byte predicates.
use backend_access_gin_gindatapage::{
    createPostingTree, ginInsertItemPointers, GinGetDownlink, GinIsPostingTree, GIN_TREE_POSTING,
};
// ginentrypage: leaf-tuple form/read + the entry-tree scan setup.
use backend_access_gin_ginentrypage::{ginPrepareEntryScan, ginReadTuple, GinFormTuple};
// ginbtree: the entry-tree descent driver.
use backend_access_gin_ginbtree::{freeGinBtreeStack, ginFindLeafPage, ginInsertValue};
// ginutil: key accessors + entry extraction.
use backend_access_gin_ginutil::{gintuple_get_attrnum, gintuple_get_key, ginExtractEntries};

use types_gin::{GinBtreeData, GinBtreeEntryInsertData, GinInsertPayload};

#[cfg(test)]
mod tests;

// ===========================================================================
// init_seams — install the GIN AM `aminsert` callback (`gininsert`) into the
// `ginutil-seams` vtable-callback registry that `ginhandler` consumes. (The
// fast-update leg's `gin_fast_insert` seam is OWNED+declared by the
// gininsert-seams crate but INSTALLED by the future ginfast owner, not here.)
// ===========================================================================

/// Install the `gininsert` (`aminsert`) callback seam that `ginhandler`
/// (ginutil.c) routes the index-AM `aminsert` dispatch through.
pub fn init_seams() {
    backend_access_gin_ginutil_seams::gininsert::set(gininsert);
}

// ===========================================================================
// clone_ginstate — deep-clone a GinState into `mcx`.
//
// In C the entry-scan `GinBtreeData` keeps a pointer to the one `ginstate`;
// `ginPrepareEntryScan` here consumes a `GinState` value (the repo carrier owns
// it by-copy). The opclass `FmgrInfo`/`Oid`/`bool` arrays clone by value; the
// `TupleDesc`s are deep-copied via `CreateTupleDescCopy` (same as `initGinState`
// and the `ginget` clone). Mirrors `ginget`'s `clone_ginstate`.
// ===========================================================================

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

// ===========================================================================
// SizeOfGinPostingList(plist) (ginblock.h) — over a compressed-segment image.
// ===========================================================================

/// `SizeOfGinPostingList(plist)` (ginblock.h): `offsetof(GinPostingList, bytes)
/// + SHORTALIGN(plist->nbytes)`. The header is the 6-byte `first` ItemPointer
/// plus the 2-byte `nbytes`; the payload is short-aligned.
#[inline]
fn size_of_gin_posting_list(seg: &[u8]) -> usize {
    const HDR: usize = 8;
    let nbytes = u16::from_ne_bytes([seg[6], seg[7]]) as usize;
    HDR + ((nbytes + 1) & !1)
}

// ===========================================================================
// GinSetPostingTree(itup, blkno) (ginblock.h) — over a tuple byte image.
// ===========================================================================

/// `GinSetPostingTree(itup, blkno)` (ginblock.h:236): mark a leaf entry tuple as
/// pointing to a posting tree rooted at `blkno`:
/// `GinSetNPosting(itup, GIN_TREE_POSTING); ItemPointerSetBlockNumber(&t_tid,
/// blkno)`. Operates on the on-disk byte image (the `ginentrypage` carrier).
#[inline]
fn gin_set_posting_tree(itup: &mut [u8], blkno: BlockNumber) {
    // GinSetNPosting: store GIN_TREE_POSTING as the offset-number of t_tid
    // (ip_posid, bytes 4..6).
    itup[4..6].copy_from_slice(&GIN_TREE_POSTING.to_ne_bytes());
    // ItemPointerSetBlockNumber(&t_tid, blkno): bi_hi (0..2), bi_lo (2..4).
    let bi_hi = (blkno >> 16) as u16;
    let bi_lo = (blkno & 0xffff) as u16;
    itup[0..2].copy_from_slice(&bi_hi.to_ne_bytes());
    itup[2..4].copy_from_slice(&bi_lo.to_ne_bytes());
}

/// Decode the 8-byte `IndexTupleData` header from a tuple byte image.
#[inline]
fn header_of(tup: &[u8]) -> IndexTupleData {
    use types_tuple::heaptuple::BlockIdData;
    let bi_hi = u16::from_ne_bytes([tup[0], tup[1]]);
    let bi_lo = u16::from_ne_bytes([tup[2], tup[3]]);
    let ip_posid = u16::from_ne_bytes([tup[4], tup[5]]);
    let t_info = u16::from_ne_bytes([tup[6], tup[7]]);
    IndexTupleData {
        t_tid: ItemPointerData {
            ip_blkid: BlockIdData { bi_hi, bi_lo },
            ip_posid,
        },
        t_info,
    }
}

// ===========================================================================
// bufmgr / predicate helpers.
// ===========================================================================

#[inline]
fn lock_buffer(buffer: Buffer, mode: i32) -> PgResult<()> {
    bufmgr::lock_buffer::call(buffer, mode)
}

#[inline]
fn buffer_get_block_number(buffer: Buffer) -> BlockNumber {
    bufmgr::buffer_get_block_number::call(buffer)
}

/// `CheckForSerializableConflictIn(index, NULL, blkno)` — the index-page
/// predicate-lock conflict check GIN runs before modifying a leaf page.
#[inline]
fn check_for_serializable_conflict_in(index_oid: types_core::primitive::Oid, blkno: BlockNumber)
-> PgResult<()> {
    predicate::check_for_serializable_conflict_in_page::call(index_oid, blkno)
}

/// `PageGetItem(BufferGetPage(buffer), PageGetItemId(page, off))` — read the
/// IndexTuple at 1-based offset `off` on the page held by `buffer`, returned as
/// an owned byte image (the caller holds the content lock while reading).
fn get_item_at(buffer: Buffer, off: OffsetNumber) -> PgResult<Vec<u8>> {
    let mut out: Vec<u8> = Vec::new();
    bufmgr::with_buffer_page::call(buffer, &mut |page: &mut [u8]| {
        let pr = PageRef::new(page)?;
        let iid = PageGetItemId(&pr, off)?;
        out = PageGetItem(&pr, &iid)?.to_vec();
        Ok(())
    })?;
    Ok(out)
}

// ===========================================================================
// addItemPointersToLeafTuple (gininsert.c:210)
// ===========================================================================

/// `addItemPointersToLeafTuple(ginstate, old, items, nitem, buildStats, buffer)`
/// (gininsert.c:210): add `items` to the posting list of an existing leaf entry
/// `old`, or convert it to a posting tree if it would overflow. Returns the new,
/// modified leaf IndexTuple byte image. `items[]` must be sorted, dup-free.
fn addItemPointersToLeafTuple<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    old: &[u8],
    items: &[ItemPointerData],
    nitem: u32,
    mut build_stats: Option<&mut GinStatsData>,
    buffer: Buffer,
) -> PgResult<Vec<u8>> {
    debug_assert!(!GinIsPostingTree(&header_of(old)));

    let attnum = gintuple_get_attrnum(ginstate, old, mcx)?;
    let (key, category) = gintuple_get_key(ginstate, old, mcx)?;

    // merge the old and new posting lists
    let old_items = ginReadTuple(old)?;
    let old_n_posting = old_items.len();

    let mut new_n_posting: i32 = 0;
    let new_items = ginMergeItemPointers(
        items,
        nitem,
        &old_items,
        old_n_posting as u32,
        &mut new_n_posting,
    );

    // Compress the posting list, and try to build a tuple with room for it.
    // C: ginCompressPostingList(.., GinMaxItemSize, NULL) returns NULL if the
    // whole list does not fit in one segment. The ported codec packs what fits
    // and reports the packed count via `nwritten`; we treat a short pack as the
    // C NULL (does-not-fit) signal.
    let mut packed: i32 = 0;
    let compressed = ginCompressPostingList(
        &new_items,
        new_n_posting,
        GinMaxItemSize as i32,
        Some(&mut packed),
    );

    let mut res: Option<Vec<u8>> = None;
    if packed == new_n_posting {
        let dlen = size_of_gin_posting_list(&compressed.bytes);
        res = GinFormTuple(
            ginstate,
            mcx,
            attnum,
            key.clone(),
            category,
            Some(&compressed.bytes),
            dlen,
            new_n_posting,
            false,
        )?;
    }

    let res = match res {
        Some(r) => r,
        None => {
            // posting list would be too big, convert to a posting tree.
            //
            // Initialize the posting tree with the OLD tuple's posting list
            // (surely small enough for one page, already ordered + dup-free).
            let posting_root = createPostingTree(
                mcx,
                index,
                &old_items,
                build_stats.as_deref_mut(),
                buffer,
            )?;

            // Now insert the TIDs-to-be-added into the posting tree.
            ginInsertItemPointers(
                mcx,
                index,
                posting_root,
                &items[..nitem as usize],
                build_stats.as_deref_mut(),
            )?;

            // And build a new posting-tree-only result tuple.
            let mut r = GinFormTuple(ginstate, mcx, attnum, key, category, None, 0, 0, true)?
                .expect("posting-tree-only GinFormTuple never returns None (errorTooBig)");
            gin_set_posting_tree(&mut r, posting_root);
            r
        }
    };

    Ok(res)
}

// ===========================================================================
// buildFreshLeafTuple (gininsert.c:291)
// ===========================================================================

/// `buildFreshLeafTuple(ginstate, attnum, key, category, items, nitem,
/// buildStats, buffer)` (gininsert.c:291): build a fresh leaf entry tuple in
/// posting-list or posting-tree format depending on whether `items` fits.
/// `items[]` must be sorted, dup-free.
fn buildFreshLeafTuple<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    attnum: OffsetNumber,
    key: Datum<'mcx>,
    category: GinNullCategory,
    items: &[ItemPointerData],
    nitem: u32,
    build_stats: Option<&mut GinStatsData>,
    buffer: Buffer,
) -> PgResult<Vec<u8>> {
    // try to build a posting list tuple with all the items
    let mut packed: i32 = 0;
    let compressed = ginCompressPostingList(
        &items[..nitem as usize],
        nitem as i32,
        GinMaxItemSize as i32,
        Some(&mut packed),
    );

    let mut res: Option<Vec<u8>> = None;
    if packed == nitem as i32 {
        let dlen = size_of_gin_posting_list(&compressed.bytes);
        res = GinFormTuple(
            ginstate,
            mcx,
            attnum,
            key.clone(),
            category,
            Some(&compressed.bytes),
            dlen,
            nitem as i32,
            false,
        )?;
    }

    let res = match res {
        Some(r) => r,
        None => {
            // posting list would be too big, build a posting tree.
            //
            // Build the posting-tree-only result tuple first so as to fail
            // quickly if the key is too big.
            let mut r = GinFormTuple(ginstate, mcx, attnum, key, category, None, 0, 0, true)?
                .expect("posting-tree-only GinFormTuple never returns None (errorTooBig)");

            // Initialize a new posting tree with the TIDs.
            let posting_root =
                createPostingTree(mcx, index, &items[..nitem as usize], build_stats, buffer)?;

            // And save the root link in the result tuple.
            gin_set_posting_tree(&mut r, posting_root);
            r
        }
    };

    Ok(res)
}

// ===========================================================================
// ginEntryInsert (gininsert.c:341)
// ===========================================================================

/// `ginEntryInsert(ginstate, attnum, key, category, items, nitem, buildStats)`
/// (gininsert.c:341): insert one or more heap TIDs for a single key value,
/// either adding a new entry or enlarging a pre-existing one.
///
/// During an index build `build_stats` is `Some` and its counters are updated.
/// `index` is the GIN index relation (C derives it from `ginstate->index`; here
/// the descent layers take a `&Relation` handle directly).
pub fn ginEntryInsert<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    attnum: OffsetNumber,
    key: Datum<'mcx>,
    category: GinNullCategory,
    items: &[ItemPointerData],
    nitem: u32,
    mut build_stats: Option<&mut GinStatsData>,
) -> PgResult<()> {
    let mut btree = GinBtreeData::default();

    ginPrepareEntryScan(
        &mut btree,
        mcx,
        attnum,
        key.clone(),
        category,
        clone_ginstate(mcx, ginstate)?,
    );
    btree.isBuild = build_stats.is_some();

    let mut stack = ginFindLeafPage(&mut btree, mcx, false, false, index)?;

    // findItem(&btree, stack) — set by ginPrepareEntryScan.
    let found = (btree.findItem.expect("entry-tree findItem set by ginPrepareEntryScan"))(
        &mut btree, &mut stack,
    )?;

    let itup: Vec<u8>;
    let is_delete;

    if found {
        // found pre-existing entry
        let existing = get_item_at(stack.buffer, stack.off)?;

        if GinIsPostingTree(&header_of(&existing)) {
            // add entries to existing posting tree
            let root_posting_tree = GinGetDownlink(&header_of(&existing));

            // release all stack
            lock_buffer(stack.buffer, GIN_UNLOCK)?;
            freeGinBtreeStack(stack);

            // insert into posting tree
            ginInsertItemPointers(
                mcx,
                index,
                root_posting_tree,
                &items[..nitem as usize],
                build_stats.as_deref_mut(),
            )?;
            return Ok(());
        }

        check_for_serializable_conflict_in(ginstate.index, buffer_get_block_number(stack.buffer))?;
        // modify an existing leaf entry
        itup = addItemPointersToLeafTuple(
            ginstate,
            mcx,
            index,
            &existing,
            items,
            nitem,
            build_stats.as_deref_mut(),
            stack.buffer,
        )?;
        is_delete = true;
    } else {
        check_for_serializable_conflict_in(ginstate.index, buffer_get_block_number(stack.buffer))?;
        // no match, so construct a new leaf entry
        itup = buildFreshLeafTuple(
            ginstate,
            mcx,
            index,
            attnum,
            key,
            category,
            items,
            nitem,
            build_stats.as_deref_mut(),
            stack.buffer,
        )?;

        // nEntries counts leaf tuples, so increment it only when we make one.
        if let Some(bs) = build_stats.as_deref_mut() {
            bs.nEntries += 1;
        }
        is_delete = false;
    }

    // Insert the new or modified leaf tuple.
    let insertdata = GinInsertPayload::Entry(GinBtreeEntryInsertData {
        entry: itup,
        isDelete: is_delete,
        _marker: core::marker::PhantomData,
    });
    ginInsertValue(&mut btree, mcx, stack, &insertdata, build_stats, index)?;
    Ok(())
}

// ===========================================================================
// ginHeapTupleInsert / gininsert (gininsert.c:833 / 851)
// ===========================================================================

/// `ginHeapTupleInsert(ginstate, attnum, value, isNull, item)` (gininsert.c:833):
/// insert the index entries for one indexable item during normal
/// (non-fast-update) insertion.
fn ginHeapTupleInsert<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    attnum: OffsetNumber,
    value: Datum<'mcx>,
    is_null: bool,
    item: &ItemPointerData,
) -> PgResult<()> {
    let (entries, categories) = ginExtractEntries(ginstate, attnum, value, is_null, mcx)?;
    let nentries = entries.len();

    for i in 0..nentries {
        ginEntryInsert(
            ginstate,
            mcx,
            index,
            attnum,
            entries[i].clone(),
            categories[i],
            core::slice::from_ref(item),
            1,
            None,
        )?;
    }
    Ok(())
}

/// The body of `gininsert` (gininsert.c) once the per-command [`GinState`] is in
/// hand: use the fast-update pending list when enabled (F3 — seam-routed), else
/// insert each entry directly. Always returns `false` (GIN never reports a
/// unique-check result).
fn gininsert_with_state<'mcx>(
    ginstate: &GinState<'mcx>,
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
) -> PgResult<bool> {
    let natts = ginstate.tupdesc.len();

    if backend_access_gin_gininsert_seams::gin_get_use_fast_update::call(ginstate.index)? {
        // The fast-update pending-list collect+insert (ginfast.c, F3) has no
        // owner crate yet, so the whole fast path crosses the seam (loud-panic
        // until F3 lands — mirror-PG-and-panic for an unported sibling dep).
        backend_access_gin_gininsert_seams::gin_fast_insert::call(
            ginstate.index,
            values[..natts].to_vec(),
            isnull[..natts].to_vec(),
            *ht_ctid,
        )?;
    } else {
        for i in 0..natts {
            ginHeapTupleInsert(
                ginstate,
                mcx,
                index,
                (i + 1) as OffsetNumber,
                values[i].clone(),
                isnull[i],
                ht_ctid,
            )?;
        }
    }

    Ok(false)
}

/// `gininsert(index, values, isnull, ht_ctid, heapRel, checkUnique,
/// indexUnchanged, indexInfo)` (gininsert.c:851): the `aminsert` callback. Insert
/// one heap tuple's index entries. Always returns `false` (GIN never reports a
/// unique-check result).
///
/// C caches the per-command [`GinState`] in `indexInfo->ii_AmCache`, lazily
/// building it via `initGinState` on the first call. Our `IndexInfo.payload`
/// carrier is `Box<dyn Any + 'static>`, which cannot hold the `'mcx`-bound
/// `GinState`, so we rebuild it on every call (behaviour-preserving — the cache
/// is a pure per-statement performance hint and changes no on-disk state). This
/// is the same approach BRIN's `brininsert` takes for its `BrinInsertState`
/// cache.
pub fn gininsert<'mcx>(
    mcx: Mcx<'mcx>,
    index: &Relation<'mcx>,
    values: &[Datum<'mcx>],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    _heap_rel: &Relation<'mcx>,
    _check_unique: IndexUniqueCheck,
    _index_unchanged: bool,
    index_info: &mut IndexInfo,
) -> PgResult<bool> {
    let _ = index_info;

    // C: ginstate = indexInfo->ii_AmCache; if NULL, initGinState(ginstate, index).
    // Rebuilt every call (see the doc comment) instead of cached in ii_AmCache.
    let ginstate = backend_access_gin_ginutil::initGinState(index, mcx)?;

    // C creates a short-lived "Gin insert temporary context" here; in the owned
    // model the per-call scratch allocations ride `mcx`.
    gininsert_with_state(&ginstate, mcx, index, values, isnull, ht_ctid)
}

// ===========================================================================
// Deferred families (F2/F3) — sanctioned panic legs.
//
//   * ginbuild / ginbuildempty / ginBuildCallback (build driver) drive the A6
//     `table_index_build_scan` heap-scan provider — the sanctioned table-AM
//     panic leg per the GIN stage rules — and the F2 BuildAccumulator
//     (ginbulk.c). They are NOT ported here.
//   * the fast-update pending list (ginfast.c, F3) is reached only through the
//     `gin_fast_insert` / `gin_get_use_fast_update` seams above.
// ===========================================================================

/// `GIN_EXCLUSIVE` is re-exported for callers that drive the descent directly.
pub const GIN_LOCK_EXCLUSIVE: i32 = GIN_EXCLUSIVE;
