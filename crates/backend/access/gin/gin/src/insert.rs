//! gininsert.c, serial half: ginbuild (accumulate + dump), gininsert (pending
//! list by default), ginEntryInsert. Parallel build is loud; ginbuildempty
//! lives in the ginbuild crate.

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext};
use ::types_core::{Buffer, OffsetNumber};
use ::types_error::PgResult;
use ::types_rel::{RdAmCacheGin, RdAmCacheGinCol, Relation};
use ::types_tuple::itemptr::ItemPointerData;

use crate::btree::{ginFindLeafPage, ginInsertValue};
use crate::datapage::{createPostingTree, ginInsertItemPointers};
use crate::entrypage::{
    gin_get_posting_tree, gin_is_posting_tree, gin_set_posting_tree, ginReadTuple, EntryBtree,
    EntryPayload, GinFormTuple,
};
use crate::postinglist::{ginCompressPostingList, ginMergeItemPointers};
use crate::util::{gin_use_fastupdate, ginExtractEntries, initGinState};
use crate::{page_ref, GinPageIsLeaf, GIN_UNLOCK};

use std::cell::RefCell;

thread_local! {
    static GIN_INSERT_SCRATCH: RefCell<MemoryContext> =
        RefCell::new(MemoryContext::new_bump("gin insert scratch"));
}

pub(crate) fn with_insert_scratch<R>(
    f: impl for<'s> FnOnce(Mcx<'s>) -> PgResult<R>,
) -> PgResult<R> {
    GIN_INSERT_SCRATCH.with(|cell| match cell.try_borrow_mut() {
        Ok(mut ctx) => {
            ctx.reset();
            // gininsert.c:906 MemoryContextDelete(insertCtx): nothing from
            // this insertion outlives the call.
            let res = f(ctx.mcx());
            ctx.reset();
            res
        }
        Err(_) => {
            let ctx = MemoryContext::new_bump("gin insert scratch (reentrant)");
            f(ctx.mcx())
        }
    })
}

#[cfg(test)]
pub(crate) fn insert_scratch_used() -> usize {
    GIN_INSERT_SCRATCH.with(|cell| cell.borrow().used())
}

// INVARIANT: rd_amcache tags round-trip with the encoder in
// cached_gin_state; rd_amcache is process-local, so an unknown tag means
// memory corruption, not an unported case.
#[cold]
fn amcache_corrupt(slot: &str, tag: u8) -> ! {
    panic!("rd_amcache gin {slot} tag {tag} not produced by the encoder (insert.rs cached_gin_state); process-local cache is corrupt")
}

/// An optional support-proc slot's tag: 0 is None.
fn amcache_optional<T>(slot: &str, tag: u8, from_tag: fn(u8) -> Option<T>) -> Option<T> {
    match tag {
        0 => None,
        t => Some(from_tag(t).unwrap_or_else(|| amcache_corrupt(slot, t))),
    }
}

/// initGinState through the relcache rd_amcache slot (rule 5; C caches per
/// statement in ii_AmCache, the relcache slot has the same invalidation).
pub(crate) fn cached_gin_state(rel: &Relation<'_>) -> PgResult<GinState> {
    // Copy out so no RefCell borrow is held across the state build.
    let cached: Option<RdAmCacheGin> = rel.rd_amcache_gin.borrow().as_deref().copied();
    if let Some(g) = cached {
        let mut cols = [GinColState::array_ops(GinCompareFn::Int4, true, 4); GIN_MAX_KEY_COLS];
        for (i, c) in g.cols.iter().enumerate().take(g.natts as usize) {
            cols[i] = GinColState {
                compare: GinCompareFn::from_tag(c.compare, c.compare_proc)
                    .unwrap_or_else(|| amcache_corrupt("compare", c.compare)),
                extract_value: GinExtractValueFn::from_tag(c.extract_value)
                    .unwrap_or_else(|| amcache_corrupt("extract_value", c.extract_value)),
                extract_query: GinExtractQueryFn::from_tag(c.extract_query)
                    .unwrap_or_else(|| amcache_corrupt("extract_query", c.extract_query)),
                consistent: amcache_optional("consistent", c.consistent, GinConsistentFn::from_tag),
                tri_consistent: amcache_optional(
                    "tri_consistent",
                    c.tri_consistent,
                    GinTriConsistentFn::from_tag,
                ),
                compare_partial: amcache_optional(
                    "compare_partial",
                    c.compare_partial,
                    GinComparePartialFn::from_tag,
                ),
                support_collation: c.support_collation,
                can_partial_match: c.can_partial_match,
                key_byval: c.key_byval,
                key_len: c.key_len,
            };
        }
        return Ok(GinState {
            natts: g.natts,
            one_col: g.natts == 1,
            cols,
        });
    }
    let state = initGinState(rel)?;
    let mut cached_cols = [RdAmCacheGinCol {
        compare: 0,
        compare_proc: ::types_core::InvalidOid,
        extract_value: 0,
        extract_query: 0,
        consistent: 0,
        tri_consistent: 0,
        compare_partial: 0,
        support_collation: ::types_core::InvalidOid,
        can_partial_match: false,
        key_byval: false,
        key_len: 0,
    }; GIN_MAX_KEY_COLS];
    for (i, col) in state.cols.iter().enumerate().take(state.natts as usize) {
        cached_cols[i] = RdAmCacheGinCol {
            compare: col.compare.tag(),
            compare_proc: match col.compare {
                GinCompareFn::Fmgr(cmp_proc) => cmp_proc,
                _ => ::types_core::InvalidOid,
            },
            extract_value: col.extract_value.tag(),
            extract_query: col.extract_query.tag(),
            consistent: col.consistent.map_or(0, GinConsistentFn::tag),
            tri_consistent: col.tri_consistent.map_or(0, GinTriConsistentFn::tag),
            compare_partial: col.compare_partial.map_or(0, GinComparePartialFn::tag),
            support_collation: col.support_collation,
            can_partial_match: col.can_partial_match,
            key_byval: col.key_byval,
            key_len: col.key_len,
        };
    }
    *rel.rd_amcache_gin.borrow_mut() = Some(Box::new(RdAmCacheGin {
        natts: state.natts,
        cols: cached_cols,
    }));
    Ok(state)
}

/// addItemPointersToLeafTuple.
fn addItemPointersToLeafTuple<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    old: crate::entrypage::ITup,
    items: &[ItemPointerData],
    mut buildStats: Option<&mut GinStatsData>,
    buffer: Buffer,
) -> PgResult<::nbtree::itup::ItupBuf<'s>> {
    // SAFETY: pin + exclusive lock held on the entry leaf.
    let (attnum, key, category, old_items) = unsafe {
        debug_assert!(!gin_is_posting_tree(old));
        let attnum = crate::entrypage::gintuple_get_attrnum(state, old);
        let mut category = GIN_CAT_NORM_KEY;
        let key = crate::entrypage::gintuple_get_key(mcx, rel, state, old, &mut category)?;
        let mut old_items = mcx::vec_new_in(mcx);
        ginReadTuple(mcx, old, &mut old_items)?;
        (attnum, key, category, old_items)
    };

    let new_items = ginMergeItemPointers(mcx, items, old_items.as_slice())?;

    let (compressed, npacked) =
        ginCompressPostingList(mcx, new_items.as_slice(), GinMaxItemSize)?;
    if npacked == new_items.len() {
        if let Some(res) = GinFormTuple(
            mcx,
            rel,
            state,
            attnum,
            key,
            category,
            &compressed,
            compressed.len(),
            new_items.len(),
            false,
        )? {
            return Ok(res);
        }
    }

    // Posting list too big: convert to a posting tree.
    let posting_root = createPostingTree(
        mcx,
        rel,
        old_items.as_slice(),
        buildStats.as_deref_mut(),
        buffer,
    )?;
    ginInsertItemPointers(mcx, rel, posting_root, items, buildStats)?;
    let mut res =
        GinFormTuple(mcx, rel, state, attnum, key, category, &[], 0, 0, true)?.expect("errorTooBig");
    // SAFETY: owned tuple image.
    unsafe { gin_set_posting_tree(res.as_mut_ptr(), posting_root) };
    Ok(res)
}

/// buildFreshLeafTuple.
fn buildFreshLeafTuple<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    items: &[ItemPointerData],
    buildStats: Option<&mut GinStatsData>,
    buffer: Buffer,
) -> PgResult<::nbtree::itup::ItupBuf<'s>> {
    let (compressed, npacked) = ginCompressPostingList(mcx, items, GinMaxItemSize)?;
    if npacked == items.len() {
        if let Some(res) = GinFormTuple(
            mcx,
            rel,
            state,
            attnum,
            key,
            category,
            &compressed,
            compressed.len(),
            items.len(),
            false,
        )? {
            return Ok(res);
        }
    }

    let mut res =
        GinFormTuple(mcx, rel, state, attnum, key, category, &[], 0, 0, true)?.expect("errorTooBig");
    let posting_root = createPostingTree(mcx, rel, items, buildStats, buffer)?;
    // SAFETY: owned tuple image.
    unsafe { gin_set_posting_tree(res.as_mut_ptr(), posting_root) };
    Ok(res)
}

/// entryLocateLeafEntry: binary search on the (locked) leaf.
pub(crate) fn entry_locate_leaf_pub(
    btree: &EntryBtree<'_, '_, '_>,
    buffer: Buffer,
) -> PgResult<(bool, ::types_core::OffsetNumber)> {
    use ::types_tuple::itemptr::FirstOffsetNumber;
    // SAFETY: pin + lock held.
    let page = unsafe { page_ref(buffer) };
    debug_assert!(GinPageIsLeaf(&crate::page_opaque(&page)));

    let mut low = FirstOffsetNumber;
    let mut high = page.max_offset_number();
    if high < low {
        return Ok((false, FirstOffsetNumber));
    }
    high += 1;
    while high > low {
        let mid = low + (high - low) / 2;
        let id = page.item_id(mid);
        let itup = page.item_raw(id).0;
        let result = btree.compare_to(itup)?;
        if result == 0 {
            return Ok((true, mid));
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    Ok((false, high))
}

/// ginEntryInsert.
pub fn ginEntryInsert<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    attnum: OffsetNumber,
    key: Datum,
    category: GinNullCategory,
    items: &[ItemPointerData],
    mut buildStats: Option<&mut GinStatsData>,
) -> PgResult<()> {
    let mut btree = EntryBtree::new(rel, state, attnum, key, category, mcx);
    btree.is_build = buildStats.is_some();

    let mut stack = ginFindLeafPage(mcx, rel, &mut btree, false, false)?;
    let buffer = stack.top().buffer;

    let (found, off) = entry_locate_leaf_pub(&btree, buffer)?;
    stack.top_mut().off = off;

    let mut is_delete = false;
    let itup;
    if found {
        // SAFETY: pin + exclusive lock held.
        let old = {
            let page = unsafe { page_ref(buffer) };
            let id = page.item_id(off);
            page.item_raw(id).0
        };
        // SAFETY: as above.
        if unsafe { gin_is_posting_tree(old) } {
            // SAFETY: as above.
            let root = unsafe { gin_get_posting_tree(old) };
            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
            crate::btree::free_stack(&stack, stack.top)?;
            return ginInsertItemPointers(mcx, rel, root, items, buildStats);
        }
        predicate_seams::check_for_serializable_conflict_in::call(
            rel,
            None,
            bm::buffer_get_block_number::call(buffer),
        )?;
        itup = addItemPointersToLeafTuple(
            mcx,
            rel,
            state,
            old,
            items,
            buildStats.as_deref_mut(),
            buffer,
        )?;
        is_delete = true;
    } else {
        predicate_seams::check_for_serializable_conflict_in::call(
            rel,
            None,
            bm::buffer_get_block_number::call(buffer),
        )?;
        itup = buildFreshLeafTuple(
            mcx,
            rel,
            state,
            attnum,
            key,
            category,
            items,
            buildStats.as_deref_mut(),
            buffer,
        )?;
        if let Some(stats) = buildStats.as_deref_mut() {
            stats.nEntries += 1;
        }
    }

    btree.payload = Some(EntryPayload {
        entry: itup,
        is_delete,
    });
    ginInsertValue(mcx, rel, &mut btree, &mut stack, buildStats)
}

/// ginHeapTupleInsert.
fn ginHeapTupleInsert<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    attnum: OffsetNumber,
    value: Datum,
    is_null: bool,
    item: &ItemPointerData,
) -> PgResult<()> {
    let (entries, categories) = ginExtractEntries(mcx, state, attnum, value, is_null)?;
    for (i, key) in entries.iter().enumerate() {
        ginEntryInsert(
            mcx,
            rel,
            state,
            attnum,
            *key,
            categories[i],
            core::slice::from_ref(item),
            None,
        )?;
    }
    Ok(())
}

/// gininsert.
pub fn gininsert<'mcx>(
    _mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    values: &[Datum],
    isnull: &[bool],
    ht_ctid: &ItemPointerData,
    heapRel: &Relation<'mcx>,
) -> PgResult<bool> {
    let _ = heapRel;
    let state = cached_gin_state(rel)?;

    with_insert_scratch(|scratch| {
        if gin_use_fastupdate(rel) {
            let mut collector = crate::fast::GinTupleCollector::new(scratch);
            for i in 0..state.natts as usize {
                crate::fast::ginHeapTupleFastCollect(
                    scratch,
                    rel,
                    &state,
                    &mut collector,
                    (i + 1) as ::types_core::OffsetNumber,
                    values[i],
                    isnull[i],
                    ht_ctid,
                )?;
            }
            crate::fast::ginHeapTupleFastInsert(scratch, rel, &state, &mut collector)?;
        } else {
            for i in 0..state.natts as usize {
                ginHeapTupleInsert(
                    scratch,
                    rel,
                    &state,
                    (i + 1) as ::types_core::OffsetNumber,
                    values[i],
                    isnull[i],
                    ht_ctid,
                )?;
            }
        }
        Ok(())
    })?;

    Ok(false)
}
