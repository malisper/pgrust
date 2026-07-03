//! gininsert.c, serial half: ginbuild (accumulate + dump), gininsert (pending
//! list by default), ginEntryInsert. Parallel build and ginbuildempty
//! (unlogged indexes) are loud.

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext};
use ::types_core::Buffer;
use ::types_error::PgResult;
use ::types_rel::{RdAmCacheGin, Relation};
use ::types_tuple::itemptr::ItemPointerData;

use crate::btree::{ginFindLeafPage, ginInsertValue};
use crate::bulk::BuildAccumulator;
use crate::datapage::{createPostingTree, ginInsertItemPointers};
use crate::entrypage::{
    gin_get_posting_tree, gin_is_posting_tree, gin_set_posting_tree, ginReadTuple, EntryBtree,
    EntryPayload, GinFormTuple,
};
use crate::postinglist::{ginCompressPostingList, ginMergeItemPointers};
use crate::util::{gin_use_fastupdate, ginExtractEntries, initGinState};
use crate::{page_ref, unported, GinPageIsLeaf, GIN_UNLOCK};

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
            f(ctx.mcx())
        }
        Err(_) => {
            let ctx = MemoryContext::new_bump("gin insert scratch (reentrant)");
            f(ctx.mcx())
        }
    })
}

/// initGinState through the relcache rd_amcache slot (rule 5; C caches per
/// statement in ii_AmCache, the relcache slot has the same invalidation).
pub(crate) fn cached_gin_state(rel: &Relation<'_>) -> PgResult<GinState> {
    if let Some(g) = rel.rd_amcache_gin.get() {
        return Ok(GinState {
            opclass: match g.opclass {
                0 => GinOpclass::JsonbOps,
                other => unported(&format!("rd_amcache gin opclass tag {other}")),
            },
            support_collation: g.support_collation,
            can_partial_match: g.can_partial_match,
            key_byval: g.key_byval,
            key_len: g.key_len,
        });
    }
    let state = initGinState(rel)?;
    rel.rd_amcache_gin.set(Some(RdAmCacheGin {
        opclass: match state.opclass {
            GinOpclass::JsonbOps => 0,
        },
        support_collation: state.support_collation,
        can_partial_match: state.can_partial_match,
        key_byval: state.key_byval,
        key_len: state.key_len,
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
    let (key, category, old_items) = unsafe {
        debug_assert!(!gin_is_posting_tree(old));
        let mut category = GIN_CAT_NORM_KEY;
        let key = crate::entrypage::gintuple_get_key(rel, old, &mut category);
        let mut old_items = mcx::vec_new_in(mcx);
        ginReadTuple(mcx, old, &mut old_items)?;
        (key, category, old_items)
    };

    let new_items = ginMergeItemPointers(mcx, items, old_items.as_slice())?;

    let (compressed, npacked) =
        ginCompressPostingList(mcx, new_items.as_slice(), GinMaxItemSize)?;
    if npacked == new_items.len() {
        if let Some(res) = GinFormTuple(
            mcx,
            rel,
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
    let mut res = GinFormTuple(mcx, rel, key, category, &[], 0, 0, true)?.expect("errorTooBig");
    // SAFETY: owned tuple image.
    unsafe { gin_set_posting_tree(res.as_mut_ptr(), posting_root) };
    Ok(res)
}

/// buildFreshLeafTuple.
fn buildFreshLeafTuple<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    key: Datum,
    category: GinNullCategory,
    items: &[ItemPointerData],
    buildStats: Option<&mut GinStatsData>,
    buffer: Buffer,
) -> PgResult<::nbtree::itup::ItupBuf<'s>> {
    let _ = state;
    let (compressed, npacked) = ginCompressPostingList(mcx, items, GinMaxItemSize)?;
    if npacked == items.len() {
        if let Some(res) = GinFormTuple(
            mcx,
            rel,
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

    let mut res = GinFormTuple(mcx, rel, key, category, &[], 0, 0, true)?.expect("errorTooBig");
    let posting_root = createPostingTree(mcx, rel, items, buildStats, buffer)?;
    // SAFETY: owned tuple image.
    unsafe { gin_set_posting_tree(res.as_mut_ptr(), posting_root) };
    Ok(res)
}

/// entryLocateLeafEntry: binary search on the (locked) leaf.
pub(crate) fn entry_locate_leaf_pub(
    btree: &EntryBtree<'_, '_, '_>,
    buffer: Buffer,
) -> (bool, ::types_core::OffsetNumber) {
    use ::types_tuple::itemptr::FirstOffsetNumber;
    // SAFETY: pin + lock held.
    let page = unsafe { page_ref(buffer) };
    debug_assert!(GinPageIsLeaf(&crate::page_opaque(&page)));

    let mut low = FirstOffsetNumber;
    let mut high = page.max_offset_number();
    if high < low {
        return (false, FirstOffsetNumber);
    }
    high += 1;
    while high > low {
        let mid = low + (high - low) / 2;
        let id = page.item_id(mid);
        let itup = page.item_raw(id).0;
        let result = btree.compare_to(itup);
        if result == 0 {
            return (true, mid);
        } else if result > 0 {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    (false, high)
}

/// ginEntryInsert.
pub fn ginEntryInsert<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    key: Datum,
    category: GinNullCategory,
    items: &[ItemPointerData],
    mut buildStats: Option<&mut GinStatsData>,
) -> PgResult<()> {
    let mut btree = EntryBtree::new(rel, state, key, category, mcx);
    btree.is_build = buildStats.is_some();

    let mut stack = ginFindLeafPage(mcx, rel, &mut btree, false, false)?;
    let buffer = stack.top().buffer;

    let (found, off) = entry_locate_leaf_pub(&btree, buffer);
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

/// ginbuildempty: unlogged-index INIT_FORKNUM lane, loud.
pub fn ginbuildempty() -> ! {
    unported("ginbuildempty (unlogged GIN index INIT_FORKNUM)")
}

/// ginHeapTupleInsert.
fn ginHeapTupleInsert<'s>(
    mcx: Mcx<'s>,
    rel: &Relation<'_>,
    state: &GinState,
    value: Datum,
    is_null: bool,
    item: &ItemPointerData,
) -> PgResult<()> {
    let (entries, categories) = ginExtractEntries(mcx, state, value, is_null)?;
    for (i, key) in entries.iter().enumerate() {
        ginEntryInsert(
            mcx,
            rel,
            state,
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
            crate::fast::ginHeapTupleFastCollect(
                scratch,
                rel,
                &state,
                &mut collector,
                values[0],
                isnull[0],
                ht_ctid,
            )?;
            crate::fast::ginHeapTupleFastInsert(scratch, rel, &state, &mut collector)?;
        } else {
            ginHeapTupleInsert(scratch, rel, &state, values[0], isnull[0], ht_ctid)?;
        }
        Ok(())
    })?;

    Ok(false)
}
