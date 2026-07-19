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
        let mut cols = [GinColState {
            opclass: GinOpclass::JsonbOps,
            elem_cmp: GinElemCmp::None,
            support_collation: ::types_core::InvalidOid,
            can_partial_match: false,
            key_byval: false,
            key_len: 0,
        }; GIN_MAX_KEY_COLS];
        for (i, c) in g.cols.iter().enumerate().take(g.natts as usize) {
            cols[i] = GinColState {
                opclass: match c.opclass {
                    0 => GinOpclass::JsonbOps,
                    1 => GinOpclass::JsonbPathOps,
                    2 => GinOpclass::TsvectorOps,
                    3 => GinOpclass::ArrayOps,
                    4 => GinOpclass::TrgmOps,
                    5 => GinOpclass::HstoreOps,
                    31 => GinOpclass::IntArrayOps,
                    other => match GinBtreeType::from_tag(other - 6) {
                        Some(ty) => GinOpclass::BtreeOps(ty),
                        None => unported(&format!("rd_amcache gin opclass tag {other}")),
                    },
                },
                elem_cmp: match c.elem_cmp {
                    0 => GinElemCmp::None,
                    1 => GinElemCmp::Int2,
                    2 => GinElemCmp::Int4,
                    3 => GinElemCmp::Int8,
                    4 => GinElemCmp::Oid,
                    5 => GinElemCmp::Text,
                    other => unported(&format!("rd_amcache gin elem_cmp tag {other}")),
                },
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
        opclass: 0,
        elem_cmp: 0,
        support_collation: ::types_core::InvalidOid,
        can_partial_match: false,
        key_byval: false,
        key_len: 0,
    }; GIN_MAX_KEY_COLS];
    for (i, col) in state.cols.iter().enumerate().take(state.natts as usize) {
        cached_cols[i] = RdAmCacheGinCol {
            opclass: match col.opclass {
                GinOpclass::JsonbOps => 0,
                GinOpclass::JsonbPathOps => 1,
                GinOpclass::TsvectorOps => 2,
                GinOpclass::ArrayOps => 3,
                GinOpclass::TrgmOps => 4,
                GinOpclass::HstoreOps => 5,
                GinOpclass::BtreeOps(ty) => 6 + ty.tag(),
                GinOpclass::IntArrayOps => 31,
            },
            elem_cmp: match col.elem_cmp {
                GinElemCmp::None => 0,
                GinElemCmp::Int2 => 1,
                GinElemCmp::Int4 => 2,
                GinElemCmp::Int8 => 3,
                GinElemCmp::Oid => 4,
                GinElemCmp::Text => 5,
            },
            support_collation: col.support_collation,
            can_partial_match: col.can_partial_match,
            key_byval: col.key_byval,
            key_len: col.key_len,
        };
    }
    rel.rd_amcache_gin.set(Some(RdAmCacheGin {
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
