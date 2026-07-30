use ::bufmgr::{
    buffer_page_ref, GetAccessStrategy, LockBuffer, ReadBufferExtended, ReleaseBuffer,
    UnlockReleaseBuffer, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK,
};
use ::gin::amcheck as ginam;
use ::gin_vocab::{
    GinPageOpaqueData, GinState, PostingItem, PostingItemGetBlockNumber, GIN_CAT_NORM_KEY,
    GIN_DATA, GIN_DELETED, GIN_LEAF, GIN_ROOT_BLKNO, MAXALIGN, SizeOfPageHeaderData,
};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::nbtree::itup::{copy_index_tuple, index_tuple_size, ITup};
use ::types_core::{catalog::GIN_AM_OID, BlockNumber, InvalidBlockNumber, OffsetNumber, Oid, BLCKSZ};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED};
use ::types_rel::Relation;
use ::types_storage::buf::BufferAccessStrategyType;
use ::types_storage::bufpage::{ItemIdData, MaxIndexTuplesPerPage, PageRef};
use ::types_storage::lock::AccessShareLock;
use ::types_storage::{buf::BufferAccessStrategy, ReadBufferMode};
use ::types_core::ForkNumber;
use ::types_tuple::itemptr::{
    FirstOffsetNumber, InvalidOffsetNumber, ItemPointerCompare, ItemPointerData, ItemPointerEquals,
    ItemPointerGetBlockNumberNoCheck, ItemPointerGetOffsetNumberNoCheck, ItemPointerIsValid,
    OffsetNumberIsValid, OffsetNumberNext,
};

struct GinEntryScanItem {
    depth: i32,
    parenttup: Option<ITup>,
    parentblk: BlockNumber,
    blkno: BlockNumber,
}

struct GinPostingScanItem {
    depth: i32,
    parentkey: ItemPointerData,
    parentblk: BlockNumber,
    blkno: BlockNumber,
}

unsafe fn copy_itup_arena(amcx: Mcx<'_>, itup: ITup) -> PgResult<ITup> {
    let buf = copy_index_tuple(amcx, itup)?;
    let p = buf.as_ptr();
    core::mem::forget(buf);
    Ok(p)
}

#[track_caller]
#[cold]
#[inline(never)]
fn corrupt(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INDEX_CORRUPTED))
}

#[inline]
fn item_pointer_set_min() -> ItemPointerData {
    ItemPointerData::new(0, 0)
}

#[inline]
fn gin_itemid_limit() -> usize {
    BLCKSZ - MAXALIGN(core::mem::size_of::<GinPageOpaqueData>())
}

#[inline]
fn line_pointer_past_end(lp_off: usize, lp_len: usize) -> bool {
    lp_off + lp_len > gin_itemid_limit()
}

pub(crate) fn gin_index_check_internal(mcx: Mcx<'_>, indrelid: Oid) -> PgResult<()> {
    crate::common::amcheck_lock_relation_and_check(
        mcx,
        indrelid,
        GIN_AM_OID,
        AccessShareLock,
        |rel, _heaprel, _readonly| gin_check_parent_keys_consistency(rel),
    )
}

unsafe fn gin_read_tuple_without_state<'a>(
    amcx: Mcx<'a>,
    itup: *const u8,
    out: &mut PgVec<'a, ItemPointerData>,
) -> PgResult<()> {
    let nipd = ginam::gin_get_nposting(itup) as usize;
    let ptr = itup.add(ginam::gin_get_posting_offset(itup));
    if ginam::gin_itup_is_compressed(itup) {
        if nipd > 0 {
            let before = out.len();
            let seglen = ginam::seg_size(core::slice::from_raw_parts(ptr, 8));
            ginam::ginPostingListDecodeAllSegments(core::slice::from_raw_parts(ptr, seglen), out)?;
            let ndecoded = out.len() - before;
            if nipd != ndecoded {
                return Err(Box::new(PgError::error(format!(
                    "number of items mismatch in GIN entry tuple, {nipd} in tuple header, {ndecoded} decoded"
                ))));
            }
        }
    } else {
        out.try_reserve(nipd).map_err(|_| amcx.oom(nipd * core::mem::size_of::<ItemPointerData>()))?;
        for i in 0..nipd {
            out.push(ptr.add(i * core::mem::size_of::<ItemPointerData>()).cast::<ItemPointerData>().read_unaligned());
        }
    }
    Ok(())
}

fn gin_check_posting_tree_parent_keys_consistency(
    rel: &Relation<'_>,
    posting_tree_root: BlockNumber,
) -> PgResult<()> {
    let strategy = GetAccessStrategy(BufferAccessStrategyType::BasBulkread);
    let arena = MemoryContext::new_bump("posting tree check context");
    let amcx = arena.mcx();

    let mut leafdepth: i32 = -1;

    let mut stack: PgVec<'_, GinPostingScanItem> = mcx::vec_new_in(amcx);
    stack.push(GinPostingScanItem {
        depth: 0,
        parentkey: ItemPointerData::invalid(),
        parentblk: InvalidBlockNumber,
        blkno: posting_tree_root,
    });

    while let Some(cur) = stack.pop() {
        ::gin::check_for_interrupts()?;

        let buffer = ReadBufferExtended(
            rel,
            ForkNumber::MAIN_FORKNUM,
            cur.blkno,
            ReadBufferMode::Normal,
            strategy.clone(),
        )?;
        LockBuffer(buffer, BUFFER_LOCK_SHARE)?;
        let page = buffer_page_ref(buffer);
        let res = check_posting_tree_page(rel, amcx, &page, &cur, &mut stack, &mut leafdepth);
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
        ReleaseBuffer(buffer)?;
        res?;
    }
    Ok(())
}

fn check_posting_tree_page(
    rel: &Relation<'_>,
    amcx: Mcx<'_>,
    page: &PageRef<'_>,
    cur: &GinPostingScanItem,
    stack: &mut PgVec<'_, GinPostingScanItem>,
    leafdepth: &mut i32,
) -> PgResult<()> {
    let bytes = ginam::page_bytes(page);
    let opaque = ginam::opaque_of(bytes);
    debug_assert!(opaque.flags & GIN_DATA != 0);

    if opaque.flags & GIN_LEAF != 0 {
        let min_item = item_pointer_set_min();

        if *leafdepth == -1 {
            *leafdepth = cur.depth;
        } else if cur.depth != *leafdepth {
            return Err(corrupt(format!(
                "index \"{}\": internal pages traversal encountered leaf page unexpectedly on block {}",
                rel.name(),
                cur.blkno
            )));
        }

        let mut list: PgVec<'_, ItemPointerData> = mcx::vec_new_in(amcx);
        ginam::gin_data_leaf_page_get_items(bytes, &min_item, &mut list)?;
        let nlist = list.len();

        if cur.parentblk != InvalidBlockNumber
            && ItemPointerGetOffsetNumberNoCheck(&cur.parentkey) != InvalidOffsetNumber
            && nlist > 0
            && ItemPointerCompare(&cur.parentkey, &list[nlist - 1]) < 0
        {
            return Err(corrupt(format!(
                "index \"{}\": tid exceeds parent's high key in postingTree leaf on block {}",
                rel.name(),
                cur.blkno
            )));
        }
        return Ok(());
    }

    let maxoff = opaque.maxoff;
    let rightlink = opaque.rightlink;

    let pd_lower = page.pd_lower();
    let lowersize = (pd_lower as usize).wrapping_sub(MAXALIGN(SizeOfPageHeaderData));
    let count = lowersize.wrapping_sub(MAXALIGN(core::mem::size_of::<ItemPointerData>()))
        / core::mem::size_of::<PostingItem>();
    if count != maxoff as usize {
        return Err(corrupt(format!(
            "index \"{}\" has unexpected pd_lower {} in posting tree block {} with maxoff {})",
            rel.name(),
            pd_lower,
            cur.blkno,
            maxoff
        )));
    }

    let bound = ginam::data_page_right_bound(bytes);
    if ItemPointerIsValid(&cur.parentkey)
        && rightlink != InvalidBlockNumber
        && !ItemPointerEquals(&cur.parentkey, &bound)
    {
        return Err(corrupt(format!(
            "index \"{}\": posting tree page's high key ({}, {}) doesn't match the downlink on block {} (parent blk {}, key ({}, {}))",
            rel.name(),
            ItemPointerGetBlockNumberNoCheck(&bound),
            ItemPointerGetOffsetNumberNoCheck(&bound),
            cur.blkno,
            cur.parentblk,
            ItemPointerGetBlockNumberNoCheck(&cur.parentkey),
            ItemPointerGetOffsetNumberNoCheck(&cur.parentkey)
        )));
    }

    let mut i = FirstOffsetNumber;
    while i <= maxoff {
        let posting_item = ginam::posting_item_at(bytes, i);

        if i == maxoff && rightlink == InvalidBlockNumber {
            if ItemPointerGetBlockNumberNoCheck(&posting_item.key) != 0
                || ItemPointerGetOffsetNumberNoCheck(&posting_item.key) != 0
            {
                return Err(corrupt(format!(
                    "index \"{}\": rightmost posting tree page (blk {}) has unexpected last key ({}, {})",
                    rel.name(),
                    cur.blkno,
                    ItemPointerGetBlockNumberNoCheck(&posting_item.key),
                    ItemPointerGetOffsetNumberNoCheck(&posting_item.key)
                )));
            }
        } else if i != FirstOffsetNumber {
            let previous_posting_item = ginam::posting_item_at(bytes, i - 1);
            if ItemPointerCompare(&posting_item.key, &previous_posting_item.key) < 0 {
                return Err(corrupt(format!(
                    "index \"{}\" has wrong tuple order in posting tree, block {}, offset {}",
                    rel.name(),
                    cur.blkno,
                    i
                )));
            }
        }

        if i == maxoff
            && ItemPointerIsValid(&cur.parentkey)
            && ItemPointerCompare(&cur.parentkey, &posting_item.key) < 0
        {
            return Err(corrupt(format!(
                "index \"{}\": posting item exceeds parent's high key in postingTree internal page on block {} offset {}",
                rel.name(),
                cur.blkno,
                i
            )));
        }

        stack.push(GinPostingScanItem {
            depth: cur.depth + 1,
            parentkey: posting_item.key,
            parentblk: cur.blkno,
            blkno: PostingItemGetBlockNumber(&posting_item),
        });

        i = OffsetNumberNext(i);
    }
    Ok(())
}

fn gin_check_parent_keys_consistency(rel: &Relation<'_>) -> PgResult<()> {
    let strategy = GetAccessStrategy(BufferAccessStrategyType::BasBulkread);
    let arena = MemoryContext::new_bump("amcheck consistency check context");
    let amcx = arena.mcx();
    let state = ::gin::build::initGinState(rel)?;

    let mut leafdepth: i32 = -1;

    let mut stack: PgVec<'_, GinEntryScanItem> = mcx::vec_new_in(amcx);
    stack.push(GinEntryScanItem {
        depth: 0,
        parenttup: None,
        parentblk: InvalidBlockNumber,
        blkno: GIN_ROOT_BLKNO,
    });

    while let Some(mut cur) = stack.pop() {
        ::gin::check_for_interrupts()?;

        let buffer = ReadBufferExtended(
            rel,
            ForkNumber::MAIN_FORKNUM,
            cur.blkno,
            ReadBufferMode::Normal,
            strategy.clone(),
        )?;
        LockBuffer(buffer, BUFFER_LOCK_SHARE)?;
        let page = buffer_page_ref(buffer);
        let res = check_entry_page(
            rel,
            &state,
            amcx,
            &strategy,
            &page,
            &mut cur,
            &mut stack,
            &mut leafdepth,
        );
        LockBuffer(buffer, BUFFER_LOCK_UNLOCK)?;
        ReleaseBuffer(buffer)?;
        res?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn check_entry_page<'a>(
    rel: &Relation<'_>,
    state: &GinState,
    amcx: Mcx<'a>,
    strategy: &BufferAccessStrategy,
    page: &PageRef<'_>,
    cur: &mut GinEntryScanItem,
    stack: &mut PgVec<'a, GinEntryScanItem>,
    leafdepth: &mut i32,
) -> PgResult<()> {
    let bytes = ginam::page_bytes(page);
    let maxoff = page.max_offset_number();
    let opaque = ginam::opaque_of(bytes);
    let rightlink = opaque.rightlink;
    let is_leaf = opaque.flags & GIN_LEAF != 0;

    check_index_page(rel, page, cur.blkno)?;

    if let Some(parenttup) = cur.parenttup {
        let mut parent_key_category = GIN_CAT_NORM_KEY;
        // SAFETY: parenttup is an owned copy of a live entry tuple.
        let parent_key = unsafe {
            ginam::gintuple_get_key(amcx, rel, state, parenttup, &mut parent_key_category)?
        };
        // SAFETY: as above.
        let parent_key_attnum = unsafe { ginam::gintuple_get_attrnum(state, parenttup) };

        let iid = page_get_item_id_careful(rel, cur.blkno, page, maxoff)?;
        // SAFETY: careful validated the line-pointer bounds.
        let idxtuple = unsafe { page.item_raw_unchecked(iid) }.0;
        // SAFETY: live tuple on the pinned + locked page.
        let page_max_key_attnum = unsafe { ginam::gintuple_get_attrnum(state, idxtuple) };
        let mut page_max_key_category = GIN_CAT_NORM_KEY;
        // SAFETY: as above.
        let page_max_key = unsafe {
            ginam::gintuple_get_key(amcx, rel, state, idxtuple, &mut page_max_key_category)?
        };

        if rightlink != InvalidBlockNumber
            && ginam::ginCompareAttEntries(
                state,
                page_max_key_attnum,
                page_max_key,
                page_max_key_category,
                parent_key_attnum,
                parent_key,
                parent_key_category,
            ) < 0
        {
            // SAFETY: parenttup is a live owned tuple copy.
            let copy = unsafe { copy_itup_arena(amcx, parenttup)? };
            stack.push(GinEntryScanItem {
                depth: cur.depth,
                parenttup: Some(copy),
                parentblk: cur.parentblk,
                blkno: rightlink,
            });
        }
    }

    if is_leaf {
        if *leafdepth == -1 {
            *leafdepth = cur.depth;
        } else if cur.depth != *leafdepth {
            return Err(corrupt(format!(
                "index \"{}\": internal pages traversal encountered leaf page unexpectedly on block {}",
                rel.name(),
                cur.blkno
            )));
        }
    }

    let mut prev_tuple: Option<ITup> = None;
    let mut prev_attnum: OffsetNumber = 0;

    let mut i = FirstOffsetNumber;
    while i <= maxoff {
        let iid = page_get_item_id_careful(rel, cur.blkno, page, i)?;
        // SAFETY: careful validated the line-pointer bounds.
        let idxtuple = unsafe { page.item_raw_unchecked(iid) }.0;
        // SAFETY: live tuple on the pinned + locked page.
        let current_attnum = unsafe { ginam::gintuple_get_attrnum(state, idxtuple) };

        // SAFETY: as above.
        if MAXALIGN(iid.lp_len() as usize) != MAXALIGN(unsafe { index_tuple_size(idxtuple) }) {
            return Err(corrupt(format!(
                "index \"{}\" has inconsistent tuple sizes, block {}, offset {}",
                rel.name(),
                cur.blkno,
                i
            )));
        }

        let mut current_key_category = GIN_CAT_NORM_KEY;
        // SAFETY: as above.
        let current_key =
            unsafe { ginam::gintuple_get_key(amcx, rel, state, idxtuple, &mut current_key_category)? };

        if i != FirstOffsetNumber && !(i == maxoff && rightlink == InvalidBlockNumber && !is_leaf) {
            let prev = prev_tuple.expect("prev_tuple set for i > First");
            let mut prev_key_category = GIN_CAT_NORM_KEY;
            // SAFETY: prev is an owned copy of the previous entry tuple.
            let prev_key =
                unsafe { ginam::gintuple_get_key(amcx, rel, state, prev, &mut prev_key_category)? };
            if ginam::ginCompareAttEntries(
                state,
                prev_attnum,
                prev_key,
                prev_key_category,
                current_attnum,
                current_key,
                current_key_category,
            ) >= 0
            {
                return Err(corrupt(format!(
                    "index \"{}\" has wrong tuple order on entry tree page, block {}, offset {}, rightlink {}",
                    rel.name(),
                    cur.blkno,
                    i,
                    rightlink
                )));
            }
        }

        if cur.parenttup.is_some() && i == maxoff {
            let parent_key_attnum;
            let parent_key;
            let parent_key_category;
            {
                let parenttup = cur.parenttup.unwrap();
                // SAFETY: owned copy of a live entry tuple.
                parent_key_attnum = unsafe { ginam::gintuple_get_attrnum(state, parenttup) };
                let mut cat = GIN_CAT_NORM_KEY;
                // SAFETY: as above.
                parent_key =
                    unsafe { ginam::gintuple_get_key(amcx, rel, state, parenttup, &mut cat)? };
                parent_key_category = cat;
            }

            if ginam::ginCompareAttEntries(
                state,
                current_attnum,
                current_key,
                current_key_category,
                parent_key_attnum,
                parent_key,
                parent_key_category,
            ) > 0
            {
                cur.parenttup = gin_refind_parent(rel, cur.parentblk, cur.blkno, strategy, amcx)?;

                match cur.parenttup {
                    None => {
                        elog_seams::ereport::call(PgError::notice(format!(
                            "Unable to find parent tuple for block {} on block {} due to concurrent split",
                            cur.blkno, cur.parentblk
                        )))?;
                    }
                    Some(parenttup) => {
                        // SAFETY: owned copy of a live entry tuple.
                        let new_attnum =
                            unsafe { ginam::gintuple_get_attrnum(state, parenttup) };
                        let mut cat = GIN_CAT_NORM_KEY;
                        // SAFETY: as above.
                        let new_key = unsafe {
                            ginam::gintuple_get_key(amcx, rel, state, parenttup, &mut cat)?
                        };
                        if ginam::ginCompareAttEntries(
                            state,
                            current_attnum,
                            current_key,
                            current_key_category,
                            new_attnum,
                            new_key,
                            cat,
                        ) > 0
                        {
                            return Err(corrupt(format!(
                                "index \"{}\" has inconsistent records on page {} offset {}",
                                rel.name(),
                                cur.blkno,
                                i
                            )));
                        }
                    }
                }
            }
        }

        if !is_leaf {
            let child_parenttup = if i == maxoff && rightlink == InvalidBlockNumber {
                None
            } else {
                // SAFETY: live tuple on the pinned + locked page.
                Some(unsafe { copy_itup_arena(amcx, idxtuple)? })
            };
            // SAFETY: as above.
            let blkno = unsafe { ginam::gin_get_downlink(idxtuple) };
            stack.push(GinEntryScanItem {
                depth: cur.depth + 1,
                parenttup: child_parenttup,
                parentblk: cur.blkno,
                blkno,
            });
        } else if
        // SAFETY: live leaf tuple on the pinned + locked page.
        unsafe { ginam::gin_is_posting_tree(idxtuple) } {
            // SAFETY: as above.
            let root_posting_tree = unsafe { ginam::gin_get_posting_tree(idxtuple) };
            gin_check_posting_tree_parent_keys_consistency(rel, root_posting_tree)?;
        } else {
            let mut ipd: PgVec<'a, ItemPointerData> = mcx::vec_new_in(amcx);
            // SAFETY: as above.
            unsafe { gin_read_tuple_without_state(amcx, idxtuple, &mut ipd)? };
            for j in 0..ipd.len() {
                if !OffsetNumberIsValid(ItemPointerGetOffsetNumberNoCheck(&ipd[j])) {
                    return Err(corrupt(format!(
                        "index \"{}\": posting list contains invalid heap pointer on block {}",
                        rel.name(),
                        cur.blkno
                    )));
                }
            }
        }

        // SAFETY: live tuple on the pinned + locked page.
        prev_tuple = Some(unsafe { copy_itup_arena(amcx, idxtuple)? });
        prev_attnum = current_attnum;

        i = OffsetNumberNext(i);
    }
    Ok(())
}

fn check_index_page(rel: &Relation<'_>, page: &PageRef<'_>, block_no: BlockNumber) -> PgResult<()> {
    if page.is_new() {
        return Err(Box::new(
            PgError::error(format!(
                "index \"{}\" contains unexpected zero page at block {}",
                rel.name(),
                block_no
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint("Please REINDEX it."),
        ));
    }

    if BLCKSZ - page.pd_special() as usize != MAXALIGN(core::mem::size_of::<GinPageOpaqueData>()) {
        return Err(Box::new(
            PgError::error(format!(
                "index \"{}\" contains corrupted page at block {}",
                rel.name(),
                block_no
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_hint("Please REINDEX it."),
        ));
    }

    let opaque = ginam::opaque_of(ginam::page_bytes(page));
    if opaque.flags & GIN_DELETED != 0 {
        if opaque.flags & GIN_LEAF == 0 {
            return Err(corrupt(format!(
                "index \"{}\" has deleted internal page {}",
                rel.name(),
                block_no
            )));
        }
        if page.max_offset_number() > InvalidOffsetNumber {
            return Err(corrupt(format!(
                "index \"{}\" has deleted page {} with tuples",
                rel.name(),
                block_no
            )));
        }
    } else if page.max_offset_number() as usize > MaxIndexTuplesPerPage {
        return Err(corrupt(format!(
            "index \"{}\" has page {} with exceeding count of tuples",
            rel.name(),
            block_no
        )));
    }
    Ok(())
}

fn gin_refind_parent<'a>(
    rel: &Relation<'_>,
    parentblkno: BlockNumber,
    childblkno: BlockNumber,
    strategy: &BufferAccessStrategy,
    amcx: Mcx<'a>,
) -> PgResult<Option<ITup>> {
    let parentbuf = ReadBufferExtended(
        rel,
        ForkNumber::MAIN_FORKNUM,
        parentblkno,
        ReadBufferMode::Normal,
        strategy.clone(),
    )?;
    LockBuffer(parentbuf, BUFFER_LOCK_SHARE)?;
    let parentpage = buffer_page_ref(parentbuf);

    if ginam::opaque_of(ginam::page_bytes(&parentpage)).flags & GIN_LEAF != 0 {
        UnlockReleaseBuffer(parentbuf)?;
        return Ok(None);
    }

    let parent_maxoff = parentpage.max_offset_number();
    let mut out: PgResult<Option<ITup>> = Ok(None);
    let mut o = FirstOffsetNumber;
    while o <= parent_maxoff {
        match page_get_item_id_careful(rel, parentblkno, &parentpage, o) {
            Err(e) => {
                out = Err(e);
                break;
            }
            Ok(p_iid) => {
                // SAFETY: careful validated the line-pointer bounds.
                let itup = unsafe { parentpage.item_raw_unchecked(p_iid) }.0;
                // SAFETY: live tuple on the pinned + locked parent page.
                if unsafe { ginam::gin_get_downlink(itup) } == childblkno {
                    // SAFETY: as above.
                    out = unsafe { copy_itup_arena(amcx, itup) }.map(Some);
                    break;
                }
            }
        }
        o = OffsetNumberNext(o);
    }

    UnlockReleaseBuffer(parentbuf)?;
    out
}

fn page_get_item_id_careful(
    rel: &Relation<'_>,
    block: BlockNumber,
    page: &PageRef<'_>,
    offset: OffsetNumber,
) -> PgResult<ItemIdData> {
    let itemid = page.item_id(offset);

    if line_pointer_past_end(itemid.lp_off() as usize, itemid.lp_len() as usize) {
        return Err(Box::new(
            PgError::error(format!(
                "line pointer points past end of tuple space in index \"{}\"",
                rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index tid=({block},{offset}) lp_off={}, lp_len={} lp_flags={}.",
                itemid.lp_off(),
                itemid.lp_len(),
                itemid.lp_flags()
            )),
        ));
    }

    if itemid.is_redirected() || !itemid.is_used() || itemid.is_dead() || itemid.lp_len() == 0 {
        return Err(Box::new(
            PgError::error(format!(
                "invalid line pointer storage in index \"{}\"",
                rel.name()
            ))
            .with_sqlstate(ERRCODE_INDEX_CORRUPTED)
            .with_detail(format!(
                "Index tid=({block},{offset}) lp_off={}, lp_len={} lp_flags={}.",
                itemid.lp_off(),
                itemid.lp_len(),
                itemid.lp_flags()
            )),
        ));
    }

    Ok(itemid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_pointer_set_min_is_zero() {
        let p = item_pointer_set_min();
        assert_eq!(ItemPointerGetBlockNumberNoCheck(&p), 0);
        assert_eq!(ItemPointerGetOffsetNumberNoCheck(&p), 0);
        assert!(ItemPointerCompare(&p, &ItemPointerData::new(0, 1)) < 0);
    }

    #[test]
    fn itemid_limit_is_blcksz_minus_opaque() {
        assert_eq!(gin_itemid_limit(), BLCKSZ - 8);
    }

    #[test]
    fn line_pointer_bounds() {
        let limit = gin_itemid_limit();
        assert!(!line_pointer_past_end(limit, 0));
        assert!(!line_pointer_past_end(limit - 1, 1));
        assert!(!line_pointer_past_end(24, limit - 24));
        assert!(line_pointer_past_end(limit, 1));
        assert!(line_pointer_past_end(limit - 1, 2));
    }
}
