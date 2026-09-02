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
    rel: &Relation<'_>,
    itup: *const u8,
    out: &mut PgVec<'a, ItemPointerData>,
) -> PgResult<()> {
    let nipd = ginam::gin_get_nposting(itup) as usize;
    let posting_offset = ginam::gin_get_posting_offset(itup);

    // The posting offset (31 bits) and posting count (16 bits) come straight
    // from the on-disk t_tid bytes, so on a corrupt page (which is exactly what
    // amcheck exists to detect) they cannot be trusted to point inside the
    // tuple. The index tuple size was already validated against the line-pointer
    // length by the caller (see the IndexTupleSize check in check_entry_page,
    // matching verify_gin.c), so treat it as the trustworthy upper bound and
    // confirm the posting data lies fully within it before deriving any
    // raw-pointer reads. Report a violation as index corruption rather than
    // reading out of bounds.
    let itupsize = index_tuple_size(itup);
    if posting_offset > itupsize {
        return Err(corrupt(format!(
            "index \"{}\": GIN entry tuple posting offset {} exceeds tuple size {}",
            rel.name(),
            posting_offset,
            itupsize
        )));
    }
    let avail = itupsize - posting_offset;
    let ptr = itup.add(posting_offset);
    if ginam::gin_itup_is_compressed(itup) {
        if nipd > 0 {
            // Need a full posting-list segment header before seg_size can read
            // the declared segment length out of it.
            if avail < 8 {
                return Err(corrupt(format!(
                    "index \"{}\": GIN entry tuple posting list header runs past tuple end (offset {}, tuple size {})",
                    rel.name(),
                    posting_offset,
                    itupsize
                )));
            }
            let seglen = ginam::seg_size(core::slice::from_raw_parts(ptr, 8));
            if seglen > avail {
                return Err(corrupt(format!(
                    "index \"{}\": GIN entry tuple posting list (offset {}, length {}) runs past tuple size {}",
                    rel.name(),
                    posting_offset,
                    seglen,
                    itupsize
                )));
            }
            let before = out.len();
            ginam::ginPostingListDecodeAllSegments(core::slice::from_raw_parts(ptr, seglen), out)?;
            let ndecoded = out.len() - before;
            if nipd != ndecoded {
                return Err(Box::new(PgError::error(format!(
                    "number of items mismatch in GIN entry tuple, {nipd} in tuple header, {ndecoded} decoded"
                ))));
            }
        }
    } else {
        let needed = nipd * core::mem::size_of::<ItemPointerData>();
        if needed > avail {
            return Err(corrupt(format!(
                "index \"{}\": GIN entry tuple posting list ({} items, offset {}) runs past tuple size {}",
                rel.name(),
                nipd,
                posting_offset,
                itupsize
            )));
        }
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

    // upstream 1f8ab91c11eb (18.6): amcheck: Fix memory leak with
    // gin_index_check(). C pfree()s the per-tuple posting-list buffer each
    // iteration; the bump arena has no free, so one reusable buffer is
    // cleared and refilled for every leaf tuple across the whole walk.
    let mut ipd: PgVec<'_, ItemPointerData> = mcx::vec_new_in(amcx);

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
            &mut ipd,
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
    ipd: &mut PgVec<'a, ItemPointerData>,
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
            // SAFETY: prev points at a tuple on the page, which stays pinned
            // + locked for the whole walk.
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
            ipd.clear();
            // SAFETY: as above.
            unsafe { gin_read_tuple_without_state(amcx, rel, idxtuple, ipd)? };
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

        // upstream 1f8ab91c11eb (18.6): the page stays pinned and locked for
        // the whole check_entry_page walk, so prev_tuple borrows the previous
        // offset's tuple in place instead of copying it into the arena (which
        // C pfree()s every iteration; the bump arena cannot).
        prev_tuple = Some(idxtuple);
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
    use ::datum::Datum;
    use ::nbtree::itup::{index_form_tuple, set_t_info, set_t_tid, t_info, ItupBuf, INDEX_SIZE_MASK};
    use ::types_storage::bufpage::PageMut;

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

    fn int4_gin_rel(mcx: Mcx<'_>) -> Relation<'_> {
        use ::types_core::catalog::INT4OID;
        use ::types_core::{INVALID_PROC_NUMBER, RELPERSISTENCE_PERMANENT};
        use ::types_rel::{
            FormData_pg_class, LockInfoData, LockRelId, RelationData, RELKIND_INDEX,
            REPLICA_IDENTITY_DEFAULT,
        };
        use ::types_tuple::tupdesc::CompactAttribute;
        use ::types_tuple::{FormData_pg_attribute, NameData, TupleDescData};
        use core::cell::Cell;
        use std::rc::Rc;

        let mut relname = NameData::default();
        relname.namestrcpy("t_gin_idx");
        let mut attrs = ::mcx::PgVec::new_in(mcx);
        attrs.push(FormData_pg_attribute {
            atttypid: INT4OID,
            attlen: 4,
            attnum: 1,
            atttypmod: -1,
            attbyval: true,
            attalign: b'i' as i8,
            attstorage: b'p' as i8,
            attislocal: true,
            ..Default::default()
        });
        let mut compact = ::mcx::PgVec::new_in(mcx);
        compact.push(CompactAttribute::populate_from(&attrs[0]));
        let one = |v: Oid| {
            let mut vec = ::mcx::PgVec::new_in(mcx);
            vec.push(v);
            vec
        };
        let mut indoption = ::mcx::PgVec::new_in(mcx);
        indoption.push(0i16);
        let data = RelationData {
            rd_locator: Cell::new(::types_storage::RelFileLocator::new(1663, 5, 5001)),
            rd_smgr: Default::default(),
            rd_id: 5001,
            rd_backend: INVALID_PROC_NUMBER,
            rd_islocaltemp: false,
            rd_isvalid: Cell::new(true),
            rd_createSubid: Cell::new(0),
            rd_newRelfilelocatorSubid: Cell::new(0),
            rd_firstRelfilelocatorSubid: Cell::new(0),
            rd_droppedSubid: Cell::new(0),
            rd_lockInfo: LockInfoData {
                lockRelId: LockRelId { relId: 5001, dbId: 5 },
            },
            rd_rel: FormData_pg_class {
                relname,
                relnamespace: 2200,
                reltype: 0,
                relowner: 10,
                relam: GIN_AM_OID,
                relfilenode: 5001,
                reltablespace: 0,
                relpages: 0,
                reltuples: -1.0,
                relallvisible: 0,
                reltoastrelid: 0,
                relhasindex: false,
                relisshared: false,
                relpersistence: RELPERSISTENCE_PERMANENT,
                relkind: RELKIND_INDEX,
                relhassubclass: false,
                relrowsecurity: false,
                relispopulated: true,
                relreplident: REPLICA_IDENTITY_DEFAULT,
                relispartition: false,
                relfrozenxid: 3,
                relminmxid: 1,
            },
            rd_att: Rc::new(TupleDescData {
                natts: 1,
                tdtypeid: 0,
                tdtypmod: -1,
                tdrefcount: 1,
                constr: None,
                compact_attrs: compact,
                attrs,
            }),
            rd_index: None,
            rd_opcintype: one(INT4OID),
            rd_opfamily: one(2745),
            rd_indoption: indoption,
            rd_indcollation: one(0),
            rd_options: None,
            pgstat_enabled: Cell::new(false),
            pgstat_link: Cell::new((0, core::ptr::null_mut())),
            rd_amcache: Default::default(),
            rd_amcache_hash: Default::default(),
            rd_amcache_gin: Default::default(),
            rd_amcache_spgist: Default::default(),
            rd_support: ::mcx::PgVec::new_in(mcx),
            rd_supportinfo: Default::default(),
            rd_opcoptions: Default::default(),
            rd_indexlist: Default::default(),
            rd_trigdesc: Default::default(),
            rd_hastriggers: false,
            rd_hasrules: false,
        };
        Relation::open(data, None)
    }

    fn int4_array_gin_state() -> GinState {
        use ::gin_vocab::{GinColState, GinElemCmp, GinOpclass, GIN_MAX_KEY_COLS};
        let col = GinColState {
            opclass: GinOpclass::ArrayOps,
            elem_cmp: GinElemCmp::Int4,
            support_collation: ::types_core::primitive::InvalidOid,
            can_partial_match: false,
            key_byval: true,
            key_len: 4,
        };
        GinState {
            natts: 1,
            one_col: true,
            cols: [col; GIN_MAX_KEY_COLS],
        }
    }

    // GinFormTuple's leaf shape with an uncompressed one-item posting list
    // (the layout ginReadTupleWithoutState still accepts).
    fn gin_leaf_tuple<'m>(mcx: Mcx<'m>, rel: &Relation<'_>, key: i32) -> ItupBuf<'m> {
        let keytup =
            index_form_tuple(mcx, rel.descr(), &[Datum::from_i32(key)], &[false]).unwrap();
        let posting_off = keytup.size();
        let size = MAXALIGN(posting_off + core::mem::size_of::<ItemPointerData>());
        let mut tup = ItupBuf::with_size(mcx, size).unwrap();
        // SAFETY: both images are owned, MAXALIGNed and sized just above.
        unsafe {
            core::ptr::copy_nonoverlapping(keytup.as_ptr(), tup.as_mut_ptr(), posting_off);
            set_t_info(
                tup.as_mut_ptr(),
                (t_info(keytup.as_ptr()) & !INDEX_SIZE_MASK) | size as u16,
            );
            set_t_tid(tup.as_mut_ptr(), ItemPointerData::new(posting_off as BlockNumber, 1));
            tup.as_mut_ptr()
                .add(posting_off)
                .cast::<ItemPointerData>()
                .write_unaligned(ItemPointerData::new(key as BlockNumber, 1));
        }
        tup
    }

    // One full GIN_LEAF entry page: ascending int4 keys, one heap TID each.
    fn full_leaf_entry_page<'m>(mcx: Mcx<'m>, rel: &Relation<'_>) -> (PgVec<'m, u64>, usize) {
        let mut img: PgVec<'m, u64> = mcx::vec_from_elem_in(mcx, 0u64, BLCKSZ / 8);
        let base = core::ptr::NonNull::new(img.as_mut_ptr().cast::<u8>()).unwrap();
        // SAFETY: img is an 8-aligned BLCKSZ image exclusively owned here.
        let mut page = unsafe { PageMut::from_raw(base) };
        page.init(core::mem::size_of::<GinPageOpaqueData>());
        // SAFETY: the special area lies inside the image, 8-aligned.
        unsafe {
            base.as_ptr()
                .add(BLCKSZ - core::mem::size_of::<GinPageOpaqueData>())
                .cast::<GinPageOpaqueData>()
                .write(GinPageOpaqueData {
                    rightlink: InvalidBlockNumber,
                    maxoff: 0,
                    flags: GIN_LEAF,
                });
        }
        let mut n = 0usize;
        loop {
            let tup = gin_leaf_tuple(mcx, rel, n as i32 + 1);
            // SAFETY: tup.size() bytes of owned tuple image.
            let bytes = unsafe { core::slice::from_raw_parts(tup.as_ptr(), tup.size()) };
            if page.add_item(bytes, InvalidOffsetNumber, 0).is_none() {
                break;
            }
            n += 1;
        }
        (img, n)
    }

    // upstream 1f8ab91c11eb (18.6): amcheck: Fix memory leak with
    // gin_index_check(). C pfree()s the prev_tuple copy and the posting-list
    // buffer every iteration; the bump arena cannot, so the walk must not
    // allocate them at all. After the first page the check arena is at its
    // steady state and further pages must not move its footprint.
    #[test]
    fn entry_page_walk_keeps_the_check_arena_flat() {
        let fixture = MemoryContext::new_bump("amcheck gin walk fixture");
        let fmcx = fixture.mcx();
        let rel = int4_gin_rel(fmcx);
        let state = int4_array_gin_state();
        let (img, ntuples) = full_leaf_entry_page(fmcx, &rel);
        assert!(ntuples > 200, "page holds only {ntuples} tuples");
        // SAFETY: img is an 8-aligned BLCKSZ image alive for the whole walk.
        let page = unsafe {
            PageRef::from_raw(core::ptr::NonNull::new(img.as_ptr().cast_mut().cast::<u8>()).unwrap())
        };

        let arena = MemoryContext::new_bump("amcheck consistency check context");
        let amcx = arena.mcx();
        let mut stack: PgVec<'_, GinEntryScanItem> = mcx::vec_new_in(amcx);
        let mut leafdepth: i32 = -1;
        let mut ipd: PgVec<'_, ItemPointerData> = mcx::vec_new_in(amcx);
        let mut after_first = 0usize;
        for pass in 0..32 {
            let mut cur = GinEntryScanItem {
                depth: 0,
                parenttup: None,
                parentblk: InvalidBlockNumber,
                blkno: GIN_ROOT_BLKNO,
            };
            check_entry_page(
                &rel,
                &state,
                amcx,
                &None,
                &page,
                &mut cur,
                &mut stack,
                &mut leafdepth,
                &mut ipd,
            )
            .unwrap();
            if pass == 0 {
                after_first = arena.stats().arena_footprint;
            }
        }
        let after_all = arena.stats().arena_footprint;
        assert!(stack.is_empty() && leafdepth == 0);
        assert_eq!(
            after_first, after_all,
            "check arena grew by {} bytes over 31 more pages of {ntuples} tuples: per-tuple copies leak",
            after_all - after_first
        );
    }
}
