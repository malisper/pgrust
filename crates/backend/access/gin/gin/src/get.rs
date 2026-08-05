//! ginget.c: gingetbitmap and the entry/key item-stream machinery, including
//! the partial-match and EMPTY_QUERY / SEARCH_MODE_ALL bitmap collection
//! lanes.

use ::bufmgr_seams as bm;
use ::datum::Datum;
use ::gin_vocab::*;
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::tidbitmap::{TbmPrivateIterator, TIDBitmap, TBM_MAX_TUPLES_PER_PAGE};
use ::types_core::{BlockNumber, Buffer, InvalidBlockNumber, InvalidBuffer, OffsetNumber};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_relscan::{IndexScanDescData, IndexScanOpaque};
use ::types_tuple::itemptr::{
    FirstOffsetNumber, InvalidOffsetNumber, ItemPointerData, ItemPointerEquals,
    OffsetNumberNext, OffsetNumberPrev,
};

use crate::btree::{free_stack, ginFindLeafPage, ginStepRight, GinStack};
use crate::datapage::{gin_data_leaf_page_get_items, gin_data_leaf_page_get_items_to_tbm};
use crate::entrypage::{
    gin_get_nposting, gin_get_posting_tree, gin_is_posting_tree, gintuple_get_key, ginReadTuple,
    EntryBtree, ITup,
};
use crate::logic::{bool_consistent, tri_consistent};
use crate::scan::{ginFreeScanKeys, ginNewScanKey, non_gin_opaque};
use crate::util::ginCompareEntries;
use crate::{
    check_for_interrupts, meta_of, page_bytes, page_opaque, page_ref, unported, GinPageIsData,
    GinPageIsLeaf, GinPageRightMost, GIN_SHARE, GIN_UNLOCK,
};

fn predicate_lock_page(
    rel: &Relation<'_>,
    blkno: BlockNumber,
    snapshot: Option<&::types_snapshot::SnapshotData<'_>>,
) -> PgResult<()> {
    // Predicate locks matter only under SERIALIZABLE; scans always carry a
    // snapshot there.
    if let Some(snap) = snapshot {
        predicate_seams::predicate_lock_page::call(rel, blkno, snap)?;
    }
    Ok(())
}

/// moveRightIfItNeeded over an entry-tree stack frame.
fn move_right_if_needed(
    rel: &Relation<'_>,
    stack: &mut GinStack<'_>,
    snapshot: Option<&::types_snapshot::SnapshotData<'_>>,
) -> PgResult<bool> {
    let f = stack.top_mut();
    // SAFETY: pin + share lock held.
    let page = unsafe { page_ref(f.buffer) };
    if f.off > page.max_offset_number() {
        if GinPageRightMost(&page_opaque(&page)) {
            return Ok(false);
        }
        f.buffer = ginStepRight(f.buffer, rel, GIN_SHARE)?;
        f.blkno = bm::buffer_get_block_number::call(f.buffer);
        f.off = FirstOffsetNumber;
        predicate_lock_page(rel, f.blkno, snapshot)?;
    }
    Ok(true)
}

/// scanPostingTree: decode the whole posting tree into the match bitmap.
fn scan_posting_tree(
    mcx: Mcx<'_>,
    rel: &Relation<'_>,
    entry: &mut GinScanEntryData,
    root: BlockNumber,
) -> PgResult<()> {
    let stack = {
        let scratch = MemoryContext::new_bump("gin posting tree scan scratch");
        let stack = crate::datapage::ginScanBeginPostingTree(scratch.mcx(), rel, root)?;
        let buffer = stack.top().buffer;
        bm::incr_buffer_ref_count::call(buffer);
        free_stack(&stack, stack.top)?;
        buffer
    };
    let mut buffer = stack;

    loop {
        // SAFETY: pin + share lock held.
        let opaque = { page_opaque(&unsafe { page_ref(buffer) }) };
        if opaque.flags & GIN_DELETED == 0 {
            // SAFETY: pin + share lock held.
            let bytes = page_bytes(&unsafe { page_ref(buffer) });
            let n = gin_data_leaf_page_get_items_to_tbm(
                mcx,
                bytes,
                entry.matchBitmap.as_mut().expect("match bitmap"),
            )?;
            entry.predictNumberResult += n as u32;
        }
        if GinPageRightMost(&opaque) {
            break;
        }
        buffer = ginStepRight(buffer, rel, GIN_SHARE)?;
    }
    bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
    bm::release_buffer::call(buffer)?;
    Ok(())
}

/// collectMatchBitmap. Returns Ok(true) when done, Ok(false) to restart.
fn collect_match_bitmap(
    rel: &Relation<'_>,
    state: &GinState,
    kcx: Mcx<'static>,
    stack: &mut GinStack<'_>,
    entry: &mut GinScanEntryData,
    snapshot: Option<&::types_snapshot::SnapshotData<'_>>,
) -> PgResult<bool> {
    entry.matchBitmap = Some(TIDBitmap::new(
        kcx,
        init_small::globals::work_mem() as usize * 1024,
    ));

    let attnum = entry.attnum;
    predicate_lock_page(rel, bm::buffer_get_block_number::call(stack.top().buffer), snapshot)?;

    loop {
        if !move_right_if_needed(rel, stack, snapshot)? {
            return Ok(true);
        }
        let buffer = stack.top().buffer;
        let off = stack.top().off;
        // SAFETY: pin + share lock held.
        let itup: ITup = {
            let page = unsafe { page_ref(buffer) };
            let id = page.item_id(off);
            page.item_raw(id).0
        };

        // Tuple stores another attribute: stop the scan.
        // SAFETY: live tuple under the lock.
        if unsafe { crate::entrypage::gintuple_get_attrnum(state, itup) } != attnum {
            return Ok(true);
        }

        let mut icategory = GIN_CAT_NORM_KEY;
        // SAFETY: live tuple under the lock.
        let idatum = unsafe { gintuple_get_key(kcx, rel, state, itup, &mut icategory)? };

        if entry.isPartialMatch {
            // Partial matches stop at any null (including placeholders).
            if icategory != GIN_CAT_NORM_KEY {
                return Ok(true);
            }
            let cmp = crate::opclass::compare_partial(
                state.col(attnum),
                entry.queryKey,
                idatum,
                entry.strategy,
                entry.queryOrig,
            );
            if cmp > 0 {
                return Ok(true);
            } else if cmp < 0 {
                stack.top_mut().off += 1;
                continue;
            }
        } else if entry.searchMode == GIN_SEARCH_MODE_ALL && icategory == GIN_CAT_NULL_ITEM {
            return Ok(true);
        }

        // SAFETY: as above.
        if unsafe { gin_is_posting_tree(itup) } {
            // SAFETY: as above.
            let root = unsafe { gin_get_posting_tree(itup) };

            // Save the key value to re-find our position after re-locking.
            let saved = if icategory == GIN_CAT_NORM_KEY {
                Some(datum_copy_key(kcx, state.col(attnum), idatum)?)
            } else {
                None
            };

            bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
            predicate_lock_page(rel, root, snapshot)?;
            scan_posting_tree(kcx, rel, entry, root)?;
            bm::lock_buffer::call(buffer, GIN_SHARE)?;
            // SAFETY: pin + share lock held.
            if !GinPageIsLeaf(&page_opaque(&unsafe { page_ref(buffer) })) {
                // Root became non-leaf while unlocked: restart.
                return Ok(false);
            }

            loop {
                if !move_right_if_needed(rel, stack, snapshot)? {
                    panic!(
                        "failed to re-find tuple within index \"{}\"",
                        rel.name()
                    );
                }
                let buffer = stack.top().buffer;
                let off = stack.top().off;
                // SAFETY: pin + share lock held.
                let itup: ITup = {
                    let page = unsafe { page_ref(buffer) };
                    let id = page.item_id(off);
                    page.item_raw(id).0
                };
                // SAFETY: as above.
                if unsafe { crate::entrypage::gintuple_get_attrnum(state, itup) } == attnum {
                    let mut newcat = GIN_CAT_NORM_KEY;
                    // SAFETY: as above.
                    let newdatum =
                        unsafe { gintuple_get_key(kcx, rel, state, itup, &mut newcat)? };
                    let cmpto = saved.unwrap_or(idatum);
                    if ginCompareEntries(state, attnum, newdatum, newcat, cmpto, icategory) == 0 {
                        break;
                    }
                }
                stack.top_mut().off += 1;
            }
        } else {
            let mut items = mcx::vec_new_in(kcx);
            // SAFETY: as above.
            unsafe { ginReadTuple(kcx, itup, &mut items)? };
            entry
                .matchBitmap
                .as_mut()
                .expect("match bitmap")
                .add_tuples(items.as_slice(), false)?;
            // SAFETY: as above.
            entry.predictNumberResult += unsafe { gin_get_nposting(itup) } as u32;
        }

        stack.top_mut().off += 1;
    }
}

fn datum_copy_key(mcx: Mcx<'static>, col: &GinColState, key: Datum) -> PgResult<Datum> {
    if col.key_byval {
        return Ok(key);
    }
    let len = if col.key_len == -1 {
        // SAFETY: non-null varlena key.
        unsafe { ::types_tuple::varatt::varsize_any(key.as_usize() as *const u8) }
    } else {
        col.key_len as usize
    };
    let mut buf: PgVec<'static, u8> = mcx::vec_with_capacity_in(mcx, len)?;
    // SAFETY: len bytes of the live key image.
    crate::vec_append(&mut buf, unsafe {
        core::slice::from_raw_parts(key.as_usize() as *const u8, len)
    })?;
    let p = buf.as_ptr();
    core::mem::forget(buf);
    Ok(Datum::from_usize(p as usize))
}

/// startScanEntry.
fn start_scan_entry(
    rel: &Relation<'_>,
    state: &GinState,
    kcx: Mcx<'static>,
    entry: &mut GinScanEntryData,
    snapshot: Option<&::types_snapshot::SnapshotData<'_>>,
) -> PgResult<()> {
    'restart: loop {
        entry.buffer = InvalidBuffer;
        item_pointer_set_min(&mut entry.curItem);
        entry.offset = 0;
        entry.list.clear();
        entry.matchBitmap = None;
        entry.matchIterator = None;
        entry.matchNtuples = -1;
        entry.matchBlockno = InvalidBlockNumber;
        entry.reduceResult = false;
        entry.predictNumberResult = 0;
        entry.postingRoot = InvalidBlockNumber;

        let scratch = MemoryContext::new_bump("gin entry scan scratch");
        let smcx = scratch.mcx();
        let mut btree = EntryBtree::new(
            rel,
            state,
            entry.attnum,
            entry.queryKey,
            entry.queryCategory,
            smcx,
        );
        // GIN_CAT_EMPTY_QUERY sorts before everything: findItem lands on the
        // leftmost item.
        let mut stack = ginFindLeafPage(smcx, rel, &mut btree, true, false)?;
        let mut need_unlock = true;

        entry.isFinished = true;

        if entry.isPartialMatch || entry.queryCategory == GIN_CAT_EMPTY_QUERY {
            let (_, off) = crate::insert::entry_locate_leaf_pub(&btree, stack.top().buffer);
            stack.top_mut().off = off;
            if !collect_match_bitmap(rel, state, kcx, &mut stack, entry, snapshot)? {
                entry.matchIterator = None;
                entry.matchBitmap = None;
                bm::lock_buffer::call(stack.top().buffer, GIN_UNLOCK)?;
                free_stack(&stack, stack.top)?;
                continue 'restart;
            }
            let empty = entry.matchBitmap.as_ref().is_none_or(|b| b.is_empty());
            if !empty {
                entry.matchIterator =
                    Some(entry.matchBitmap.as_mut().unwrap().begin_private_iterate()?);
                entry.isFinished = false;
            }
        } else {
            let (found, off) = crate::insert::entry_locate_leaf_pub(&btree, stack.top().buffer);
            stack.top_mut().off = off;
            if found {
                let buffer = stack.top().buffer;
                // SAFETY: pin + share lock held.
                let itup: ITup = {
                    let page = unsafe { page_ref(buffer) };
                    let id = page.item_id(off);
                    page.item_raw(id).0
                };
                // SAFETY: as above.
                if unsafe { gin_is_posting_tree(itup) } {
                    // SAFETY: as above.
                    let root = unsafe { gin_get_posting_tree(itup) };
                    predicate_lock_page(rel, root, snapshot)?;
                    bm::lock_buffer::call(buffer, GIN_UNLOCK)?;
                    need_unlock = false;

                    let dstack = crate::datapage::ginScanBeginPostingTree(smcx, rel, root)?;
                    entry.buffer = dstack.top().buffer;
                    entry.postingRoot = root;
                    bm::incr_buffer_ref_count::call(entry.buffer);

                    // SAFETY: pin + share lock held on entry.buffer.
                    {
                        let bytes = page_bytes(&unsafe { page_ref(entry.buffer) });
                        let min = ItemPointerData::new(0, 0);
                        gin_data_leaf_page_get_items(bytes, &min, &mut entry.list)?;
                    }
                    entry.predictNumberResult =
                        dstack.top().predictNumber * entry.list.len() as u32;
                    bm::lock_buffer::call(entry.buffer, GIN_UNLOCK)?;
                    free_stack(&dstack, dstack.top)?;
                    entry.isFinished = false;
                } else {
                    predicate_lock_page(rel, bm::buffer_get_block_number::call(buffer), snapshot)?;
                    // SAFETY: as above.
                    if unsafe { gin_get_nposting(itup) } > 0 {
                        // SAFETY: as above.
                        unsafe { ginReadTuple(kcx, itup, &mut entry.list)? };
                        entry.predictNumberResult = entry.list.len() as u32;
                        entry.isFinished = false;
                    }
                }
            } else {
                predicate_lock_page(
                    rel,
                    bm::buffer_get_block_number::call(stack.top().buffer),
                    snapshot,
                )?;
            }
        }

        if need_unlock {
            bm::lock_buffer::call(stack.top().buffer, GIN_UNLOCK)?;
        }
        free_stack(&stack, stack.top)?;
        return Ok(());
    }
}

/// startScanKey: split entries into required/additional by frequency.
fn start_scan_key(state: &GinState, work: &mut GinScanWork, key_idx: usize) -> PgResult<()> {
    {
        let key = &mut work.keys[key_idx];
        item_pointer_set_min(&mut key.curItem);
        key.curItemMatches = false;
        key.recheckCurItem = false;
        key.isFinished = false;
        key.requiredEntries.clear();
        key.additionalEntries.clear();
    }

    let nentries = work.keys[key_idx].nentries as usize;
    if work.keys[key_idx].excludeOnly {
        for i in 0..nentries {
            let id = work.keys[key_idx].scanEntry[i];
            work.keys[key_idx].additionalEntries.push(id);
        }
    } else if nentries > 1 {
        let mut idx: Vec<usize> = (0..nentries).collect();
        idx.sort_by_key(|&i| {
            let e = work.keys[key_idx].scanEntry[i];
            work.entries[e as usize].predictNumberResult
        });

        for &i in idx.iter().skip(1) {
            work.keys[key_idx].entryRes[i] = GIN_MAYBE;
        }
        let mut last_required = nentries - 1;
        for pos in 0..nentries - 1 {
            work.keys[key_idx].entryRes[idx[pos]] = GIN_FALSE;
            let res = tri_consistent(&mut work.temp_ctx, state, &mut work.keys[key_idx])?;
            if res == GIN_FALSE {
                last_required = pos;
                break;
            }
            check_for_interrupts()?;
        }
        // Restore entryRes (consistent calls scribbled on it).
        for i in 0..nentries {
            work.keys[key_idx].entryRes[i] = GIN_FALSE;
        }

        let nrequired = last_required + 1;
        for (pos, &i) in idx.iter().enumerate() {
            let id = work.keys[key_idx].scanEntry[i];
            if pos < nrequired {
                work.keys[key_idx].requiredEntries.push(id);
            } else {
                work.keys[key_idx].additionalEntries.push(id);
            }
        }
    } else {
        let id = work.keys[key_idx].scanEntry[0];
        work.keys[key_idx].requiredEntries.push(id);
    }
    Ok(())
}

/// startScan.
fn start_scan(
    rel: &Relation<'_>,
    state: &GinState,
    work: &mut GinScanWork,
    snapshot: Option<&::types_snapshot::SnapshotData<'_>>,
) -> PgResult<()> {
    // SAFETY: everything allocated below is stored in `work` (kcx contract).
    let kcx = unsafe { work.kcx() };
    for entry in work.entries.iter_mut() {
        start_scan_entry(rel, state, kcx, entry, snapshot)?;
    }

    let fuzzy = guc_tables::vars::GinFuzzySearchLimit.read() as u32;
    if fuzzy > 0 {
        let total = work.entries.len() as u32;
        let mut reduce = true;
        for e in work.entries.iter() {
            if e.predictNumberResult <= total * fuzzy {
                reduce = false;
                break;
            }
        }
        if reduce {
            for e in work.entries.iter_mut() {
                e.predictNumberResult /= total;
                e.reduceResult = true;
            }
        }
    }

    for i in 0..work.keys.len() {
        start_scan_key(state, work, i)?;
    }
    Ok(())
}

/// entryLoadMoreItems.
fn entry_load_more_items(
    rel: &Relation<'_>,
    entry: &mut GinScanEntryData,
    advance_past: &ItemPointerData,
) -> PgResult<()> {
    if entry.buffer == InvalidBuffer {
        entry.isFinished = true;
        return Ok(());
    }

    let mut stepright;
    if ginCompareItemPointers(&entry.curItem, advance_past) == 0 {
        stepright = true;
        bm::lock_buffer::call(entry.buffer, GIN_SHARE)?;
    } else {
        bm::release_buffer::call(entry.buffer)?;
        entry.buffer = InvalidBuffer;

        let target = if item_pointer_is_lossy_page(advance_past) {
            ItemPointerData::new(
                gin_item_pointer_block(advance_past) + 1,
                FirstOffsetNumber,
            )
        } else {
            ItemPointerData::new(
                gin_item_pointer_block(advance_past),
                OffsetNumberNext(gin_item_pointer_offset(advance_past)),
            )
        };
        let scratch = MemoryContext::new_bump("gin posting descent scratch");
        let smcx = scratch.mcx();
        let mut btree = crate::datapage::DataBtree::new(rel, entry.postingRoot, smcx);
        btree.itemptr = target;
        let stack = ginFindLeafPage(smcx, rel, &mut btree, true, false)?;
        entry.buffer = stack.top().buffer;
        bm::incr_buffer_ref_count::call(entry.buffer);
        free_stack(&stack, stack.top)?;
        stepright = false;
    }

    loop {
        entry.offset = 0;
        entry.list.clear();

        if stepright {
            // SAFETY: pin + share lock held.
            let opaque = { page_opaque(&unsafe { page_ref(entry.buffer) }) };
            if GinPageRightMost(&opaque) {
                bm::lock_buffer::call(entry.buffer, GIN_UNLOCK)?;
                bm::release_buffer::call(entry.buffer)?;
                entry.buffer = InvalidBuffer;
                entry.isFinished = true;
                return Ok(());
            }
            entry.buffer = ginStepRight(entry.buffer, rel, GIN_SHARE)?;
        }
        stepright = true;

        // SAFETY: pin + share lock held.
        let opaque = { page_opaque(&unsafe { page_ref(entry.buffer) }) };
        if opaque.flags & GIN_DELETED != 0 {
            continue;
        }

        // SAFETY: pin + share lock held.
        let bytes = page_bytes(&unsafe { page_ref(entry.buffer) });
        if !GinPageRightMost(&opaque)
            && ginCompareItemPointers(
                advance_past,
                &crate::datapage::data_page_right_bound(bytes),
            ) >= 0
        {
            continue;
        }

        gin_data_leaf_page_get_items(bytes, advance_past, &mut entry.list)?;

        for i in 0..entry.list.len() {
            if ginCompareItemPointers(advance_past, &entry.list[i]) < 0 {
                entry.offset = i;
                if GinPageRightMost(&opaque) {
                    bm::lock_buffer::call(entry.buffer, GIN_UNLOCK)?;
                    bm::release_buffer::call(entry.buffer)?;
                    entry.buffer = InvalidBuffer;
                } else {
                    bm::lock_buffer::call(entry.buffer, GIN_UNLOCK)?;
                }
                return Ok(());
            }
        }
    }
}

fn drop_item(entry: &GinScanEntryData) -> bool {
    let fuzzy = guc_tables::vars::GinFuzzySearchLimit.read() as f64;
    pg_prng::global_prng(pg_prng::PgPrng::next_f64)
        > fuzzy / entry.predictNumberResult.max(1) as f64
}

/// entryGetItem.
fn entry_get_item(
    rel: &Relation<'_>,
    entry: &mut GinScanEntryData,
    advance_past: &ItemPointerData,
) -> PgResult<()> {
    debug_assert!(!entry.isFinished);

    if entry.matchBitmap.is_some() {
        let advance_past_blk = gin_item_pointer_block(advance_past);
        let advance_past_off = gin_item_pointer_offset(advance_past);

        loop {
            while entry.matchBlockno == InvalidBlockNumber
                || (!entry.matchLossy && entry.offset >= entry.matchNtuples.max(0) as usize)
                || entry.matchBlockno < advance_past_blk
                || (item_pointer_is_lossy_page(advance_past)
                    && entry.matchBlockno == advance_past_blk)
            {
                let advanced = {
                    let bitmap = entry.matchBitmap.as_ref().unwrap();
                    let iter = entry.matchIterator.as_mut().unwrap();
                    match iter.next(bitmap) {
                        None => None,
                        Some(r) => {
                            let ntuples = if !r.lossy {
                                r.extract_page_tuples(entry.matchOffsets.as_mut_slice()) as i32
                            } else {
                                -1
                            };
                            Some((r.blockno, r.lossy, ntuples))
                        }
                    }
                };
                match advanced {
                    None => {
                        entry.curItem = ItemPointerData::invalid();
                        entry.matchIterator = None;
                        entry.isFinished = true;
                        break;
                    }
                    Some((blockno, lossy, ntuples)) => {
                        entry.matchBlockno = blockno;
                        entry.matchLossy = lossy;
                        entry.matchNtuples = ntuples;
                        entry.offset = 0;
                    }
                }
            }
            if entry.isFinished {
                break;
            }

            if entry.matchLossy {
                item_pointer_set_lossy_page(&mut entry.curItem, entry.matchBlockno);
                break;
            }

            debug_assert!(entry.matchNtuples > -1);

            if entry.matchBlockno == advance_past_blk {
                debug_assert!(entry.matchNtuples > 0);
                if entry.matchOffsets[entry.matchNtuples as usize - 1] <= advance_past_off {
                    entry.offset = entry.matchNtuples as usize;
                    continue;
                }
                while entry.matchOffsets[entry.offset] <= advance_past_off {
                    entry.offset += 1;
                }
            }

            entry.curItem =
                ItemPointerData::new(entry.matchBlockno, entry.matchOffsets[entry.offset]);
            entry.offset += 1;

            if !entry.reduceResult || !drop_item(entry) {
                break;
            }
        }
    } else if entry.buffer == InvalidBuffer {
        loop {
            if entry.offset >= entry.list.len() {
                entry.curItem = ItemPointerData::invalid();
                entry.isFinished = true;
                break;
            }
            entry.curItem = entry.list[entry.offset];
            entry.offset += 1;
            if ginCompareItemPointers(&entry.curItem, advance_past) <= 0 {
                continue;
            }
            if !entry.reduceResult || !drop_item(entry) {
                break;
            }
        }
    } else {
        let mut advance_past = *advance_past;
        loop {
            while entry.offset >= entry.list.len() {
                entry_load_more_items(rel, entry, &advance_past)?;
                if entry.isFinished {
                    entry.curItem = ItemPointerData::invalid();
                    return Ok(());
                }
            }
            entry.curItem = entry.list[entry.offset];
            entry.offset += 1;
            if ginCompareItemPointers(&entry.curItem, &advance_past) <= 0 {
                continue;
            }
            if !entry.reduceResult || !drop_item(entry) {
                break;
            }
            advance_past = entry.curItem;
        }
    }
    Ok(())
}

/// keyGetItem.
fn key_get_item(
    rel: &Relation<'_>,
    state: &GinState,
    work: &mut GinScanWork,
    key_idx: usize,
    advance_past: &ItemPointerData,
) -> PgResult<()> {
    let mut advance_past = *advance_past;

    debug_assert!(!work.keys[key_idx].isFinished);

    if ginCompareItemPointers(&work.keys[key_idx].curItem, &advance_past) > 0 {
        return Ok(());
    }

    let mut min_item = ItemPointerData::invalid();
    item_pointer_set_max(&mut min_item);
    let mut all_finished = true;

    for ri in 0..work.keys[key_idx].requiredEntries.len() {
        let eid = work.keys[key_idx].requiredEntries[ri] as usize;
        if work.entries[eid].isFinished {
            continue;
        }
        if ginCompareItemPointers(&work.entries[eid].curItem, &advance_past) <= 0 {
            entry_get_item(rel, &mut work.entries[eid], &advance_past)?;
            if work.entries[eid].isFinished {
                continue;
            }
        }
        all_finished = false;
        if ginCompareItemPointers(&work.entries[eid].curItem, &min_item) < 0 {
            min_item = work.entries[eid].curItem;
        }
    }

    let exclude_only = work.keys[key_idx].excludeOnly;
    if all_finished && !exclude_only {
        work.keys[key_idx].isFinished = true;
        return Ok(());
    }

    if !exclude_only {
        if item_pointer_is_lossy_page(&min_item) {
            if gin_item_pointer_block(&advance_past) < gin_item_pointer_block(&min_item) {
                advance_past =
                    ItemPointerData::new(gin_item_pointer_block(&min_item), InvalidOffsetNumber);
            }
        } else {
            debug_assert!(gin_item_pointer_offset(&min_item) > 0);
            advance_past = ItemPointerData::new(
                gin_item_pointer_block(&min_item),
                OffsetNumberPrev(gin_item_pointer_offset(&min_item)),
            );
        }
    } else {
        debug_assert!(work.keys[key_idx].requiredEntries.is_empty());
        min_item = ItemPointerData::new(
            gin_item_pointer_block(&advance_past),
            OffsetNumberNext(gin_item_pointer_offset(&advance_past)),
        );
    }

    for ai in 0..work.keys[key_idx].additionalEntries.len() {
        let eid = work.keys[key_idx].additionalEntries[ai] as usize;
        if work.entries[eid].isFinished {
            continue;
        }
        if ginCompareItemPointers(&work.entries[eid].curItem, &advance_past) <= 0 {
            entry_get_item(rel, &mut work.entries[eid], &advance_past)?;
            if work.entries[eid].isFinished {
                continue;
            }
        }
        if ginCompareItemPointers(&work.entries[eid].curItem, &min_item) < 0 {
            debug_assert!(item_pointer_is_lossy_page(&min_item));
            min_item = work.entries[eid].curItem;
        }
    }

    work.keys[key_idx].curItem = min_item;
    let mut cur_page_lossy = ItemPointerData::invalid();
    item_pointer_set_lossy_page(&mut cur_page_lossy, gin_item_pointer_block(&min_item));

    let mut have_lossy_entry = false;
    let nentries = work.keys[key_idx].nentries as usize;
    let nuserentries = work.keys[key_idx].nuserentries as usize;
    for i in 0..nentries {
        let eid = work.keys[key_idx].scanEntry[i] as usize;
        let res = if !work.entries[eid].isFinished
            && ginCompareItemPointers(&work.entries[eid].curItem, &cur_page_lossy) == 0
        {
            have_lossy_entry = true;
            if i < nuserentries {
                GIN_MAYBE
            } else {
                GIN_TRUE
            }
        } else {
            GIN_FALSE
        };
        work.keys[key_idx].entryRes[i] = res;
    }

    if have_lossy_entry {
        let res = tri_consistent(&mut work.temp_ctx, state, &mut work.keys[key_idx])?;
        if res == GIN_TRUE || res == GIN_MAYBE {
            let key = &mut work.keys[key_idx];
            key.curItem = cur_page_lossy;
            key.curItemMatches = true;
            key.recheckCurItem = true;
            return Ok(());
        }
    }

    for i in 0..nentries {
        let eid = work.keys[key_idx].scanEntry[i] as usize;
        let res = if work.entries[eid].isFinished {
            GIN_FALSE
        } else if ginCompareItemPointers(&work.entries[eid].curItem, &cur_page_lossy) == 0 {
            GIN_MAYBE
        } else if ginCompareItemPointers(&work.entries[eid].curItem, &min_item) == 0 {
            GIN_TRUE
        } else {
            GIN_FALSE
        };
        work.keys[key_idx].entryRes[i] = res;
    }

    let res = tri_consistent(&mut work.temp_ctx, state, &mut work.keys[key_idx])?;
    let key = &mut work.keys[key_idx];
    match res {
        GIN_TRUE => {
            key.curItemMatches = true;
            // C leaves recheckCurItem as the tri fn left it; the direct tri
            // fns never set it, and startScanKey initialized it false.
            key.recheckCurItem = false;
        }
        GIN_FALSE => {
            key.curItemMatches = false;
        }
        _ => {
            key.curItemMatches = true;
            key.recheckCurItem = true;
        }
    }
    Ok(())
}

/// scanGetItem.
fn scan_get_item(
    rel: &Relation<'_>,
    state: &GinState,
    work: &mut GinScanWork,
    advance_past: &ItemPointerData,
    item: &mut ItemPointerData,
) -> PgResult<Option<bool>> {
    let mut advance_past = *advance_past;
    loop {
        check_for_interrupts()?;
        item_pointer_set_min(item);
        let mut match_ = true;
        for i in 0..work.keys.len() {
            if item_pointer_is_lossy_page(item) && work.keys[i].excludeOnly {
                debug_assert!(i > 0);
                continue;
            }

            key_get_item(rel, state, work, i, &advance_past)?;

            if work.keys[i].isFinished {
                return Ok(None);
            }

            if !work.keys[i].curItemMatches {
                advance_past = work.keys[i].curItem;
                match_ = false;
                break;
            }

            let cur = work.keys[i].curItem;
            if item_pointer_is_lossy_page(&cur) {
                if gin_item_pointer_block(&advance_past) < gin_item_pointer_block(&cur) {
                    advance_past =
                        ItemPointerData::new(gin_item_pointer_block(&cur), InvalidOffsetNumber);
                }
            } else {
                debug_assert!(gin_item_pointer_offset(&cur) > 0);
                advance_past = ItemPointerData::new(
                    gin_item_pointer_block(&cur),
                    OffsetNumberPrev(gin_item_pointer_offset(&cur)),
                );
            }

            if i == 0 {
                *item = cur;
            } else if item_pointer_is_lossy_page(&cur) || item_pointer_is_lossy_page(item) {
                debug_assert!(gin_item_pointer_block(&cur) >= gin_item_pointer_block(item));
                match_ = gin_item_pointer_block(&cur) == gin_item_pointer_block(item);
                if !match_ {
                    break;
                }
            } else {
                debug_assert!(ginCompareItemPointers(&cur, item) >= 0);
                match_ = ginCompareItemPointers(&cur, item) == 0;
                if !match_ {
                    break;
                }
            }
        }
        if match_ {
            break;
        }
    }
    debug_assert!(!item_pointer_is_min(item));

    let mut recheck = false;
    for key in work.keys.iter() {
        if key.recheckCurItem {
            recheck = true;
            break;
        }
    }
    Ok(Some(recheck))
}


struct PendingPosition {
    pending_buffer: Buffer,
    first_offset: OffsetNumber,
    last_offset: OffsetNumber,
    item: ItemPointerData,
}

/// scanGetCandidate.
fn scan_get_candidate(rel: &Relation<'_>, pos: &mut PendingPosition) -> PgResult<bool> {
    pos.item = ItemPointerData::invalid();
    loop {
        // SAFETY: pin + share lock held.
        let (maxoff, opaque) = {
            let page = unsafe { page_ref(pos.pending_buffer) };
            (page.max_offset_number(), page_opaque(&page))
        };
        if pos.first_offset > maxoff {
            let blkno = opaque.rightlink;
            if blkno == InvalidBlockNumber {
                bm::lock_buffer::call(pos.pending_buffer, GIN_UNLOCK)?;
                bm::release_buffer::call(pos.pending_buffer)?;
                pos.pending_buffer = InvalidBuffer;
                return Ok(false);
            }
            let tmpbuf = bm::read_buffer::call(rel, blkno)?;
            bm::lock_buffer::call(tmpbuf, GIN_SHARE)?;
            bm::lock_buffer::call(pos.pending_buffer, GIN_UNLOCK)?;
            bm::release_buffer::call(pos.pending_buffer)?;
            pos.pending_buffer = tmpbuf;
            pos.first_offset = FirstOffsetNumber;
        } else {
            // SAFETY: pin + share lock held.
            let page = unsafe { page_ref(pos.pending_buffer) };
            let id = page.item_id(pos.first_offset);
            let itup = page.item_raw(id).0;
            // SAFETY: pending tuples carry the heap TID in t_tid.
            pos.item = unsafe { ::nbtree::itup::t_tid(itup) };
            if opaque.flags & GIN_LIST_FULLROW != 0 {
                pos.last_offset = pos.first_offset + 1;
                while pos.last_offset <= maxoff {
                    let id = page.item_id(pos.last_offset);
                    let it = page.item_raw(id).0;
                    // SAFETY: as above.
                    if !ItemPointerEquals(&pos.item, &unsafe { ::nbtree::itup::t_tid(it) }) {
                        break;
                    }
                    pos.last_offset += 1;
                }
            } else {
                pos.last_offset = maxoff + 1;
            }
            return Ok(true);
        }
    }
}

/// matchPartialInPendingList.
#[allow(clippy::too_many_arguments)]
fn match_partial_in_pending_list(
    mcx: Mcx<'_>,
    state: &GinState,
    rel: &Relation<'_>,
    buffer: Buffer,
    mut off: OffsetNumber,
    maxoff: OffsetNumber,
    entry: &GinScanEntryData,
    datum: &mut [Datum],
    category: &mut [GinNullCategory],
    extracted: &mut [bool],
) -> PgResult<bool> {
    // Partial match to a null is not possible.
    if entry.queryCategory != GIN_CAT_NORM_KEY {
        return Ok(false);
    }
    while off < maxoff {
        // SAFETY: pin + share lock held by the caller.
        let itup = {
            let page = unsafe { page_ref(buffer) };
            let id = page.item_id(off);
            page.item_raw(id).0
        };
        // Tuple stores another attribute: stop.
        // SAFETY: live tuple under the lock.
        if unsafe { crate::entrypage::gintuple_get_attrnum(state, itup) } != entry.attnum {
            return Ok(false);
        }
        let mi = off as usize - 1;
        if !extracted[mi] {
            let mut cat = GIN_CAT_NORM_KEY;
            // SAFETY: live tuple under the lock.
            datum[mi] = unsafe { gintuple_get_key(mcx, rel, state, itup, &mut cat)? };
            category[mi] = cat;
            extracted[mi] = true;
        }
        // Once we hit nulls, no further match is possible.
        if category[mi] != GIN_CAT_NORM_KEY {
            return Ok(false);
        }
        let cmp = crate::opclass::compare_partial(
            state.col(entry.attnum),
            entry.queryKey,
            datum[mi],
            entry.strategy,
            entry.queryOrig,
        );
        if cmp == 0 {
            return Ok(true);
        } else if cmp > 0 {
            return Ok(false);
        }
        off += 1;
    }
    Ok(false)
}

/// collectMatchesForHeapRow.
fn collect_matches_for_heap_row(
    rel: &Relation<'_>,
    state: &GinState,
    work: &mut GinScanWork,
    pos: &mut PendingPosition,
    has_match_key: &mut [bool],
) -> PgResult<bool> {
    for key in work.keys.iter_mut() {
        for r in key.entryRes.iter_mut() {
            *r = GIN_FALSE;
        }
    }
    for m in has_match_key.iter_mut() {
        *m = false;
    }

    loop {
        debug_assert!(pos.last_offset > pos.first_offset);

        // Per-page key cache (C's datum/category/datumExtracted arrays).
        let mut datum = [Datum::null(); 512];
        let mut category = [GIN_CAT_NORM_KEY; 512];
        let mut extracted = [false; 512];

        // SAFETY: pin + share lock held.
        let full_row = {
            let page = unsafe { page_ref(pos.pending_buffer) };
            page_opaque(&page).flags & GIN_LIST_FULLROW != 0
        };

        for ki in 0..work.keys.len() {
            let nentries = work.keys[ki].nentries as usize;
            for j in 0..nentries {
                if work.keys[ki].entryRes[j] != GIN_FALSE {
                    continue;
                }
                let eid = work.keys[ki].scanEntry[j] as usize;
                let entry = &work.entries[eid];

                let key_attnum = work.keys[ki].attnum;
                let mut stop_low = pos.first_offset;
                let mut stop_high = pos.last_offset;
                let mut found_eq = false;
                while stop_low < stop_high {
                    let stop_middle = stop_low + ((stop_high - stop_low) >> 1);
                    // SAFETY: pin + share lock held.
                    let itup = {
                        let page = unsafe { page_ref(pos.pending_buffer) };
                        let id = page.item_id(stop_middle);
                        page.item_raw(id).0
                    };
                    // Pending tuples are ordered by (attnum, datum).
                    // SAFETY: live tuple under the lock.
                    let tup_attnum =
                        unsafe { crate::entrypage::gintuple_get_attrnum(state, itup) };
                    if key_attnum < tup_attnum {
                        stop_high = stop_middle;
                        continue;
                    }
                    if key_attnum > tup_attnum {
                        stop_low = stop_middle + 1;
                        continue;
                    }
                    let mi = stop_middle as usize - 1;
                    if !extracted[mi] {
                        let mut cat = GIN_CAT_NORM_KEY;
                        // SAFETY: live tuple under the lock; kcx is the
                        // scan-lifetime key context (transient tupdesc only).
                        let kcx2 = unsafe { work.kcx() };
                        datum[mi] =
                            unsafe { gintuple_get_key(kcx2, rel, state, itup, &mut cat)? };
                        category[mi] = cat;
                        extracted[mi] = true;
                    }

                    let res = if entry.queryCategory == GIN_CAT_EMPTY_QUERY {
                        if entry.searchMode == GIN_SEARCH_MODE_ALL {
                            if category[mi] == GIN_CAT_NULL_ITEM {
                                -1
                            } else {
                                0
                            }
                        } else {
                            0
                        }
                    } else {
                        ginCompareEntries(
                            state,
                            entry.attnum,
                            entry.queryKey,
                            entry.queryCategory,
                            datum[mi],
                            category[mi],
                        )
                    };

                    if res == 0 {
                        work.keys[ki].entryRes[j] = if entry.isPartialMatch {
                            // SAFETY: kcx contract as above.
                            let kcx2 = unsafe { work.kcx() };
                            if match_partial_in_pending_list(
                                kcx2,
                                state,
                                rel,
                                pos.pending_buffer,
                                stop_middle,
                                pos.last_offset,
                                entry,
                                &mut datum,
                                &mut category,
                                &mut extracted,
                            )? {
                                GIN_TRUE
                            } else {
                                GIN_FALSE
                            }
                        } else {
                            GIN_TRUE
                        };
                        found_eq = true;
                        break;
                    } else if res < 0 {
                        stop_high = stop_middle;
                    } else {
                        stop_low = stop_middle + 1;
                    }
                }

                if !found_eq && entry.isPartialMatch {
                    // No exact match: scan forward from the first tuple
                    // greater than the target value.
                    // SAFETY: kcx contract as above.
                    let kcx2 = unsafe { work.kcx() };
                    work.keys[ki].entryRes[j] = if match_partial_in_pending_list(
                        kcx2,
                        state,
                        rel,
                        pos.pending_buffer,
                        stop_high,
                        pos.last_offset,
                        entry,
                        &mut datum,
                        &mut category,
                        &mut extracted,
                    )? {
                        GIN_TRUE
                    } else {
                        GIN_FALSE
                    };
                }

                if work.keys[ki].entryRes[j] == GIN_TRUE {
                    has_match_key[ki] = true;
                }
            }
        }

        pos.first_offset = pos.last_offset;

        if full_row {
            break;
        }
        let item = pos.item;
        if !scan_get_candidate(rel, pos)? || !ItemPointerEquals(&pos.item, &item) {
            panic!("could not find additional pending pages for same heap tuple");
        }
    }

    for (i, key) in work.keys.iter().enumerate() {
        if !has_match_key[i] && !key.excludeOnly {
            return Ok(false);
        }
    }
    Ok(true)
}

/// scanPendingInsert.
fn scan_pending_insert(
    rel: &Relation<'_>,
    state: &GinState,
    work: &mut GinScanWork,
    tbm: &mut TIDBitmap<'_>,
    snapshot: Option<&::types_snapshot::SnapshotData<'_>>,
) -> PgResult<i64> {
    let mut ntids = 0i64;
    let metabuffer = bm::read_buffer::call(rel, GIN_METAPAGE_BLKNO)?;
    predicate_lock_page(rel, GIN_METAPAGE_BLKNO, snapshot)?;
    bm::lock_buffer::call(metabuffer, GIN_SHARE)?;
    // SAFETY: pin + share lock held.
    let blkno = { meta_of(page_bytes(&unsafe { page_ref(metabuffer) })).head };

    if blkno == InvalidBlockNumber {
        bm::lock_buffer::call(metabuffer, GIN_UNLOCK)?;
        bm::release_buffer::call(metabuffer)?;
        return Ok(0);
    }

    let mut pos = PendingPosition {
        pending_buffer: bm::read_buffer::call(rel, blkno)?,
        first_offset: FirstOffsetNumber,
        last_offset: 0,
        item: ItemPointerData::invalid(),
    };
    bm::lock_buffer::call(pos.pending_buffer, GIN_SHARE)?;
    bm::lock_buffer::call(metabuffer, GIN_UNLOCK)?;
    bm::release_buffer::call(metabuffer)?;

    let mut has_match_key = vec![false; work.keys.len()];

    while scan_get_candidate(rel, &mut pos)? {
        if !collect_matches_for_heap_row(rel, state, work, &mut pos, &mut has_match_key)? {
            continue;
        }

        let mut recheck = false;
        let mut matches = true;
        for i in 0..work.keys.len() {
            if !bool_consistent(&mut work.temp_ctx, state, &mut work.keys[i])? {
                matches = false;
                break;
            }
            recheck |= work.keys[i].recheckCurItem;
        }

        if matches {
            tbm.add_tuples(core::slice::from_ref(&pos.item), recheck)?;
            ntids += 1;
        }
    }

    Ok(ntids)
}

/// gingetbitmap.
pub fn gingetbitmap(
    scan: &mut IndexScanDescData<'_>,
    tbm: &mut TIDBitmap<'_>,
) -> PgResult<i64> {
    let IndexScanDescData {
        indexRelation,
        xs_snapshot,
        keyData,
        xs_pgstat_index_scans,
        opaque,
        ..
    } = scan;
    let rel: &Relation<'_> = indexRelation.as_ref().expect("index scan parked (skeleton)");
    let snapshot = xs_snapshot.as_deref();
    let IndexScanOpaque::Gin(so) = opaque else {
        non_gin_opaque()
    };

    ginFreeScanKeys(so)?;
    ginNewScanKey(rel, keyData.as_slice(), so)?;
    *xs_pgstat_index_scans += 1;

    if so.isVoidRes {
        return Ok(0);
    }
    let state = so.ginstate.expect("ginstate");
    let work = so.work.as_mut().expect("scan work");

    let mut ntids = scan_pending_insert(rel, &state, work, tbm, snapshot)?;

    start_scan(rel, &state, work, snapshot)?;

    let mut iptr = ItemPointerData::invalid();
    item_pointer_set_min(&mut iptr);

    loop {
        let mut item = iptr;
        let Some(recheck) = scan_get_item(rel, &state, work, &iptr, &mut item)? else {
            break;
        };
        iptr = item;
        if item_pointer_is_lossy_page(&iptr) {
            tbm.add_page(gin_item_pointer_block(&iptr))?;
        } else {
            tbm.add_tuples(core::slice::from_ref(&iptr), recheck)?;
        }
        ntids += 1;
    }

    Ok(ntids)
}
