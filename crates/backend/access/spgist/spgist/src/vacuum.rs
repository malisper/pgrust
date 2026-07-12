//! spgvacuum.c: spgbulkdelete/spgvacuumcleanup. Physical-order scan with a
//! pending-TID list for redirects added after the scan started. The
//! bulkdelete callback is monomorphized to the sorted dead-TID slice
//! (vac_tid_reaped) or validate_index's never-delete collect callback, as
//! nbtree/gist render it.
use ::bufmgr_seams::{self as bufmgr};
use ::types_core::xact::{TransactionIdFollowsOrEquals, TransactionIdPrecedes};
use ::types_core::{
    BlockNumber, Buffer, ForkNumber, InvalidBlockNumber, OffsetNumber, TransactionId,
};
use ::types_error::PgResult;
use ::types_rel::Relation;
use ::types_spgist::xlog::{
    spgxlogState, spgxlogVacuumLeaf, spgxlogVacuumRedirect, spgxlogVacuumRoot,
    XLOG_SPGIST_VACUUM_LEAF, XLOG_SPGIST_VACUUM_REDIRECT, XLOG_SPGIST_VACUUM_ROOT,
};
use ::types_spgist::{
    inner_tuple_nodes, leaf_set_next_offset, node_tuple_tid, page_opaque, page_opaque_update,
    spgPageIndexMultiDelete, tuple_state, SpGistBlockIsRoot, SpGistDeadTupleHeader,
    SpGistLeafTupleHeader, SpGistPageIsDeleted, SpGistPageIsLeaf, SPGIST_DEAD,
    SPGIST_LAST_FIXED_BLKNO, SPGIST_LIVE, SPGIST_METAPAGE_BLKNO, SPGIST_PLACEHOLDER,
    SPGIST_REDIRECT,
};
use ::types_storage::bufpage::MaxIndexTuplesPerPage;
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::{
    FirstOffsetNumber, InvalidOffsetNumber, ItemPointerCompare, ItemPointerData,
    ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber, ItemPointerIsValid,
};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD};

use crate::doinsert::RM_SPGIST_ID;
use crate::utils::{
    buf_page_mut, item_slice, item_slice_mut, relation_needs_wal, tuple_state_error,
    unlock_release, SpGistSetLastUsedPage, SpGistUpdateMetaPage,
};

pub use ::nbtree::IndexVacuumInfo;
use ::types_nbtree::IndexBulkDeleteResult;

// spgBulkDeleteState. The SpGistState is reduced to the two fields the vacuum
// paths consume (redirectXid + isBuild=false, C's initSpGistState values).
struct SpgVacState<'a, 'cb, 'mcx> {
    info: &'a IndexVacuumInfo<'a, 'mcx>,
    stats: &'a mut IndexBulkDeleteResult,
    dead_items: Option<&'a [ItemPointerData]>,
    collect: Option<&'a mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + 'cb)>,
    redirect_xid: TransactionId,
    // spgVacPendingItem list: (tid, done); appended-at-end order preserved.
    pending: Vec<(ItemPointerData, bool)>,
    my_xmin: TransactionId,
    last_filled_block: BlockNumber,
}

fn vacuum_delay_point() -> PgResult<()> {
    crate::check_for_interrupts()?;
    if init_small::globals::VacuumCostActive() {
        vacuum_seams::vacuum_delay_point::call(false)?;
    }
    Ok(())
}

// vac_tid_reaped over the sorted dead-TID image; the collect callback never
// deletes.
fn tid_deletable(state: &mut SpgVacState<'_, '_, '_>, tid: &ItemPointerData) -> PgResult<bool> {
    if let Some(cb) = state.collect.as_deref_mut() {
        cb(tid)?;
        return Ok(false);
    }
    if let Some(dead) = state.dead_items {
        return Ok(dead
            .binary_search_by(|probe| ItemPointerCompare(probe, tid).cmp(&0))
            .is_ok());
    }
    Ok(false)
}

// spgAddPendingTID.
fn add_pending_tid(pending: &mut Vec<(ItemPointerData, bool)>, tid: &ItemPointerData) {
    if pending.iter().any(|(t, _)| ItemPointerCompare(t, tid) == 0) {
        return;
    }
    pending.push((*tid, false));
}

fn offnum_bytes(v: &[OffsetNumber]) -> &[u8] {
    // SAFETY: OffsetNumber (u16) reinterpreted as ne bytes.
    unsafe { core::slice::from_raw_parts(v.as_ptr().cast::<u8>(), v.len() * 2) }
}

fn read_vacuum_buffer<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    blkno: BlockNumber,
) -> PgResult<Buffer> {
    bufmgr::read_buffer_extended::call(
        info.index,
        ForkNumber::MAIN_FORKNUM,
        blkno,
        ReadBufferMode::Normal,
        info.strategy.clone(),
    )
}

/// vacuumLeafPage: non-root leaf. forPending pages don't add to the live
/// count; the sequential scan visits them too.
fn vacuum_leaf_page(
    state: &mut SpgVacState<'_, '_, '_>,
    index: &Relation<'_>,
    buffer: Buffer,
    blkno: BlockNumber,
    for_pending: bool,
) -> PgResult<()> {
    let max = buf_page_mut(buffer).as_ref().max_offset_number();
    let mut predecessor = vec![InvalidOffsetNumber; max as usize + 1];
    let mut deletable = vec![false; max as usize + 1];
    let mut n_deletable = 0usize;

    {
        let pm = buf_page_mut(buffer);
        let page = pm.as_ref();
        for i in FirstOffsetNumber..=max {
            let lt = item_slice(&page, i);
            match tuple_state(lt) {
                SPGIST_LIVE => {
                    let hdr = SpGistLeafTupleHeader::decode(lt);
                    debug_assert!(ItemPointerIsValid(&hdr.heapPtr));
                    if tid_deletable(state, &hdr.heapPtr)? {
                        state.stats.tuples_removed += 1.0;
                        deletable[i as usize] = true;
                        n_deletable += 1;
                    } else if !for_pending {
                        state.stats.num_index_tuples += 1.0;
                    }

                    let next = hdr.nextOffset();
                    if next != InvalidOffsetNumber {
                        // paranoia about corrupted chain links
                        if next < FirstOffsetNumber
                            || next > max
                            || predecessor[next as usize] != InvalidOffsetNumber
                        {
                            panic!(
                                "inconsistent tuple chain links in page {blkno} of index \"{}\"",
                                index.name()
                            );
                        }
                        predecessor[next as usize] = i;
                    }
                }
                SPGIST_REDIRECT => {
                    let dt = SpGistDeadTupleHeader::decode(lt);
                    debug_assert!(ItemPointerIsValid(&dt.pointer));
                    // Chase redirections that could postdate VACUUM's start;
                    // an invalid xid means REINDEX CONCURRENTLY, which locks
                    // out VACUUM.
                    if TransactionIdFollowsOrEquals(dt.xid, state.my_xmin) {
                        add_pending_tid(&mut state.pending, &dt.pointer);
                    }
                }
                _ => {}
            }
        }
    }

    if n_deletable == 0 {
        return Ok(());
    }

    // Plan the page update as the six WAL-image arrays: DEAD replacements,
    // PLACEHOLDER replacements, line-pointer moves, chain-link updates.
    let mut to_dead: Vec<OffsetNumber> = Vec::new();
    let mut to_placeholder: Vec<OffsetNumber> = Vec::new();
    let mut move_src: Vec<OffsetNumber> = Vec::new();
    let mut move_dest: Vec<OffsetNumber> = Vec::new();
    let mut chain_src: Vec<OffsetNumber> = Vec::new();
    let mut chain_dest: Vec<OffsetNumber> = Vec::new();

    {
        let pm = buf_page_mut(buffer);
        let page = pm.as_ref();
        for i in FirstOffsetNumber..=max {
            let head = item_slice(&page, i);
            if tuple_state(head) != SPGIST_LIVE {
                continue; // can't be a chain member
            }
            if predecessor[i as usize] != InvalidOffsetNumber {
                continue; // not a chain head
            }

            let mut intervening_deletable = false;
            let mut prev_live = if deletable[i as usize] { InvalidOffsetNumber } else { i };

            let mut j = SpGistLeafTupleHeader::decode(head).nextOffset();
            while j != InvalidOffsetNumber {
                let lt = item_slice(&page, j);
                if tuple_state(lt) != SPGIST_LIVE {
                    tuple_state_error(tuple_state(lt));
                }

                if deletable[j as usize] {
                    to_placeholder.push(j);
                    intervening_deletable = true;
                } else if prev_live == InvalidOffsetNumber {
                    // First live tuple in the chain moves to the head slot.
                    move_src.push(j);
                    move_dest.push(i);
                    prev_live = i;
                    intervening_deletable = false;
                } else {
                    if intervening_deletable {
                        chain_src.push(prev_live);
                        chain_dest.push(j);
                    }
                    prev_live = j;
                    intervening_deletable = false;
                }

                j = SpGistLeafTupleHeader::decode(lt).nextOffset();
            }

            if prev_live == InvalidOffsetNumber {
                // Entirely-removable chain needs a DEAD tuple at the head.
                to_dead.push(i);
            } else if intervening_deletable {
                chain_src.push(prev_live);
                chain_dest.push(InvalidOffsetNumber);
            }
        }
    }

    if n_deletable != to_dead.len() + to_placeholder.len() + move_src.len() {
        panic!("inconsistent counts of deletable tuples");
    }
    debug_assert!(n_deletable <= MaxIndexTuplesPerPage);

    {
        let mut pm = buf_page_mut(buffer);
        spgPageIndexMultiDelete(
            state.redirect_xid,
            &mut pm,
            &to_dead,
            SPGIST_DEAD,
            SPGIST_DEAD,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );
        spgPageIndexMultiDelete(
            state.redirect_xid,
            &mut pm,
            &to_placeholder,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        // The move step swaps the source/target line pointers, then replaces
        // the newly-source tuples with placeholders; no page-overflow risk.
        for k in 0..move_src.len() {
            let id_src = pm.as_ref().item_id(move_src[k]);
            let id_dest = pm.as_ref().item_id(move_dest[k]);
            pm.set_item_id(move_src[k], id_dest);
            pm.set_item_id(move_dest[k], id_src);
        }

        spgPageIndexMultiDelete(
            state.redirect_xid,
            &mut pm,
            &move_src,
            SPGIST_PLACEHOLDER,
            SPGIST_PLACEHOLDER,
            InvalidBlockNumber,
            InvalidOffsetNumber,
        );

        for k in 0..chain_src.len() {
            let lt = item_slice_mut(&mut pm, chain_src[k]);
            debug_assert!(tuple_state(lt) == SPGIST_LIVE);
            leaf_set_next_offset(lt, chain_dest[k]);
        }
    }

    bufmgr::mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(index) {
        let xlrec = spgxlogVacuumLeaf {
            nDead: to_dead.len() as u16,
            nPlaceholder: to_placeholder.len() as u16,
            nMove: move_src.len() as u16,
            nChain: chain_src.len() as u16,
            stateSrc: spgxlogState { redirectXid: state.redirect_xid, isBuild: false },
        };
        let xl = xlrec.encode();
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SPGIST_ID,
            XLOG_SPGIST_VACUUM_LEAF,
            0,
            &[
                &xl,
                offnum_bytes(&to_dead),
                offnum_bytes(&to_placeholder),
                offnum_bytes(&move_src),
                offnum_bytes(&move_dest),
                offnum_bytes(&chain_src),
                offnum_bytes(&chain_dest),
            ],
            &[XLogRegBuf { block_id: 0, buffer, flags: REGBUF_STANDARD, bufdata: &[] }],
        )?;
        buf_page_mut(buffer).set_lsn(recptr);
    }
    Ok(())
}

/// vacuumLeafRoot: on a root leaf just delete dead tuples; no chain business.
fn vacuum_leaf_root(
    state: &mut SpgVacState<'_, '_, '_>,
    index: &Relation<'_>,
    buffer: Buffer,
) -> PgResult<()> {
    let mut to_delete: Vec<OffsetNumber> = Vec::new();

    {
        let pm = buf_page_mut(buffer);
        let page = pm.as_ref();
        for i in FirstOffsetNumber..=page.max_offset_number() {
            let lt = item_slice(&page, i);
            if tuple_state(lt) != SPGIST_LIVE {
                // all tuples on root should be live
                tuple_state_error(tuple_state(lt));
            }
            let hdr = SpGistLeafTupleHeader::decode(lt);
            debug_assert!(ItemPointerIsValid(&hdr.heapPtr));
            if tid_deletable(state, &hdr.heapPtr)? {
                state.stats.tuples_removed += 1.0;
                to_delete.push(i);
            } else {
                state.stats.num_index_tuples += 1.0;
            }
        }
    }

    if to_delete.is_empty() {
        return Ok(());
    }

    // Offsets are already in order: plain PageIndexMultiDelete.
    buf_page_mut(buffer).index_multi_delete(&to_delete);
    bufmgr::mark_buffer_dirty::call(buffer)?;

    if relation_needs_wal(index) {
        let xlrec = spgxlogVacuumRoot {
            nDelete: to_delete.len() as u16,
            stateSrc: spgxlogState { redirectXid: state.redirect_xid, isBuild: false },
        };
        let xl = xlrec.encode();
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SPGIST_ID,
            XLOG_SPGIST_VACUUM_ROOT,
            0,
            &[&xl, offnum_bytes(&to_delete)],
            &[XLogRegBuf { block_id: 0, buffer, flags: REGBUF_STANDARD, bufdata: &[] }],
        )?;
        buf_page_mut(buffer).set_lsn(recptr);
    }
    Ok(())
}

/// vacuumRedirectAndPlaceholder: age REDIRECTs into PLACEHOLDERs, trim
/// trailing PLACEHOLDERs; works on both leaf and inner pages.
fn vacuum_redirect_and_placeholder(
    index: &Relation<'_>,
    heaprel: &::types_rel::RelationData<'_>,
    buffer: Buffer,
) -> PgResult<()> {
    let mut item_to_placeholder: Vec<OffsetNumber> = Vec::new();
    let mut snapshot_conflict_horizon: TransactionId = 0;
    let mut first_placeholder = InvalidOffsetNumber;
    let mut has_non_placeholder = false;
    let mut has_update = false;
    // isCatalogRel: RelationIsAccessibleInLogicalDecoding const-false
    let is_catalog_rel = false;
    let _ = heaprel;

    let vistest = procarray_seams::global_vis_test_for::call(heaprel);

    {
        let mut pm = buf_page_mut(buffer);
        let max = pm.as_ref().max_offset_number();
        let mut n_redirection = page_opaque(&pm.as_ref()).nRedirection;

        // Backwards scan: convert removable redirects, find the trailing
        // placeholder run.
        let mut i = max;
        while i >= FirstOffsetNumber && (n_redirection > 0 || !has_non_placeholder) {
            let dt_slice = item_slice_mut(&mut pm, i);
            let mut dt = SpGistDeadTupleHeader::decode(dt_slice);

            // A REDIRECT becomes a PLACEHOLDER once no index scan can be
            // in flight to it: its XID below global xmin, or invalid
            // (REINDEX CONCURRENTLY).
            if dt.tupstate == SPGIST_REDIRECT
                && (dt.xid == 0
                    || procarray_seams::global_vis_test_is_removable_xid::call(
                        vistest, dt.xid,
                    )?)
            {
                debug_assert!(n_redirection > 0);
                n_redirection -= 1;

                if snapshot_conflict_horizon == 0
                    || TransactionIdPrecedes(snapshot_conflict_horizon, dt.xid)
                {
                    snapshot_conflict_horizon = dt.xid;
                }

                dt.tupstate = SPGIST_PLACEHOLDER;
                dt.pointer = ItemPointerData::default();
                dt.encode(dt_slice);

                item_to_placeholder.push(i);
                has_update = true;
            }

            if tuple_state(item_slice(&pm.as_ref(), i)) == SPGIST_PLACEHOLDER {
                if !has_non_placeholder {
                    first_placeholder = i;
                }
            } else {
                has_non_placeholder = true;
            }
            i -= 1;
        }

        if !item_to_placeholder.is_empty() {
            page_opaque_update(&mut pm, |op| {
                op.nRedirection -= item_to_placeholder.len() as u16;
                op.nPlaceholder += item_to_placeholder.len() as u16;
            });
        }

        // Trailing placeholders can go; earlier ones would renumber
        // non-placeholder tuples.
        if first_placeholder != InvalidOffsetNumber {
            let itemnos: Vec<OffsetNumber> = (first_placeholder..=max).collect();
            page_opaque_update(&mut pm, |op| {
                debug_assert!(op.nPlaceholder as usize >= itemnos.len());
                op.nPlaceholder -= itemnos.len() as u16;
            });
            pm.index_multi_delete(&itemnos);
            has_update = true;
        }
    }

    if has_update {
        bufmgr::mark_buffer_dirty::call(buffer)?;
    }

    if has_update && relation_needs_wal(index) {
        let xlrec = spgxlogVacuumRedirect {
            nToPlaceholder: item_to_placeholder.len() as u16,
            firstPlaceholder: first_placeholder,
            snapshotConflictHorizon: snapshot_conflict_horizon,
            isCatalogRel: is_catalog_rel,
        };
        let xl = xlrec.encode();
        let recptr = xloginsert_seams::xlog_insert_record::call(
            RM_SPGIST_ID,
            XLOG_SPGIST_VACUUM_REDIRECT,
            0,
            &[&xl, offnum_bytes(&item_to_placeholder)],
            &[XLogRegBuf { block_id: 0, buffer, flags: REGBUF_STANDARD, bufdata: &[] }],
        )?;
        buf_page_mut(buffer).set_lsn(recptr);
    }
    Ok(())
}

/// spgvacuumpage: one page of the bulkdelete scan; buffer comes in pinned,
/// leaves unlocked+unpinned.
fn spgvacuumpage(state: &mut SpgVacState<'_, '_, '_>, buffer: Buffer) -> PgResult<()> {
    let index = state.info.index;
    let blkno = bufmgr::buffer_get_block_number::call(buffer);

    bufmgr::lock_buffer::call(buffer, bufmgr::BUFFER_LOCK_EXCLUSIVE)?;

    let (is_new, is_empty, is_leaf) = {
        let pm = buf_page_mut(buffer);
        let page = pm.as_ref();
        (page.is_new(), page.max_offset_number() == 0, !page.is_new() && SpGistPageIsLeaf(&page))
    };

    if is_new {
        // All-zero page from a crash mid-extension; recycle below.
    } else if is_empty {
        // nothing to do
    } else if is_leaf {
        if SpGistBlockIsRoot(blkno) {
            vacuum_leaf_root(state, index, buffer)?;
            // no need for vacuumRedirectAndPlaceholder
        } else {
            vacuum_leaf_page(state, index, buffer, blkno, false)?;
            vacuum_redirect_and_placeholder(index, state.info.heaprel, buffer)?;
        }
    } else {
        // inner page
        vacuum_redirect_and_placeholder(index, state.info.heaprel, buffer)?;
    }

    // Root pages are never deleted nor FSM-listed; searches for insertion
    // space must not land on them.
    if !SpGistBlockIsRoot(blkno) {
        let now_empty = {
            let pm = buf_page_mut(buffer);
            let page = pm.as_ref();
            page.is_new() || page.max_offset_number() == 0
        };
        if now_empty {
            freespace::RecordFreeIndexPage(index, blkno)?;
            state.stats.pages_deleted += 1;
        } else {
            SpGistSetLastUsedPage(index, buffer)?;
            state.last_filled_block = blkno;
        }
    }

    unlock_release(buffer)
}

/// spgprocesspending: drain the pending-TID list between pages of the scan.
fn spgprocesspending(state: &mut SpgVacState<'_, '_, '_>) -> PgResult<()> {
    let index = state.info.index;

    let mut idx = 0;
    while idx < state.pending.len() {
        if state.pending[idx].1 {
            idx += 1;
            continue; // already done
        }

        // vacuum_delay_point while not holding any buffer lock
        vacuum_delay_point()?;

        let blkno = ItemPointerGetBlockNumber(&state.pending[idx].0);
        let buffer = read_vacuum_buffer(state.info, blkno)?;
        bufmgr::lock_buffer::call(buffer, bufmgr::BUFFER_LOCK_EXCLUSIVE)?;

        let (is_new_or_deleted, is_leaf) = {
            let pm = buf_page_mut(buffer);
            let page = pm.as_ref();
            let dead = page.is_new() || SpGistPageIsDeleted(&page);
            (dead, !dead && SpGistPageIsLeaf(&page))
        };

        if is_new_or_deleted {
            // Probably shouldn't happen, but ignore it
        } else if is_leaf {
            if SpGistBlockIsRoot(blkno) {
                panic!("redirection leads to root page of index \"{}\"", index.name());
            }

            vacuum_leaf_page(state, index, buffer, blkno, true)?;
            vacuum_redirect_and_placeholder(index, state.info.heaprel, buffer)?;
            SpGistSetLastUsedPage(index, buffer)?;

            // The whole page was vacuumed: every pending item on it is done.
            for (tid, done) in state.pending.iter_mut() {
                if ItemPointerGetBlockNumber(tid) == blkno {
                    *done = true;
                }
            }
        } else {
            // Visit each pending inner tuple on this page and queue its
            // downlinks (or its redirect target).
            let mut jdx = idx;
            while jdx < state.pending.len() {
                let (tid, done) = state.pending[jdx];
                if !done && ItemPointerGetBlockNumber(&tid) == blkno {
                    let offset = ItemPointerGetOffsetNumber(&tid);
                    let mut queued: Vec<ItemPointerData> = Vec::new();
                    {
                        let pm = buf_page_mut(buffer);
                        let page = pm.as_ref();
                        let inner = item_slice(&page, offset);
                        match tuple_state(inner) {
                            SPGIST_LIVE => {
                                for (_, node_off) in inner_tuple_nodes(inner) {
                                    let t_tid = node_tuple_tid(&inner[node_off..]);
                                    if ItemPointerIsValid(&t_tid) {
                                        queued.push(t_tid);
                                    }
                                }
                            }
                            SPGIST_REDIRECT => {
                                queued.push(SpGistDeadTupleHeader::decode(inner).pointer);
                            }
                            other => tuple_state_error(other),
                        }
                    }
                    for t in &queued {
                        add_pending_tid(&mut state.pending, t);
                    }
                    state.pending[jdx].1 = true;
                }
                jdx += 1;
            }
        }

        unlock_release(buffer)?;
    }

    // spgClearPendingList
    debug_assert!(state.pending.iter().all(|(_, done)| *done));
    state.pending.clear();
    Ok(())
}

/// spgvacuumscan: physical-order scan; the relation length is rechecked so
/// pages added by concurrent splits are visited.
/// LockRelationForExtension around the length check: single-backend no-op.
fn spgvacuumscan<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: &mut IndexBulkDeleteResult,
    dead_items: Option<&[ItemPointerData]>,
    collect: Option<&mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_)>,
) -> PgResult<()> {
    let index = info.index;

    let mut state = SpgVacState {
        info,
        stats,
        dead_items,
        collect,
        redirect_xid: xact::GetTopTransactionIdIfAny(),
        pending: Vec::new(),
        my_xmin: snapmgr_seams::active_snapshot_xmin::call(),
        last_filled_block: SPGIST_LAST_FIXED_BLKNO,
    };

    // Reset the counts the scan accumulates; a VACUUM can scan twice.
    state.stats.estimated_count = false;
    state.stats.num_index_tuples = 0.0;
    state.stats.pages_deleted = 0;

    let mut current: BlockNumber = SPGIST_METAPAGE_BLKNO + 1;
    let mut num_pages;
    loop {
        num_pages =
            bufmgr::relation_get_number_of_blocks_in_fork::call(index, ForkNumber::MAIN_FORKNUM)?;
        if current >= num_pages {
            break;
        }
        while current < num_pages {
            // vacuum_delay_point while not holding any buffer lock
            vacuum_delay_point()?;
            let buffer = read_vacuum_buffer(info, current)?;
            spgvacuumpage(&mut state, buffer)?;
            if !state.pending.is_empty() {
                spgprocesspending(&mut state)?;
            }
            current += 1;
        }
    }

    // Propagate the local lastUsedPages cache to the metablock.
    SpGistUpdateMetaPage(index)?;

    // Push any empty pages recorded in the FSM up to the upper FSM levels so
    // searchers can find them promptly.
    if state.stats.pages_deleted > 0 {
        freespace::IndexFreeSpaceMapVacuum(index)?;
    }

    // Index truncation: C keeps it disabled (concurrent-insert unsafe).
    let _ = state.last_filled_block;

    state.stats.num_pages = num_pages;
    state.stats.pages_newly_deleted = state.stats.pages_deleted;
    state.stats.pages_free = state.stats.pages_deleted;
    Ok(())
}

/// spgbulkdelete.
pub fn spgbulkdelete<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    let mut stats = istat.unwrap_or_default();
    spgvacuumscan(info, &mut stats, Some(dead_items), None)?;
    Ok(stats)
}

/// spgbulkdelete with C's collect-only callback shape (validate_index).
pub fn spgbulkdelete_collect<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    callback: &mut (dyn FnMut(&ItemPointerData) -> PgResult<()> + '_),
) -> PgResult<IndexBulkDeleteResult> {
    let mut stats = IndexBulkDeleteResult::default();
    spgvacuumscan(info, &mut stats, None, Some(callback))?;
    Ok(stats)
}

/// spgvacuumcleanup.
pub fn spgvacuumcleanup<'mcx>(
    info: &IndexVacuumInfo<'_, 'mcx>,
    istat: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    if info.analyze_only {
        return Ok(istat);
    }
    // A preceding bulkdelete pass makes the scan unnecessary; otherwise run a
    // no-delete pass for redirect/placeholder cleanup, FSM housekeeping, and
    // stats.
    let mut stats = match istat {
        Some(s) => s,
        None => {
            let mut s = IndexBulkDeleteResult::default();
            spgvacuumscan(info, &mut s, None, None)?;
            s
        }
    };
    // Concurrent tuple moves can double-count; disbelieve totals beyond the
    // heap's own count when that count is accurate.
    if !info.estimated_count && stats.num_index_tuples > info.num_heap_tuples {
        stats.num_index_tuples = info.num_heap_tuples;
    }
    Ok(Some(stats))
}
