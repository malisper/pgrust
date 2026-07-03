//! nbtree.c VACUUM arms: btbulkdelete/btvacuumcleanup/btvacuumscan/
//! btvacuumpage + posting-list vacuum (_bt_update_posting from nbtdedup.c).
//! C divergences (recorded): the bulkdelete callback is monomorphized to the
//! sorted dead-TID slice (vac_tid_reaped is its only producer); the read
//! stream collapses to sync per-block reads; the relation-extension lock is
//! skipped (C's own XXX: EB_LOCK_FIRST already closes the race); ereport
//! DEBUG/LOG chatter elided.

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::mcx::{Mcx, MemoryContext, PgVec};
use ::types_core::{BlockNumber, ForkNumber, OffsetNumber};
use ::types_error::PgResult;
use ::types_nbtree::{
    BTCycleId, IndexBulkDeleteResult, BTP_SPLIT_END, BTREE_METAPAGE, P_FIRSTDATAKEY, P_ISDELETED,
    P_ISHALFDEAD, P_ISLEAF, P_NONE, P_RIGHTMOST,
};
use ::types_rel::Relation;
use ::types_storage::buf::BufferAccessStrategy;
use ::types_storage::bufpage::MaxIndexTuplesPerPage;
use ::types_storage::ReadBufferMode;
use ::types_tuple::itemptr::{ItemPointerCompare, ItemPointerData};

use crate::itup::{
    bt_tuple_get_nposting, bt_tuple_get_posting_n, bt_tuple_get_posting_offset,
    bt_tuple_is_pivot, bt_tuple_is_posting, bt_tuple_set_posting, copy_index_tuple,
    index_tuple_size, maxalign, set_t_info, t_info, t_tid, ITup, ItupBuf, INDEX_SIZE_MASK,
};
use crate::page::{
    bt_checkpage, bt_lockbuf, bt_page_is_recyclable, bt_relbuf, bt_upgradelockbufcleanup,
    page_item, page_of_mut, page_opaque, write_opaque,
};
use crate::pagedel::{bt_pagedel, bt_pendingfsm_finalize, bt_pendingfsm_init};
use crate::unported_phase2;
use crate::utils::{bt_end_vacuum, bt_start_vacuum};

// IndexVacuumInfo (access/genam.h); message_level/report_progress dropped
// (logging + progress lanes unported).
pub struct IndexVacuumInfo<'a, 'mcx> {
    pub index: &'a Relation<'mcx>,
    pub heaprel: &'a ::types_rel::RelationData<'mcx>,
    pub analyze_only: bool,
    pub estimated_count: bool,
    pub num_heap_tuples: f64,
    pub strategy: BufferAccessStrategy,
}

pub(crate) struct BTVacState<'a, 'mcx> {
    pub info: &'a IndexVacuumInfo<'a, 'mcx>,
    pub stats: &'a mut IndexBulkDeleteResult,
    pub dead_items: Option<&'a [ItemPointerData]>,
    pub cycleid: BTCycleId,
    pub pendingpages: PgVec<'mcx, ::types_nbtree::BTPendingFSM>,
    pub maxbufsize: usize,
}

fn vacuum_delay_point() -> PgResult<()> {
    crate::check_for_interrupts();
    if init_small::globals::VacuumCostActive() {
        unported_phase2("vacuum_delay_point cost-based delay (VacuumCostActive)");
    }
    Ok(())
}

pub(crate) fn tid_is_member(dead_items: &[ItemPointerData], tid: &ItemPointerData) -> bool {
    dead_items
        .binary_search_by(|probe| ItemPointerCompare(probe, tid).cmp(&0))
        .is_ok()
}

/// btbulkdelete. `dead_items` is the sorted TID-store image.
pub fn btbulkdelete<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
    dead_items: &[ItemPointerData],
) -> PgResult<IndexBulkDeleteResult> {
    let rel = info.index;
    let mut stats = stats.unwrap_or_default();

    // C's PG_ENSURE_ERROR_CLEANUP: the slot is freed on the PgResult error
    // path; a panic aborts the backend, so the leak C guards against is moot.
    let cycleid = bt_start_vacuum(rel)?;
    let res = btvacuumscan(mcx, info, &mut stats, Some(dead_items), cycleid);
    bt_end_vacuum(rel);
    res?;

    Ok(stats)
}

/// btvacuumcleanup. `None` when no bulkdelete ran and no cleanup is needed.
pub fn btvacuumcleanup<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: Option<IndexBulkDeleteResult>,
) -> PgResult<Option<IndexBulkDeleteResult>> {
    if info.analyze_only {
        return Ok(stats);
    }

    let mut stats = match stats {
        Some(stats) => stats,
        None => {
            if !crate::pagedel::bt_vacuum_needs_cleanup(info.index)? {
                return Ok(None);
            }
            let mut stats = IndexBulkDeleteResult::default();
            btvacuumscan(mcx, info, &mut stats, None, 0)?;
            stats.estimated_count = true;
            stats
        }
    };

    debug_assert!(stats.pages_deleted >= stats.pages_free);
    let num_delpages = stats.pages_deleted - stats.pages_free;
    crate::pagedel::bt_set_cleanup_info(info.index, num_delpages)?;

    if !info.estimated_count && stats.num_index_tuples > info.num_heap_tuples {
        stats.num_index_tuples = info.num_heap_tuples;
    }

    Ok(Some(stats))
}

fn btvacuumscan<'mcx>(
    mcx: Mcx<'mcx>,
    info: &IndexVacuumInfo<'_, 'mcx>,
    stats: &mut IndexBulkDeleteResult,
    dead_items: Option<&[ItemPointerData]>,
    cycleid: BTCycleId,
) -> PgResult<()> {
    let rel = info.index;

    stats.num_pages = 0;
    stats.num_index_tuples = 0.0;
    stats.pages_deleted = 0;
    stats.pages_free = 0;

    let mut vstate = BTVacState {
        info,
        stats,
        dead_items,
        cycleid,
        pendingpages: PgVec::new_in(mcx),
        maxbufsize: 0,
    };
    bt_pendingfsm_init(&mut vstate, dead_items.is_none())?;

    let mut scratch = MemoryContext::new("btvacuumpage");

    let mut current: BlockNumber = BTREE_METAPAGE + 1;
    let mut num_pages;
    loop {
        num_pages =
            bufmgr::relation_get_number_of_blocks_in_fork::call(rel, ForkNumber::MAIN_FORKNUM)?;
        if current >= num_pages {
            break;
        }
        while current < num_pages {
            vacuum_delay_point()?;
            let pin = BufferPin::adopt(bufmgr::read_buffer_extended::call(
                rel,
                ForkNumber::MAIN_FORKNUM,
                current,
                ReadBufferMode::Normal,
                info.strategy.clone(),
            )?)
            .expect("ReadBufferExtended returned InvalidBuffer");
            btvacuumpage(&mut vstate, &mut scratch, pin)?;
            current += 1;
        }
    }

    vstate.stats.num_pages = num_pages;

    bt_pendingfsm_finalize(&mut vstate)?;
    if vstate.stats.pages_free > 0 {
        ::freespace::IndexFreeSpaceMapVacuum(rel)?;
    }
    Ok(())
}

fn btvacuumpage(
    vstate: &mut BTVacState<'_, '_>,
    scratch: &mut MemoryContext,
    pin: BufferPin,
) -> PgResult<()> {
    let rel = vstate.info.index;
    let heaprel = vstate.info.heaprel;
    let scanblkno = pin.block_number();
    let mut blkno = scanblkno;
    let mut pin = pin;

    scratch.reset();
    let scx = scratch.mcx();

    loop {
        let mut attempt_pagedel = false;
        let mut backtrack_to: BlockNumber = P_NONE;

        bt_lockbuf(rel, &pin, ::types_nbtree::BT_READ)?;
        let mut opaque = None;
        if !pin.page().is_new() {
            bt_checkpage(rel, &pin)?;
            opaque = Some(page_opaque(&pin.page()));
        }

        debug_assert!(blkno <= scanblkno);
        if blkno != scanblkno {
            // Backtracked to a right sibling: only a live leaf page carrying
            // the current cycle ID needs work (C LOGs corruption here).
            let ok = opaque
                .as_ref()
                .is_some_and(|o| P_ISLEAF(o) && !P_ISHALFDEAD(o));
            if !ok {
                debug_assert!(false);
                bt_relbuf(rel, pin)?;
                return Ok(());
            }
            let o = opaque.as_ref().expect("checked above");
            if o.btpo_cycleid != vstate.cycleid || P_ISDELETED(o) {
                bt_relbuf(rel, pin)?;
                return Ok(());
            }
        }

        if opaque.is_none() || bt_page_is_recyclable(&pin.page(), heaprel)? {
            ::freespace::RecordFreeIndexPage(rel, blkno)?;
            vstate.stats.pages_deleted += 1;
            vstate.stats.pages_free += 1;
        } else if P_ISDELETED(opaque.as_ref().expect("non-new page")) {
            vstate.stats.pages_deleted += 1;
        } else if P_ISHALFDEAD(opaque.as_ref().expect("non-new page")) {
            attempt_pagedel = true;
        } else if P_ISLEAF(opaque.as_ref().expect("non-new page")) {
            bt_upgradelockbufcleanup(rel, &pin)?;
            // Re-read below the lock trade: the page may have changed while
            // unlocked (C reads through the live pointer).
            let opaque = page_opaque(&pin.page());

            if vstate.cycleid != 0
                && opaque.btpo_cycleid == vstate.cycleid
                && (opaque.btpo_flags & BTP_SPLIT_END) == 0
                && !P_RIGHTMOST(&opaque)
                && opaque.btpo_next < scanblkno
            {
                backtrack_to = opaque.btpo_next;
            }

            let mut deletable = [0 as OffsetNumber; MaxIndexTuplesPerPage];
            let mut ndeletable = 0usize;
            let mut updatable: PgVec<'_, VacPosting<'_>> = PgVec::new_in(scx);
            let minoff = P_FIRSTDATAKEY(&opaque);
            let mut maxoff = pin.page().max_offset_number();
            let mut nhtidsdead = 0usize;
            let mut nhtidslive = 0usize;

            if let Some(dead) = vstate.dead_items {
                let mut offnum = minoff;
                while offnum <= maxoff {
                    let page = pin.page();
                    let itup = page_item(&page, page.item_id(offnum));
                    // SAFETY: on-page tuple under the cleanup lock.
                    unsafe {
                        debug_assert!(!bt_tuple_is_pivot(itup));
                        if !bt_tuple_is_posting(itup) {
                            if tid_is_member(dead, &t_tid(itup)) {
                                deletable[ndeletable] = offnum;
                                ndeletable += 1;
                                nhtidsdead += 1;
                            } else {
                                nhtidslive += 1;
                            }
                        } else {
                            let nposting = bt_tuple_get_nposting(itup);
                            let (vacposting, nremaining) =
                                btreevacuumposting(scx, dead, itup, offnum)?;
                            match vacposting {
                                None => debug_assert!(nremaining == nposting),
                                Some(vacposting) if nremaining > 0 => {
                                    debug_assert!(nremaining < nposting);
                                    updatable.push(vacposting);
                                    nhtidsdead += nposting - nremaining;
                                }
                                Some(_) => {
                                    deletable[ndeletable] = offnum;
                                    ndeletable += 1;
                                    nhtidsdead += nposting;
                                }
                            }
                            nhtidslive += nremaining;
                        }
                    }
                    offnum += 1;
                }
            }

            if ndeletable > 0 || !updatable.is_empty() {
                debug_assert!(nhtidsdead >= ndeletable + updatable.len());
                crate::pagedel::bt_delitems_vacuum(
                    scx,
                    rel,
                    &pin,
                    &deletable[..ndeletable],
                    &mut updatable,
                )?;
                vstate.stats.tuples_removed += nhtidsdead as f64;
                maxoff = pin.page().max_offset_number();
            } else {
                debug_assert!(nhtidsdead == 0);
                if vstate.cycleid != 0 && opaque.btpo_cycleid == vstate.cycleid {
                    let mut o = page_opaque(&pin.page());
                    o.btpo_cycleid = 0;
                    write_opaque(&mut page_of_mut(&pin), &o);
                    bufmgr::mark_buffer_dirty_hint::call(pin.buffer(), true)?;
                }
            }

            if minoff > maxoff {
                attempt_pagedel = blkno == scanblkno;
            } else if vstate.dead_items.is_some() {
                vstate.stats.num_index_tuples += nhtidslive as f64;
            } else {
                vstate.stats.num_index_tuples += (maxoff - minoff + 1) as f64;
            }
            debug_assert!(!attempt_pagedel || nhtidslive == 0);
        }

        if attempt_pagedel {
            debug_assert!(blkno == scanblkno);
            bt_pagedel(scx, rel, pin, vstate)?;
        } else {
            bt_relbuf(rel, pin)?;
        }

        if backtrack_to == P_NONE {
            return Ok(());
        }
        blkno = backtrack_to;

        vacuum_delay_point()?;

        // As C: no _bt_getbuf (all-zero pages must be recyclable, not fatal),
        // and the caller's strategy applies.
        pin = BufferPin::adopt(bufmgr::read_buffer_extended::call(
            rel,
            ForkNumber::MAIN_FORKNUM,
            blkno,
            ReadBufferMode::Normal,
            vstate.info.strategy.clone(),
        )?)
        .expect("ReadBufferExtended returned InvalidBuffer");
    }
}

// BTVacuumPostingData; itup is an owned image (C points into the page until
// _bt_update_posting replaces it — copying keeps the borrow local).
pub(crate) struct VacPosting<'s> {
    pub itup: ItupBuf<'s>,
    pub updatedoffset: OffsetNumber,
    pub deletetids: PgVec<'s, u16>,
}

/// btreevacuumposting: `(replacement metadata, TIDs remaining)`.
///
/// # Safety
/// `posting` is a live posting tuple on the cleanup-locked page.
unsafe fn btreevacuumposting<'s>(
    scx: Mcx<'s>,
    dead_items: &[ItemPointerData],
    posting: ITup,
    updatedoffset: OffsetNumber,
) -> PgResult<(Option<VacPosting<'s>>, usize)> {
    let nitem = bt_tuple_get_nposting(posting);
    let mut live = 0usize;
    let mut vacposting: Option<VacPosting<'s>> = None;

    for i in 0..nitem {
        let tid = bt_tuple_get_posting_n(posting, i);
        if !tid_is_member(dead_items, &tid) {
            live += 1;
        } else {
            if vacposting.is_none() {
                vacposting = Some(VacPosting {
                    itup: copy_index_tuple(scx, posting)?,
                    updatedoffset,
                    deletetids: PgVec::new_in(scx),
                });
            }
            vacposting
                .as_mut()
                .expect("created above")
                .deletetids
                .push(i as u16);
        }
    }

    Ok((vacposting, live))
}

/// _bt_update_posting (nbtdedup.c): replace `vacposting.itup` with the image
/// lacking the deleted TIDs.
pub(crate) fn bt_update_posting<'s>(scx: Mcx<'s>, vacposting: &mut VacPosting<'s>) -> PgResult<()> {
    let orig = vacposting.itup.as_ptr();
    // SAFETY: owned image captured by btreevacuumposting.
    unsafe {
        let norig = bt_tuple_get_nposting(orig);
        let nhtids = norig - vacposting.deletetids.len();
        debug_assert!(nhtids > 0 && nhtids < norig);

        let keysize = bt_tuple_get_posting_offset(orig);
        let newsize = if nhtids > 1 {
            maxalign(keysize + nhtids * core::mem::size_of::<ItemPointerData>())
        } else {
            keysize
        };
        debug_assert!(newsize <= INDEX_SIZE_MASK as usize);
        debug_assert!(newsize == maxalign(newsize));

        let mut itup = ItupBuf::with_size(scx, newsize)?;
        core::ptr::copy_nonoverlapping(orig, itup.as_mut_ptr(), keysize);
        let info = (t_info(itup.as_ptr()) & !INDEX_SIZE_MASK) | newsize as u16;
        set_t_info(itup.as_mut_ptr(), info);

        let htids_off = if nhtids > 1 {
            bt_tuple_set_posting(itup.as_mut_ptr(), nhtids as u16, keysize);
            keysize
        } else {
            set_t_info(
                itup.as_mut_ptr(),
                t_info(itup.as_ptr()) & !::types_nbtree::INDEX_ALT_TID_MASK,
            );
            0
        };

        let mut ui = 0usize;
        let mut d = 0usize;
        for i in 0..norig {
            if d < vacposting.deletetids.len() && vacposting.deletetids[d] as usize == i {
                d += 1;
                continue;
            }
            let tid = bt_tuple_get_posting_n(orig, i);
            itup.as_mut_ptr()
                .add(htids_off + ui * core::mem::size_of::<ItemPointerData>())
                .cast::<ItemPointerData>()
                .write_unaligned(tid);
            ui += 1;
        }
        debug_assert!(ui == nhtids);
        debug_assert!(d == vacposting.deletetids.len());

        vacposting.itup = itup;
    }
    Ok(())
}
