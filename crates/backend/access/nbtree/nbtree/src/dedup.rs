//! nbtdedup.c write side: _bt_dedup_pass + single value strategy +
//! _bt_bottomupdel_pass. Loud: _bt_update_posting (vacuum lane).

use ::bufmgr_seams::{self as bufmgr, BufferPin};
use ::mcx::{vec_with_capacity_in, Mcx};
use ::tableam::{TM_IndexDelete, TM_IndexDeleteOp, TM_IndexStatus};
use ::types_core::{OffsetNumber, BLCKSZ};
use ::types_error::{PgError, PgResult};
use ::types_nbtree::dedup::BTDedupState;
use ::types_nbtree::{
    BTMaxItemSize, BTPageOpaqueData, MaxTIDsPerBTreePage, BTP_HAS_GARBAGE,
    BTREE_SINGLEVAL_FILLFACTOR, P_FIRSTDATAKEY, P_HAS_GARBAGE, P_HIKEY, P_RIGHTMOST,
    XLOG_BTREE_DEDUP,
};
use ::types_rel::Relation;
use ::types_storage::bufpage::{PageMut, PageRef, SizeOfPageHeaderData};
use ::types_tuple::itemptr::{ItemPointerData, ItemPointerGetBlockNumber};
use ::xloginsert_seams::{XLogRegBuf, REGBUF_STANDARD};

use crate::itup::{
    bt_tuple_get_max_heap_tid, bt_tuple_get_nposting, bt_tuple_get_posting_n,
    bt_tuple_is_posting, maxalign, t_tid, ITup, INDEX_SIZE_MASK,
};
use crate::page::{page_item, page_of_mut, page_opaque, write_opaque};
use crate::relation_needs_wal;
use crate::utils::bt_keep_natts_fast;

const SizeOfBtreeOpaque: usize = core::mem::size_of::<BTPageOpaqueData>();
const SizeOfItemId: usize = core::mem::size_of::<::types_storage::bufpage::ItemIdData>();

#[repr(align(8))]
struct TempPage([u8; BLCKSZ]);

#[track_caller]
#[cold]
#[inline(never)]
fn dedup_add_failed(what: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("deduplication failed to add {what}")))
}

/// _bt_dedup_pass.
/// # Safety
/// `buf`: pinned + write-locked leaf, no LP_DEAD items; `newitem` live;
/// `newitemsz` MAXALIGNed, sans line pointer.
pub(crate) unsafe fn bt_dedup_pass(
    rel: &Relation<'_>,
    buf: &BufferPin,
    newitem: ITup,
    newitemsz: usize,
    bottomupdedup: bool,
) -> PgResult<()> {
    let page = buf.page();
    let opaque = page_opaque(&page);
    let mut pagesaving = 0usize;
    let mut singlevalstrat = false;
    let nkeyatts = rel.indnkeyatts();

    let newitemsz = newitemsz + SizeOfItemId;

    let mut state = BTDedupState::new((BTMaxItemSize / 2).min(INDEX_SIZE_MASK as usize));

    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = page.max_offset_number();

    if !bottomupdedup {
        singlevalstrat = bt_do_singleval(rel, &page, &state, minoff, newitem);
    }

    // PageGetTempPageCopySpecial; the copy claims the original's LSN (FPI).
    let mut temp = TempPage([0u8; BLCKSZ]);
    let mut newpage =
        PageMut::from_raw(core::ptr::NonNull::new(temp.0.as_mut_ptr()).expect("stack page"));
    newpage.init(SizeOfBtreeOpaque);
    core::ptr::copy_nonoverlapping(
        page.as_ptr().add(page.pd_special() as usize),
        newpage.as_ref().as_ptr().cast_mut().add(BLCKSZ - SizeOfBtreeOpaque),
        SizeOfBtreeOpaque,
    );
    newpage.set_lsn(page.lsn());

    if !P_RIGHTMOST(&opaque) {
        let hitemid = page.item_id(P_HIKEY);
        let hitem = page_item(&page, hitemid);
        let hslice = core::slice::from_raw_parts(hitem, hitemid.lp_len() as usize);
        if newpage.add_item(hslice, P_HIKEY, 0).is_none() {
            return Err(dedup_add_failed("highkey"));
        }
    }

    for offnum in minoff..=maxoff {
        let itemid = page.item_id(offnum);
        let itup = page_item(&page, itemid);
        debug_assert!(!itemid.is_dead());

        if offnum == minoff {
            state.start_pending(itup, offnum);
        } else if state.deduplicate
            && bt_keep_natts_fast(rel, state.base, itup) > nkeyatts
            && state.save_htid(itup)
        {
        } else {
            pagesaving += state
                .finish_pending(&mut newpage)
                .map_err(|()| dedup_add_failed("tuple to page"))?;

            if singlevalstrat {
                // sixth capped posting ends merging: the tail waits for the split
                if state.nmaxitems == 5 {
                    bt_singleval_fillfactor(&mut state, newitemsz);
                } else if state.nmaxitems == 6 {
                    state.deduplicate = false;
                    singlevalstrat = false;
                }
            }

            state.start_pending(itup, offnum);
        }
    }

    pagesaving += state
        .finish_pending(&mut newpage)
        .map_err(|()| dedup_add_failed("tuple to page"))?;

    if state.nintervals == 0 {
        return Ok(());
    }

    if P_HAS_GARBAGE(&opaque) {
        let mut nopaque = page_opaque(&newpage.as_ref());
        nopaque.btpo_flags &= !BTP_HAS_GARBAGE;
        write_opaque(&mut newpage, &nopaque);
    }

    // critical section: PageRestoreTempPage + WAL, no early returns.
    {
        let orig = page_of_mut(buf);
        // SAFETY: whole-page overwrite under the exclusive lock held by caller.
        core::ptr::copy_nonoverlapping(
            temp.0.as_ptr(),
            orig.as_ref().as_ptr().cast_mut(),
            BLCKSZ,
        );
    }
    bufmgr::mark_buffer_dirty::call(buf.buffer())?;

    if relation_needs_wal(rel) {
        let xlrec = crate::wal::xl_btree_dedup(state.nintervals as u16);
        // the intervals array rides as block 0 data: dropped whenever the
        // whole buffer image is stored
        let frags: [&[u8]; 1] = [state.intervals_bytes()];
        let reg0 = XLogRegBuf {
            block_id: 0,
            buffer: buf.buffer(),
            flags: REGBUF_STANDARD,
            bufdata: &frags,
        };
        let recptr = ::xloginsert_seams::xlog_insert_record::call(
            ::rmgr::RM_BTREE_ID as u8,
            XLOG_BTREE_DEDUP,
            0,
            &[&xlrec],
            &[reg0],
        )?;
        page_of_mut(buf).set_lsn(recptr);
    }

    debug_assert!(
        pagesaving < newitemsz || buf.page().exact_free_space() >= newitemsz
    );
    Ok(())
}

/// _bt_bottomupdel_pass: returns true when enough space was freed (or when a
/// follow-up dedup pass would be useless).
/// # Safety
/// As [`bt_dedup_pass`]; page has no LP_DEAD items.
pub(crate) unsafe fn bt_bottomupdel_pass<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    buf: &BufferPin,
    heap_rel: &Relation<'mcx>,
    newitemsz: usize,
) -> PgResult<bool> {
    let page = buf.page();
    let opaque = page_opaque(&page);
    let nkeyatts = rel.indnkeyatts();

    let newitemsz = newitemsz + SizeOfItemId;

    // "not really deduplicating": TIDs feed delstate, never a posting image
    let mut state = BTDedupState::new(BLCKSZ);

    let mut delstate = TM_IndexDeleteOp {
        irel: rel.alias(),
        iblknum: buf.block_number(),
        bottomup: true,
        bottomupfreespace: (BLCKSZ / 16).max(newitemsz) as i32,
        ndeltids: 0,
        deltids: vec_with_capacity_in(mcx, MaxTIDsPerBTreePage)?,
        status: vec_with_capacity_in(mcx, MaxTIDsPerBTreePage)?,
    };

    let minoff = P_FIRSTDATAKEY(&opaque);
    let maxoff = page.max_offset_number();
    for offnum in minoff..=maxoff {
        let itemid = page.item_id(offnum);
        let itup = page_item(&page, itemid);
        debug_assert!(!itemid.is_dead());

        if offnum == minoff {
            state.start_pending(itup, offnum);
        } else if bt_keep_natts_fast(rel, state.base, itup) > nkeyatts && state.save_htid(itup) {
        } else {
            bt_bottomupdel_finish_pending(&page, &mut state, &mut delstate);
            state.start_pending(itup, offnum);
        }
    }
    bt_bottomupdel_finish_pending(&page, &mut state, &mut delstate);

    // zero promising tuples: still ask the tableam, but tell caller to skip
    // the pointless deduplication pass that would otherwise follow
    let neverdedup = state.nintervals == 0;

    crate::delete::bt_delitems_delete_check(mcx, rel, buf, heap_rel, &mut delstate)?;

    if neverdedup {
        return Ok(true);
    }

    Ok(buf.page().exact_free_space() >= (BLCKSZ / 24).max(newitemsz))
}

// _bt_bottomupdel_finish_pending: intervals become deletion candidates.
unsafe fn bt_bottomupdel_finish_pending(
    page: &PageRef<'_>,
    state: &mut BTDedupState,
    delstate: &mut TM_IndexDeleteOp<'_>,
) {
    let dupinterval = state.nitems > 1;
    debug_assert!(state.nitems > 0);
    debug_assert!(state.nitems <= state.nhtids);
    debug_assert!(state.intervals[state.nintervals].baseoff == state.baseoff);

    for i in 0..state.nitems {
        let offnum = state.baseoff + i as OffsetNumber;
        let itemid = page.item_id(offnum);
        let itup = page_item(page, itemid);

        if !bt_tuple_is_posting(itup) {
            delstate.deltids.push(TM_IndexDelete {
                tid: t_tid(itup),
                id: delstate.deltids.len() as i16,
            });
            delstate.status.push(TM_IndexStatus {
                idxoffnum: offnum,
                knowndeletable: false,
                promising: dupinterval,
                freespace: (itemid.lp_len() as usize + SizeOfItemId) as i16,
            });
        } else {
            // at most one promising TID per posting list: first or last, and
            // only when its table block predominates within the posting list
            let nitem = bt_tuple_get_nposting(itup);
            let mut firstpromising = false;
            let mut lastpromising = false;

            if dupinterval {
                let minblk = ItemPointerGetBlockNumber(&bt_tuple_get_posting_n(itup, 0));
                let midblk = ItemPointerGetBlockNumber(&bt_tuple_get_posting_n(itup, nitem / 2));
                let maxblk = ItemPointerGetBlockNumber(&bt_tuple_get_max_heap_tid(itup));
                firstpromising = minblk == midblk;
                lastpromising = !firstpromising && midblk == maxblk;
            }

            for p in 0..nitem {
                delstate.deltids.push(TM_IndexDelete {
                    tid: bt_tuple_get_posting_n(itup, p),
                    id: delstate.deltids.len() as i16,
                });
                delstate.status.push(TM_IndexStatus {
                    idxoffnum: offnum,
                    knowndeletable: false,
                    promising: (firstpromising && p == 0)
                        || (lastpromising && p == nitem - 1),
                    freespace: core::mem::size_of::<ItemPointerData>() as i16, // at worst
                });
            }
        }
    }
    delstate.ndeltids = delstate.deltids.len() as i32;

    if dupinterval {
        state.intervals[state.nintervals].nitems = state.nitems as u16;
        state.nintervals += 1;
    }

    state.nhtids = 0;
    state.nitems = 0;
    state.phystupsize = 0;
}

/// _bt_do_singleval.
/// # Safety
/// As [`bt_dedup_pass`].
unsafe fn bt_do_singleval(
    rel: &Relation<'_>,
    page: &::types_storage::bufpage::PageRef<'_>,
    _state: &BTDedupState,
    minoff: OffsetNumber,
    newitem: ITup,
) -> bool {
    let nkeyatts = rel.indnkeyatts();

    let itup = page_item(page, page.item_id(minoff));
    if bt_keep_natts_fast(rel, newitem, itup) > nkeyatts {
        let itup = page_item(page, page.item_id(page.max_offset_number()));
        if bt_keep_natts_fast(rel, newitem, itup) > nkeyatts {
            return true;
        }
    }

    false
}

// _bt_singleval_fillfactor: calculation must match nbtsplitloc.c.
fn bt_singleval_fillfactor(state: &mut BTDedupState, newitemsz: usize) {
    let mut leftfree =
        BLCKSZ - SizeOfPageHeaderData - maxalign(core::mem::size_of::<BTPageOpaqueData>());
    // new high key includes pivot heap TID space
    leftfree -= newitemsz + maxalign(core::mem::size_of::<ItemPointerData>());

    let reduction =
        (leftfree as f64 * ((100 - BTREE_SINGLEVAL_FILLFACTOR) as f64 / 100.0)) as usize;
    if state.maxpostingsize > reduction {
        state.maxpostingsize -= reduction;
    } else {
        state.maxpostingsize = 0;
    }
}
