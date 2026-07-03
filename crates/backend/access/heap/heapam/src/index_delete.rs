//! heapam.c index-deletion arm: heap_index_delete_tuples (simple deletion) +
//! index_delete_sort/index_delete_check_htid. Loud: bottom-up deletion
//! (bottomup_sort_and_shrink and its costing). C divergence (recorded):
//! index_delete_prefetch_buffer elided — PrefetchBuffer substrate unported.

use ::bufmgr_seams::{BufferPin, BUFFER_LOCK_SHARE, BUFFER_LOCK_UNLOCK};
use ::mcx::Mcx;
use ::tableam_vocab::{TM_IndexDelete, TM_IndexDeleteOp, TM_IndexStatus};
use ::types_core::xact::{InvalidTransactionId, TransactionIdIsValid};
use ::types_core::{BlockNumber, InvalidBlockNumber, OffsetNumber, TransactionId};
use ::types_error::{PgError, PgResult, ERRCODE_INDEX_CORRUPTED};
use ::types_rel::Relation;
use ::types_snapshot::{SnapshotData, SnapshotType};
use ::types_storage::bufpage::PageRef;
use ::types_tuple::{
    FirstOffsetNumber, HeapTupleHeaderData, ItemPointerData, ItemPointerGetBlockNumber,
    ItemPointerGetOffsetNumber,
};

use crate::fetch::heap_hot_search_buffer;
use crate::{unported, HeapTupleHeaderAdvanceConflictHorizon, HeapTupleHeaderGetUpdateXid};

const _: () = assert!(core::mem::size_of::<TM_IndexDelete>() == 8);

/// heap_index_delete_tuples, the tableam index_delete_tuples implementation.
pub fn heap_index_delete_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    delstate: &mut TM_IndexDeleteOp<'mcx>,
) -> PgResult<TransactionId> {
    // earlier pruning is assumed to have covered the conflict, initially
    let mut snapshot_conflict_horizon = InvalidTransactionId;
    let mut blkno = InvalidBlockNumber;
    let mut pin: Option<BufferPin> = None;
    let mut maxoff: OffsetNumber = 0;

    if delstate.bottomup {
        unported("heap_index_delete_tuples bottom-up arm (bottomup_sort_and_shrink)");
    }

    // InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(rel))
    let mut snapshot = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_NON_VACUUMABLE);
    snapshot.vistest = procarray_seams::global_vis_test_for::call(rel);

    let n = delstate.ndeltids as usize;
    index_delete_sort(&mut delstate.deltids[..n]);

    debug_assert!(delstate.ndeltids > 0);
    let mut finalndeltids = 0usize;

    for i in 0..delstate.ndeltids as usize {
        let ideltid = delstate.deltids[i];
        let id = ideltid.id as usize;
        let htid = ideltid.tid;

        if blkno == InvalidBlockNumber || ItemPointerGetBlockNumber(&htid) != blkno {
            if let Some(old) = pin.take() {
                unlock_release(old)?;
            }
            blkno = ItemPointerGetBlockNumber(&htid);
            let p = BufferPin::adopt(bufmgr_seams::read_buffer::call(rel, blkno)?)
                .expect("ReadBuffer returned InvalidBuffer");
            bufmgr_seams::lock_buffer::call(p.buffer(), BUFFER_LOCK_SHARE)?;
            maxoff = p.page().max_offset_number();
            pin = Some(p);
        }
        let pinref = pin.as_ref().expect("pinned above");
        let page = pinref.page();

        index_delete_check_htid(
            &delstate.irel,
            delstate.iblknum,
            &page,
            maxoff,
            &htid,
            delstate.status[id].idxoffnum,
        )?;

        if delstate.status[id].knowndeletable {
            debug_assert!(!delstate.bottomup && !delstate.status[id].promising);
        } else {
            // any non-vacuumable member of the HOT chain blocks deletion
            if heap_hot_search_buffer(htid, rel, pinref, &snapshot, false, true)?.found {
                continue;
            }
            delstate.status[id].knowndeletable = true;
            // bottomup freespace accounting lives behind the loud arm above
        }

        // advance the conflict horizon along the HOT chain (prune-style walk)
        let mut offnum = ItemPointerGetOffsetNumber(&htid);
        let mut prior_xmax = InvalidTransactionId;
        loop {
            if offnum < FirstOffsetNumber || offnum > maxoff {
                break;
            }
            let lp = page.item_id(offnum);
            if lp.is_redirected() {
                offnum = lp.lp_off();
                continue;
            }
            // LP_DEAD: the prune that made it dead already covered the horizon
            if !lp.is_normal() {
                break;
            }
            let (ptr, _len) = page.item_raw(lp);
            // SAFETY: normal line pointer on a pinned + share-locked page.
            let htup = unsafe { &*ptr.cast::<HeapTupleHeaderData>() };

            if TransactionIdIsValid(prior_xmax) && htup.xmin() != prior_xmax {
                break;
            }
            HeapTupleHeaderAdvanceConflictHorizon(htup, &mut snapshot_conflict_horizon)?;

            if !htup.is_hot_updated() {
                break;
            }
            debug_assert!(ItemPointerGetBlockNumber(&htup.t_ctid) == blkno);
            offnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
            prior_xmax = HeapTupleHeaderGetUpdateXid(htup)?;
        }

        finalndeltids = i + 1;
    }

    unlock_release(pin.take().expect("at least one deltid processed"))?;

    // shrink deltids so the index AM may rely on ndeltids' final value
    debug_assert!(finalndeltids > 0 || delstate.bottomup);
    delstate.ndeltids = finalndeltids as i32;

    Ok(snapshot_conflict_horizon)
}

fn unlock_release(pin: BufferPin) -> PgResult<()> {
    bufmgr_seams::lock_buffer::call(pin.buffer(), BUFFER_LOCK_UNLOCK)?;
    pin.release();
    Ok(())
}

#[inline]
fn index_delete_sort_cmp(deltid1: &TM_IndexDelete, deltid2: &TM_IndexDelete) -> i32 {
    let blk1 = ItemPointerGetBlockNumber(&deltid1.tid);
    let blk2 = ItemPointerGetBlockNumber(&deltid2.tid);
    if blk1 != blk2 {
        return if blk1 < blk2 { -1 } else { 1 };
    }
    let pos1 = ItemPointerGetOffsetNumber(&deltid1.tid);
    let pos2 = ItemPointerGetOffsetNumber(&deltid2.tid);
    if pos1 != pos2 {
        return if pos1 < pos2 { -1 } else { 1 };
    }
    debug_assert!(false);
    0
}

// index_delete_sort: specialized shellsort (Sedgewick-Incerpi gaps), adaptive
// to the mostly-presorted arrays this path sees.
pub(crate) fn index_delete_sort(deltids: &mut [TM_IndexDelete]) {
    let ndeltids = deltids.len();
    const GAPS: [usize; 9] = [1968, 861, 336, 112, 48, 21, 7, 3, 1];

    for &hi in GAPS.iter() {
        for i in hi..ndeltids {
            let d = deltids[i];
            let mut j = i;
            while j >= hi && index_delete_sort_cmp(&deltids[j - hi], &d) >= 0 {
                deltids[j] = deltids[j - hi];
                j -= hi;
            }
            deltids[j] = d;
        }
    }
}

// index_delete_check_htid: in-passing corruption checks; the index AM holds
// the index-page buffer lock, so no concurrent VACUUM can move these TIDs.
fn index_delete_check_htid(
    irel: &Relation<'_>,
    iblknum: BlockNumber,
    page: &PageRef<'_>,
    maxoff: OffsetNumber,
    htid: &ItemPointerData,
    idxoffnum: OffsetNumber,
) -> PgResult<()> {
    let indexpagehoffnum = ItemPointerGetOffsetNumber(htid);
    debug_assert!(idxoffnum != 0);

    if indexpagehoffnum > maxoff {
        return Err(index_corrupted(format!(
            "heap tid from index tuple ({},{}) points past end of heap page line pointer array at offset {} of block {} in index \"{}\"",
            ItemPointerGetBlockNumber(htid), indexpagehoffnum, idxoffnum, iblknum, irel.name()
        )));
    }

    let iid = page.item_id(indexpagehoffnum);
    if !iid.is_used() {
        return Err(index_corrupted(format!(
            "heap tid from index tuple ({},{}) points to unused heap page item at offset {} of block {} in index \"{}\"",
            ItemPointerGetBlockNumber(htid), indexpagehoffnum, idxoffnum, iblknum, irel.name()
        )));
    }

    if iid.has_storage() {
        debug_assert!(iid.is_normal());
        let (ptr, _len) = page.item_raw(iid);
        // SAFETY: normal line pointer on a pinned + share-locked page.
        let htup = unsafe { &*ptr.cast::<HeapTupleHeaderData>() };
        if htup.is_heap_only() {
            return Err(index_corrupted(format!(
                "heap tid from index tuple ({},{}) points to heap-only tuple at offset {} of block {} in index \"{}\"",
                ItemPointerGetBlockNumber(htid), indexpagehoffnum, idxoffnum, iblknum, irel.name()
            )));
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn index_corrupted(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INDEX_CORRUPTED))
}
