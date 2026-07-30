//! heapam.c index-deletion arm: heap_index_delete_tuples (simple + bottom-up)
//! + index_delete_sort/index_delete_check_htid + bottomup_sort_and_shrink.
//! C divergence (recorded): index_delete_prefetch_buffer elided —
//! PrefetchBuffer substrate unported.

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
use crate::{HeapTupleHeaderAdvanceConflictHorizon, HeapTupleHeaderGetUpdateXid};

const BOTTOMUP_MAX_NBLOCKS: usize = 6;
const BOTTOMUP_TOLERANCE_NBLOCKS: i64 = 3;

// IndexDeleteCounts (heapam.c)
#[derive(Clone, Copy)]
struct IndexDeleteCounts {
    npromisingtids: i16,
    ntids: i16,
    ifirsttid: i32,
}

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
    let mut nblocksaccessed = 0usize;

    let mut nblocksfavorable = 0usize;
    let mut curtargetfreespace = delstate.bottomupfreespace;
    let mut lastfreespace = 0i32;
    let mut actualfreespace = 0i32;
    let mut bottomup_final_block = false;

    // InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(rel))
    let mut snapshot = SnapshotData::sentinel(mcx, SnapshotType::SNAPSHOT_NON_VACUUMABLE);
    snapshot.vistest = procarray_seams::global_vis_test_for::call(rel);

    let n = delstate.ndeltids as usize;
    index_delete_sort(&mut delstate.deltids[..n]);

    if delstate.bottomup {
        nblocksfavorable = bottomup_sort_and_shrink(mcx, delstate)?;
    }

    debug_assert!(delstate.ndeltids > 0);
    let mut finalndeltids = 0usize;

    for i in 0..delstate.ndeltids as usize {
        let ideltid = delstate.deltids[i];
        let id = ideltid.id as usize;
        let htid = ideltid.tid;

        if blkno == InvalidBlockNumber || ItemPointerGetBlockNumber(&htid) != blkno {
            if delstate.bottomup {
                if bottomup_final_block {
                    break;
                }
                // main cost-control rule: the last page must have freed space
                if nblocksaccessed >= 1 && actualfreespace == lastfreespace {
                    break;
                }
                lastfreespace = actualfreespace;

                // decay the target unless the next block is favorable/contiguous
                debug_assert!(nblocksaccessed > 0 || nblocksfavorable > 0);
                if nblocksfavorable > 0 {
                    nblocksfavorable -= 1;
                } else {
                    curtargetfreespace /= 2;
                }
            }

            if let Some(old) = pin.take() {
                unlock_release(old)?;
            }
            blkno = ItemPointerGetBlockNumber(&htid);
            let p = BufferPin::adopt(bufmgr_seams::read_buffer::call(rel, blkno)?)
                .expect("ReadBuffer returned InvalidBuffer");
            nblocksaccessed += 1;
            debug_assert!(!delstate.bottomup || nblocksaccessed <= BOTTOMUP_MAX_NBLOCKS);
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

            if delstate.bottomup {
                debug_assert!(delstate.status[id].freespace > 0);
                actualfreespace += delstate.status[id].freespace as i32;
                if actualfreespace >= curtargetfreespace {
                    bottomup_final_block = true;
                }
            }
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

#[track_caller]
#[cold]
#[inline(never)]
fn index_corrupted(msg: String) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INDEX_CORRUPTED))
}

fn bottomup_nblocksfavorable(
    blockgroups: &[IndexDeleteCounts],
    deltids: &[TM_IndexDelete],
) -> usize {
    let mut lastblock: i64 = -1;
    let mut nblocksfavorable = 0usize;

    debug_assert!(!blockgroups.is_empty() && blockgroups.len() <= BOTTOMUP_MAX_NBLOCKS);

    // tolerate slightly out-of-order blocks (bucketing blips)
    for group in blockgroups {
        let firstdtid = &deltids[group.ifirsttid as usize];
        let block = ItemPointerGetBlockNumber(&firstdtid.tid) as i64;

        if lastblock != -1
            && (block < lastblock - BOTTOMUP_TOLERANCE_NBLOCKS
                || block > lastblock + BOTTOMUP_TOLERANCE_NBLOCKS)
        {
            break;
        }
        nblocksfavorable += 1;
        lastblock = block;
    }

    debug_assert!(nblocksfavorable >= 1);
    nblocksfavorable
}

fn bottomup_sort_and_shrink_cmp(
    g1: &IndexDeleteCounts,
    g2: &IndexDeleteCounts,
) -> core::cmp::Ordering {
    use core::cmp::Ordering;
    // npromisingtids desc (caller bucketed to powers of two)
    match g2.npromisingtids.cmp(&g1.npromisingtids) {
        Ordering::Equal => {}
        o => return o,
    }
    // ntids desc, bucketed dynamically
    let ntids1 = (g1.ntids as u32).max(1).next_power_of_two();
    let ntids2 = (g2.ntids as u32).max(1).next_power_of_two();
    match ntids2.cmp(&ntids1) {
        Ordering::Equal => {}
        o => return o,
    }
    // ifirsttid asc == heap block number asc among equals
    g1.ifirsttid.cmp(&g2.ifirsttid)
}

// bottomup_sort_and_shrink: regroup deltids by promising-ness of their heap
// blocks and keep only the BOTTOMUP_MAX_NBLOCKS best blocks.
fn bottomup_sort_and_shrink<'mcx>(
    mcx: Mcx<'mcx>,
    delstate: &mut TM_IndexDeleteOp<'mcx>,
) -> PgResult<usize> {
    debug_assert!(delstate.bottomup);
    debug_assert!(delstate.ndeltids > 0);
    let n = delstate.ndeltids as usize;

    let mut blockgroups: ::mcx::PgVec<'mcx, IndexDeleteCounts> =
        ::mcx::vec_with_capacity_in(mcx, n)?;
    let mut curblock = InvalidBlockNumber;
    for i in 0..n {
        let ideltid = &delstate.deltids[i];
        let promising = delstate.status[ideltid.id as usize].promising;

        if curblock != ItemPointerGetBlockNumber(&ideltid.tid) {
            debug_assert!(
                curblock == InvalidBlockNumber
                    || curblock < ItemPointerGetBlockNumber(&ideltid.tid)
            );
            curblock = ItemPointerGetBlockNumber(&ideltid.tid);
            blockgroups.push(IndexDeleteCounts {
                ifirsttid: i as i32,
                ntids: 1,
                npromisingtids: 0,
            });
        } else {
            blockgroups.last_mut().expect("group started").ntids += 1;
        }
        if promising {
            blockgroups
                .last_mut()
                .expect("group started")
                .npromisingtids += 1;
        }
    }

    // power-of-two bucketing: small npromisingtids differences are noise
    for group in blockgroups.iter_mut() {
        if group.npromisingtids <= 4 {
            group.npromisingtids = 4;
        } else {
            group.npromisingtids = (group.npromisingtids as u32).next_power_of_two() as i16;
        }
    }

    blockgroups.sort_unstable_by(bottomup_sort_and_shrink_cmp);
    let nblockgroups = blockgroups.len().min(BOTTOMUP_MAX_NBLOCKS);
    let nblocksfavorable =
        bottomup_nblocksfavorable(&blockgroups[..nblockgroups], &delstate.deltids);

    let mut reordered: ::mcx::PgVec<'mcx, TM_IndexDelete> = ::mcx::vec_with_capacity_in(mcx, n)?;
    for group in &blockgroups[..nblockgroups] {
        let first = group.ifirsttid as usize;
        reordered.extend_from_slice(&delstate.deltids[first..first + group.ntids as usize]);
    }
    let ncopied = reordered.len();
    delstate.deltids[..ncopied].copy_from_slice(&reordered);
    delstate.ndeltids = ncopied as i32;

    Ok(nblocksfavorable)
}
