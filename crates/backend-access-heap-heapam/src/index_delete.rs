//! F6 — heapam's tableam `index_delete_tuples` implementation
//! (`access/heap/heapam.c`).
//!
//! This module ports `heap_index_delete_tuples` and its helpers
//! (`index_delete_check_htid`, `index_delete_prefetch_buffer`,
//! `index_delete_sort`/`index_delete_sort_cmp`, and the bottom-up
//! sort/shrink/favorable-block costing). The two heapam.c primitives it leans
//! on that belong to *other* (not-yet-ported) heapam families —
//! `heap_hot_search_buffer` (the HOT-chain scan) and the on-page
//! `HeapTupleHeader` deform (`(HeapTupleHeader) PageGetItem`) — cross the
//! family boundary by honest seam `::call`s (panic-until-landed).
//!
//! Page access goes through the repo's `Buffer`-id-through-seams model: we read
//! a snapshot copy of the heap page (`buffer_get_page`) under the share lock
//! and walk it with the `backend-storage-page` line-pointer accessors, exactly
//! mirroring C's direct `Page` pointer reads.

use mcx::Mcx;
use types_core::primitive::{
    BlockNumber, InvalidBlockNumber, OffsetNumber, TransactionId,
};
use types_error::PgResult;
use types_nbtree::{TmIndexDelete, TmIndexDeleteOp};
use types_rel::Relation;
use types_snapshot::snapshot::{SnapshotData, SnapshotType};
use types_storage::buf::BUFFER_LOCK_SHARE;
use types_storage::{Buffer, InvalidBuffer};
use types_tuple::heaptuple::{
    HeapTupleHeaderData, ItemPointerData, HEAP_HOT_UPDATED, HEAP_MOVED,
    HEAP_ONLY_TUPLE, HEAP_XMAX_INVALID, INVALID_OFFSET_NUMBER, FIRST_OFFSET_NUMBER,
};

use backend_storage_page::{
    ItemIdHasStorage, ItemIdIsNormal, ItemIdIsRedirected, ItemIdIsUsed,
    ItemIdGetRedirect, ItemPointerGetBlockNumber, ItemPointerGetOffsetNumber,
    PageGetItemId, PageGetMaxOffsetNumber, PageRef,
};

use backend_access_heap_heapam_seams as heapam_seam;
use backend_access_heap_vacuumlazy_seams as vacuumlazy_seam;
use backend_storage_buffer_bufmgr_seams as bufmgr_seam;
// Only the `#[cfg]`-gated prefetch path consults the catalog-relation check.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "illumos",
    target_os = "solaris",
))]
use backend_catalog_catalog_seams as catalog_seam;

use backend_access_heap_heapam_visibility::htup::{
    HeapTupleHeaderGetXmin, HeapTupleHeaderGetXvac, HeapTupleHeaderXminInvalid,
};
use types_tuple::heaptuple::HeapTupleHeaderXminCommitted;

// htup_details.h IsHotUpdated/IsHeapOnly use this crate's GetUpdateXid for the
// chain advance.
use backend_access_heap_heapam_visibility::HeapTupleHeaderGetUpdateXid;

/// `InvalidTransactionId`.
const InvalidTransactionId: TransactionId = 0;
/// `InvalidOffsetNumber`.
const InvalidOffsetNumber: OffsetNumber = INVALID_OFFSET_NUMBER;
/// `MAIN_FORKNUM` (common/relpath.h). Only the `#[cfg]`-gated prefetch path
/// references it.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "illumos",
    target_os = "solaris",
))]
const MAIN_FORKNUM: i32 = 0;

// heap_index_delete_tuples bottom-up index deletion costing constants.
const BOTTOMUP_MAX_NBLOCKS: i32 = 6;
const BOTTOMUP_TOLERANCE_NBLOCKS: i64 = 3;

/// `USE_PREFETCH` (pg_config_manual.h) — defined when `posix_fadvise` is
/// available (`USE_POSIX_FADVISE`). macOS/OpenBSD/Windows lack it. Mirrors the
/// `backend-access-transam-xlogprefetcher` port's gate so the prefetch path is
/// compiled out on the same platforms C does.
const USE_PREFETCH: bool = cfg!(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "illumos",
    target_os = "solaris",
));

// ===========================================================================
// helpers mirroring the C macros used below.
// ===========================================================================

/// `OffsetNumberIsValid(offsetNumber)` (off.h).
#[inline]
fn OffsetNumberIsValid(offset_number: OffsetNumber) -> bool {
    offset_number != INVALID_OFFSET_NUMBER
}

/// `BlockNumberIsValid(blockNumber)` (block.h).
#[inline]
fn BlockNumberIsValid(block_number: BlockNumber) -> bool {
    block_number != InvalidBlockNumber
}

/// `BufferIsValid(bufnum)` (bufmgr.h).
#[inline]
fn BufferIsValid(buf: Buffer) -> bool {
    buf != InvalidBuffer
}

/// `pg_nextpower2_32(num)` (`port/pg_bitutils.h`, static inline) — the next
/// power of 2 >= num (num must be > 0 and <= 2^31).
#[inline]
fn pg_nextpower2_32(num: u32) -> u32 {
    debug_assert!(num > 0 && num <= 0x8000_0000);
    1u32 << (32 - (num - 1).leading_zeros())
}

/// `HeapTupleHeaderIsHotUpdated(tup)` (htup_details.h).
#[inline]
fn HeapTupleHeaderIsHotUpdated(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask2 & HEAP_HOT_UPDATED) != 0
        && (tup.t_infomask & HEAP_XMAX_INVALID) == 0
        && !HeapTupleHeaderXminInvalid(tup)
}

/// `HeapTupleHeaderIsHeapOnly(tup)` (htup_details.h).
#[inline]
fn HeapTupleHeaderIsHeapOnly(tup: &HeapTupleHeaderData) -> bool {
    (tup.t_infomask2 & HEAP_ONLY_TUPLE) != 0
}

/// `TransactionIdDidCommit(xid)` — clog lookup through the transam owner seam.
fn transaction_id_did_commit(xid: TransactionId) -> PgResult<bool> {
    let transaction_xmin =
        backend_utils_time_snapmgr_pc_seams::transaction_xmin::call()?;
    backend_access_transam_transam_seams::transaction_id_did_commit::call(xid, transaction_xmin)
}

// ===========================================================================
// IndexDeleteCounts — per-heap-block grouping used for bottom-up costing
// (heapam.c struct, file-local).
// ===========================================================================

/// `IndexDeleteCounts` (heapam.c) — the per-heap-block count of TIDs that
/// drives bottom-up deletion's block-visit ordering.
#[derive(Clone, Copy, Debug, Default)]
struct IndexDeleteCounts {
    /// Number of "promising" TIDs in group.
    npromisingtids: i16,
    /// Number of TIDs in group.
    ntids: i16,
    /// Offset to group's first deltid.
    ifirsttid: i16,
}

// ===========================================================================
// IndexDeletePrefetchState — coordinates prefetching (USE_PREFETCH only).
// ===========================================================================

/// `IndexDeletePrefetchState` (heapam.c, `#ifdef USE_PREFETCH`) — tracks which
/// buffers we can prefetch and which have already been prefetched.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "illumos",
    target_os = "solaris",
))]
struct IndexDeletePrefetchState {
    cur_hblkno: BlockNumber,
    next_item: i32,
    ndeltids: i32,
}

/// `index_delete_prefetch_buffer(rel, prefetch_state, prefetch_count)`
/// (heapam.c, `#ifdef USE_PREFETCH`) — issue prefetch requests for
/// `prefetch_count` buffers, picking up where the previous call left off.
///
/// Note: we expect the deltids array to be sorted in an order that groups TIDs
/// by heap block, with all TIDs for each block appearing together in exactly
/// one group.
#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "dragonfly",
    target_os = "illumos",
    target_os = "solaris",
))]
fn index_delete_prefetch_buffer(
    rel: &Relation,
    deltids: &[TmIndexDelete],
    prefetch_state: &mut IndexDeletePrefetchState,
    prefetch_count: i32,
) -> PgResult<()> {
    let mut cur_hblkno = prefetch_state.cur_hblkno;
    let mut count = 0;
    let ndeltids = prefetch_state.ndeltids;

    let mut i = prefetch_state.next_item;
    while i < ndeltids && count < prefetch_count {
        let htid = &deltids[i as usize].tid;

        if cur_hblkno == InvalidBlockNumber
            || ItemPointerGetBlockNumber(htid) != cur_hblkno
        {
            cur_hblkno = ItemPointerGetBlockNumber(htid);
            vacuumlazy_seam::prefetch_buffer::call(rel.rd_id, MAIN_FORKNUM, cur_hblkno)?;
            count += 1;
        }
        i += 1;
    }

    /*
     * Save the prefetch position so that next time we can continue from that
     * position.
     */
    prefetch_state.next_item = i;
    prefetch_state.cur_hblkno = cur_hblkno;
    Ok(())
}

// ===========================================================================
// index_delete_check_htid — index-corruption check (heapam.c).
// ===========================================================================

/// `index_delete_check_htid(delstate, page, maxoff, htid, istatus)` (heapam.c)
/// — check for index corruption involving an invalid TID in the index AM
/// caller's index page.
///
/// This is an ideal place for these checks: the index AM holds a buffer lock on
/// the index page containing the TIDs we examine, so concurrent VACUUMs cannot
/// interfere. We can be sure the index is corrupt when `htid` points directly
/// to an LP_UNUSED item or a heap-only tuple, which is not the case during
/// standard index scans.
fn index_delete_check_htid(
    mcx: Mcx,
    rel: &Relation,
    delstate: &TmIndexDeleteOp,
    buf: Buffer,
    page: &PageRef,
    maxoff: OffsetNumber,
    htid: &ItemPointerData,
    idxoffnum: OffsetNumber,
) -> PgResult<()> {
    let indexpagehoffnum = ItemPointerGetOffsetNumber(htid);

    debug_assert!(OffsetNumberIsValid(idxoffnum));

    if indexpagehoffnum > maxoff {
        return Err(types_error::PgError::error(format!(
            "heap tid from index tuple ({},{}) points past end of heap page line pointer array at offset {} of block {} in index \"{}\"",
            ItemPointerGetBlockNumber(htid),
            indexpagehoffnum,
            idxoffnum,
            delstate.iblknum,
            rel.name()
        )));
    }

    let iid = PageGetItemId(page, indexpagehoffnum)?;
    if !ItemIdIsUsed(&iid) {
        return Err(types_error::PgError::error(format!(
            "heap tid from index tuple ({},{}) points to unused heap page item at offset {} of block {} in index \"{}\"",
            ItemPointerGetBlockNumber(htid),
            indexpagehoffnum,
            idxoffnum,
            delstate.iblknum,
            rel.name()
        )));
    }

    if ItemIdHasStorage(&iid) {
        debug_assert!(ItemIdIsNormal(&iid));
        // `(HeapTupleHeader) PageGetItem(page, iid)` — deform the on-page
        // header via the scan-family seam (buf + offset is the LP_NORMAL item).
        let _ = buf;
        let htup = heapam_seam::heap_page_tuple_header::call(mcx, buf, indexpagehoffnum)?;

        if HeapTupleHeaderIsHeapOnly(&htup) {
            return Err(types_error::PgError::error(format!(
                "heap tid from index tuple ({},{}) points to heap-only tuple at offset {} of block {} in index \"{}\"",
                ItemPointerGetBlockNumber(htid),
                indexpagehoffnum,
                idxoffnum,
                delstate.iblknum,
                rel.name()
            )));
        }
    }

    Ok(())
}

// ===========================================================================
// heap_index_delete_tuples — the tableam index_delete_tuples implementation.
// ===========================================================================

/// `heap_index_delete_tuples(rel, delstate)` (heapam.c) — the heapam
/// implementation of tableam's `index_delete_tuples` interface. See the
/// tableam header for the interface and general theory of operation. Each call
/// is either a simple index deletion call or a bottom-up index deletion call.
///
/// It's possible for this to generate a fair amount of I/O, since we may be
/// deleting hundreds of tuples from a single index block. To amortize that
/// cost, this uses prefetching and combines repeat accesses to the same heap
/// block.
pub fn heap_index_delete_tuples<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    delstate: &mut TmIndexDeleteOp<'mcx>,
) -> PgResult<TransactionId> {
    /* Initial assumption is that earlier pruning took care of conflict */
    let mut snapshot_conflict_horizon = InvalidTransactionId;
    let mut blkno = InvalidBlockNumber;
    let mut buf = InvalidBuffer;
    let mut page: Option<PageRef> = None;
    let mut page_bytes;
    let mut maxoff = InvalidOffsetNumber;
    let mut prior_xmax: TransactionId;
    let mut finalndeltids: i32 = 0;
    let mut nblocksaccessed: i32 = 0;

    /* State that's only used in bottom-up index deletion case */
    let mut nblocksfavorable: i32 = 0;
    let mut curtargetfreespace: i32 = delstate.bottomupfreespace;
    let mut lastfreespace: i32 = 0;
    let mut actualfreespace: i32 = 0;
    let mut bottomup_final_block = false;

    /* InitNonVacuumableSnapshot(SnapshotNonVacuumable, GlobalVisTestFor(rel)) */
    // `mut` because heap_hot_search_buffer now takes `&mut` snapshot (it is the
    // dirty-snapshot output param). HeapTupleSatisfiesNonVacuumable does not
    // write the xmin/xmax/speculativeToken fields, so reusing it across the
    // HOT-chain loop stays faithful.
    let mut snapshot_non_vacuumable = init_non_vacuumable_snapshot(rel)?;

    /* Sort caller's deltids array by TID for further processing */
    index_delete_sort(delstate);

    /*
     * Bottom-up case: resort deltids array in an order attuned to where the
     * greatest number of promising TIDs are to be found, and determine how
     * many blocks from the start of sorted array should be considered
     * favorable. This will also shrink the deltids array in order to
     * eliminate completely unfavorable blocks up front.
     */
    if delstate.bottomup {
        nblocksfavorable = bottomup_sort_and_shrink(delstate);
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "illumos",
        target_os = "solaris",
    ))]
    let mut prefetch_state = IndexDeletePrefetchState {
        cur_hblkno: InvalidBlockNumber,
        next_item: 0,
        ndeltids: delstate.deltids.len() as i32,
    };

    if USE_PREFETCH {
        #[cfg(any(
            target_os = "linux",
            target_os = "android",
            target_os = "freebsd",
            target_os = "netbsd",
            target_os = "dragonfly",
            target_os = "illumos",
            target_os = "solaris",
        ))]
        {
            /*
             * Determine the prefetch distance that we will attempt to maintain.
             *
             * Since the caller holds a buffer lock somewhere in rel, we'd
             * better make sure that isn't a catalog relation before we call
             * code that does syscache lookups, to avoid risk of deadlock.
             */
            let mut prefetch_distance = if catalog_seam::is_catalog_relation::call(rel) {
                backend_utils_init_small_seams::maintenance_io_concurrency::call()
            } else {
                backend_utils_cache_spccache::get_tablespace_maintenance_io_concurrency(
                    rel.rd_rel.reltablespace,
                    backend_utils_init_small_seams::my_database_table_space::call(),
                    backend_utils_init_small_seams::maintenance_io_concurrency::call(),
                )?
            };

            /* Cap initial prefetch distance for bottom-up deletion caller */
            if delstate.bottomup {
                debug_assert!(nblocksfavorable >= 1);
                debug_assert!(nblocksfavorable <= BOTTOMUP_MAX_NBLOCKS);
                prefetch_distance = prefetch_distance.min(nblocksfavorable);
            }

            /* Start prefetching. */
            index_delete_prefetch_buffer(
                rel,
                &delstate.deltids,
                &mut prefetch_state,
                prefetch_distance,
            )?;
        }
    }

    /* Iterate over deltids, determine which to delete, check their horizon */
    debug_assert!(!delstate.deltids.is_empty());
    let ndeltids = delstate.deltids.len();
    for i in 0..ndeltids {
        let ideltid = delstate.deltids[i];
        let htid = ideltid.tid;
        let istatus_idx = ideltid.id as usize;
        let mut offnum: OffsetNumber;

        /*
         * Read buffer, and perform required extra steps each time a new block
         * is encountered. Avoid refetching if it's the same block as the one
         * from the last htid.
         */
        if blkno == InvalidBlockNumber || ItemPointerGetBlockNumber(&htid) != blkno {
            /*
             * Consider giving up early for bottom-up index deletion caller
             * first. (Only prefetch next-next block afterwards, when it
             * becomes clear that we're at least going to access the next block
             * in line.)
             *
             * Sometimes the first block frees so much space for bottom-up
             * caller that the deletion process can end without accessing any
             * more blocks. It is usually necessary to access 2 or 3 blocks per
             * bottom-up deletion operation, though.
             */
            if delstate.bottomup {
                /*
                 * We often allow caller to delete a few additional items whose
                 * entries we reached after the point that space target from
                 * caller was satisfied. The cost of accessing the page was
                 * already paid at that point, so it made sense to finish it
                 * off. When that happened, we finalize everything here (by
                 * finishing off the whole bottom-up deletion operation without
                 * needlessly paying the cost of accessing any more blocks).
                 */
                if bottomup_final_block {
                    break;
                }

                /*
                 * Give up when we didn't enable our caller to free any
                 * additional space as a result of processing the page that we
                 * just finished up with. This rule is the main way in which we
                 * keep the cost of bottom-up deletion under control.
                 */
                if nblocksaccessed >= 1 && actualfreespace == lastfreespace {
                    break;
                }
                lastfreespace = actualfreespace; /* for next time */

                /*
                 * Deletion operation (which is bottom-up) will definitely
                 * access the next block in line. Prepare for that now.
                 *
                 * Decay target free space so that we don't hang on for too
                 * long with a marginal case. (Space target is only truly
                 * helpful when it allows us to recognize that we don't need to
                 * access more than 1 or 2 blocks to satisfy caller due to
                 * agreeable workload characteristics.)
                 *
                 * We are a bit more patient when we encounter contiguous
                 * blocks, though: these are treated as favorable blocks. The
                 * decay process is only applied when the next block in line is
                 * not a favorable/contiguous block. This is not an exception to
                 * the general rule; we still insist on finding at least one
                 * deletable item per block accessed. See
                 * bottomup_nblocksfavorable() for full details of the theory
                 * behind favorable blocks and heap block locality in general.
                 *
                 * Note: The first block in line is always treated as a
                 * favorable block, so the earliest possible point that the
                 * decay can be applied is just before we access the second
                 * block in line. The Assert() verifies this for us.
                 */
                debug_assert!(nblocksaccessed > 0 || nblocksfavorable > 0);
                if nblocksfavorable > 0 {
                    nblocksfavorable -= 1;
                } else {
                    curtargetfreespace /= 2;
                }
            }

            /* release old buffer */
            if BufferIsValid(buf) {
                bufmgr_seam::unlock_release_buffer::call(buf);
            }

            blkno = ItemPointerGetBlockNumber(&htid);
            buf = bufmgr_seam::read_buffer_extended::call(rel, blkno)?;
            nblocksaccessed += 1;
            debug_assert!(!delstate.bottomup || nblocksaccessed <= BOTTOMUP_MAX_NBLOCKS);

            if USE_PREFETCH {
                #[cfg(any(
                    target_os = "linux",
                    target_os = "android",
                    target_os = "freebsd",
                    target_os = "netbsd",
                    target_os = "dragonfly",
                    target_os = "illumos",
                    target_os = "solaris",
                ))]
                {
                    /*
                     * To maintain the prefetch distance, prefetch one more page
                     * for each page we read.
                     */
                    index_delete_prefetch_buffer(
                        rel,
                        &delstate.deltids,
                        &mut prefetch_state,
                        1,
                    )?;
                }
            }

            bufmgr_seam::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;

            /*
             * `page = BufferGetPage(buf)` — snapshot the share-locked page; all
             * subsequent reads for this block walk this copy (the buffer
             * remains pinned + share-locked until we re-fetch or release it).
             */
            page_bytes = bufmgr_seam::buffer_get_page::call(mcx, buf)?;
            let p = PageRef::new(&page_bytes)?;
            maxoff = PageGetMaxOffsetNumber(&p);
            page = Some(p);
        }

        let page_ref = page
            .as_ref()
            .expect("heap_index_delete_tuples: page must be set after first block");

        /*
         * In passing, detect index corruption involving an index page with a
         * TID that points to a location in the heap that couldn't possibly be
         * correct. We only do this with actual TIDs from caller's index page
         * (not items reached by traversing through a HOT chain).
         */
        index_delete_check_htid(
            mcx,
            rel,
            delstate,
            buf,
            page_ref,
            maxoff,
            &htid,
            delstate.status[istatus_idx].idxoffnum,
        )?;

        if delstate.status[istatus_idx].knowndeletable {
            debug_assert!(!delstate.bottomup && !delstate.status[istatus_idx].promising);
        } else {
            let tmp = htid;

            /* Are any tuples from this HOT chain non-vacuumable? */
            let res = heapam_seam::heap_hot_search_buffer::call(
                mcx,
                tmp,
                rel,
                buf,
                &mut snapshot_non_vacuumable,
                false, /* all_dead == NULL */
                true,  /* first_call */
            )?;
            if res.found {
                continue; /* can't delete entry */
            }

            /* Caller will delete, since whole HOT chain is vacuumable */
            delstate.status[istatus_idx].knowndeletable = true;

            /* Maintain index free space info for bottom-up deletion case */
            if delstate.bottomup {
                debug_assert!(delstate.status[istatus_idx].freespace > 0);
                actualfreespace += delstate.status[istatus_idx].freespace as i32;
                if actualfreespace >= curtargetfreespace {
                    bottomup_final_block = true;
                }
            }
        }

        /*
         * Maintain snapshotConflictHorizon value for deletion operation as a
         * whole by advancing current value using heap tuple headers. This is
         * loosely based on the logic for pruning a HOT chain.
         */
        offnum = ItemPointerGetOffsetNumber(&htid);
        prior_xmax = InvalidTransactionId; /* cannot check first XMIN */
        loop {
            /* Sanity check (pure paranoia) */
            if offnum < FIRST_OFFSET_NUMBER {
                break;
            }

            /*
             * An offset past the end of page's line pointer array is possible
             * when the array was truncated
             */
            if offnum > maxoff {
                break;
            }

            let lp = PageGetItemId(page_ref, offnum)?;
            if ItemIdIsRedirected(&lp) {
                offnum = ItemIdGetRedirect(&lp);
                continue;
            }

            /*
             * We'll often encounter LP_DEAD line pointers (especially with an
             * entry marked knowndeletable by our caller up front). No heap
             * tuple headers get examined for an htid that leads us to an
             * LP_DEAD item. This is okay because the earlier pruning operation
             * that made the line pointer LP_DEAD in the first place must have
             * considered the original tuple header as part of generating its
             * own snapshotConflictHorizon value.
             *
             * Relying on XLOG_HEAP2_PRUNE_VACUUM_SCAN records like this is the
             * same strategy that index vacuuming uses in all cases. Index
             * VACUUM WAL records don't even have a snapshotConflictHorizon
             * field of their own for this reason.
             */
            if !ItemIdIsNormal(&lp) {
                break;
            }

            /* htup = (HeapTupleHeader) PageGetItem(page, lp) */
            let htup = heapam_seam::heap_page_tuple_header::call(mcx, buf, offnum)?;

            /*
             * Check the tuple XMIN against prior XMAX, if any
             */
            if TransactionIdIsValid(prior_xmax)
                && HeapTupleHeaderGetXmin(&htup) != prior_xmax
            {
                break;
            }

            HeapTupleHeaderAdvanceConflictHorizon(&htup, &mut snapshot_conflict_horizon)?;

            /*
             * If the tuple is not HOT-updated, then we are at the end of this
             * HOT-chain. No need to visit later tuples from the same update
             * chain (they get their own index entries) -- just move on to next
             * htid from index AM caller.
             */
            if !HeapTupleHeaderIsHotUpdated(&htup) {
                break;
            }

            /* Advance to next HOT chain member */
            debug_assert!(ItemPointerGetBlockNumber(&htup.t_ctid) == blkno);
            offnum = ItemPointerGetOffsetNumber(&htup.t_ctid);
            prior_xmax = HeapTupleHeaderGetUpdateXid(&htup)?;
        }

        /* Enable further/final shrinking of deltids for caller */
        finalndeltids = (i + 1) as i32;
    }

    bufmgr_seam::unlock_release_buffer::call(buf);

    /*
     * Shrink deltids array to exclude non-deletable entries at the end. This
     * is not just a minor optimization. Final deltids array size might be zero
     * for a bottom-up caller. Index AM is explicitly allowed to rely on
     * ndeltids being zero in all cases with zero total deletable entries.
     */
    debug_assert!(finalndeltids > 0 || delstate.bottomup);
    delstate.deltids.truncate(finalndeltids as usize);

    Ok(snapshot_conflict_horizon)
}

/// `TransactionIdIsValid(xid)`.
#[inline]
fn TransactionIdIsValid(xid: TransactionId) -> bool {
    xid != InvalidTransactionId
}

/// `InitNonVacuumableSnapshot(snapshot, GlobalVisTestFor(rel))` (snapmgr.h):
/// a zeroed `SnapshotData` with `snapshot_type = SNAPSHOT_NON_VACUUMABLE` and
/// `vistest` set from the relation's global-visibility test state.
fn init_non_vacuumable_snapshot(rel: &Relation) -> PgResult<SnapshotData> {
    let vistest = vacuumlazy_seam::global_vis_test_for::call(rel.rd_id)?;
    let mut snapshot = SnapshotData::sentinel(SnapshotType::SNAPSHOT_NON_VACUUMABLE);
    snapshot.vistest = vistest;
    Ok(snapshot)
}

// ===========================================================================
// HeapTupleHeaderAdvanceConflictHorizon — ratchet the conflict horizon
// (heapam.c, public; also used by pruneheap.c).
// ===========================================================================

/// `HeapTupleHeaderAdvanceConflictHorizon(tuple, snapshotConflictHorizon)`
/// (heapam.c) — maintain `snapshotConflictHorizon` by ratcheting it forward
/// using any committed XIDs contained in `tuple`, an obsolescent heap tuple
/// that the caller is physically removing (e.g. via HOT pruning or index
/// deletion).
///
/// Caller must initialize its value to `InvalidTransactionId`, generally
/// interpreted as "definitely no need for a recovery conflict". The final value
/// must reflect all heap tuples the caller will physically remove (or remove
/// TID references to). `ResolveRecoveryConflictWithSnapshot()` is passed the
/// final value by the REDO routine when it replays the caller's operation.
pub fn HeapTupleHeaderAdvanceConflictHorizon(
    tuple: &HeapTupleHeaderData,
    snapshot_conflict_horizon: &mut TransactionId,
) -> PgResult<()> {
    let xmin = HeapTupleHeaderGetXmin(tuple);
    let xmax = HeapTupleHeaderGetUpdateXid(tuple)?;
    let xvac = HeapTupleHeaderGetXvac(tuple);

    if tuple.t_infomask & HEAP_MOVED != 0 {
        if TransactionIdPrecedes(*snapshot_conflict_horizon, xvac) {
            *snapshot_conflict_horizon = xvac;
        }
    }

    /*
     * Ignore tuples inserted by an aborted transaction or if the tuple was
     * updated/deleted by the inserting transaction.
     *
     * Look for a committed hint bit, or if no xmin bit is set, check clog.
     */
    if HeapTupleHeaderXminCommitted(tuple)
        || (!HeapTupleHeaderXminInvalid(tuple) && transaction_id_did_commit(xmin)?)
    {
        if xmax != xmin && TransactionIdFollows(xmax, *snapshot_conflict_horizon) {
            *snapshot_conflict_horizon = xmax;
        }
    }
    Ok(())
}

/// `TransactionIdPrecedes(id1, id2)` (transam.c) — id1 logically precedes id2.
#[inline]
fn TransactionIdPrecedes(id1: TransactionId, id2: TransactionId) -> bool {
    /*
     * If either ID is a permanent XID then we can just do unsigned comparison.
     * If both are normal, do a modulo-2^32 comparison.
     */
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 < id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff < 0
}

/// `TransactionIdFollows(id1, id2)` (transam.c) — id1 logically follows id2.
#[inline]
fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    if !TransactionIdIsNormal(id1) || !TransactionIdIsNormal(id2) {
        return id1 > id2;
    }
    let diff = id1.wrapping_sub(id2) as i32;
    diff > 0
}

/// `TransactionIdIsNormal(xid)` (transam.h) — `xid >= FirstNormalTransactionId`.
#[inline]
fn TransactionIdIsNormal(xid: TransactionId) -> bool {
    const FirstNormalTransactionId: TransactionId = 3;
    xid >= FirstNormalTransactionId
}

// ===========================================================================
// index_delete_sort / index_delete_sort_cmp — TID-order shellsort (heapam.c).
// ===========================================================================

/// `index_delete_sort_cmp(deltid1, deltid2)` (heapam.c) — the specialized
/// inlineable comparison function for [`index_delete_sort`].
#[inline]
fn index_delete_sort_cmp(deltid1: &TmIndexDelete, deltid2: &TmIndexDelete) -> i32 {
    let tid1 = &deltid1.tid;
    let tid2 = &deltid2.tid;

    {
        let blk1 = ItemPointerGetBlockNumber(tid1);
        let blk2 = ItemPointerGetBlockNumber(tid2);

        if blk1 != blk2 {
            return if blk1 < blk2 { -1 } else { 1 };
        }
    }
    {
        let pos1 = ItemPointerGetOffsetNumber(tid1);
        let pos2 = ItemPointerGetOffsetNumber(tid2);

        if pos1 != pos2 {
            return if pos1 < pos2 { -1 } else { 1 };
        }
    }

    debug_assert!(false);

    0
}

/// `index_delete_sort(delstate)` (heapam.c) — sort `delstate->deltids` by TID,
/// preparing it for further processing by [`heap_index_delete_tuples`].
///
/// This operation becomes a noticeable consumer of CPU cycles with some
/// workloads, so we go to the trouble of specialization/micro optimization. We
/// use shellsort because it's easy to specialize, compiles to relatively few
/// instructions, and is adaptive to presorted inputs/subsets (typical here).
fn index_delete_sort(delstate: &mut TmIndexDeleteOp) {
    let deltids = &mut delstate.deltids;
    let ndeltids = deltids.len() as i32;

    /*
     * Shellsort gap sequence (taken from Sedgewick-Incerpi paper).
     *
     * This implementation is fast with array sizes up to ~4500. This covers
     * all supported BLCKSZ values.
     */
    const GAPS: [i32; 9] = [1968, 861, 336, 112, 48, 21, 7, 3, 1];

    for g in 0..GAPS.len() {
        let hi = GAPS[g];
        let mut i = hi;
        while i < ndeltids {
            let d = deltids[i as usize];
            let mut j = i;

            while j >= hi && index_delete_sort_cmp(&deltids[(j - hi) as usize], &d) >= 0 {
                deltids[j as usize] = deltids[(j - hi) as usize];
                j -= hi;
            }
            deltids[j as usize] = d;
            i += 1;
        }
    }
}

// ===========================================================================
// bottom-up sort/shrink + favorable-block costing (heapam.c).
// ===========================================================================

/// `bottomup_nblocksfavorable(blockgroups, nblockgroups, deltids)` (heapam.c) —
/// how many blocks should be considered favorable/contiguous for a bottom-up
/// index deletion pass, starting from and including the first block in line.
///
/// There is always at least one favorable block: in the worst case (totally
/// random heap blocks) the first block in line is a degenerate array of
/// contiguous blocks consisting of a single block.
fn bottomup_nblocksfavorable(
    blockgroups: &[IndexDeleteCounts],
    nblockgroups: i32,
    deltids: &[TmIndexDelete],
) -> i32 {
    let mut lastblock: i64 = -1;
    let mut nblocksfavorable = 0;

    debug_assert!(nblockgroups >= 1);
    debug_assert!(nblockgroups <= BOTTOMUP_MAX_NBLOCKS);

    /*
     * We tolerate heap blocks that will be accessed only slightly out of
     * physical order. Small blips occur when a pair of almost-contiguous
     * blocks happen to fall into different buckets (perhaps due only to a
     * small difference in npromisingtids that the bucketing scheme didn't
     * quite manage to ignore). We effectively ignore these blips by applying a
     * small tolerance. The precise tolerance we use is a little arbitrary, but
     * it works well enough in practice.
     */
    for b in 0..nblockgroups {
        let group = &blockgroups[b as usize];
        let firstdtid = &deltids[group.ifirsttid as usize];
        let block = ItemPointerGetBlockNumber(&firstdtid.tid);

        if lastblock != -1
            && ((block as i64) < lastblock - BOTTOMUP_TOLERANCE_NBLOCKS
                || (block as i64) > lastblock + BOTTOMUP_TOLERANCE_NBLOCKS)
        {
            break;
        }

        nblocksfavorable += 1;
        lastblock = block as i64;
    }

    /* Always indicate that there is at least 1 favorable block */
    debug_assert!(nblocksfavorable >= 1);

    nblocksfavorable
}

/// `bottomup_sort_and_shrink_cmp(arg1, arg2)` (heapam.c) — qsort comparison for
/// [`bottomup_sort_and_shrink`].
fn bottomup_sort_and_shrink_cmp(
    group1: &IndexDeleteCounts,
    group2: &IndexDeleteCounts,
) -> core::cmp::Ordering {
    use core::cmp::Ordering;

    /*
     * Most significant field is npromisingtids (which we invert the order of so
     * as to sort in desc order).
     *
     * Caller should have already normalized npromisingtids fields into
     * power-of-two values (buckets).
     */
    if group1.npromisingtids > group2.npromisingtids {
        return Ordering::Less;
    }
    if group1.npromisingtids < group2.npromisingtids {
        return Ordering::Greater;
    }

    /*
     * Tiebreak: desc ntids sort order.
     *
     * We cannot expect power-of-two values for ntids fields. We should behave
     * as if they were already rounded up for us instead.
     */
    if group1.ntids != group2.ntids {
        let ntids1 = pg_nextpower2_32(group1.ntids as u32);
        let ntids2 = pg_nextpower2_32(group2.ntids as u32);

        if ntids1 > ntids2 {
            return Ordering::Less;
        }
        if ntids1 < ntids2 {
            return Ordering::Greater;
        }
    }

    /*
     * Tiebreak: asc offset-into-deltids-for-block (offset to first TID for
     * block in deltids array) order.
     *
     * This is equivalent to sorting in ascending heap block number order
     * (among otherwise equal subsets of the array). This approach allows us to
     * avoid accessing the out-of-line TID. (We rely on the assumption that the
     * deltids array was sorted in ascending heap TID order when these offsets
     * to the first TID from each heap block group were formed.)
     */
    if group1.ifirsttid > group2.ifirsttid {
        return Ordering::Greater;
    }
    if group1.ifirsttid < group2.ifirsttid {
        return Ordering::Less;
    }

    /* pg_unreachable() */
    Ordering::Equal
}

/// `bottomup_sort_and_shrink(delstate)` (heapam.c) — [`heap_index_delete_tuples`]
/// helper for bottom-up deletion callers.
///
/// Sorts `delstate->deltids` in the order needed for useful processing by
/// bottom-up deletion. The array should already be sorted in TID order when
/// called. The sort process groups heap TIDs into heap block groupings;
/// earlier/more-promising groups/blocks are usually those known to have the
/// most "promising" TIDs.
///
/// Sets the new size of the deltids array. `deltids` will only have TIDs from
/// the `BOTTOMUP_MAX_NBLOCKS` most promising heap blocks on return — often
/// shrinking it to a small fraction of its original size. Returns the number of
/// "favorable" blocks (see [`bottomup_nblocksfavorable`]).
fn bottomup_sort_and_shrink(delstate: &mut TmIndexDeleteOp) -> i32 {
    let mut curblock = InvalidBlockNumber;
    let mut nblockgroups: i32 = 0;
    let mut ncopied: i32 = 0;
    let nblocksfavorable;

    debug_assert!(delstate.bottomup);
    debug_assert!(!delstate.deltids.is_empty());

    let ndeltids = delstate.deltids.len();

    /* Calculate per-heap-block count of TIDs */
    let mut blockgroups: Vec<IndexDeleteCounts> =
        vec![IndexDeleteCounts::default(); ndeltids];
    for i in 0..ndeltids {
        let ideltid = &delstate.deltids[i];
        let istatus = &delstate.status[ideltid.id as usize];
        let htid = &ideltid.tid;
        let promising = istatus.promising;

        if curblock != ItemPointerGetBlockNumber(htid) {
            /* New block group */
            nblockgroups += 1;

            debug_assert!(
                curblock < ItemPointerGetBlockNumber(htid) || !BlockNumberIsValid(curblock)
            );

            curblock = ItemPointerGetBlockNumber(htid);
            let g = (nblockgroups - 1) as usize;
            blockgroups[g].ifirsttid = i as i16;
            blockgroups[g].ntids = 1;
            blockgroups[g].npromisingtids = 0;
        } else {
            blockgroups[(nblockgroups - 1) as usize].ntids += 1;
        }

        if promising {
            blockgroups[(nblockgroups - 1) as usize].npromisingtids += 1;
        }
    }

    /*
     * We're about ready to sort block groups to determine the optimal order
     * for visiting heap blocks. But before we do, round the number of
     * promising tuples for each block group up to the next power-of-two,
     * unless it is very low (less than 4), in which case we round up to 4.
     * npromisingtids is far too noisy to trust when choosing between a pair of
     * block groups that both have very low values.
     *
     * This scheme divides heap blocks/block groups into buckets. Each bucket
     * contains blocks that have _approximately_ the same number of promising
     * TIDs as each other. The goal is to ignore relatively small differences
     * in the total number of promising entries, so that the whole process can
     * give a little weight to heapam factors (like heap block locality)
     * instead. This isn't a trade-off, really -- we have nothing to lose. It
     * would be foolish to interpret small differences in npromisingtids values
     * as anything more than noise.
     *
     * We tiebreak on nhtids when sorting block group subsets that have the
     * same npromisingtids, but this has the same issues as npromisingtids, and
     * so nhtids is subject to the same power-of-two bucketing scheme. The only
     * reason that we don't fix nhtids in the same way here too is that we'll
     * need accurate nhtids values after the sort. We handle nhtids
     * bucketization dynamically instead (in the sort comparator).
     *
     * See bottomup_nblocksfavorable() for a full explanation of when and how
     * heap locality/favorable blocks can significantly influence when and how
     * heap blocks are accessed.
     */
    for b in 0..nblockgroups {
        let group = &mut blockgroups[b as usize];

        /* Better off falling back on nhtids with low npromisingtids */
        if group.npromisingtids <= 4 {
            group.npromisingtids = 4;
        } else {
            group.npromisingtids = pg_nextpower2_32(group.npromisingtids as u32) as i16;
        }
    }

    /* Sort groups and rearrange caller's deltids array */
    let mut groups: Vec<IndexDeleteCounts> =
        blockgroups[..nblockgroups as usize].to_vec();
    groups.sort_by(bottomup_sort_and_shrink_cmp);

    let mut reordereddeltids: Vec<TmIndexDelete> = Vec::with_capacity(ndeltids);

    nblockgroups = BOTTOMUP_MAX_NBLOCKS.min(nblockgroups);
    /* Determine number of favorable blocks at the start of final deltids */
    nblocksfavorable = bottomup_nblocksfavorable(&groups, nblockgroups, &delstate.deltids);

    for b in 0..nblockgroups {
        let group = &groups[b as usize];
        let firstidx = group.ifirsttid as usize;

        for k in 0..(group.ntids as usize) {
            reordereddeltids.push(delstate.deltids[firstidx + k]);
        }
        ncopied += group.ntids as i32;
    }

    /* Copy final grouped and sorted TIDs back into start of caller's array */
    for (k, dt) in reordereddeltids.iter().enumerate().take(ncopied as usize) {
        delstate.deltids[k] = *dt;
    }
    delstate.deltids.truncate(ncopied as usize);

    nblocksfavorable
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::{MemoryContext, PgVec};
    use types_nbtree::TmIndexStatus;

    fn tid(blk: BlockNumber, off: OffsetNumber) -> ItemPointerData {
        ItemPointerData::new(blk, off)
    }

    fn make_op<'mcx>(
        mcx: mcx::Mcx<'mcx>,
        entries: &[(BlockNumber, OffsetNumber, bool, i16)],
        bottomup: bool,
    ) -> TmIndexDeleteOp<'mcx> {
        // entries: (blk, off, promising, freespace); id == position.
        let mut deltids: PgVec<TmIndexDelete> = PgVec::new_in(mcx);
        let mut status: PgVec<TmIndexStatus> = PgVec::new_in(mcx);
        for (i, &(blk, off, promising, freespace)) in entries.iter().enumerate() {
            deltids.push(TmIndexDelete {
                tid: tid(blk, off),
                id: i as i16,
            });
            status.push(TmIndexStatus {
                idxoffnum: 1,
                knowndeletable: false,
                promising,
                freespace,
            });
        }
        TmIndexDeleteOp {
            iblknum: 0,
            bottomup,
            bottomupfreespace: 0,
            deltids,
            status,
        }
    }

    #[test]
    fn pg_nextpower2_32_matches_c() {
        assert_eq!(pg_nextpower2_32(1), 1);
        assert_eq!(pg_nextpower2_32(2), 2);
        assert_eq!(pg_nextpower2_32(3), 4);
        assert_eq!(pg_nextpower2_32(5), 8);
        assert_eq!(pg_nextpower2_32(2048), 2048);
        assert_eq!(pg_nextpower2_32(2049), 4096);
        assert_eq!(pg_nextpower2_32(0x4000_0001), 0x8000_0000);
    }

    #[test]
    fn index_delete_sort_cmp_orders_by_block_then_offset() {
        // Lower block sorts first.
        assert_eq!(
            index_delete_sort_cmp(
                &TmIndexDelete { tid: tid(1, 9), id: 0 },
                &TmIndexDelete { tid: tid(2, 1), id: 1 },
            ),
            -1
        );
        // Same block: lower offset sorts first.
        assert_eq!(
            index_delete_sort_cmp(
                &TmIndexDelete { tid: tid(5, 2), id: 0 },
                &TmIndexDelete { tid: tid(5, 7), id: 1 },
            ),
            -1
        );
        assert_eq!(
            index_delete_sort_cmp(
                &TmIndexDelete { tid: tid(5, 7), id: 0 },
                &TmIndexDelete { tid: tid(5, 2), id: 1 },
            ),
            1
        );
    }

    #[test]
    fn index_delete_sort_sorts_by_tid() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        // Deliberately scrambled, plus a large run to exercise the gaps.
        let mut entries: Vec<(BlockNumber, OffsetNumber, bool, i16)> = Vec::new();
        for k in 0..200u32 {
            // pseudo-random-ish block/offset
            let blk = (k.wrapping_mul(7) % 17) as BlockNumber;
            let off = ((k.wrapping_mul(13) % 23) + 1) as OffsetNumber;
            entries.push((blk, off, false, 0));
        }
        let mut op = make_op(mcx, &entries, false);

        index_delete_sort(&mut op);

        // Verify fully sorted by (block, offset).
        for w in op.deltids.windows(2) {
            assert!(index_delete_sort_cmp(&w[0], &w[1]) <= 0);
        }
        // No entries lost.
        assert_eq!(op.deltids.len(), entries.len());
    }

    #[test]
    fn bottomup_nblocksfavorable_tolerance() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        // Three groups whose first-TID blocks are 10, 11, 99: the third is out
        // of tolerance, so only the first two are favorable.
        let entries = [
            (10u32, 1u16, true, 1i16),
            (11, 1, true, 1),
            (99, 1, true, 1),
        ];
        let op = make_op(mcx, &entries, true);
        let groups = [
            IndexDeleteCounts { npromisingtids: 4, ntids: 1, ifirsttid: 0 },
            IndexDeleteCounts { npromisingtids: 4, ntids: 1, ifirsttid: 1 },
            IndexDeleteCounts { npromisingtids: 4, ntids: 1, ifirsttid: 2 },
        ];
        assert_eq!(bottomup_nblocksfavorable(&groups, 3, &op.deltids), 2);
    }

    #[test]
    fn bottomup_sort_and_shrink_groups_and_shrinks() {
        let ctx = MemoryContext::new("test");
        let mcx = ctx.mcx();
        // Eight heap blocks. The array is already in TID order (precondition of
        // bottomup_sort_and_shrink). Block 7 gets 8 promising TIDs, so its
        // npromisingtids bucket (8) beats every other block's (which round up to
        // the floor of 4). Blocks 0..=6 get one non-promising TID each. Only the
        // BOTTOMUP_MAX_NBLOCKS (6) most promising blocks survive.
        let mut entries: Vec<(BlockNumber, OffsetNumber, bool, i16)> = Vec::new();
        for blk in 0..7u32 {
            entries.push((blk, 1, false, 1));
        }
        // Block 7: eight promising TIDs at distinct offsets.
        for off in 1..=8u16 {
            entries.push((7, off, true, 1));
        }
        let mut op = make_op(mcx, &entries, true);
        let total = op.deltids.len();

        let favorable = bottomup_sort_and_shrink(&mut op);

        // Block 7 (the most promising bucket) is first.
        assert_eq!(ItemPointerGetBlockNumber(&op.deltids[0].tid), 7);
        // Only the 6 most promising blocks' TIDs survive: block 7 (8 TIDs) plus
        // 5 single-TID blocks = 13. The 2 least-favorable blocks are dropped.
        assert_eq!(op.deltids.len(), 8 + 5);
        assert!(op.deltids.len() < total);
        // There is always at least one favorable block.
        assert!(favorable >= 1);
    }
}
