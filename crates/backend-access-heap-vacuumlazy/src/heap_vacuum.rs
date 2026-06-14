//! Phase III — the second heap pass (`vacuumlazy.c`).
//!
//!   * [`lazy_vacuum_heap_rel`] (vacuumlazy.c:2720) — iterate the TID store, lock
//!     each block, reap its LP_DEAD items, record freed space.
//!   * [`lazy_vacuum_heap_page`] (vacuumlazy.c:2838) — reap one page's LP_DEAD
//!     items, WAL-log, re-check / update the VM.
//!   * [`heap_page_is_all_visible`] (vacuumlazy.c:3607) — the stripped-down
//!     visibility recheck used on the second pass and from `lazy_scan_prune`.
//!
//! The on-page substrate (bufpage accessors, item-id flag reads / set-unused,
//! WAL logging, VM, `HeapTupleSatisfiesVacuum`, `heap_tuple_needs_eventual_freeze`)
//! is reached through the seam crate; the reap loop, the TID-store iteration
//! callback ([`crate::scan_block::vacuum_reap_lp_read_stream_next`]) and the
//! all-visible recheck control flow are ported 1:1 in-crate.

use backend_utils_error::ereport;
use types_error::{ErrorLocation, DEBUG2, ERROR};
use types_core::{BlockNumber, Buffer, OffsetNumber, TransactionId};
use types_error::PgResult;

use crate::consts::{
    buffer_is_valid, offset_number_next, transaction_id_follows, transaction_id_is_normal,
    transaction_id_is_valid, transaction_id_precedes, FirstOffsetNumber, InvalidBlockNumber,
    InvalidBuffer, InvalidOffsetNumber, InvalidTransactionId, InvalidXLogRecPtr,
    BUFFER_LOCK_EXCLUSIVE, HEAPTUPLE_DEAD, HEAPTUPLE_DELETE_IN_PROGRESS,
    HEAPTUPLE_INSERT_IN_PROGRESS, HEAPTUPLE_LIVE, HEAPTUPLE_RECENTLY_DEAD, MAIN_FORKNUM,
    PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_VACUUM_HEAP,
    PRUNE_VACUUM_CLEANUP, READ_STREAM_MAINTENANCE, READ_STREAM_USE_BATCHING,
    VISIBILITYMAP_ALL_FROZEN, VISIBILITYMAP_ALL_VISIBLE,
};
use crate::core::{LVRelState, LVSavedErrInfo, VacErrPhase};
use crate::errcb::{restore_vacuum_error_info, update_vacuum_error_info};
use crate::scan_block::{vacuum_reap_lp_read_stream_next, ReapNextBlock};

use backend_access_heap_vacuumlazy_seams as vl;

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("vacuumlazy.c", 0, funcname)
}

/// `lazy_vacuum_heap_rel()` (vacuumlazy.c:2720) — second pass over the heap.
/// Marks LP_DEAD items in `vacrel.dead_items` as LP_UNUSED. Pages that never had
/// `lazy_scan_prune` record LP_DEAD items are not visited.
pub fn lazy_vacuum_heap_rel(vacrel: &mut LVRelState) -> PgResult<()> {
    let mut vacuumed_pages: BlockNumber = 0;
    let mut vmbuffer: Buffer = InvalidBuffer;
    let mut saved_err_info = LVSavedErrInfo {
        blkno: 0,
        offnum: 0,
        phase: VacErrPhase::Unknown,
    };

    debug_assert!(vacrel.do_index_vacuuming);
    debug_assert!(vacrel.do_index_cleanup);
    debug_assert!(vacrel.num_index_scans > 0);

    /* Report that we are now vacuuming the heap. */
    vl::pgstat_progress_update_param::call(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_VACUUM_HEAP)?;

    update_vacuum_error_info(
        vacrel,
        Some(&mut saved_err_info),
        VacErrPhase::VacuumHeap,
        InvalidBlockNumber,
        InvalidOffsetNumber,
    );

    let iter = vl::tidstore_begin_iterate::call(vacrel.dead_items)?;

    /*
     * Set up the read stream for vacuum's second pass through the heap.
     *
     * It is safe to use batchmode, as vacuum_reap_lp_read_stream_next() does
     * not need to wait for IO and does not perform locking. Once we support
     * parallelism it should still be fine, as presumably the holder of locks
     * would never be blocked by IO while holding the lock.
     *
     * In the owned model the C read-stream callback runs in-crate: the
     * `vacuum_reap_lp_read_stream_next` state machine pulls the next block
     * (and its `TidStoreIterResult`) from the TID store, then the chosen
     * block's buffer is read through the buffer-manager seam — symmetric with
     * the phase-I scan in `lazy_scan_heap`.
     */
    let stream = vl::read_stream_begin_relation::call(
        READ_STREAM_MAINTENANCE | READ_STREAM_USE_BATCHING,
        vacrel.bstrategy,
        vacrel.rel,
        MAIN_FORKNUM,
        types_vacuum::vacuumlazy::ScanCallback::ReapNextBlock,
        iter,
    )?;

    loop {
        vl::vacuum_delay_point::call(false)?;

        /* Pull the next block to reap from the TID store (in-crate callback). */
        let reap = match vacuum_reap_lp_read_stream_next(iter)? {
            /* The relation is exhausted. */
            ReapNextBlock::Exhausted => break,
            ReapNextBlock::Block { reap } => reap,
        };

        /* Read (and pin) the chosen block's buffer. */
        let buf =
            vl::read_buffer_extended::call(vacrel.rel, MAIN_FORKNUM, reap.blkno, vacrel.bstrategy)?;

        let blkno = vl::buffer_get_block_number::call(buf)?;
        vacrel.blkno = blkno;

        debug_assert!(blkno == reap.blkno);

        /* Pin the visibility map page in case we need to mark the page all-visible. */
        vmbuffer = vl::visibilitymap_pin::call(vacrel.rel, blkno, vmbuffer)?;

        /* We need a non-cleanup exclusive lock to mark dead_items unused. */
        vl::lock_buffer::call(buf, BUFFER_LOCK_EXCLUSIVE)?;
        lazy_vacuum_heap_page(vacrel, blkno, buf, &reap.offsets, vmbuffer)?;

        /* Record the page's available space. */
        let freespace = vl::page_get_heap_free_space::call(buf)?;
        vl::unlock_release_buffer::call(buf)?;
        vl::record_page_with_free_space::call(vacrel.rel, blkno, freespace)?;
        vacuumed_pages += 1;
    }

    vl::read_stream_end::call(stream)?;
    vl::tidstore_end_iterate::call(iter)?;

    vacrel.blkno = InvalidBlockNumber;
    if buffer_is_valid(vmbuffer) {
        vl::release_buffer::call(vmbuffer)?;
    }

    debug_assert!(
        vacrel.num_index_scans > 1
            || (vacrel.dead_items_info.num_items == vacrel.lpdead_items
                && vacuumed_pages == vacrel.lpdead_item_pages)
    );

    ereport(DEBUG2)
        .errmsg(format!(
            "table \"{}\": removed {} dead item identifiers in {} pages",
            vacrel.relname, vacrel.dead_items_info.num_items, vacuumed_pages
        ))
        .finish(here("lazy_vacuum_heap_rel"))
        .ok();

    restore_vacuum_error_info(vacrel, &saved_err_info);
    Ok(())
}

/// `lazy_vacuum_heap_page()` (vacuumlazy.c:2838) — free a page's LP_DEAD items
/// listed in `deadoffsets`. Caller must hold an exclusive buffer lock; `vmbuffer`
/// must already pin `blkno`'s visibility map page.
pub fn lazy_vacuum_heap_page(
    vacrel: &mut LVRelState,
    blkno: BlockNumber,
    buffer: Buffer,
    deadoffsets: &[OffsetNumber],
    vmbuffer: Buffer,
) -> PgResult<()> {
    let mut unused: Vec<OffsetNumber> = Vec::new();
    let mut saved_err_info = LVSavedErrInfo {
        blkno: 0,
        offnum: 0,
        phase: VacErrPhase::Unknown,
    };

    debug_assert!(vacrel.do_index_vacuuming);

    vl::pgstat_progress_update_param::call(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, blkno as i64)?;

    update_vacuum_error_info(
        vacrel,
        Some(&mut saved_err_info),
        VacErrPhase::VacuumHeap,
        blkno,
        InvalidOffsetNumber,
    );

    for &toff in deadoffsets {
        debug_assert!({
            let lp = vl::page_item_id_state::call(buffer, toff)?;
            lp.is_dead && !lp.has_storage
        });
        vl::page_item_id_set_unused::call(buffer, toff)?;
        unused.push(toff);
    }

    debug_assert!(!unused.is_empty());

    /* Attempt to truncate line pointer array now. */
    vl::page_truncate_line_pointer_array::call(buffer)?;

    /* Mark buffer dirty before we write WAL. */
    vl::mark_buffer_dirty::call(buffer)?;

    /* XLOG stuff. */
    if vl::relation_needs_wal::call(vacrel.rel)? {
        vl::log_heap_prune_and_freeze::call(
            vacrel.rel,
            buffer,
            InvalidTransactionId,
            false, /* no cleanup lock required */
            PRUNE_VACUUM_CLEANUP,
            Vec::new(), /* no freeze plans */
            Vec::new(), /* no redirections */
            Vec::new(), /* no newly-dead items */
            unused.clone(),
        )?;
    }

    /*
     * Now that we removed the LP_DEAD items, check again if the page has become
     * all-visible.
     */
    debug_assert!(!vl::page_is_all_visible::call(buffer)?);
    let (all_visible, visibility_cutoff_xid, all_frozen) = heap_page_is_all_visible(vacrel, buffer)?;
    if all_visible {
        let mut flags = VISIBILITYMAP_ALL_VISIBLE;

        if all_frozen {
            debug_assert!(!transaction_id_is_valid(visibility_cutoff_xid));
            flags |= VISIBILITYMAP_ALL_FROZEN;
        }

        vl::page_set_all_visible::call(buffer)?;
        vl::visibilitymap_set::call(types_vacuum::vacuumlazy::VmSetArgs {
            rel: vacrel.rel,
            heap_blk: blkno,
            heap_buf: buffer,
            rec_ptr: InvalidXLogRecPtr,
            vm_buf: vmbuffer,
            cutoff_xid: visibility_cutoff_xid,
            flags,
        })?;

        /* Count the newly set VM page for logging. */
        vacrel.vm_new_visible_pages += 1;
        if all_frozen {
            vacrel.vm_new_visible_frozen_pages += 1;
        }
    }

    restore_vacuum_error_info(vacrel, &saved_err_info);
    Ok(())
}

/// `heap_page_is_all_visible()` (vacuumlazy.c:3607) — check if every tuple in the
/// page is visible to all current and future transactions. Returns
/// `(all_visible, visibility_cutoff_xid, all_frozen)`.
///
/// This is a stripped-down version of `lazy_scan_prune()`; keep the two in sync.
pub fn heap_page_is_all_visible(
    vacrel: &mut LVRelState,
    buf: Buffer,
) -> PgResult<(bool, TransactionId, bool)> {
    let blockno = vl::buffer_get_block_number::call(buf)?;
    let _ = blockno;
    let mut all_visible = true;
    let mut visibility_cutoff_xid: TransactionId = InvalidTransactionId;
    let mut all_frozen = true;

    let maxoff = vl::page_get_max_offset_number::call(buf)?;
    let mut offnum = FirstOffsetNumber;
    while offnum <= maxoff && all_visible {
        vacrel.offnum = offnum;
        let lp = vl::page_item_id_state::call(buf, offnum)?;

        /* Unused or redirect line pointers are of no interest. */
        if !lp.is_used || lp.is_redirected {
            offnum = offset_number_next(offnum);
            continue;
        }

        /* Dead line pointers can't be treated as visible. */
        if lp.is_dead {
            all_visible = false;
            all_frozen = false;
            break;
        }

        debug_assert!(lp.is_normal);

        let htsv = vl::heap_tuple_satisfies_vacuum::call(
            vacrel.rel,
            buf,
            offnum,
            vacrel.cutoffs.OldestXmin,
        )?;
        match htsv {
            x if x == HEAPTUPLE_LIVE => {
                /* Check comments in lazy_scan_prune. */
                if !heap_tuple_header_xmin_committed(buf, offnum)? {
                    all_visible = false;
                    all_frozen = false;
                } else {
                    /*
                     * The inserter committed.  But is it old enough that
                     * everyone sees it as committed?
                     */
                    let xmin = heap_tuple_header_get_xmin(buf, offnum)?;
                    if !transaction_id_precedes(xmin, vacrel.cutoffs.OldestXmin) {
                        all_visible = false;
                        all_frozen = false;
                    } else {
                        /* Track newest xmin on page. */
                        if transaction_id_follows(xmin, visibility_cutoff_xid)
                            && transaction_id_is_normal(xmin)
                        {
                            visibility_cutoff_xid = xmin;
                        }

                        /* Check whether this tuple is already frozen or not. */
                        if all_visible
                            && all_frozen
                            && vl::heap_tuple_needs_eventual_freeze::call(buf, offnum)?
                        {
                            all_frozen = false;
                        }
                    }
                }
            }
            x if x == HEAPTUPLE_DEAD
                || x == HEAPTUPLE_RECENTLY_DEAD
                || x == HEAPTUPLE_INSERT_IN_PROGRESS
                || x == HEAPTUPLE_DELETE_IN_PROGRESS =>
            {
                all_visible = false;
                all_frozen = false;
            }
            _ => {
                return Err(ereport(ERROR)
                    .errmsg_internal("unexpected HeapTupleSatisfiesVacuum result")
                    .into_error());
            }
        }

        offnum = offset_number_next(offnum);
    }

    /* Clear the offset information once we have processed the page. */
    vacrel.offnum = InvalidOffsetNumber;

    Ok((all_visible, visibility_cutoff_xid, all_frozen))
}

/// `HeapTupleHeaderXminCommitted(tuple->t_data)` over the page-resident tuple at
/// `(buffer, offnum)`. Reached through the on-page header-read seam (the
/// page-resident header is owned by the buffer substrate; there is no idiomatic
/// bufpage tuple-header reader exposed to this crate).
#[inline]
fn heap_tuple_header_xmin_committed(buffer: Buffer, offnum: OffsetNumber) -> PgResult<bool> {
    vl::header_xmin_committed::call(buffer, offnum)
}

/// `HeapTupleHeaderGetXmin(tuple->t_data)` (frozen xmin → `FrozenTransactionId`)
/// over the page-resident tuple at `(buffer, offnum)`.
#[inline]
fn heap_tuple_header_get_xmin(buffer: Buffer, offnum: OffsetNumber) -> PgResult<TransactionId> {
    vl::header_get_xmin::call(buffer, offnum)
}
