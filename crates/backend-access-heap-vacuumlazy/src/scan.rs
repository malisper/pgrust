//! Phase I — relation scanning, pruning and freezing (`vacuumlazy.c`).
//!
//!   * [`lazy_scan_heap`] (vacuumlazy.c:1200) — the main phase-I loop.
//!
//! In the owned model the C read-stream (which ran the
//! `heap_vac_scan_next_block` callback inside the stream over a `void *vacrel`)
//! is driven explicitly: each iteration the in-crate
//! [`crate::scan_block::heap_vac_scan_next_block`] state machine selects the next
//! block (1:1 with C, threading `&mut LVRelState`), then the chosen block's
//! buffer is read through the buffer-manager seam. This keeps all of the
//! skip/eager-scan decision logic in-crate and seams only the actual buffer
//! read/lock; the per-page processing is unchanged.

use backend_utils_error::{ereport};
use types_error::{ErrorLocation, DEBUG2, INFO};
use types_core::{BlockNumber, Buffer};
use types_error::PgResult;

use crate::consts::{
    buffer_is_valid, InvalidBlockNumber, InvalidBuffer, InvalidOffsetNumber, BUFFER_LOCK_SHARE,
    BUFFER_LOCK_UNLOCK, MAIN_FORKNUM, PROGRESS_VACUUM_HEAP_BLKS_SCANNED,
    PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES, PROGRESS_VACUUM_PHASE,
    PROGRESS_VACUUM_PHASE_SCAN_HEAP, PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
};
use crate::core::{
    LVRelState, VacErrPhase, FAILSAFE_EVERY_PAGES, VACUUM_FSM_EVERY_PAGES,
    VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM, VAC_BLK_WAS_EAGER_SCANNED,
};
use crate::errcb::update_vacuum_error_info;
use crate::scan_block::{heap_vac_scan_next_block, NextBlock};
use crate::scan_page::{lazy_scan_new_or_empty, lazy_scan_noprune, lazy_scan_prune};

use backend_access_heap_vacuumlazy_seams as vl;

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("vacuumlazy.c", 0, funcname)
}

/// `lazy_scan_heap()` (vacuumlazy.c:1200) — scan the heap, pruning and freezing
/// tuples, recording dead TIDs, and (between TID-store fills) vacuuming indexes
/// and the heap. The workhorse of phase I.
pub fn lazy_scan_heap(vacrel: &mut LVRelState) -> PgResult<()> {
    let rel_pages: BlockNumber = vacrel.rel_pages;
    let mut blkno: BlockNumber = 0;
    let mut next_fsm_block_to_vacuum: BlockNumber = 0;
    let orig_eager_scan_success_limit: BlockNumber = vacrel.eager_scan_remaining_successes;
    let mut vmbuffer: Buffer = InvalidBuffer;

    /* Report that we're scanning the heap, advertising total # of blocks. */
    vl::pgstat_progress_update_multi_param::call(
        vec![
            PROGRESS_VACUUM_PHASE,
            PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
            PROGRESS_VACUUM_MAX_DEAD_TUPLE_BYTES,
        ],
        vec![
            PROGRESS_VACUUM_PHASE_SCAN_HEAP,
            rel_pages as i64,
            vacrel.dead_items_info.max_bytes as i64,
        ],
    )?;

    /* Initialize for the first heap_vac_scan_next_block() call. */
    vacrel.current_block = InvalidBlockNumber;
    vacrel.next_unskippable_block = InvalidBlockNumber;
    vacrel.next_unskippable_allvis = false;
    vacrel.next_unskippable_eager_scanned = false;
    vacrel.next_unskippable_vmbuffer = InvalidBuffer;

    loop {
        vl::vacuum_delay_point::call(false)?;

        /* Regularly check if the wraparound failsafe should trigger. */
        if vacrel.scanned_pages > 0 && vacrel.scanned_pages % FAILSAFE_EVERY_PAGES == 0 {
            crate::vacuum_phase::lazy_check_wraparound_failsafe(vacrel)?;
        }

        /*
         * If we are close to overrunning the available space for dead_items
         * TIDs, pause and do a cycle of vacuuming before this page. Force at
         * least one page-worth of tuples to be stored.
         */
        if vacrel.dead_items_info.num_items > 0
            && vl::tidstore_memory_usage::call(vacrel.dead_items)? > vacrel.dead_items_info.max_bytes
        {
            /* Release any pin on the visibility map page. */
            if buffer_is_valid(vmbuffer) {
                vl::release_buffer::call(vmbuffer)?;
                vmbuffer = InvalidBuffer;
            }

            /* Perform a round of index and heap vacuuming. */
            vacrel.consider_bypass_optimization = false;
            crate::vacuum_phase::lazy_vacuum(vacrel)?;

            /* Vacuum the FSM to make newly-freed space visible. */
            vl::free_space_map_vacuum_range::call(vacrel.rel, next_fsm_block_to_vacuum, blkno + 1)?;
            next_fsm_block_to_vacuum = blkno;

            /* Report that we are once again scanning the heap. */
            vl::pgstat_progress_update_param::call(
                PROGRESS_VACUUM_PHASE,
                PROGRESS_VACUUM_PHASE_SCAN_HEAP,
            )?;
        }

        /* Select the next block (the in-crate read-stream state machine). */
        let blk_info: u8;
        match heap_vac_scan_next_block(vacrel)? {
            NextBlock::Exhausted => break,
            NextBlock::Block {
                blkno: b,
                blk_info: bi,
            } => {
                blkno = b;
                blk_info = bi;
            }
        }

        /* Read (and pin) the chosen block's buffer. */
        let buf =
            vl::read_buffer_extended::call(vacrel.rel, MAIN_FORKNUM, blkno, vacrel.bstrategy)?;

        vl::check_buffer_is_pinned_once::call(buf)?;

        vacrel.scanned_pages += 1;
        if blk_info & VAC_BLK_WAS_EAGER_SCANNED != 0 {
            vacrel.eager_scanned_pages += 1;
        }

        /* Report as block scanned, update error traceback information. */
        vl::pgstat_progress_update_param::call(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, blkno as i64)?;
        update_vacuum_error_info(
            vacrel,
            None,
            VacErrPhase::ScanHeap,
            blkno,
            InvalidOffsetNumber,
        );

        /* Pin the visibility map page in case we need to mark the page all-visible. */
        vmbuffer = vl::visibilitymap_pin::call(vacrel.rel, blkno, vmbuffer)?;

        /*
         * We need a buffer cleanup lock to prune/defragment. If we can't get one
         * right away, settle for reduced processing via lazy_scan_noprune.
         */
        let mut got_cleanup_lock = vl::conditional_lock_buffer_for_cleanup::call(buf)?;

        if !got_cleanup_lock {
            vl::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;
        }

        /* Check for new or empty pages before lazy_scan_[no]prune call. */
        if lazy_scan_new_or_empty(vacrel, buf, blkno, !got_cleanup_lock, vmbuffer)? {
            /* Processed as new/empty page (lock and pin released). */
            continue;
        }

        /*
         * If we didn't get the cleanup lock, collect what we can via
         * lazy_scan_noprune; if it can't do all the required processing, wait
         * for a cleanup lock and call lazy_scan_prune in the usual way.
         */
        let mut has_lpdead_items = false;
        if !got_cleanup_lock {
            let (processed, hl) = lazy_scan_noprune(vacrel, buf, blkno)?;
            has_lpdead_items = hl;
            if !processed {
                debug_assert!(vacrel.aggressive);
                vl::lock_buffer::call(buf, BUFFER_LOCK_UNLOCK)?;
                vl::lock_buffer_for_cleanup::call(buf)?;
                got_cleanup_lock = true;
            }
        }

        /*
         * If we have a cleanup lock, prune, freeze, and count tuples.
         */
        let mut ndeleted: i32 = 0;
        let mut vm_page_frozen = false;
        if got_cleanup_lock {
            let (nd, hl, vmf) = lazy_scan_prune(
                vacrel,
                buf,
                blkno,
                vmbuffer,
                blk_info & VAC_BLK_ALL_VISIBLE_ACCORDING_TO_VM != 0,
            )?;
            ndeleted = nd;
            has_lpdead_items = hl;
            vm_page_frozen = vmf;
        }

        /* Count an eagerly scanned page as a failure or a success. */
        if got_cleanup_lock && (blk_info & VAC_BLK_WAS_EAGER_SCANNED != 0) {
            debug_assert!(!vacrel.aggressive);

            if vm_page_frozen {
                if vacrel.eager_scan_remaining_successes > 0 {
                    vacrel.eager_scan_remaining_successes -= 1;
                }

                if vacrel.eager_scan_remaining_successes == 0 {
                    if vacrel.eager_scan_max_fails_per_region > 0 {
                        ereport(if vacrel.verbose { INFO } else { DEBUG2 })
                            .errmsg(format!(
                                "disabling eager scanning after freezing {} eagerly scanned blocks of relation \"{}.{}.{}\"",
                                orig_eager_scan_success_limit,
                                vacrel.dbname, vacrel.relnamespace, vacrel.relname,
                            ))
                            .finish(here("lazy_scan_heap"))
                            .ok();
                    }

                    /* Permanently disable eager scanning. */
                    vacrel.eager_scan_remaining_fails = 0;
                    vacrel.next_eager_scan_region_start = InvalidBlockNumber;
                    vacrel.eager_scan_max_fails_per_region = 0;
                }
            } else if vacrel.eager_scan_remaining_fails > 0 {
                vacrel.eager_scan_remaining_fails -= 1;
            }
        }

        /*
         * Now drop the buffer lock and, potentially, update the FSM.
         */
        if vacrel.nindexes == 0 || !vacrel.do_index_vacuuming || !has_lpdead_items {
            let freespace = vl::page_get_heap_free_space::call(buf)?;

            vl::unlock_release_buffer::call(buf)?;
            vl::record_page_with_free_space::call(vacrel.rel, blkno, freespace)?;

            /* Periodically perform FSM vacuuming for tables without indexes. */
            if got_cleanup_lock
                && vacrel.nindexes == 0
                && ndeleted > 0
                && blkno - next_fsm_block_to_vacuum >= VACUUM_FSM_EVERY_PAGES
            {
                vl::free_space_map_vacuum_range::call(
                    vacrel.rel,
                    next_fsm_block_to_vacuum,
                    blkno,
                )?;
                next_fsm_block_to_vacuum = blkno;
            }
        } else {
            vl::unlock_release_buffer::call(buf)?;
        }
    }

    vacrel.blkno = InvalidBlockNumber;
    if buffer_is_valid(vmbuffer) {
        vl::release_buffer::call(vmbuffer)?;
    }

    /* Report that everything is now scanned. */
    vl::pgstat_progress_update_param::call(PROGRESS_VACUUM_HEAP_BLKS_SCANNED, rel_pages as i64)?;

    /* Compute the new value for pg_class.reltuples. */
    vacrel.new_live_tuples = vl::vac_estimate_reltuples::call(
        vacrel.rel,
        rel_pages,
        vacrel.scanned_pages,
        vacrel.live_tuples as f64,
    )?;

    /* Total number of surviving heap entries (clamp new_live_tuples >= 0). */
    vacrel.new_rel_tuples = vacrel.new_live_tuples.max(0.0)
        + vacrel.recently_dead_tuples as f64
        + vacrel.missed_dead_tuples as f64;

    /* Do index vacuuming + related heap vacuuming if any dead items remain. */
    if vacrel.dead_items_info.num_items > 0 {
        crate::vacuum_phase::lazy_vacuum(vacrel)?;
    }

    /* Vacuum the remainder of the Free Space Map. */
    if rel_pages > next_fsm_block_to_vacuum {
        vl::free_space_map_vacuum_range::call(vacrel.rel, next_fsm_block_to_vacuum, rel_pages)?;
    }

    /* Report all blocks vacuumed. */
    vl::pgstat_progress_update_param::call(PROGRESS_VACUUM_HEAP_BLKS_VACUUMED, rel_pages as i64)?;

    /* Do final index cleanup. */
    if vacrel.nindexes > 0 && vacrel.do_index_cleanup {
        crate::index::lazy_cleanup_all_indexes(vacrel)?;
    }

    Ok(())
}
