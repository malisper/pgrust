//! Relation truncation (`vacuumlazy.c`).
//!
//!   * [`should_attempt_truncation`] (vacuumlazy.c:3180).
//!   * [`lazy_truncate_heap`] (vacuumlazy.c:3200).
//!   * [`count_nondeletable_pages`] (vacuumlazy.c:3331).

use std::time::Instant;

use ::utils_error::{ereport};
use ::types_error::{ErrorLocation, DEBUG2, INFO};
use ::types_core::BlockNumber;
use ::types_error::PgResult;

use crate::consts::{
    offset_number_next, AccessExclusiveLock, FirstOffsetNumber, InvalidBlockNumber,
    InvalidOffsetNumber, BUFFER_LOCK_SHARE, MAIN_FORKNUM, PROGRESS_VACUUM_PHASE,
    PROGRESS_VACUUM_PHASE_TRUNCATE, WAIT_EVENT_VACUUM_TRUNCATE, WL_EXIT_ON_PM_DEATH, WL_LATCH_SET,
    WL_TIMEOUT,
};
use crate::core::{
    LVRelState, VacErrPhase, PREFETCH_SIZE, REL_TRUNCATE_FRACTION, REL_TRUNCATE_MINIMUM,
    VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL, VACUUM_TRUNCATE_LOCK_TIMEOUT,
    VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL,
};
use crate::errcb::update_vacuum_error_info;

use vacuumlazy_seams as vl;

fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("vacuumlazy.c", 0, funcname)
}

/// `should_attempt_truncation()` (vacuumlazy.c:3180) — decide whether to try to
/// truncate empty pages off the end of the relation.
pub fn should_attempt_truncation<'mcx>(vacrel: &mut LVRelState<'mcx>) -> PgResult<bool> {
    if !vacrel.do_rel_truncate || vl::vacuum_failsafe_active::call()? {
        return Ok(false);
    }

    let possibly_freeable = vacrel.rel_pages.wrapping_sub(vacrel.nonempty_pages);
    if possibly_freeable > 0
        && (possibly_freeable >= REL_TRUNCATE_MINIMUM
            || possibly_freeable >= vacrel.rel_pages / REL_TRUNCATE_FRACTION)
    {
        return Ok(true);
    }

    Ok(false)
}

/// `lazy_truncate_heap()` (vacuumlazy.c:3200) — try to truncate the relation down
/// to the last nonempty page, taking an `AccessExclusiveLock` with the timeout
/// heuristics and looping while pages remain to remove.
pub fn lazy_truncate_heap<'mcx>(vacrel: &mut LVRelState<'mcx>) -> PgResult<()> {
    let mut orig_rel_pages: BlockNumber = vacrel.rel_pages;
    let mut new_rel_pages: BlockNumber;
    let mut lock_waiter_detected: bool;
    let mut lock_retry: i32;

    /* Report that we are now truncating. */
    vl::pgstat_progress_update_param::call(PROGRESS_VACUUM_PHASE, PROGRESS_VACUUM_PHASE_TRUNCATE)?;

    /* Update error traceback information one last time. */
    let nonempty_pages = vacrel.nonempty_pages;
    update_vacuum_error_info(
        vacrel,
        None,
        VacErrPhase::Truncate,
        nonempty_pages,
        InvalidOffsetNumber,
    );

    /* Loop until no more truncating can be done. */
    loop {
        /*
         * We need full exclusive lock on the relation. If we can't get it, give
         * up rather than waiting --- we don't want to block other backends or
         * deadlock.
         */
        lock_waiter_detected = false;
        lock_retry = 0;
        loop {
            if vl::conditional_lock_relation::call(&vacrel.rel, AccessExclusiveLock)? {
                break;
            }

            /* Check for interrupts while trying to (re-)acquire the lock. */
            postgres_seams::check_for_interrupts::call()?;

            lock_retry += 1;
            if lock_retry > (VACUUM_TRUNCATE_LOCK_TIMEOUT / VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL) {
                /* Give up truncating. */
                ereport(if vacrel.verbose { INFO } else { DEBUG2 })
                    .errmsg(format!(
                        "\"{}\": stopping truncate due to conflicting lock request",
                        vacrel.relname
                    ))
                    .finish(here("lazy_truncate_heap"))
                    .ok();
                return Ok(());
            }

            vl::wait_latch::call(
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                VACUUM_TRUNCATE_LOCK_WAIT_INTERVAL as i64,
                WAIT_EVENT_VACUUM_TRUNCATE,
            )?;
            vl::reset_latch::call()?;
        }

        /*
         * Now that we have exclusive lock, look to see if the rel has grown
         * whilst we were vacuuming with non-exclusive lock.
         */
        new_rel_pages = vl::relation_get_number_of_blocks::call(&vacrel.rel)?;
        if new_rel_pages != orig_rel_pages {
            vl::unlock_relation::call(&vacrel.rel, AccessExclusiveLock)?;
            return Ok(());
        }

        /*
         * Scan backwards from the end to verify that the end pages actually
         * contain no tuples. This is *necessary*, not optional.
         */
        new_rel_pages = count_nondeletable_pages(vacrel, &mut lock_waiter_detected)?;
        vacrel.blkno = new_rel_pages;

        if new_rel_pages >= orig_rel_pages {
            /* can't do anything after all */
            vl::unlock_relation::call(&vacrel.rel, AccessExclusiveLock)?;
            return Ok(());
        }

        /* Okay to truncate. */
        vl::relation_truncate::call(&vacrel.rel, new_rel_pages)?;

        /* Release the exclusive lock as soon as we have truncated. */
        vl::unlock_relation::call(&vacrel.rel, AccessExclusiveLock)?;

        /* Update statistics. */
        vacrel.removed_pages = vacrel
            .removed_pages
            .wrapping_add(orig_rel_pages.wrapping_sub(new_rel_pages));
        vacrel.rel_pages = new_rel_pages;

        ereport(if vacrel.verbose { INFO } else { DEBUG2 })
            .errmsg(format!(
                "table \"{}\": truncated {} to {} pages",
                vacrel.relname, orig_rel_pages, new_rel_pages
            ))
            .finish(here("lazy_truncate_heap"))
            .ok();
        orig_rel_pages = new_rel_pages;

        if !(new_rel_pages > vacrel.nonempty_pages && lock_waiter_detected) {
            break;
        }
    }
    Ok(())
}

/// `count_nondeletable_pages()` (vacuumlazy.c:3331) — scan backwards from the end
/// of the relation to find the last page that cannot be truncated away. Sets
/// `*lock_waiter_detected` if a conflicting lock request appeared. Returns the
/// new (smaller) relation length in blocks.
pub fn count_nondeletable_pages<'mcx>(
    vacrel: &mut LVRelState<'mcx>,
    lock_waiter_detected: &mut bool,
) -> PgResult<BlockNumber> {
    let mut blkno: BlockNumber;
    let mut prefetched_until: BlockNumber;
    let mut starttime: Instant = Instant::now();

    blkno = vacrel.rel_pages;
    const _: () = assert!(
        (PREFETCH_SIZE & (PREFETCH_SIZE - 1)) == 0,
        "prefetch size must be power of 2"
    );
    prefetched_until = InvalidBlockNumber;
    while blkno > vacrel.nonempty_pages {
        let mut hastup: bool;

        /*
         * Check, once per VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL (and only every 32
         * blocks), if another process wants a lock on our relation.
         */
        if (blkno % 32) == 0 {
            let currenttime = Instant::now();
            let elapsed = currenttime.duration_since(starttime);
            if (elapsed.as_micros() / 1000) >= VACUUM_TRUNCATE_LOCK_CHECK_INTERVAL as u128 {
                if vl::lock_has_waiters_relation::call(&vacrel.rel, AccessExclusiveLock)? {
                    ereport(if vacrel.verbose { INFO } else { DEBUG2 })
                        .errmsg(format!(
                            "table \"{}\": suspending truncate due to conflicting lock request",
                            vacrel.relname
                        ))
                        .finish(here("count_nondeletable_pages"))
                        .ok();

                    *lock_waiter_detected = true;
                    return Ok(blkno);
                }
                starttime = currenttime;
            }
        }

        /* Still need to check for interrupts (we hold the exclusive lock). */
        postgres_seams::check_for_interrupts::call()?;

        blkno -= 1;

        /* If we haven't prefetched this lot yet, do so now. */
        if prefetched_until > blkno {
            let prefetch_start = blkno & !(PREFETCH_SIZE - 1);
            let mut pblkno = prefetch_start;
            while pblkno <= blkno {
                vl::prefetch_buffer::call(&vacrel.rel, MAIN_FORKNUM, pblkno)?;
                postgres_seams::check_for_interrupts::call()?;
                pblkno += 1;
            }
            prefetched_until = prefetch_start;
        }

        let buf =
            vl::read_buffer_extended::call(&vacrel.rel, MAIN_FORKNUM, blkno, vacrel.bstrategy.clone())?;

        /* In this phase we only need shared access to the buffer. */
        bufmgr_seams::lock_buffer::call(buf, BUFFER_LOCK_SHARE)?;

        if vl::page_is_new::call(buf)? || vl::page_is_empty::call(buf)? {
            bufmgr_seams::unlock_release_buffer::call(buf);
            continue;
        }

        hastup = false;
        let maxoff = vl::page_get_max_offset_number::call(buf)?;
        let mut offnum = FirstOffsetNumber;
        while offnum <= maxoff {
            let lp = vl::page_item_id_state::call(buf, offnum)?;

            /*
             * Any non-unused item (even an LP_DEAD item) makes truncation
             * unsafe.
             */
            if lp.is_used {
                hastup = true;
                break; /* can stop scanning */
            }

            offnum = offset_number_next(offnum);
        }

        bufmgr_seams::unlock_release_buffer::call(buf);

        /* Done scanning if we found a tuple here. */
        if hastup {
            return Ok(blkno.wrapping_add(1));
        }
    }

    /*
     * If we fall out of the loop, all the previously-thought-to-be-empty pages
     * still are.
     */
    Ok(vacrel.nonempty_pages)
}
