//! BgBufferSync (bufmgr.c): the bgwriter's LRU-scan arm over the clock sweep.
//! The WritebackContext lives here (C: BgWriterMain's frame) so the bgwriter
//! crate needs no access to the private writeback plumbing.

use core::cell::{Cell, RefCell};

use types_error::PgResult;
use crate::write::{SyncOneBuffer, WritebackContext, BUF_REUSABLE, BUF_WRITTEN};

// PendingBgWriterStats (C home pgstat_bgwriter.c, written from here as
// extern); relocates when the pgstat activity unit ports.
pub mod pending_bgwriter_stats {
    use core::cell::Cell;
    thread_local! {
        pub static BUF_WRITTEN_CLEAN: Cell<u64> = const { Cell::new(0) };
        pub static MAXWRITTEN_CLEAN: Cell<u64> = const { Cell::new(0) };
        pub static BUF_ALLOC: Cell<u64> = const { Cell::new(0) };
    }
}

thread_local! {
    static SAVED_INFO_VALID: Cell<bool> = const { Cell::new(false) };
    static PREV_STRATEGY_BUF_ID: Cell<i32> = const { Cell::new(0) };
    static PREV_STRATEGY_PASSES: Cell<u32> = const { Cell::new(0) };
    static NEXT_TO_CLEAN: Cell<i32> = const { Cell::new(0) };
    static NEXT_PASSES: Cell<u32> = const { Cell::new(0) };
    static SMOOTHED_ALLOC: Cell<f32> = const { Cell::new(0.0) };
    static SMOOTHED_DENSITY: Cell<f32> = const { Cell::new(10.0) };

    static BGW_WB: RefCell<Option<WritebackContext>> = const { RefCell::new(None) };
}

/// WritebackContextInit(&wb_context, &bgwriter_flush_after) — also the
/// bgwriter's error-recovery re-init.
pub fn bgwriter_writeback_context_init() {
    BGW_WB.with(|c| *c.borrow_mut() = Some(WritebackContext::new(crate::gucs::bgwriter_flush_after)));
}

pub fn BgBufferSync() -> PgResult<bool> {
    let (strategy_buf_id, strategy_passes, recent_alloc) = crate::freelist::StrategySyncStart();

    pending_bgwriter_stats::BUF_ALLOC.with(|c| c.set(c.get() + recent_alloc as u64));

    let bgwriter_lru_maxpages = crate::gucs::bgwriter_lru_maxpages();
    if bgwriter_lru_maxpages <= 0 {
        SAVED_INFO_VALID.set(false);
        return Ok(true);
    }

    let nbuffers = crate::buf_hdr::NBuffersInited();
    let smoothing_samples = 16.0f32;
    let scan_whole_pool_milliseconds = 120000.0f32;

    let strategy_delta: i64;
    let bufs_to_lap: i32;
    if SAVED_INFO_VALID.get() {
        let passes_delta = strategy_passes.wrapping_sub(PREV_STRATEGY_PASSES.get()) as i32;
        strategy_delta =
            (strategy_buf_id - PREV_STRATEGY_BUF_ID.get()) as i64 + passes_delta as i64 * nbuffers as i64;
        debug_assert!(strategy_delta >= 0);

        if (NEXT_PASSES.get().wrapping_sub(strategy_passes) as i32) > 0 {
            bufs_to_lap = strategy_buf_id - NEXT_TO_CLEAN.get();
        } else if NEXT_PASSES.get() == strategy_passes && NEXT_TO_CLEAN.get() >= strategy_buf_id {
            bufs_to_lap = nbuffers - (NEXT_TO_CLEAN.get() - strategy_buf_id);
        } else {
            NEXT_TO_CLEAN.set(strategy_buf_id);
            NEXT_PASSES.set(strategy_passes);
            bufs_to_lap = nbuffers;
        }
    } else {
        strategy_delta = 0;
        NEXT_TO_CLEAN.set(strategy_buf_id);
        NEXT_PASSES.set(strategy_passes);
        bufs_to_lap = nbuffers;
    }

    PREV_STRATEGY_BUF_ID.set(strategy_buf_id);
    PREV_STRATEGY_PASSES.set(strategy_passes);
    SAVED_INFO_VALID.set(true);

    if strategy_delta > 0 && recent_alloc > 0 {
        let scans_per_alloc = strategy_delta as f32 / recent_alloc as f32;
        SMOOTHED_DENSITY.set(
            SMOOTHED_DENSITY.get() + (scans_per_alloc - SMOOTHED_DENSITY.get()) / smoothing_samples,
        );
    }

    let bufs_ahead = nbuffers - bufs_to_lap;
    let reusable_buffers_est = (bufs_ahead as f32 / SMOOTHED_DENSITY.get()) as i32;

    if SMOOTHED_ALLOC.get() <= recent_alloc as f32 {
        SMOOTHED_ALLOC.set(recent_alloc as f32);
    } else {
        SMOOTHED_ALLOC.set(
            SMOOTHED_ALLOC.get() + (recent_alloc as f32 - SMOOTHED_ALLOC.get()) / smoothing_samples,
        );
    }

    let mut upcoming_alloc_est =
        (SMOOTHED_ALLOC.get() as f64 * crate::gucs::bgwriter_lru_multiplier()) as i32;

    if upcoming_alloc_est == 0 {
        SMOOTHED_ALLOC.set(0.0);
    }

    let bg_writer_delay = guc_tables::vars::BgWriterDelay.read();
    let min_scan_buffers =
        (nbuffers as f32 / (scan_whole_pool_milliseconds / bg_writer_delay as f32)) as i32;

    if upcoming_alloc_est < min_scan_buffers + reusable_buffers_est {
        upcoming_alloc_est = min_scan_buffers + reusable_buffers_est;
    }

    let mut num_to_scan = bufs_to_lap;
    let mut num_written: i32 = 0;
    let mut reusable_buffers = reusable_buffers_est;

    BGW_WB.with(|cell| -> PgResult<()> {
        let mut slot = cell.borrow_mut();
        let wb = slot
            .get_or_insert_with(|| WritebackContext::new(crate::gucs::bgwriter_flush_after));

        while num_to_scan > 0 && reusable_buffers < upcoming_alloc_est {
            let sync_state = SyncOneBuffer(NEXT_TO_CLEAN.get(), true, wb)?;

            NEXT_TO_CLEAN.set(NEXT_TO_CLEAN.get() + 1);
            if NEXT_TO_CLEAN.get() >= nbuffers {
                NEXT_TO_CLEAN.set(0);
                NEXT_PASSES.set(NEXT_PASSES.get().wrapping_add(1));
            }
            num_to_scan -= 1;

            if sync_state & BUF_WRITTEN != 0 {
                reusable_buffers += 1;
                num_written += 1;
                if num_written >= bgwriter_lru_maxpages {
                    pending_bgwriter_stats::MAXWRITTEN_CLEAN.with(|c| c.set(c.get() + 1));
                    break;
                }
            } else if sync_state & BUF_REUSABLE != 0 {
                reusable_buffers += 1;
            }
        }
        Ok(())
    })?;

    pending_bgwriter_stats::BUF_WRITTEN_CLEAN.with(|c| c.set(c.get() + num_written as u64));

    let new_strategy_delta = (bufs_to_lap - num_to_scan) as i64;
    let new_recent_alloc = reusable_buffers - reusable_buffers_est;
    if new_strategy_delta > 0 && new_recent_alloc > 0 {
        let scans_per_alloc = new_strategy_delta as f32 / new_recent_alloc as f32;
        SMOOTHED_DENSITY.set(
            SMOOTHED_DENSITY.get() + (scans_per_alloc - SMOOTHED_DENSITY.get()) / smoothing_samples,
        );
    }

    Ok(bufs_to_lap == 0 && recent_alloc == 0)
}
