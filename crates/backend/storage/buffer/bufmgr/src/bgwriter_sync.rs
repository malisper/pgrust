//! BgBufferSync (bufmgr.c): the bgwriter's LRU-scan arm over the clock sweep.
//! The WritebackContext lives here (C: BgWriterMain's frame) so the bgwriter
//! crate needs no access to the private writeback plumbing.
//!
//! M4 bgjobs increment 3 (docs/design/m4-bgjobs.md §3.1): the control state
//! C kept in BgBufferSync statics — the clock-sweep tracking and EWMA
//! smoothing whose CONTINUITY the algorithm depends on — is an explicit
//! [`BgwSyncState`] owned by the caller, not thread-locals. The thread
//! daemon owns one on its main frame (identical behavior: the TLS was
//! per-daemon-thread state, the struct is per-daemon state); the job mode
//! (increment 4) owns one in the job envelope so cycles may execute on any
//! pool worker without resetting the control loop.

use pgstat::bgwriter::with_pending_bgwriter_stats;
use types_error::PgResult;
use crate::write::{SyncOneBuffer, WritebackContext, BUF_REUSABLE, BUF_WRITTEN};

/// The bgwriter's per-daemon control state (C: BgBufferSync's statics +
/// BgWriterMain's WritebackContext frame slot).
pub struct BgwSyncState {
    saved_info_valid: bool,
    prev_strategy_buf_id: i32,
    prev_strategy_passes: u32,
    next_to_clean: i32,
    next_passes: u32,
    smoothed_alloc: f32,
    smoothed_density: f32,
    wb: Option<WritebackContext>,
}

impl Default for BgwSyncState {
    fn default() -> Self {
        Self::new()
    }
}

impl BgwSyncState {
    /// Boot image (C's static initializers).
    pub fn new() -> BgwSyncState {
        BgwSyncState {
            saved_info_valid: false,
            prev_strategy_buf_id: 0,
            prev_strategy_passes: 0,
            next_to_clean: 0,
            next_passes: 0,
            smoothed_alloc: 0.0,
            smoothed_density: 10.0,
            wb: None,
        }
    }

    /// WritebackContextInit(&wb_context, &bgwriter_flush_after) — the
    /// daemon-start init and the error-recovery re-init.
    pub fn reset_writeback_context(&mut self) {
        self.wb = Some(WritebackContext::new(crate::gucs::bgwriter_flush_after));
    }
}

pub fn BgBufferSync(state: &mut BgwSyncState) -> PgResult<bool> {
    let (strategy_buf_id, strategy_passes, recent_alloc) = crate::freelist::StrategySyncStart();

    with_pending_bgwriter_stats(|s| s.buf_alloc += recent_alloc as i64);

    let bgwriter_lru_maxpages = crate::gucs::bgwriter_lru_maxpages();
    if bgwriter_lru_maxpages <= 0 {
        state.saved_info_valid = false;
        return Ok(true);
    }

    let nbuffers = crate::buf_hdr::NBuffersInited();
    let smoothing_samples = 16.0f32;
    let scan_whole_pool_milliseconds = 120000.0f32;

    let strategy_delta: i64;
    let bufs_to_lap: i32;
    if state.saved_info_valid {
        let passes_delta = strategy_passes.wrapping_sub(state.prev_strategy_passes) as i32;
        strategy_delta =
            (strategy_buf_id - state.prev_strategy_buf_id) as i64 + passes_delta as i64 * nbuffers as i64;
        debug_assert!(strategy_delta >= 0);

        if (state.next_passes.wrapping_sub(strategy_passes) as i32) > 0 {
            bufs_to_lap = strategy_buf_id - state.next_to_clean;
        } else if state.next_passes == strategy_passes && state.next_to_clean >= strategy_buf_id {
            bufs_to_lap = nbuffers - (state.next_to_clean - strategy_buf_id);
        } else {
            state.next_to_clean = strategy_buf_id;
            state.next_passes = strategy_passes;
            bufs_to_lap = nbuffers;
        }
    } else {
        strategy_delta = 0;
        state.next_to_clean = strategy_buf_id;
        state.next_passes = strategy_passes;
        bufs_to_lap = nbuffers;
    }

    state.prev_strategy_buf_id = strategy_buf_id;
    state.prev_strategy_passes = strategy_passes;
    state.saved_info_valid = true;

    if strategy_delta > 0 && recent_alloc > 0 {
        let scans_per_alloc = strategy_delta as f32 / recent_alloc as f32;
        state.smoothed_density += (scans_per_alloc - state.smoothed_density) / smoothing_samples;
    }

    let bufs_ahead = nbuffers - bufs_to_lap;
    let reusable_buffers_est = (bufs_ahead as f32 / state.smoothed_density) as i32;

    if state.smoothed_alloc <= recent_alloc as f32 {
        state.smoothed_alloc = recent_alloc as f32;
    } else {
        state.smoothed_alloc += (recent_alloc as f32 - state.smoothed_alloc) / smoothing_samples;
    }

    let mut upcoming_alloc_est =
        (state.smoothed_alloc as f64 * crate::gucs::bgwriter_lru_multiplier()) as i32;

    if upcoming_alloc_est == 0 {
        state.smoothed_alloc = 0.0;
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

    // Take the writeback context out for the scan (split borrow vs the
    // clock-sweep fields), restore it before any return: an error
    // propagates immediately — as C's longjmp — WITHOUT the post-loop
    // pending-stat adds (num_written accumulated so far is lost, C-exact).
    let mut wb = state
        .wb
        .take()
        .unwrap_or_else(|| WritebackContext::new(crate::gucs::bgwriter_flush_after));
    let mut sync_err = None;
    while num_to_scan > 0 && reusable_buffers < upcoming_alloc_est {
        let sync_state = match SyncOneBuffer(state.next_to_clean, true, &mut wb) {
            Ok(s) => s,
            Err(e) => {
                sync_err = Some(e);
                break;
            }
        };

        state.next_to_clean += 1;
        if state.next_to_clean >= nbuffers {
            state.next_to_clean = 0;
            state.next_passes = state.next_passes.wrapping_add(1);
        }
        num_to_scan -= 1;

        if sync_state & BUF_WRITTEN != 0 {
            reusable_buffers += 1;
            num_written += 1;
            if num_written >= bgwriter_lru_maxpages {
                with_pending_bgwriter_stats(|s| s.maxwritten_clean += 1);
                break;
            }
        } else if sync_state & BUF_REUSABLE != 0 {
            reusable_buffers += 1;
        }
    }
    state.wb = Some(wb);
    if let Some(e) = sync_err {
        return Err(e);
    }

    with_pending_bgwriter_stats(|s| s.buf_written_clean += num_written as i64);

    let new_strategy_delta = (bufs_to_lap - num_to_scan) as i64;
    let new_recent_alloc = reusable_buffers - reusable_buffers_est;
    if new_strategy_delta > 0 && new_recent_alloc > 0 {
        let scans_per_alloc = new_strategy_delta as f32 / new_recent_alloc as f32;
        state.smoothed_density += (scans_per_alloc - state.smoothed_density) / smoothing_samples;
    }

    Ok(bufs_to_lap == 0 && recent_alloc == 0)
}
