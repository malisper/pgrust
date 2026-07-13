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
    let plan = bgw_plan_scan(
        state,
        BgwScanInputs {
            strategy_buf_id,
            strategy_passes,
            recent_alloc,
            nbuffers,
            lru_multiplier: crate::gucs::bgwriter_lru_multiplier(),
            delay_ms: guc_tables::vars::BgWriterDelay.read(),
        },
    );

    let mut num_to_scan = plan.bufs_to_lap;
    let mut num_written: i32 = 0;
    let mut reusable_buffers = plan.reusable_buffers_est;
    let upcoming_alloc_est = plan.upcoming_alloc_est;

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

        advance_sweep(state, nbuffers);
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

    let new_strategy_delta = (plan.bufs_to_lap - num_to_scan) as i64;
    let new_recent_alloc = reusable_buffers - plan.reusable_buffers_est;
    if new_strategy_delta > 0 && new_recent_alloc > 0 {
        let scans_per_alloc = new_strategy_delta as f32 / new_recent_alloc as f32;
        state.smoothed_density += (scans_per_alloc - state.smoothed_density) / 16.0f32;
    }

    Ok(plan.bufs_to_lap == 0 && recent_alloc == 0)
}

/// Inputs to one BgBufferSync planning step (everything the pure math
/// reads from shared state / GUCs).
#[derive(Clone, Copy, Debug)]
pub struct BgwScanInputs {
    pub strategy_buf_id: i32,
    pub strategy_passes: u32,
    pub recent_alloc: u32,
    pub nbuffers: i32,
    pub lru_multiplier: f64,
    pub delay_ms: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BgwScanPlan {
    pub bufs_to_lap: i32,
    pub reusable_buffers_est: i32,
    pub upcoming_alloc_est: i32,
}

/// The PURE planning half of BgBufferSync (C bufmgr.c lines up to the scan
/// loop): clock-sweep tracking + the two EWMAs + the scan budget. Extracted
/// so the control loop's exact numeric behavior is unit-testable with fixed
/// input sequences (M4 gate decision: exact-output determinism at the unit
/// altitude; the e2e A/B uses rate windows).
pub fn bgw_plan_scan(state: &mut BgwSyncState, inp: BgwScanInputs) -> BgwScanPlan {
    let smoothing_samples = 16.0f32;
    let scan_whole_pool_milliseconds = 120000.0f32;
    let nbuffers = inp.nbuffers;

    let strategy_delta: i64;
    let bufs_to_lap: i32;
    if state.saved_info_valid {
        let passes_delta = inp.strategy_passes.wrapping_sub(state.prev_strategy_passes) as i32;
        strategy_delta = (inp.strategy_buf_id - state.prev_strategy_buf_id) as i64
            + passes_delta as i64 * nbuffers as i64;
        debug_assert!(strategy_delta >= 0);

        if (state.next_passes.wrapping_sub(inp.strategy_passes) as i32) > 0 {
            bufs_to_lap = inp.strategy_buf_id - state.next_to_clean;
        } else if state.next_passes == inp.strategy_passes
            && state.next_to_clean >= inp.strategy_buf_id
        {
            bufs_to_lap = nbuffers - (state.next_to_clean - inp.strategy_buf_id);
        } else {
            state.next_to_clean = inp.strategy_buf_id;
            state.next_passes = inp.strategy_passes;
            bufs_to_lap = nbuffers;
        }
    } else {
        strategy_delta = 0;
        state.next_to_clean = inp.strategy_buf_id;
        state.next_passes = inp.strategy_passes;
        bufs_to_lap = nbuffers;
    }

    state.prev_strategy_buf_id = inp.strategy_buf_id;
    state.prev_strategy_passes = inp.strategy_passes;
    state.saved_info_valid = true;

    if strategy_delta > 0 && inp.recent_alloc > 0 {
        let scans_per_alloc = strategy_delta as f32 / inp.recent_alloc as f32;
        state.smoothed_density += (scans_per_alloc - state.smoothed_density) / smoothing_samples;
    }

    let bufs_ahead = nbuffers - bufs_to_lap;
    let reusable_buffers_est = (bufs_ahead as f32 / state.smoothed_density) as i32;

    if state.smoothed_alloc <= inp.recent_alloc as f32 {
        state.smoothed_alloc = inp.recent_alloc as f32;
    } else {
        state.smoothed_alloc +=
            (inp.recent_alloc as f32 - state.smoothed_alloc) / smoothing_samples;
    }

    let mut upcoming_alloc_est = (state.smoothed_alloc as f64 * inp.lru_multiplier) as i32;

    if upcoming_alloc_est == 0 {
        state.smoothed_alloc = 0.0;
    }

    let min_scan_buffers =
        (nbuffers as f32 / (scan_whole_pool_milliseconds / inp.delay_ms as f32)) as i32;

    if upcoming_alloc_est < min_scan_buffers + reusable_buffers_est {
        upcoming_alloc_est = min_scan_buffers + reusable_buffers_est;
    }

    BgwScanPlan { bufs_to_lap, reusable_buffers_est, upcoming_alloc_est }
}

/// Scan-loop clock-sweep advance (shared by the loop and the unit tests):
/// the wraparound leg the continuity argument depends on.
#[inline]
fn advance_sweep(state: &mut BgwSyncState, nbuffers: i32) {
    state.next_to_clean += 1;
    if state.next_to_clean >= nbuffers {
        state.next_to_clean = 0;
        state.next_passes = state.next_passes.wrapping_add(1);
    }
}

#[cfg(test)]
mod bgw_plan_tests {
    //! Deterministic exact-output tests of the extracted control loop
    //! (M4 bgjobs gate): fixed input sequences, asserted trajectories.
    //! These pin the numeric behavior the job mode must carry across pool
    //! workers via BgwSyncState (the TLS-extraction increment's oracle).

    use super::*;

    fn inp(buf_id: i32, passes: u32, alloc: u32) -> BgwScanInputs {
        BgwScanInputs {
            strategy_buf_id: buf_id,
            strategy_passes: passes,
            recent_alloc: alloc,
            nbuffers: 1024,
            lru_multiplier: 2.0,
            delay_ms: 200,
        }
    }

    /// First call: no saved info — full-lap plan, sweep pinned to the
    /// strategy point, density untouched at its 10.0 boot value.
    #[test]
    fn first_call_full_lap() {
        let mut st = BgwSyncState::new();
        let plan = bgw_plan_scan(&mut st, inp(100, 0, 50));
        assert_eq!(plan.bufs_to_lap, 1024);
        assert_eq!(plan.reusable_buffers_est, 0);
        // smoothed_alloc jumps to 50; upcoming = 50*2.0 = 100 >= min_scan
        // (1024/600=1) + 0.
        assert_eq!(plan.upcoming_alloc_est, 100);
        assert_eq!(st.next_to_clean, 100);
        assert_eq!(st.prev_strategy_buf_id, 100);
        assert!(st.saved_info_valid);
        assert_eq!(st.smoothed_density, 10.0);
        assert_eq!(st.smoothed_alloc, 50.0);
    }

    /// Steady advance: density EWMA moves 1/16 of the way toward
    /// scans-per-alloc each step; alloc EWMA jumps up, decays by 1/16 down.
    #[test]
    fn ewma_trajectories_exact() {
        let mut st = BgwSyncState::new();
        let _ = bgw_plan_scan(&mut st, inp(0, 0, 64));
        st.next_to_clean = 0; // sweep caught up (as if the scan ran)

        // Strategy advanced 128 buffers for 64 allocs => 2.0 scans/alloc.
        let _ = bgw_plan_scan(&mut st, inp(128, 0, 64));
        // density: 10 + (2 - 10)/16 = 9.5, exact in f32.
        assert_eq!(st.smoothed_density, 9.5);
        // alloc: 64 <= 64 => jump-to (stays 64).
        assert_eq!(st.smoothed_alloc, 64.0);

        st.next_to_clean = 128;
        // Decay leg: alloc drops to 0; 64 + (0-64)/16 = 60, exact in f32.
        let _ = bgw_plan_scan(&mut st, inp(128, 0, 0));
        assert_eq!(st.smoothed_alloc, 60.0);
        // No strategy movement + no allocs => density unchanged.
        assert_eq!(st.smoothed_density, 9.5);
    }

    /// Pass-wraparound continuity: the strategy point lapping the sweep
    /// resets next_to_clean to the strategy point and plans a full lap.
    #[test]
    fn sweep_lapped_resets() {
        let mut st = BgwSyncState::new();
        let _ = bgw_plan_scan(&mut st, inp(1000, 0, 8));
        st.next_to_clean = 1000;
        // Strategy wraps past the sweep point (passes+1, ahead of sweep).
        let plan = bgw_plan_scan(&mut st, inp(200, 1, 8));
        assert_eq!(plan.bufs_to_lap, 1024, "lapped sweep must replan a full lap");
        assert_eq!(st.next_to_clean, 200);
        assert_eq!(st.next_passes, 1);
    }

    /// Sweep ahead of strategy within the same pass: partial lap of
    /// exactly the distance between them (the common steady state).
    #[test]
    fn sweep_ahead_partial_lap() {
        let mut st = BgwSyncState::new();
        let _ = bgw_plan_scan(&mut st, inp(0, 0, 16));
        st.next_to_clean = 300;
        st.next_passes = 0;
        let plan = bgw_plan_scan(&mut st, inp(100, 0, 16));
        // nbuffers - (next_to_clean - strategy) = 1024 - 200 = 824.
        assert_eq!(plan.bufs_to_lap, 824);
        // bufs_ahead = 200 at density 10.0 => 20 reusable est.
        assert_eq!(plan.reusable_buffers_est, 20);
    }

    /// advance_sweep wraparound bumps the pass counter exactly at nbuffers.
    #[test]
    fn sweep_advance_wraps() {
        let mut st = BgwSyncState::new();
        st.next_to_clean = 1022;
        st.next_passes = 7;
        advance_sweep(&mut st, 1024);
        assert_eq!((st.next_to_clean, st.next_passes), (1023, 7));
        advance_sweep(&mut st, 1024);
        assert_eq!((st.next_to_clean, st.next_passes), (0, 8));
    }
}
