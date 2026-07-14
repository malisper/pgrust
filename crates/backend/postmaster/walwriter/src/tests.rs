use transam_xlog::{wal_flush_pacing_decide, WalFlushPacing};

use super::*;

#[test]
fn main_fn_matches_child_main_shape() {
    let f: fn(&types_startup::StartupData) -> ! = super::WalWriterMain;
    let _ = f;
}

// ---------------------------------------------------------------------------
// Deterministic hibernation-FSM oracle (the walwriter analog of
// bufmgr::bgw_plan_scan): exact trajectories of the C loop's
// flag/counter/timeout lines (walwriter.c:234-266).
// ---------------------------------------------------------------------------

/// Idle trajectory: from a fresh state, the flag flips exactly when the
/// counter reaches 1, and the ×25 stretch engages on the SAME iteration
/// the counter hits 0 — C's "the long sleep starts in the iteration the
/// flag turned on".
#[test]
fn hibernation_idle_trajectory_exact() {
    let mut left = LOOPS_UNTIL_HIBERNATE;
    let mut hibernating = false;
    let delay = 200;

    let mut flag_flip_iteration = None;
    let mut stretch_iteration = None;
    for i in 1..=60 {
        // Cycle top: flag step from PREVIOUS counter.
        let (flag, changed) = hibernate_flag_step(left, hibernating);
        if changed {
            hibernating = flag;
            assert!(flag, "idle run only ever enters hibernation");
            flag_flip_iteration = flag_flip_iteration.or(Some(i));
        }
        // Body found no work.
        left = hibernate_count_step(left, false);
        if cycle_timeout_ms(left, delay) != delay as i64 && stretch_iteration.is_none() {
            stretch_iteration = Some(i);
            assert_eq!(cycle_timeout_ms(left, delay), delay as i64 * HIBERNATE_FACTOR);
        }
    }
    // Counter 50 decrements once per idle cycle: it reaches 1 after 49
    // cycles, so the flag (left <= 1 test at cycle TOP) first flips on
    // iteration 50; the counter hits 0 that same iteration and the ×25
    // stretch engages.
    assert_eq!(flag_flip_iteration, Some(50));
    assert_eq!(stretch_iteration, Some(50));
    assert!(hibernating);
    assert_eq!(left, 0);
    // Steady hibernation: no further flag changes, counter floors at 0.
    let (_, changed) = hibernate_flag_step(left, hibernating);
    assert!(!changed);
    assert_eq!(hibernate_count_step(left, false), 0);
}

/// Work resets the counter and wakes from hibernation with exactly one
/// flag publication (hysteresis: SetWalWriterSleeping only on change).
#[test]
fn hibernation_exit_on_work_exact() {
    let mut left = 0;
    let mut hibernating = true;

    // A wake delivers work: the flag is still updated from the PRE-body
    // counter (0 → stays hibernating this cycle top — C order), then the
    // body's work resets the counter, and the NEXT cycle top exits
    // hibernation.
    let (flag, changed) = hibernate_flag_step(left, hibernating);
    assert!(flag && !changed, "cycle top before the body sees the old counter");
    left = hibernate_count_step(left, true);
    assert_eq!(left, LOOPS_UNTIL_HIBERNATE);
    assert_eq!(cycle_timeout_ms(left, 200), 200, "stretch retracts with the reset counter");

    let (flag, changed) = hibernate_flag_step(left, hibernating);
    assert!(!flag && changed, "next cycle top publishes the exit exactly once");
    hibernating = flag;
    let (_, changed) = hibernate_flag_step(left, hibernating);
    assert!(!changed, "no republication while awake");
}

/// The counter only rearms to full on work — idle cycles never give
/// progress back.
#[test]
fn hibernation_counter_monotone_idle() {
    let mut left = LOOPS_UNTIL_HIBERNATE;
    for expected in (0..LOOPS_UNTIL_HIBERNATE).rev() {
        left = hibernate_count_step(left, false);
        assert_eq!(left, expected);
    }
    assert_eq!(hibernate_count_step(0, false), 0);
    assert_eq!(hibernate_count_step(17, true), LOOPS_UNTIL_HIBERNATE);
}

// ---------------------------------------------------------------------------
// Deterministic flush-pacing oracle (xlog.c XLogBackgroundFlush's
// lastflush block — the state this migration extracted from a
// worker-hopping thread-local into WalWriterState).
// ---------------------------------------------------------------------------

const DELAY_US: i64 = 200_000; // wal_writer_delay=200ms

/// First call always flushes (lastflush==0), then time-based pacing:
/// within the delay window small backlogs defer; delay expiry flushes and
/// re-arms the clock.
#[test]
fn pacing_first_call_and_delay_window_exact() {
    let mut p = WalFlushPacing::new();
    assert!(wal_flush_pacing_decide(&mut p, 1_000_000, 1, 128, DELAY_US), "first call flushes");
    assert!(
        !wal_flush_pacing_decide(&mut p, 1_000_000 + DELAY_US - 1, 127, 128, DELAY_US),
        "inside the window, below the block threshold: defer"
    );
    assert!(
        wal_flush_pacing_decide(&mut p, 1_000_000 + DELAY_US, 1, 128, DELAY_US),
        "window expired: flush"
    );
    // The clock re-armed at the second flush.
    assert!(!wal_flush_pacing_decide(&mut p, 1_000_000 + DELAY_US + 1, 1, 128, DELAY_US));
}

/// Block-threshold flushes inside the window (wal_writer_flush_after
/// accumulation), and re-arms the clock.
#[test]
fn pacing_block_threshold_exact() {
    let mut p = WalFlushPacing::new();
    assert!(wal_flush_pacing_decide(&mut p, 1_000_000, 0, 128, DELAY_US));
    assert!(
        wal_flush_pacing_decide(&mut p, 1_000_100, 128, 128, DELAY_US),
        "flushblocks >= flush_after flushes inside the window"
    );
    assert!(
        !wal_flush_pacing_decide(&mut p, 1_000_200, 127, 128, DELAY_US),
        "clock re-armed by the block-threshold flush"
    );
}

/// flush_after == 0 disables pacing entirely: every call flushes.
#[test]
fn pacing_disabled_flushes_every_call() {
    let mut p = WalFlushPacing::new();
    for now in [10, 11, 12] {
        assert!(wal_flush_pacing_decide(&mut p, now, 0, 0, DELAY_US));
    }
}

/// Deferral does NOT advance the clock: a burst of deferred calls still
/// flushes exactly at the original window expiry.
#[test]
fn pacing_deferral_keeps_the_original_deadline() {
    let mut p = WalFlushPacing::new();
    assert!(wal_flush_pacing_decide(&mut p, 1_000_000, 0, 128, DELAY_US));
    for dt in [10_000, 50_000, 150_000] {
        assert!(!wal_flush_pacing_decide(&mut p, 1_000_000 + dt, 1, 128, DELAY_US));
    }
    assert!(wal_flush_pacing_decide(&mut p, 1_000_000 + DELAY_US, 1, 128, DELAY_US));
}
