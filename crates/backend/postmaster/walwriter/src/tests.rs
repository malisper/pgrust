//! Tests for the walwriter state machine.
//!
//! These exercise the pure flush-cadence / hibernation logic 1:1 against the C
//! (`walwriter.c`) and the GUC accessor surface. The actual latch wait and the
//! seam-driven boundary calls are not driven here (they require the cross-crate
//! seam stack to be installed); the cycle's pure decisions are tested directly.

use super::*;

#[test]
fn flush_after_default_is_128() {
    // (1024 * 1024) / 8192 == 128.
    assert_eq!(DEFAULT_WAL_WRITER_FLUSH_AFTER, 128);
    assert_eq!(WalWriterFlushAfter(), 128);
}

#[test]
fn guc_delay_default_is_200() {
    assert_eq!(WalWriterDelay(), 200);
}

#[test]
fn guc_accessors_roundtrip() {
    let saved_delay = WalWriterDelay();
    let saved_after = WalWriterFlushAfter();

    set_WalWriterDelay(77);
    assert_eq!(WalWriterDelay(), 77);
    set_WalWriterFlushAfter(999);
    assert_eq!(WalWriterFlushAfter(), 999);

    set_WalWriterDelay(saved_delay);
    set_WalWriterFlushAfter(saved_after);
    assert_eq!(WalWriterDelay(), saved_delay);
    assert_eq!(WalWriterFlushAfter(), saved_after);
}

#[test]
fn hibernate_constants() {
    assert_eq!(LOOPS_UNTIL_HIBERNATE, 50);
    assert_eq!(HIBERNATE_FACTOR, 25);
}

#[test]
fn recompute_hibernation_flips_true_at_one() {
    // left_till_hibernate <= 1 -> want hibernate; was false, so it changes.
    let mut state = LoopState {
        left_till_hibernate: 1,
        hibernating: false,
    };
    assert_eq!(state.recompute_hibernation(), Some(true));
    assert!(state.hibernating);
}

#[test]
fn recompute_hibernation_no_change_when_unchanged() {
    // left_till_hibernate large (>1) and hibernating already false -> no change.
    let mut state = LoopState {
        left_till_hibernate: 50,
        hibernating: false,
    };
    assert_eq!(state.recompute_hibernation(), None);
    assert!(!state.hibernating);

    // Already hibernating with low counter -> still no change.
    let mut state = LoopState {
        left_till_hibernate: 0,
        hibernating: true,
    };
    assert_eq!(state.recompute_hibernation(), None);
    assert!(state.hibernating);
}

#[test]
fn recompute_hibernation_flips_false_when_busy_again() {
    // Was hibernating, but counter climbed back above 1 -> flip to false.
    let mut state = LoopState {
        left_till_hibernate: LOOPS_UNTIL_HIBERNATE,
        hibernating: true,
    };
    assert_eq!(state.recompute_hibernation(), Some(false));
    assert!(!state.hibernating);
}

#[test]
fn apply_flush_result_resets_counter_on_work() {
    let mut state = LoopState {
        left_till_hibernate: 3,
        hibernating: false,
    };
    state.apply_flush_result(true);
    assert_eq!(state.left_till_hibernate, LOOPS_UNTIL_HIBERNATE);
}

#[test]
fn apply_flush_result_decrements_when_idle() {
    let mut state = LoopState {
        left_till_hibernate: 3,
        hibernating: false,
    };
    state.apply_flush_result(false);
    assert_eq!(state.left_till_hibernate, 2);
}

#[test]
fn apply_flush_result_does_not_go_negative() {
    // left_till_hibernate == 0 stays 0 when no work (the `> 0` guard).
    let mut state = LoopState {
        left_till_hibernate: 0,
        hibernating: true,
    };
    state.apply_flush_result(false);
    assert_eq!(state.left_till_hibernate, 0);
}

#[test]
fn cur_timeout_uses_plain_delay_when_active() {
    let saved = WalWriterDelay();
    set_WalWriterDelay(200);
    let state = LoopState {
        left_till_hibernate: 5,
        hibernating: false,
    };
    assert_eq!(state.cur_timeout(), 200);
    set_WalWriterDelay(saved);
}

#[test]
fn cur_timeout_lengthens_when_hibernating() {
    let saved = WalWriterDelay();
    set_WalWriterDelay(200);
    let state = LoopState {
        left_till_hibernate: 0,
        hibernating: true,
    };
    // 200 * 25 == 5000.
    assert_eq!(state.cur_timeout(), 5000);
    set_WalWriterDelay(saved);
}
