//! Tests for the launcher's own arithmetic / shmem-sizing logic (no seamed
//! callees, so these run without installing any owner seam).

use super::*;

#[test]
fn maxalign_rounds_up_to_8() {
    assert_eq!(MAXALIGN(0), 0);
    assert_eq!(MAXALIGN(1), 8);
    assert_eq!(MAXALIGN(8), 8);
    assert_eq!(MAXALIGN(9), 16);
    assert_eq!(MAXALIGN(16), 16);
}

#[test]
fn shmem_size_matches_c_formula() {
    // max_logical_replication_workers defaults to 4.
    set_max_logical_replication_workers(4);
    // MAXALIGN(16) + 4 * 128 = 16 + 512 = 528.
    assert_eq!(ApplyLauncherShmemSize(), 16 + 4 * 128);

    set_max_logical_replication_workers(0);
    assert_eq!(ApplyLauncherShmemSize(), 16);

    // restore default for any later tests in this thread
    set_max_logical_replication_workers(4);
}

#[test]
fn min_picks_smaller() {
    assert_eq!(Min(5, 10), 5);
    assert_eq!(Min(10, 5), 5);
    assert_eq!(Min(7, 7), 7);
}

#[test]
fn on_commit_wakeup_flag_toggles() {
    on_commit_launcher_wakeup_set(false);
    assert!(!on_commit_launcher_wakeup_get());
    ApplyLauncherWakeupAtCommit();
    assert!(on_commit_launcher_wakeup_get());
    on_commit_launcher_wakeup_set(false);
}

#[test]
fn xlogrecptr_invalid() {
    assert!(XLogRecPtrIsInvalid(InvalidXLogRecPtr));
    assert!(!XLogRecPtrIsInvalid(42));
}
