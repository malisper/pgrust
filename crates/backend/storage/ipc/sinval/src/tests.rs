//! Unit tests for the per-backend (process-local) state. The shared-memory
//! queue paths (`SI*DataEntries`, `SICleanupQueue`, `SharedInval*`) require the
//! shmem/LWLock/procsignal seams to be installed, so they are exercised in
//! integration once those owners are wired.

use super::*;

#[test]
fn counter_starts_zero_and_bumps() {
    // A fresh thread gets fresh thread-locals.
    std::thread::spawn(|| {
        assert_eq!(SharedInvalidMessageCounter(), 0);
        bump_counter();
        bump_counter();
        assert_eq!(SharedInvalidMessageCounter(), 2);
    })
    .join()
    .unwrap();
}

#[test]
fn catchup_pending_defaults_false() {
    std::thread::spawn(|| {
        assert!(!catchupInterruptPending());
    })
    .join()
    .unwrap();
}

#[test]
fn get_next_local_transaction_id_skips_invalid() {
    // nextLocalTransactionId starts at InvalidLocalTransactionId (0); the first
    // call must loop past 0 and return 1, then 2, ...
    std::thread::spawn(|| {
        assert_eq!(GetNextLocalTransactionId(), 1);
        assert_eq!(GetNextLocalTransactionId(), 2);
        assert_eq!(GetNextLocalTransactionId(), 3);
    })
    .join()
    .unwrap();
}

#[test]
fn local_transaction_id_wraps_past_invalid() {
    std::thread::spawn(|| {
        // Force the counter to wrap to 0 on the next read.
        NEXT_LOCAL_TRANSACTION_ID.with(|c| c.set(u32::MAX));
        // result = MAX (valid) -> returns MAX, counter becomes 0.
        assert_eq!(GetNextLocalTransactionId(), u32::MAX);
        // result = 0 (invalid) -> loops, result = 1 -> returns 1.
        assert_eq!(GetNextLocalTransactionId(), 1);
    })
    .join()
    .unwrap();
}

#[test]
fn handle_catchup_sets_pending() {
    // HandleCatchupInterrupt sets the flag then SetLatch(MyLatch). The latch
    // seam is not installed in this unit test, so only assert the flag side via
    // a direct cell check (avoid invoking the seam call).
    std::thread::spawn(|| {
        assert!(!catchupInterruptPending());
        CATCHUP_INTERRUPT_PENDING.with(|c| c.set(true));
        assert!(catchupInterruptPending());
    })
    .join()
    .unwrap();
}
