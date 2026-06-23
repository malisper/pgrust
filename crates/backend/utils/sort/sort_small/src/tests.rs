//! Tests for `qsort_interruptible`. The C `CHECK_FOR_INTERRUPTS()` macro is the
//! workspace's centralized interrupt seam; here we install one shared
//! implementation (the seam slot is a process-global `OnceLock`) that counts
//! calls and, when armed, returns the cancel error — letting each test observe
//! that interrupts are checked and that an interrupt aborts the sort.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, Once};
use types_error::PgError;

static CHECK_COUNT: AtomicU64 = AtomicU64::new(0);
static CANCEL_ARMED: AtomicBool = AtomicBool::new(false);
static INSTALL: Once = Once::new();

/// Serializes the tests: the `check_for_interrupts` seam slot and the
/// `CANCEL_ARMED` / `CHECK_COUNT` globals are process-wide, so tests (which run
/// in parallel threads) must not race on them.
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn guard() -> MutexGuard<'static, ()> {
    TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

/// Install the single shared `check_for_interrupts` implementation (idempotent).
fn install_seam() {
    INSTALL.call_once(|| {
        postgres_seams::check_for_interrupts::set(|| {
            CHECK_COUNT.fetch_add(1, Ordering::Relaxed);
            if CANCEL_ARMED.load(Ordering::Relaxed) {
                Err(PgError::error("canceling statement due to user request"))
            } else {
                Ok(())
            }
        });
    });
}

/// Reset the per-test interrupt state (no cancel, zeroed counter).
fn reset() {
    install_seam();
    CANCEL_ARMED.store(false, Ordering::Relaxed);
    CHECK_COUNT.store(0, Ordering::Relaxed);
}

fn cmp_i32(a: &i32, b: &i32) -> i32 {
    if a < b {
        -1
    } else if a > b {
        1
    } else {
        0
    }
}

#[test]
fn qsort_interruptible_small_insertion_sort() {
    let _g = guard();
    reset();
    let mut values = [5, 2, 4, 1, 3];
    qsort_interruptible(&mut values, cmp_i32).unwrap();
    assert_eq!(values, [1, 2, 3, 4, 5]);
}

#[test]
fn qsort_interruptible_presorted_returns_early() {
    let _g = guard();
    reset();
    let mut values: Vec<i32> = (0..50).collect();
    qsort_interruptible(&mut values, cmp_i32).unwrap();
    assert!(values.windows(2).all(|w| w[0] <= w[1]));
    // The presorted scan calls CHECK_FOR_INTERRUPTS() at least once.
    assert!(CHECK_COUNT.load(Ordering::Relaxed) > 0);
}

#[test]
fn qsort_interruptible_large_with_duplicates() {
    let _g = guard();
    reset();
    // Exercise the full Bentley–McIlroy quicksort (n >= 7, n > 40 median-of-
    // medians, the fat-pivot equal-key runs, and the recurse/iterate split).
    let mut state: u64 = 0x1234_5678_9abc_def0;
    let mut next = || {
        // xorshift64
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut values: Vec<i32> = (0..1000).map(|_| (next() % 50) as i32).collect();
    let mut expected = values.clone();
    expected.sort();

    qsort_interruptible(&mut values, cmp_i32).unwrap();
    assert_eq!(values, expected);
}

#[test]
fn qsort_interruptible_all_equal() {
    let _g = guard();
    reset();
    let mut values = vec![7i32; 200];
    qsort_interruptible(&mut values, cmp_i32).unwrap();
    assert_eq!(values, vec![7i32; 200]);
}

#[test]
fn qsort_interruptible_reverse_sorted() {
    let _g = guard();
    reset();
    let mut values: Vec<i32> = (0..300).rev().collect();
    let mut expected = values.clone();
    expected.sort();
    qsort_interruptible(&mut values, cmp_i32).unwrap();
    assert_eq!(values, expected);
}

#[test]
fn qsort_interruptible_propagates_cancel() {
    let _g = guard();
    reset();
    CANCEL_ARMED.store(true, Ordering::Relaxed);
    let mut values: Vec<i32> = (0..100).rev().collect();
    let err = qsort_interruptible(&mut values, cmp_i32).unwrap_err();
    assert!(err.message().contains("canceling statement"));
    CANCEL_ARMED.store(false, Ordering::Relaxed);
}

#[test]
fn qsort_interruptible_empty_and_singleton() {
    let _g = guard();
    reset();
    let mut empty: [i32; 0] = [];
    qsort_interruptible(&mut empty, cmp_i32).unwrap();

    let mut one = [42];
    qsort_interruptible(&mut one, cmp_i32).unwrap();
    assert_eq!(one, [42]);
}
