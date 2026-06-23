//! Unit tests for the bitmap-heap-scan node's owned, seam-free logic.

use super::*;
use nodes::SpinLockGuard;
use types_storage::Spinlock;

#[test]
fn bm_state_constants_match_c() {
    // SharedBitmapState enumerators (execnodes.h) — repr(i32) discriminants.
    assert_eq!(BM_INITIAL as i32, 0);
    assert_eq!(BM_INPROGRESS as i32, 1);
    assert_eq!(BM_FINISHED as i32, 2);
}

#[test]
fn maxalign_rounds_up_to_8() {
    assert_eq!(maxalign(0), 0);
    assert_eq!(maxalign(1), 8);
    assert_eq!(maxalign(8), 8);
    assert_eq!(maxalign(9), 16);
}

#[test]
fn shared_instrumentation_header_offset_is_8() {
    // offsetof(SharedBitmapHeapInstrumentation, sinstrument): int num_workers
    // padded to the uint64 array alignment.
    assert_eq!(SharedBitmapHeapInstrumentation::offset_of_sinstrument(), 8);
}

#[test]
fn spinlock_guard_acquires_and_releases() {
    let lock = Spinlock::new();
    assert!(lock.is_free());
    {
        let _g = SpinLockGuard::acquire(&lock);
        assert!(!lock.is_free());
    }
    // Drop releases.
    assert!(lock.is_free());
}

#[test]
fn instr_counters_default_zero() {
    let s = BitmapHeapScanInstrumentation::default();
    assert_eq!(s.exact_pages, 0);
    assert_eq!(s.lossy_pages, 0);
}
