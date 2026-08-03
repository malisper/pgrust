//! H0 SCRIBBLER detector controls (task #112).
//!
//! The detector (`pg_tsdiff_cache_check`, wired into `OracleSerial::drop` at
//! depth 0) validates the timestamp oracle's `datecache`/`deltacache` statics
//! against `datetktbl`/`deltatktbl` on every final oracle exit. These tests
//! prove both directions: the clean path does not fire on legal cache
//! population, and the must-fail control fires on a planted poison pointer
//! carrying the SCRIBBLER's one-byte signature. A detector nobody has seen
//! fire is not a detector.

use crate::c_oracle_serial;

extern "C" {
    // pg_timestamp_io.c oracle entry — populates deltacache legally via
    // DecodeInterval -> DecodeUnits.
    fn pg_tsdiff_interval_in(
        s: *const std::ffi::c_char,
        typmod: i32,
        istyle: i32,
        t: *mut i64,
        day: *mut i32,
        month: *mut i32,
    ) -> i32;
    // Test-only poison planter, csrc/pg_timestamp_io.c.
    fn pg_tsdiff_cache_poison_for_test();
}

/// Clean path: a real oracle exec that populates the caches with legal
/// in-table pointers must not trip the detector at guard drop.
#[test]
fn h0_detector_clean_path_does_not_fire() {
    let guard = c_oracle_serial();
    let cs = std::ffi::CString::new("1 day 2 hours").unwrap();
    let (mut t, mut d, mut m) = (0i64, 0i32, 0i32);
    unsafe { pg_tsdiff_interval_in(cs.as_ptr(), -1, 0, &mut t, &mut d, &mut m) };
    // Drop runs the detector; a panic here fails the test.
    drop(guard);
}

/// Must-fail control: plant the SCRIBBLER signature (valid pointer, byte
/// index 2 zeroed) in deltacache and assert the guard's Drop panics naming
/// the poisoned slot. The C checker clears the caches on detection, so the
/// poison cannot leak to sibling tests after this control fires.
#[test]
fn h0_detector_fires_on_poisoned_cache() {
    let guard = c_oracle_serial();
    unsafe { pg_tsdiff_cache_poison_for_test() };
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(guard)));
    let err = r.expect_err("H0 detector failed to fire on a poisoned deltacache slot");
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("SCRIBBLER H0") && msg.contains("deltacache slot 3"),
        "unexpected detector panic message: {msg:?}"
    );
    // And the detector self-heals: the next oracle exit is clean again.
    let guard = c_oracle_serial();
    drop(guard);
}
