//! Smoke tests for the encoding-independent helper math.
//!
//! The public entry points all reach the `utils/mb` and `tcop` owner seams,
//! which are only installed in a fully wired runtime; the unit tests here cover
//! the pure overflow/alloc-size helpers that are exercised on every path.

use crate::{pg_add_s32_overflow, pg_mul_s32_overflow, AllocSizeIsValid, MAX_ALLOC_SIZE};

#[test]
fn mul_overflow_matches_c() {
    let mut out = 0;
    assert!(!pg_mul_s32_overflow(3, 4, &mut out));
    assert_eq!(out, 12);
    assert!(pg_mul_s32_overflow(i32::MAX, 2, &mut out));
    assert_eq!(out, 0);
}

#[test]
fn add_overflow_matches_c() {
    let mut out = 0;
    assert!(!pg_add_s32_overflow(10, 5, &mut out));
    assert_eq!(out, 15);
    assert!(pg_add_s32_overflow(i32::MAX, 1, &mut out));
    assert_eq!(out, 0);
}

#[test]
fn alloc_size_valid_bounds() {
    assert!(AllocSizeIsValid(0));
    assert!(AllocSizeIsValid(MAX_ALLOC_SIZE));
    assert!(!AllocSizeIsValid(MAX_ALLOC_SIZE + 1));
    assert!(!AllocSizeIsValid(-1));
}
