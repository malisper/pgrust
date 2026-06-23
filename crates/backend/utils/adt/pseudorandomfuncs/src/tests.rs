//! Tests for the `pseudorandomfuncs.c` port.
//!
//! The lazy `initialize_prng` fallback path consults the
//! `get_current_timestamp` and `my_proc_pid` seams, so we install deterministic
//! stubs once before exercising the random functions. Because all the random
//! functions share one process-global `PgPrng`, we serialize every test that
//! touches the shared state behind one mutex, and either seed explicitly (via
//! `setseed`) for determinism or only assert range/domain invariants that hold
//! regardless of the seed.

use std::sync::{Mutex, MutexGuard, Once};

use ::adt_numeric::io::numeric_in;
use ::adt_numeric::ops_sql::numeric_cmp;
use ::mcx::MemoryContext;

use super::*;

static INSTALL_SEAMS: Once = Once::new();

/// Serialize every test that drives the single process-global `PRNG` static
/// (the faithful model of the C file-scope `static pg_prng_state prng_state`).
/// Poison-tolerant: a panic in one test must not cascade `PoisonError`.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// Install the `GetCurrentTimestamp` and `MyProcPid` seams once,
/// single-threaded, so the lazy `initialize_prng` fallback path does not hit
/// the seams' panicking defaults. Tolerate a prior install (e.g. from another
/// crate's tests in the same process).
fn install_seams() {
    INSTALL_SEAMS.call_once(|| {
        if !get_current_timestamp::is_installed() {
            // A fixed, nonzero PostgreSQL-epoch microsecond timestamp.
            get_current_timestamp::set(|| 1_234_567_890_123_456);
        }
        if !my_proc_pid::is_installed() {
            my_proc_pid::set(|| 4242);
        }
    });
}

#[test]
fn setseed_accepts_the_boundaries_and_unit_interior() {
    let _guard = serial();
    assert!(setseed(-1.0).is_ok());
    assert!(setseed(1.0).is_ok());
    assert!(setseed(0.0).is_ok());
    assert!(setseed(0.5).is_ok());
    assert!(setseed(-0.25).is_ok());
}

#[test]
fn setseed_rejects_out_of_range_and_nan() {
    let too_low = setseed(-1.5).unwrap_err();
    assert!(too_low.message().contains("out of allowed range [-1,1]"));

    let too_high = setseed(2.0).unwrap_err();
    assert!(too_high.message().contains("out of allowed range [-1,1]"));

    let nan = setseed(f64::NAN).unwrap_err();
    assert!(nan.message().contains("out of allowed range [-1,1]"));
}

#[test]
fn setseed_makes_drandom_deterministic() {
    let _guard = serial();
    // Seeding from a fixed value must produce a reproducible stream, mirroring
    // the C `setseed`/`random()` contract.
    setseed(0.5).unwrap();
    let a: Vec<f64> = (0..5).map(|_| drandom()).collect();

    setseed(0.5).unwrap();
    let b: Vec<f64> = (0..5).map(|_| drandom()).collect();

    assert_eq!(a, b);
}

#[test]
fn drandom_is_in_unit_interval() {
    let _guard = serial();
    install_seams();
    setseed(0.123).unwrap();
    for _ in 0..1000 {
        let v = drandom();
        assert!((0.0..1.0).contains(&v), "drandom() = {v} not in [0.0, 1.0)");
    }
}

#[test]
fn drandom_normal_applies_mean_and_stddev() {
    // With stddev == 0.0 the transform `(stddev * z) + mean` collapses to
    // exactly `mean` for any draw `z`, so this is deterministic.
    let _guard = serial();
    install_seams();
    setseed(-0.5).unwrap();
    assert_eq!(drandom_normal(42.0, 0.0), 42.0);
    assert_eq!(drandom_normal(-7.5, 0.0), -7.5);
}

#[test]
fn int4random_respects_inclusive_range() {
    let _guard = serial();
    install_seams();
    setseed(0.9).unwrap();
    for _ in 0..1000 {
        let v = int4random(-10, 10).unwrap();
        assert!((-10..=10).contains(&v), "int4random = {v} out of range");
    }
}

#[test]
fn int4random_equal_bounds_returns_that_bound() {
    let _guard = serial();
    install_seams();
    setseed(0.9).unwrap();
    assert_eq!(int4random(7, 7).unwrap(), 7);
}

#[test]
fn int4random_rejects_inverted_bounds() {
    let err = int4random(10, -10).unwrap_err();
    assert!(err
        .message()
        .contains("lower bound must be less than or equal to upper bound"));
}

#[test]
fn int8random_respects_inclusive_range() {
    let _guard = serial();
    install_seams();
    setseed(0.3).unwrap();
    for _ in 0..1000 {
        let v = int8random(-1_000_000, 1_000_000).unwrap();
        assert!(
            (-1_000_000..=1_000_000).contains(&v),
            "int8random = {v} out of range"
        );
    }
}

#[test]
fn int8random_rejects_inverted_bounds() {
    let err = int8random(5, 4).unwrap_err();
    assert!(err
        .message()
        .contains("lower bound must be less than or equal to upper bound"));
}

#[test]
fn numeric_random_is_within_inclusive_range() {
    let _guard = serial();
    install_seams();
    setseed(0.7).unwrap();

    let ctx = MemoryContext::new_bump("numeric_random test");
    let mcx = ctx.mcx();
    let lo = numeric_in(mcx, "0.0", -1).unwrap();
    let hi = numeric_in(mcx, "10.0", -1).unwrap();

    for _ in 0..200 {
        let v = numeric_random(mcx, &lo, &hi).unwrap();
        assert!(
            numeric_cmp(&v, &lo).is_ge(),
            "numeric_random below lower bound"
        );
        assert!(
            numeric_cmp(&v, &hi).is_le(),
            "numeric_random above upper bound"
        );
    }
}
