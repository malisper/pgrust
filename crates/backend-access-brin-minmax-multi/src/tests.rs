//! Unit tests for the pure distance computations (the fmgr-dispatched range
//! arithmetic needs a full BrinDesc + installed fmgr seams; these cover the
//! type-specific distance math against the C formulas).

use crate::distance::*;

#[test]
fn distance_scalar_types() {
    assert_eq!(dist_int2(1, 5), 4.0);
    assert_eq!(dist_int4(-3, 7), 10.0);
    assert_eq!(dist_int8(0, 1_000_000), 1_000_000.0);
    assert_eq!(dist_date(10, 25), 15.0);
    assert_eq!(dist_time(0, 86_400_000_000), 86_400_000_000.0);
    assert_eq!(dist_timestamp(-5, 5), 10.0);
    assert_eq!(dist_pg_lsn(100, 250), 150.0);
}

#[test]
fn distance_float_nan_handling() {
    assert_eq!(dist_float4(1.0, 2.0), 1.0);
    assert_eq!(dist_float8(1.0, 2.5), 1.5);
    assert_eq!(dist_float4(f32::NAN, f32::NAN), 0.0);
    assert!(dist_float8(f64::NAN, 1.0).is_infinite());
    assert!(dist_float4(1.0, f32::NAN).is_infinite());
}

#[test]
fn distance_uuid_macaddr() {
    let z = [0u8; 16];
    assert_eq!(dist_uuid(&z, &z), 0.0);
    let mut hi = [0u8; 16];
    hi[0] = 1; // most significant byte
    assert!(dist_uuid(&z, &hi) > 0.0);

    let a = [0u8; 6];
    let mut b = [0u8; 6];
    // C processes octet "f" (byte index 5) first (divided by 256 six times) down
    // to "a" (byte index 0), divided once. So a delta in byte 0 contributes 1/256.
    b[0] = 16;
    assert!((dist_macaddr(&a, &b) - 16.0 / 256.0).abs() < 1e-12);

    let a8 = [0u8; 8];
    assert_eq!(dist_macaddr8(&a8, &a8), 0.0);
}

#[test]
fn distance_tid_mapping() {
    // (block, off) -> block * MaxHeapTuplesPerPage + off
    let mhtpp = types_storage::bufpage::MaxHeapTuplesPerPage as f64;
    assert_eq!(dist_tid((0, 1), (0, 5)), 4.0);
    assert_eq!(dist_tid((0, 0), (1, 0)), mhtpp);
}

#[test]
fn distance_timetz_interval() {
    // timetz: (time, zone); zone diff scaled by USECS_PER_SEC.
    assert_eq!(dist_timetz((0, 0), (1_000_000, 0)), 1_000_000.0);
    assert_eq!(dist_timetz((0, 0), (0, 1)), 1_000_000.0);

    // interval: month -> 30 days; 1 day == USECS_PER_DAY in the fraction.
    let one_day_usec = 86_400_000_000i64;
    assert_eq!(dist_interval((0, 0, 0), (0, 1, 0)), 1.0);
    assert_eq!(dist_interval((0, 0, 0), (0, 0, 1)), 30.0);
    assert_eq!(dist_interval((0, 0, 0), (one_day_usec, 0, 0)), 1.0);
}

#[test]
fn distance_inet_families_and_mask() {
    // different families -> 1.0
    assert_eq!(dist_inet(2, &[10, 0, 0, 1], 32, 10, &[0; 16], 128), 1.0);
    // same family, same address -> 0.0
    assert_eq!(dist_inet(2, &[10, 0, 0, 1], 32, 2, &[10, 0, 0, 1], 32), 0.0);
    // masked /24 cuts the last byte of both
    let d = dist_inet(2, &[10, 0, 0, 5], 24, 2, &[10, 0, 0, 9], 24);
    assert_eq!(d, 0.0);
}
