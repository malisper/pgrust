use super::*;

#[test]
fn arithmetic_basic() {
    assert_eq!(int8pl(2, 3).unwrap(), 5);
    assert_eq!(int8mi(2, 3).unwrap(), -1);
    assert_eq!(int8mul(6, 7).unwrap(), 42);
    assert_eq!(int8div(20, 4).unwrap(), 5);
    assert_eq!(int8mod(20, 6).unwrap(), 2);
    assert_eq!(int8abs(-5).unwrap(), 5);
    assert_eq!(int8um(5).unwrap(), -5);
}

#[test]
fn overflow_errors() {
    assert!(int8pl(i64::MAX, 1).is_err());
    assert!(int8mi(i64::MIN, 1).is_err());
    assert!(int8mul(i64::MAX, 2).is_err());
    assert!(int8um(i64::MIN).is_err());
    assert!(int8abs(i64::MIN).is_err());
}

#[test]
fn div_special_cases() {
    assert!(int8div(1, 0).is_err());
    assert!(int8mod(1, 0).is_err());
    // INT64_MIN / -1 overflows; % -1 is zero.
    assert!(int8div(i64::MIN, -1).is_err());
    assert_eq!(int8mod(i64::MIN, -1).unwrap(), 0);
    assert_eq!(int8div(-10, -1).unwrap(), 10);
}

#[test]
fn relational() {
    assert!(int8eq(3, 3));
    assert!(int84lt(3, 4));
    assert!(int48gt(5i32, 4i64));
    assert!(int82le(3, 3i16));
    assert!(int28ge(5i16, 4i64));
}

#[test]
fn gcd_lcm() {
    assert_eq!(int8gcd(12, 18).unwrap(), 6);
    assert_eq!(int8gcd(0, 0).unwrap(), 0);
    assert_eq!(int8gcd(i64::MIN, -1).unwrap(), 1);
    assert!(int8gcd(i64::MIN, 0).is_err());
    assert_eq!(int8lcm(4, 6).unwrap(), 12);
    assert_eq!(int8lcm(0, 5).unwrap(), 0);
}

#[test]
fn conversions() {
    assert_eq!(int48(7i32), 7i64);
    assert_eq!(int84(7i64).unwrap(), 7i32);
    assert!(int84(i64::MAX).is_err());
    assert!(int82(100000).is_err());
    assert!(i8tooid(-1).is_err());
    assert_eq!(oidtoi8(42u32), 42i64);
    assert_eq!(dtoi8(3.7).unwrap(), 4);
    assert!(dtoi8(f64::NAN).is_err());
    assert!(ftoi8(1e30f32).is_err());
}

#[test]
fn bitwise() {
    assert_eq!(int8and(0b1100, 0b1010), 0b1000);
    assert_eq!(int8or(0b1100, 0b1010), 0b1110);
    assert_eq!(int8xor(0b1100, 0b1010), 0b0110);
    assert_eq!(int8not(0), -1);
    assert_eq!(int8shl(1, 4), 16);
    assert_eq!(int8shr(16, 4), 1);
}

#[test]
fn text_io() {
    assert_eq!(int8out(-12345), "-12345");
    assert_eq!(int8out(0), "0");
    assert_eq!(int8in("9876", None).unwrap(), 9876);
}

#[test]
fn in_range() {
    // val >= base + offset when !sub, !less
    assert!(in_range_int8_int8(10, 5, 3, false, false).unwrap());
    assert!(in_range_int8_int8(-1, 0, 5, false, false).is_ok());
    assert!(in_range_int8_int8(0, 0, -1, false, false).is_err());
}

#[test]
fn series() {
    assert!(generate_series_int8_check_step(0).is_err());
    assert!(generate_series_int8_check_step(2).is_ok());
    assert_eq!(generate_series_int8_step(1, 3, 1), Some(Some(2)));
    assert_eq!(generate_series_int8_step(3, 3, 1), Some(Some(4)));
    assert_eq!(generate_series_int8_step(4, 3, 1), None);
    assert_eq!(generate_series_int8_step(i64::MAX, i64::MAX, 1), Some(None));
}
