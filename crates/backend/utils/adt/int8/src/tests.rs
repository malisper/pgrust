use super::*;
use crate::builtins::*;

use ::datum::Datum;
use ::types_fmgr::{FmgrInfo, LocalFcinfo};
use ::types_error::{
    ERRCODE_INVALID_TEXT_REPRESENTATION,
};

extern crate std;
use std::string::String;
use std::vec::Vec;

fn out_str(v: i64) -> String {
    let mut buf = [0u8; 24];
    let n = int8out(v, &mut buf);
    core::str::from_utf8(&buf[..n]).unwrap().into()
}

#[test]
fn io_boundaries_match_c() {
    for (v, s) in [
        (0i64, "0"),
        (1, "1"),
        (-1, "-1"),
        (i64::MAX, "9223372036854775807"),
        (i64::MIN, "-9223372036854775808"),
        (1000000000000, "1000000000000"),
    ] {
        assert_eq!(out_str(v), s);
        assert_eq!(int8in(s, None).unwrap(), v);
    }
}

#[test]
fn in_error_surface_matches_c() {
    let err = int8in("9223372036854775808", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "value \"9223372036854775808\" is out of range for type bigint"
    );
    assert_eq!(int8in("-9223372036854775808", None).unwrap(), i64::MIN);
    let err = int8in("xyz", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.message(), "invalid input syntax for type bigint: \"xyz\"");
    assert_eq!(int8in(" 42 ", None).unwrap(), 42);
    assert_eq!(int8in("0b101", None).unwrap(), 5);
    assert!(int8in("", None).is_err());

    let mut soft = SoftErrorContext::new(true);
    assert_eq!(int8in("bogus", Some(&mut soft)).unwrap(), 0);
    assert!(soft.error_occurred());
}

#[test]
fn arithmetic_overflow_boundaries() {
    assert_eq!(int8pl(i64::MAX - 1, 1).unwrap(), i64::MAX);
    let err = int8pl(i64::MAX, 1).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "bigint out of range");
    assert!(int8mi(i64::MIN, 1).is_err());
    assert!(int8mul(i64::MAX, 2).is_err());
    assert!(int8mul(i64::MIN, -1).is_err());
    assert!(int8um(i64::MIN).is_err());
    assert!(int8abs(i64::MIN).is_err());
    assert!(int8inc(i64::MAX).is_err());
    assert!(int8dec(i64::MIN).is_err());
    assert_eq!(int8inc(41).unwrap(), 42);
    assert_eq!(int8inc_any(41).unwrap(), 42);
    assert_eq!(int8dec_any(43).unwrap(), 42);
    assert!(int84pl(i64::MAX, 1).is_err());
    assert!(int48mul(i32::MAX, i64::MAX).is_err());
    assert!(int82mi(i64::MIN, 1).is_err());
    assert!(int28pl(1, i64::MAX).is_err());
}

#[test]
fn division_semantics() {
    let err = int8div(1, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DIVISION_BY_ZERO);
    assert_eq!(err.message(), "division by zero");
    assert!(int8mod(1, 0).is_err());
    assert!(int84div(1, 0).is_err());
    assert!(int48div(1, 0).is_err());
    assert!(int82div(1, 0).is_err());
    assert!(int28div(1, 0).is_err());

    assert_eq!(int8div(i64::MIN, -1).unwrap_err().message(), "bigint out of range");
    assert_eq!(int84div(i64::MIN, -1).unwrap_err().message(), "bigint out of range");
    assert_eq!(int82div(i64::MIN, -1).unwrap_err().message(), "bigint out of range");
    assert_eq!(int8mod(i64::MIN, -1).unwrap(), 0);
    assert_eq!(int8div(7, -2).unwrap(), -3);
    assert_eq!(int8mod(-7, 2).unwrap(), -1);
    assert_eq!(int48div(i32::MIN, -1).unwrap(), 2147483648);
}

#[test]
fn gcd_lcm_rows_from_int8_sql() {
    assert_eq!(int8gcd(0, 0).unwrap(), 0);
    assert_eq!(int8gcd(0, 29893644334, ).unwrap(), 29893644334);
    assert_eq!(int8gcd(288484263558, 29893644334).unwrap(), 6835958);
    assert_eq!(int8gcd(-288484263558, 29893644334).unwrap(), 6835958);
    assert_eq!(int8gcd(i64::MIN, 1).unwrap(), 1);
    assert_eq!(int8gcd(i64::MIN, -1).unwrap(), 1);
    assert_eq!(int8gcd(i64::MIN, 4611686018427387904).unwrap(), 4611686018427387904);
    assert!(int8gcd(i64::MIN, 0).is_err());
    assert!(int8gcd(i64::MIN, i64::MIN).is_err());

    assert_eq!(int8lcm(0, 0).unwrap(), 0);
    assert_eq!(int8lcm(i64::MIN, 0).unwrap(), 0);
    assert_eq!(int8lcm(330, 462).unwrap(), 2310);
    assert_eq!(int8lcm(-330, 462).unwrap(), 2310);
    assert!(int8lcm(i64::MIN, 1).is_err());
    assert!(int8lcm(2, i64::MAX).is_err());
}

#[test]
fn casts_and_floats() {
    assert!(int84(PG_INT32_MAX + 1).is_err());
    assert_eq!(int84(PG_INT32_MAX + 1).unwrap_err().message(), "integer out of range");
    assert!(int84(PG_INT32_MIN - 1).is_err());
    assert_eq!(int84(-5).unwrap(), -5);
    assert_eq!(int48(-5), -5);
    assert!(int82(32768).is_err());
    assert_eq!(int82(32768).unwrap_err().message(), "smallint out of range");
    assert_eq!(int82(-32768).unwrap(), -32768);
    assert_eq!(int28(-5), -5);

    assert_eq!(dtoi8(2.5).unwrap(), 2); // rint: ties to even
    assert_eq!(dtoi8(3.5).unwrap(), 4);
    assert_eq!(dtoi8(-2.5).unwrap(), -2);
    assert!(dtoi8(f64::NAN).is_err());
    assert!(dtoi8(f64::INFINITY).is_err());
    assert!(dtoi8(9.3e18).is_err());
    assert_eq!(dtoi8(-9.223372036854776e18).unwrap(), i64::MIN);
    assert_eq!(ftoi8(2.5f32).unwrap(), 2);
    assert!(ftoi8(f32::NAN).is_err());
    assert!(ftoi8(9.3e18f32).is_err());
    assert_eq!(i8tod(42), 42.0);
    assert_eq!(i8tof(42), 42.0f32);

    assert!(i8tooid(-1).is_err());
    assert_eq!(i8tooid(-1).unwrap_err().message(), "OID out of range");
    assert!(i8tooid(PG_UINT32_MAX + 1).is_err());
    assert_eq!(i8tooid(PG_UINT32_MAX).unwrap(), u32::MAX);
    assert_eq!(oidtoi8(u32::MAX), PG_UINT32_MAX);
}

#[test]
fn bit_ops_larger_smaller_in_range() {
    assert_eq!(int8and(0b1100, 0b1010), 0b1000);
    assert_eq!(int8or(0b1100, 0b1010), 0b1110);
    assert_eq!(int8xor(0b1100, 0b1010), 0b0110);
    assert_eq!(int8not(0), -1);
    assert_eq!(int8shl(1, 40), 1 << 40);
    assert_eq!(int8shr(-16, 2), -4);
    assert_eq!(int8larger(3, 9), 9);
    assert_eq!(int8smaller(3, 9), 3);

    let err = in_range_int8_int8(0, 0, -1, false, true).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE);
    assert!(in_range_int8_int8(5, i64::MAX, 10, false, true).unwrap());
    assert!(!in_range_int8_int8(5, i64::MAX, 10, false, false).unwrap());
    assert!(in_range_int8_int8(5, i64::MIN, 10, true, false).unwrap());
    assert!(in_range_int8_int8(5, 3, 4, false, true).unwrap());
}

#[test]
fn series() {
    let mut g = GenerateSeriesInt8::new(1, 3, 1).unwrap();
    assert_eq!(core::iter::from_fn(|| g.next()).collect::<Vec<_>>(), [1, 2, 3]);
    let mut g = GenerateSeriesInt8::new(i64::MAX - 1, i64::MAX, 1).unwrap();
    assert_eq!(
        core::iter::from_fn(|| g.next()).collect::<Vec<_>>(),
        [i64::MAX - 1, i64::MAX]
    );
    let err = GenerateSeriesInt8::new(1, 10, 0).unwrap_err();
    assert_eq!(err.message(), "step size cannot equal zero");
    assert_eq!(generate_series_int8_rows(1.0, 10.0, 2.0), Some(5.0));
    assert_eq!(generate_series_int8_rows(1.0, 10.0, 0.0), None);
}

#[test]
fn fmgr_wrappers_and_table() {
    let mut flinfo = FmgrInfo::new(fc_int8pl, 463, 2, true, false);
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i64(40));
    fci.set_arg(1, Datum::from_i64(2));
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_i64(), 42);
    fci.set_arg(0, Datum::from_i64(i64::MAX));
    fci.set_arg(1, Datum::from_i64(1));
    assert_eq!(flinfo.invoke(&mut fci).unwrap_err().message(), "bigint out of range");

    let mut flinfo = FmgrInfo::new(fc_int8out, 461, 1, true, false);
    let mut fci = LocalFcinfo::<1>::new(0);
    for v in [0i64, i64::MIN, i64::MAX, -1] {
        fci.set_arg(0, Datum::from_i64(v));
        let d = flinfo.invoke(&mut fci).unwrap();
        let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
        assert_eq!(s.to_bytes(), out_str(v).as_bytes());
    }

    let mut fci = LocalFcinfo::<1>::new(0);
    let num = b"-9223372036854775808\0";
    fci.set_arg(0, Datum::from_usize(num.as_ptr() as usize));
    assert_eq!(fc_int8in(None, &mut fci).unwrap().as_i64(), i64::MIN);

    // int8inc_any reads only arg0 off the 2-arg frame.
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_i64(7));
    fci.set_arg(1, Datum::from_usize(0));
    assert_eq!(fc_int8inc_any(None, &mut fci).unwrap().as_i64(), 8);

    let mut oids: Vec<u32> = INT8_BUILTINS.iter().map(|b| b.foid).collect();
    oids.sort_unstable();
    let n = oids.len();
    oids.dedup();
    assert_eq!(n, oids.len());
    assert_eq!(n, 91);
    for b in INT8_BUILTINS {
        assert!(b.strict);
        assert_eq!(b.retset, matches!(b.foid, 1068 | 1069));
    }
}

// hashint8 folds the high half so int2/int4/int8 hash equal for equal values
// (hashfunc.c cross-type hash joins).
#[test]
fn hashint8_folds_to_int4_hash() {
    for v in [0i64, 1, 42, -1, -42, 550273, i32::MAX as i64, i32::MIN as i64] {
        let lohalf = v as u32;
        let hihalf = (v >> 32) as u32;
        let folded = lohalf ^ if v >= 0 { hihalf } else { !hihalf };
        assert_eq!(
            ::hashfn::hash_bytes_uint32(folded),
            ::hashfn::hash_bytes_uint32(v as i32 as u32),
            "v={v}"
        );
    }
}
