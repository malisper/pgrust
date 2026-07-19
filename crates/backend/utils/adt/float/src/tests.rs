use super::*;
use crate::aggregates::*;
use crate::builtins::*;

use ::datum::Datum;
use ::types_error::{
    SoftErrorContext, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_ARGUMENT_FOR_LOG,
    ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
    ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROTOCOL_VIOLATION,
};
use ::types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction};

fn out8(v: f64) -> String {
    let mut buf = [0u8; MAXDOUBLEWIDTH];
    let n = float8out(v, &mut buf);
    core::str::from_utf8(&buf[..n]).unwrap().into()
}

fn out8_with(v: f64, efd: i32) -> String {
    let mut buf = [0u8; MAXDOUBLEWIDTH];
    let n = float8out_internal_with(v, efd, &mut buf);
    core::str::from_utf8(&buf[..n]).unwrap().into()
}

fn out4_with(v: f32, efd: i32) -> String {
    let mut buf = [0u8; MAXDOUBLEWIDTH];
    let n = float4out_with(v, efd, &mut buf);
    core::str::from_utf8(&buf[..n]).unwrap().into()
}

#[test]
fn float8in_basic_and_specials() {
    assert_eq!(float8in("1.5", None).unwrap(), 1.5);
    assert_eq!(float8in("  -2.25  ", None).unwrap(), -2.25);
    assert_eq!(float8in("1e10", None).unwrap(), 1e10);
    assert_eq!(float8in(".5", None).unwrap(), 0.5);
    assert_eq!(float8in("5.", None).unwrap(), 5.0);
    assert_eq!(float8in("\x0b1.5", None).unwrap(), 1.5); // \v is C isspace

    assert!(float8in("NaN", None).unwrap().is_nan());
    assert!(float8in("nan", None).unwrap().is_nan());
    assert!(float8in("-nan", None).unwrap().is_nan());
    assert!(float8in("+nan", None).unwrap().is_nan());
    assert!(float8in("nan(123)", None).unwrap().is_nan()); // live PG 18.3: NaN
    assert!(float8in("nan(0x1)", None).unwrap().is_nan());
    assert!(float8in("NAN()", None).unwrap().is_nan());
    assert!(float8in("nan()x", None).is_err()); // live PG 18.3: 22P02
    assert!(float8in("nan(12", None).is_err());
    assert_eq!(float8in("Infinity", None).unwrap(), f64::INFINITY);
    assert_eq!(float8in("-Infinity", None).unwrap(), f64::NEG_INFINITY);
    assert_eq!(float8in(" +inf ", None).unwrap(), f64::INFINITY);
    assert_eq!(float8in("-inf", None).unwrap(), f64::NEG_INFINITY);
    assert!(float4in("nan(xyz_12)", None).unwrap().is_nan());
    assert_eq!(float4in("-Infinity", None).unwrap(), f32::NEG_INFINITY);
}

#[test]
fn float8in_error_surface_matches_live_pg() {
    let err = float8in("1e400", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "\"1e400\" is out of range for type double precision");
    let err = float8in("1e-400", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    let err = float8in(" 1.5x", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.message(), "invalid input syntax for type double precision: \" 1.5x\"");
    let err = float4in("1e40", None).unwrap_err();
    assert_eq!(err.message(), "\"1e40\" is out of range for type real");
    assert!(float4in("1e-50", None).is_err());
    let err = float8in("", None).unwrap_err();
    assert_eq!(err.message(), "invalid input syntax for type double precision: \"\"");
    assert!(float8in("  ", None).is_err());
    assert!(float8in("xyz", None).is_err());

    let mut soft = SoftErrorContext::new(true);
    assert_eq!(float8in("bogus", Some(&mut soft)).unwrap(), 0.0);
    assert!(soft.error_occurred());
}

#[test]
fn hex_floats_match_pg() {
    let cases: &[(&str, u64)] = &[
        ("0x1p4", 0x4030000000000000),
        ("0X1.8p1", 0x4008000000000000),
        ("0x10", 0x4030000000000000),
        ("0xA", 0x4024000000000000),
        ("0x.8p1", 0x3ff0000000000000),
        ("-0x1p4", 0xc030000000000000),
        ("0x1.999999999999ap-4", 0x3fb999999999999a),
        ("0x0", 0x0000000000000000),
        ("0x1p-1074", 0x0000000000000001),
        ("0x1.fffffffffffffp1023", 0x7fefffffffffffff),
        ("0x1.0000000000001p0", 0x3ff0000000000001),
        ("0x1.00000000000008p0", 0x3ff0000000000000),
        ("0x1.00000000000018p0", 0x3ff0000000000002),
        ("0x1.fffffffffffff8p0", 0x4000000000000000),
        ("0x3p-1", 0x3ff8000000000000),
        ("0xabcdefp0", 0x416579bde0000000),
        ("0x1p-1022", 0x0010000000000000),
        ("0x1p-1023", 0x0008000000000000),
    ];
    for &(lit, bits) in cases {
        let v = float8in(lit, None).unwrap_or_else(|e| panic!("{lit}: {}", e.message()));
        assert_eq!(v.to_bits(), bits, "float8 {lit}");
    }

    let cases4: &[(&str, u32)] = &[
        ("0x1p4", 0x41800000),
        ("0x1.8p1", 0x40400000),
        ("0x1p-149", 0x00000001),
        ("0x1.fffffep127", 0x7f7fffff),
        ("0x1.000001p0", 0x3f800000),
        ("0x1.000002p0", 0x3f800001),
        ("0x1.000003p0", 0x3f800002),
        ("0x1p-126", 0x00800000),
        ("0x1p-127", 0x00400000),
    ];
    for &(lit, bits) in cases4 {
        let v = float4in(lit, None).unwrap_or_else(|e| panic!("{lit}: {}", e.message()));
        assert_eq!(v.to_bits(), bits, "float4 {lit}");
    }

    let err = float8in("0x1p1024", None).unwrap_err();
    assert_eq!(err.message(), "\"0x1p1024\" is out of range for type double precision");
    assert!(float8in("0x1p-1075", None).is_err());
    assert!(float4in("0x1p128", None).is_err());
    assert!(float8in("0x", None).is_err());
    assert!(float8in("0x1p", None).unwrap_err().sqlstate() == ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(float8in("  0x1p4  ", None).unwrap(), 16.0);
}

#[test]
fn endptr_path_reports_consumed() {
    let mut endptr = 0usize;
    let v = float8in_internal("2.71, 2.0", Some(&mut endptr), "point", "2.71, 2.0", None).unwrap();
    assert_eq!((v, endptr), (2.71, 4));

    let mut endptr = 0usize;
    let v = float8in_internal("  2.71  rest", Some(&mut endptr), "box", "  2.71  rest", None).unwrap();
    assert_eq!((v, endptr), (2.71, 8));

    let mut endptr = 0usize;
    let v = float8in_internal("0x1p,5", Some(&mut endptr), "point", "0x1p,5", None).unwrap();
    assert_eq!((v, endptr), (1.0, 3));
}

// Expected strings are live psql output from PostgreSQL 18.3.
#[test]
fn out_shortest_matches_live_pg() {
    assert_eq!(out8(0.1), "0.1");
    assert_eq!(out8(1e-5), "1e-05");
    assert_eq!(out8(3.141592653589793), "3.141592653589793");
    assert_eq!(out8(1e300), "1e+300");
    assert_eq!(out8(5e-324), "5e-324");
    assert_eq!(out8(123456.789), "123456.789");
    assert_eq!(out8(f64::NAN), "NaN");
    assert_eq!(out8(f64::INFINITY), "Infinity");
    assert_eq!(out8(-0.0), "-0");

    assert_eq!(out4_with(0.1, 1), "0.1");
    assert_eq!(out4_with(1.234567, 1), "1.234567");
    assert_eq!(out4_with(1e-5, 1), "1e-05");
    assert_eq!(out4_with(1e20, 1), "1e+20");
    assert_eq!(out4_with(3.4028235e38, 1), "3.4028235e+38");
}

// Expected strings are live psql output from PostgreSQL 18.3 under
// set extra_float_digits = {0, -5, -15, -3}.
#[test]
fn out_legacy_g_matches_live_pg() {
    assert_eq!(out8_with(0.1, 0), "0.1");
    assert_eq!(out8_with(1.0, 0), "1");
    assert_eq!(out8_with(1e-5, 0), "1e-05");
    assert_eq!(out8_with(1e20, 0), "1e+20");
    assert_eq!(out8_with(123456.789, 0), "123456.789");
    assert_eq!(out8_with(3.141592653589793, 0), "3.14159265358979");
    assert_eq!(out8_with(f64::INFINITY, 0), "Infinity");
    assert_eq!(out8_with(-0.0, 0), "-0");
    assert_eq!(out8_with(0.000123, 0), "0.000123");

    assert_eq!(out8_with(3.141592653589793, -5), "3.141592654");
    assert_eq!(out8_with(123456.789, -5), "123456.789");
    assert_eq!(out8_with(0.1, -5), "0.1");

    assert_eq!(out8_with(3.141592653589793, -15), "3");
    assert_eq!(out8_with(123456.789, -15), "1e+05");
    assert_eq!(out8_with(0.5, -15), "0.5");

    assert_eq!(out4_with(0.1, 0), "0.1");
    assert_eq!(out4_with(1.234567, 0), "1.23457");
    assert_eq!(out4_with(1e-5, 0), "1e-05");
    assert_eq!(out4_with(1e20, 0), "1e+20");
    assert_eq!(out4_with(123456.7, 0), "123457");

    assert_eq!(out4_with(0.1, -3), "0.1");
    assert_eq!(out4_with(123456.7, -3), "1.23e+05");
}

#[test]
fn guc_default_and_live_read() {
    assert_eq!(get_extra_float_digits(), 1);
    set_extra_float_digits(0);
    assert_eq!(out8(3.141592653589793), "3.14159265358979");
    set_extra_float_digits(1);
    assert_eq!(out8(3.141592653589793), "3.141592653589793");
}

#[test]
fn roundtrip_in_out() {
    for &s in &["1.5", "3.14159265358979", "1e10", "-2.5e-3", "0", "123456.789"] {
        let v = float8in(s, None).unwrap();
        assert_eq!(float8in(&out8(v), None).unwrap(), v, "roundtrip {s}");
    }
}

#[test]
fn wire_codec() {
    assert_eq!(float8send(1.0), [0x3F, 0xF0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(float4send(1.0_f32), [0x3F, 0x80, 0, 0]);
    for &v in &[0.0_f64, 1.5, -2.25, 1e10, f64::INFINITY] {
        assert_eq!(float8recv(&float8send(v)).unwrap(), v);
    }
    assert!(float8recv(&float8send(f64::NAN)).unwrap().is_nan());
    for &v in &[0.0_f32, 1.5, -2.25, f32::NEG_INFINITY] {
        assert_eq!(float4recv(&float4send(v)).unwrap(), v);
    }
    let e = float4recv(&[0u8; 3]).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
    assert_eq!(e.message(), "insufficient data left in message");
    assert!(float8recv(&[0u8; 7]).is_err());
}

#[test]
fn nan_ordering_and_arith_errors() {
    let nan = f64::NAN;
    assert!(float8_eq(nan, nan));
    assert!(!float8_eq(nan, 1.0));
    assert!(float8_gt(nan, f64::INFINITY));
    assert!(float8_ge(nan, nan));
    assert_eq!(float8_cmp_internal(nan, nan), 0);
    assert_eq!(float8_cmp_internal(nan, 1.0), 1);
    assert_eq!(float8_cmp_internal(1.0, nan), -1);
    assert_eq!(btfloat48cmp(1.0_f32, 2.0), -1);
    assert_eq!(btfloat84cmp(2.0, 1.0_f32), 1);
    assert!(float8larger(nan, 1.0).is_nan());
    assert_eq!(float8smaller(1.0, 2.0), 1.0);

    let err = float8_pl(f64::MAX, f64::MAX).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "value out of range: overflow");
    assert_eq!(float8_pl(f64::INFINITY, 1.0).unwrap(), f64::INFINITY);

    let err = float8_div(1.0, 0.0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DIVISION_BY_ZERO);
    assert_eq!(err.message(), "division by zero");
    assert!(float8_div(f64::NAN, 0.0).unwrap().is_nan());

    let err = float8_mul(f64::MIN_POSITIVE, f64::MIN_POSITIVE).unwrap_err();
    assert_eq!(err.message(), "value out of range: underflow");
    assert!(float4_mul(1e30_f32, 1e30_f32).is_err());
    assert_eq!(float48pl(1.5_f32, 2.5).unwrap(), 4.0);
    assert_eq!(float84mul(2.0, 3.0_f32).unwrap(), 6.0);
    assert!(float48eq(1.0_f32, 1.0));
    assert!(float84lt(1.0, 2.0_f32));
}

#[test]
fn conversions_match_live_pg() {
    // live PG 18.3: 2147483647|2|4|-2
    assert_eq!(dtoi4(2147483647.4).unwrap(), 2147483647);
    assert_eq!(dtoi4(2.5).unwrap(), 2);
    assert_eq!(dtoi4(3.5).unwrap(), 4);
    assert_eq!(dtoi4(-2.5).unwrap(), -2);
    let err = dtoi4(2147483648.0).unwrap_err();
    assert_eq!(err.message(), "integer out of range");
    assert!(dtoi4(f64::NAN).is_err());
    let err = dtoi2(40000.0).unwrap_err();
    assert_eq!(err.message(), "smallint out of range");
    assert_eq!(ftoi2(100.4_f32).unwrap(), 100);

    assert!(dtof(1e40).is_err());
    assert!(dtof(1e-50).is_err());
    assert_eq!(dtof(1.5).unwrap(), 1.5_f32);
    assert_eq!(dtof(f64::INFINITY).unwrap(), f32::INFINITY);
    assert_eq!(ftod(1.5_f32), 1.5);
    assert_eq!(i4tod(7), 7.0);
    assert_eq!(i2tof(-3), -3.0_f32);
}

#[test]
#[cfg_attr(miri, ignore)] // Miri approximates libm; exact-value KATs
fn math_domains_and_live_pg_values() {
    let err = dsqrt(-1.0).unwrap_err();
    assert_eq!(err.message(), "cannot take square root of a negative number");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION);
    let err = dpow(0.0, -1.0).unwrap_err();
    assert_eq!(err.message(), "zero raised to a negative power is undefined");
    let err = dlog1(0.0).unwrap_err();
    assert_eq!(err.message(), "cannot take logarithm of zero");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ARGUMENT_FOR_LOG);
    assert_eq!(dlog1(-1.0).unwrap_err().message(), "cannot take logarithm of a negative number");
    assert_eq!(
        dpow(-2.0, 0.5).unwrap_err().message(),
        "a negative number raised to a non-integer power yields a complex result"
    );

    // live PG 18.3 values, byte-compared through float8out.
    assert_eq!(out8(dcbrt(27.0).unwrap()), "3");
    assert_eq!(out8(dexp(1.0).unwrap()), "2.718281828459045");
    assert_eq!(out8(dpow(2.0, 0.5).unwrap()), "1.4142135623730951");
    assert_eq!(out8(datan2(1.0, 2.0).unwrap()), "0.4636476090008061");
    assert_eq!(out8(dsinh(1.0)), "1.1752011936438014");
    assert_eq!(out8(dcosh(1.0).unwrap()), "1.5430806348152437");
    assert_eq!(out8(dtanh(1.0).unwrap()), "0.7615941559557649");
    assert_eq!(out8(derf(1.0).unwrap()), "0.8427007929497149");
    assert_eq!(out8(derfc(1.0).unwrap()), "0.15729920705028513");
    assert_eq!(out8(dgamma(5.5).unwrap()), "52.34277778455352");
    // glibc's lgamma(10.5) differs by 1 ULP between aarch64 and x86_64;
    // funcs.rs binds the SYSTEM libm (C's parity reference), so each arm
    // below byte-matches the same-arch live C PG (x86 stage-1 bring-up
    // lane, 2026-07-17: PGDG 18.4 x86_64 SELECT lgamma(10.5) =
    // 13.940625219403762 verified in-pod; every other literal in this
    // block matched cross-arch).
    #[cfg(target_arch = "x86_64")]
    assert_eq!(out8(dlgamma(10.5).unwrap()), "13.940625219403762");
    #[cfg(not(target_arch = "x86_64"))]
    assert_eq!(out8(dlgamma(10.5).unwrap()), "13.940625219403763");

    assert_eq!(dpow(f64::NAN, 0.0).unwrap(), 1.0);
    assert_eq!(dpow(1.0, f64::NAN).unwrap(), 1.0);
    assert!(dpow(f64::NAN, 2.0).unwrap().is_nan());
    assert_eq!(dpow(2.0, f64::INFINITY).unwrap(), f64::INFINITY);
    assert_eq!(dpow(f64::NEG_INFINITY, 3.0).unwrap(), f64::NEG_INFINITY);
    assert!(dpow(10.0, 400.0).is_err());
    assert_eq!(dexp(f64::NEG_INFINITY).unwrap(), 0.0);
    assert!(dexp(1000.0).is_err());
    assert!(dgamma(f64::NEG_INFINITY).is_err());
    assert!(dlgamma(0.0).is_err());

    assert!(dacos(2.0).is_err());
    assert!(dacos(f64::NAN).unwrap().is_nan());
    assert!(dsin(f64::INFINITY).is_err());
    assert!(dcos(f64::INFINITY).unwrap_err().message() == "input is out of range");
    assert!(dacosh(0.5).is_err());
    assert!(datanh(1.0).unwrap().is_infinite());
}

#[test]
#[cfg_attr(miri, ignore)] // Miri approximates libm; exact-value KATs
fn degree_trig_exact_cardinals() {
    // live PG 18.3: 60|30|1|0.5|0.5|45
    assert_eq!(dacosd(0.5).unwrap(), 60.0);
    assert_eq!(dasind(0.5).unwrap(), 30.0);
    assert_eq!(dtand(45.0).unwrap(), 1.0);
    assert_eq!(dsind(30.0).unwrap(), 0.5);
    assert_eq!(dcosd(60.0).unwrap(), 0.5);
    assert_eq!(datand(1.0).unwrap(), 45.0);
    assert_eq!(dsind(90.0).unwrap(), 1.0);
    assert_eq!(dsind(180.0).unwrap(), 0.0);
    assert_eq!(dcosd(0.0).unwrap(), 1.0);
    assert_eq!(dcosd(90.0).unwrap(), 0.0);
    assert!(dsind(f64::INFINITY).is_err());
    assert!(dsind(f64::NAN).unwrap().is_nan());
    assert_eq!(dpi(), core::f64::consts::PI);
}

#[test]
fn width_bucket_and_in_range() {
    // live PG 18.3: width_bucket(5.35, 0.024, 10.06, 5) = 3
    assert_eq!(width_bucket_float8(5.35, 0.024, 10.06, 5).unwrap(), 3);
    assert_eq!(width_bucket_float8(-1.0, 0.0, 10.0, 5).unwrap(), 0);
    assert_eq!(width_bucket_float8(100.0, 0.0, 10.0, 5).unwrap(), 6);
    assert_eq!(width_bucket_float8(5.0, 10.0, 0.0, 5).unwrap(), 3);
    let err = width_bucket_float8(5.0, 0.0, 10.0, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION);
    assert!(width_bucket_float8(f64::NAN, 0.0, 10.0, 5).is_err());
    assert!(width_bucket_float8(5.0, 0.0, 0.0, 5).is_err());
    assert!(width_bucket_float8(5.0, f64::INFINITY, 10.0, 5).is_err());

    assert!(in_range_float8_float8(5.0, 3.0, 2.0, false, false).unwrap());
    assert!(!in_range_float8_float8(6.0, 3.0, 2.0, false, true).unwrap());
    let err = in_range_float8_float8(1.0, 1.0, -1.0, false, false).unwrap_err();
    assert_eq!(err.message(), "invalid preceding or following size in window function");
    assert!(in_range_float8_float8(f64::NAN, f64::NAN, 1.0, false, true).unwrap());
    assert!(in_range_float4_float8(1.0, 1.0, f64::INFINITY, false, true).unwrap());
}

#[test]
#[cfg_attr(miri, ignore)] // Miri approximates libm; exact-value KATs
fn aggregates_match_live_pg() {
    // avg/var_samp/var_pop/stddev_samp over (1.0, 2.5, 4.25, -3.5).
    let mut t = [0.0f64; 3];
    for v in [1.0, 2.5, 4.25, -3.5] {
        t = float8_accum(t, v).unwrap();
    }
    assert_eq!(out8(float8_avg(t).unwrap()), "1.0625");
    assert_eq!(out8(float8_var_samp(t).unwrap()), "11.015625");
    assert_eq!(out8(float8_var_pop(t).unwrap()), "8.26171875");
    assert_eq!(out8(float8_stddev_samp(t).unwrap()), "3.3189795118379384");
    assert_eq!(float8_avg([0.0; 3]), None);
    assert_eq!(float8_var_samp([1.0, 5.0, 0.0]), None);

    // regr family over (y,x) = (1,2),(2.5,4.1),(4.25,7.9),(-3.5,-6).
    let mut r = [0.0f64; 6];
    for (y, x) in [(1.0, 2.0), (2.5, 4.1), (4.25, 7.9), (-3.5, -6.0)] {
        r = float8_regr_accum(r, y, x).unwrap();
    }
    assert_eq!(out8(float8_corr(r).unwrap()), "0.9986369273154668");
    assert_eq!(out8(float8_regr_slope(r).unwrap()), "0.5650552218562294");
    assert_eq!(out8(float8_regr_intercept(r).unwrap()), "-0.06761044371245872");
    assert_eq!(out8(float8_regr_r2(r).unwrap()), "0.997275712598077");
    assert_eq!(out8(float8_covar_pop(r).unwrap()), "14.581249999999999");
    assert_eq!(out8(float8_covar_samp(r).unwrap()), "19.441666666666666");
    assert_eq!(float8_corr([0.0; 6]), None);

    // combine = concat of halves.
    let mut a = [0.0f64; 3];
    let mut b = [0.0f64; 3];
    for v in [1.0, 2.5] {
        a = float8_accum(a, v).unwrap();
    }
    for v in [4.25, -3.5] {
        b = float8_accum(b, v).unwrap();
    }
    let c = float8_combine(a, b).unwrap();
    assert_eq!(c[0], 4.0);
    assert_eq!(out8(float8_avg(c).unwrap()), "1.0625");
    assert_eq!(float8_combine([0.0; 3], b).unwrap(), b);

    // NaN/Inf poisoning.
    let t = float8_accum([2.0, 3.0, 1.0], f64::INFINITY).unwrap();
    assert!(t[1].is_infinite() && t[2].is_nan());
    assert!(float8_accum([1.0, f64::MAX, 0.0], f64::MAX).is_err());
}

#[test]
fn transarray_image_layout() {
    let vals = [1.0f64, -2.5, 0.0];
    let mut img = [0u8; float8_transarray_size(3)];
    let n = write_float8_transarray(&vals, &mut img);
    assert_eq!(n, 48);
    let words: Vec<i32> = img[..24]
        .chunks(4)
        .map(|c| i32::from_ne_bytes(c.try_into().unwrap()))
        .collect();
    // vl_len_ == SET_VARSIZE(48), ndim 1, dataoffset 0 (no nulls),
    // elemtype FLOAT8OID, dim1 3, lbound1 1.
    assert_eq!(words, [48 << 2, 1, 0, 701, 3, 1]);
    assert_eq!(check_float8_array::<3>(&img, "t").unwrap(), vals);

    let err = check_float8_array::<6>(&img, "float8_regr_sxx").unwrap_err();
    assert_eq!(err.message(), "float8_regr_sxx: expected 6-element float8 array");
    assert!(check_float8_array::<3>(&img[..20], "t").is_err());
}

#[test]
fn fmgr_wrappers_and_table() {
    let mut flinfo = FmgrInfo::new(fc_float8pl, 218, 2, true, false);
    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_f64(40.5));
    fci.set_arg(1, Datum::from_f64(1.5));
    assert_eq!(flinfo.invoke(&mut fci).unwrap().as_f64(), 42.0);
    fci.set_arg(0, Datum::from_f64(f64::MAX));
    fci.set_arg(1, Datum::from_f64(f64::MAX));
    let err = flinfo.invoke(&mut fci).unwrap_err();
    assert_eq!(err.message(), "value out of range: overflow");

    let mut fci = LocalFcinfo::<2>::new(0);
    fci.set_arg(0, Datum::from_f32(1.5));
    fci.set_arg(1, Datum::from_f32(1.5));
    assert!(fc_float4eq(None, &mut fci).unwrap().as_bool());
    fci.set_arg(1, Datum::from_f32(2.0));
    assert!(!fc_float4eq(None, &mut fci).unwrap().as_bool());
    assert_eq!(fc_btfloat4cmp(None, &mut fci).unwrap().as_i32(), -1);

    let mut flinfo = FmgrInfo::new(fc_float8out, 215, 1, true, false);
    let mut fci = LocalFcinfo::<1>::new(0);
    for v in [0.1f64, -0.0, 1e300, f64::NAN, 3.141592653589793] {
        fci.set_arg(0, Datum::from_f64(v));
        let d = flinfo.invoke(&mut fci).unwrap();
        let s = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
        assert_eq!(s.to_bytes(), out8(v).as_bytes());
    }

    let mut fci = LocalFcinfo::<1>::new(0);
    let num = b"-2.5e-3\0";
    fci.set_arg(0, Datum::from_usize(num.as_ptr() as usize));
    assert_eq!(fc_float8in(None, &mut fci).unwrap().as_f64(), -2.5e-3);
    let num = b"0x1p4\0";
    fci.set_arg(0, Datum::from_usize(num.as_ptr() as usize));
    assert_eq!(fc_float4in(None, &mut fci).unwrap().as_f32(), 16.0);

    let mut fci = LocalFcinfo::<0>::new(0);
    assert_eq!(fc_dpi(None, &mut fci).unwrap().as_f64(), core::f64::consts::PI);

    // Table sanity: unique OIDs, all rows strict/non-retset, and every row
    // matches the canonical pg_proc projection (name + nargs).
    let mut oids: Vec<u32> = FLOAT_BUILTINS.iter().map(|b| b.foid).collect();
    oids.sort_unstable();
    let n = oids.len();
    oids.dedup();
    assert_eq!(n, oids.len());
    assert_eq!(n, 153);
    for b in FLOAT_BUILTINS {
        assert!(b.strict && !b.retset);
        let c = fmgr_core::CANONICAL
            .iter()
            .find(|c| c.0 == b.foid)
            .unwrap_or_else(|| panic!("OID {} not in canonical table", b.foid));
        assert_eq!((c.1, c.2), (b.name, b.nargs), "OID {}", b.foid);
    }
}

// hashfloat4/8 (hashfunc.c): ±0 collapse, float4 widens to float8 (cross-type
// joins), NaN bit patterns collapse to the standard NaN.
#[test]
fn float_hash_image_rules() {
    use crate::builtins::float8_hash_image;
    assert_eq!(float8_hash_image(f64::NAN), float8_hash_image(-f64::NAN));
    assert_eq!(
        float8_hash_image(1.5f32 as f64),
        float8_hash_image(1.5f64),
        "float4 widening must hash like the equal float8"
    );
    let h0 = ::hashfn::hash_bytes(&float8_hash_image(0.0));
    let hneg0 = ::hashfn::hash_bytes(&float8_hash_image(-0.0));
    assert_eq!(h0, hneg0);
}

// fmgr frames for the float8[] transvalue family: the agg-context leg
// updates arg0 in place (C's AggCheckCallContext cheat); the bare leg
// builds a fresh construct_array image in the result mcx.
#[test]
#[cfg_attr(miri, ignore)] // exact-value KATs shared with aggregates_match_live_pg
fn float_agg_fmgr_frames() {
    use ::mcx::MemoryContext;
    use ::types_fmgr::AggStateNode;

    let ctx = MemoryContext::new_bump("float-agg-test");
    let read3 = |d: Datum| {
        // SAFETY: a live float8[3] image datum from the frame under test.
        let img = unsafe {
            core::slice::from_raw_parts(d.as_usize() as *const u8, float8_transarray_size(3))
        };
        check_float8_array::<3>(img, "t").unwrap()
    };

    // Bare call: fresh image, source untouched.
    let mut img = [0u8; float8_transarray_size(3)];
    write_float8_transarray(&[0.0; 3], &mut img);
    let mut fci = LocalFcinfo::<2>::fresh(0);
    // SAFETY: ctx outlives every call through the frame.
    unsafe { fci.set_result_mcx(ctx.mcx()) };
    fci.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
    fci.set_arg(1, Datum::from_f64(2.0));
    let d = fc_float8_accum(None, &mut fci).unwrap();
    assert_ne!(d.as_usize(), img.as_ptr() as usize);
    assert_eq!(read3(d), [1.0, 2.0, 0.0]);
    assert_eq!(check_float8_array::<3>(&img, "t").unwrap(), [0.0; 3]);

    // Agg frame: in-place, avg/stddev finals match the kernel KATs.
    let mut agg = AggStateNode::new(MemoryContext::new_bump("float-aggctx"));
    let mut trans = [0u8; float8_transarray_size(3)];
    write_float8_transarray(&[0.0; 3], &mut trans);
    let tp = trans.as_ptr() as usize;
    for v in [1.0f64, 2.5, 4.25, -3.5] {
        let mut fci = LocalFcinfo::<2>::fresh(0);
        fci.context = agg.fm_node_ptr();
        fci.set_arg(0, Datum::from_usize(tp));
        fci.set_arg(1, Datum::from_f64(v));
        assert_eq!(fc_float8_accum(None, &mut fci).unwrap().as_usize(), tp);
    }
    let mut fci = LocalFcinfo::<1>::fresh(0);
    fci.set_arg(0, Datum::from_usize(tp));
    assert_eq!(out8(fc_float8_avg(None, &mut fci).unwrap().as_f64()), "1.0625");
    assert!(!fci.isnull);
    let mut fci = LocalFcinfo::<1>::fresh(0);
    fci.set_arg(0, Datum::from_usize(tp));
    assert_eq!(
        out8(fc_float8_stddev_samp(None, &mut fci).unwrap().as_f64()),
        "3.3189795118379384"
    );

    // Empty-state final: SQL NULL.
    let mut empty = [0u8; float8_transarray_size(3)];
    write_float8_transarray(&[0.0; 3], &mut empty);
    let mut fci = LocalFcinfo::<1>::fresh(0);
    fci.set_arg(0, Datum::from_usize(empty.as_ptr() as usize));
    fc_float8_avg(None, &mut fci).unwrap();
    assert!(fci.isnull);

    // combine in the agg frame folds t2 into t1 in place.
    let mut t1 = [0u8; float8_transarray_size(3)];
    write_float8_transarray(&float8_accum(float8_accum([0.0; 3], 1.0).unwrap(), 2.5).unwrap(), &mut t1);
    let mut t2 = [0u8; float8_transarray_size(3)];
    write_float8_transarray(&float8_accum(float8_accum([0.0; 3], 4.25).unwrap(), -3.5).unwrap(), &mut t2);
    let mut fci = LocalFcinfo::<2>::fresh(0);
    fci.context = agg.fm_node_ptr();
    fci.set_arg(0, Datum::from_usize(t1.as_ptr() as usize));
    fci.set_arg(1, Datum::from_usize(t2.as_ptr() as usize));
    let d = fc_float8_combine(None, &mut fci).unwrap();
    assert_eq!(d.as_usize(), t1.as_ptr() as usize);
    let c = check_float8_array::<3>(&t1, "t").unwrap();
    assert_eq!(c[0], 4.0);
    assert_eq!(out8(float8_avg(c).unwrap()), "1.0625");

    // regr transfn (state, Y, X) + finals through the frame.
    let mut r = [0u8; float8_transarray_size(6)];
    write_float8_transarray(&[0.0; 6], &mut r);
    let rp = r.as_ptr() as usize;
    for (y, x) in [(1.0, 2.0), (2.5, 4.1), (4.25, 7.9), (-3.5, -6.0)] {
        let mut fci = LocalFcinfo::<3>::fresh(0);
        fci.context = agg.fm_node_ptr();
        fci.set_arg(0, Datum::from_usize(rp));
        fci.set_arg(1, Datum::from_f64(y));
        fci.set_arg(2, Datum::from_f64(x));
        assert_eq!(fc_float8_regr_accum(None, &mut fci).unwrap().as_usize(), rp);
    }
    let final6 = |f: PGFunction| {
        let mut fci = LocalFcinfo::<1>::fresh(0);
        fci.set_arg(0, Datum::from_usize(rp));
        out8(f(None, &mut fci).unwrap().as_f64())
    };
    assert_eq!(final6(fc_float8_regr_slope), "0.5650552218562294");
    assert_eq!(final6(fc_float8_corr), "0.9986369273154668");
    assert_eq!(final6(fc_float8_covar_samp), "19.441666666666666");

    // Wrong-shape transarray: C's elog text.
    let mut fci = LocalFcinfo::<1>::fresh(0);
    fci.set_arg(0, Datum::from_usize(tp));
    let err = fc_float8_regr_sxx(None, &mut fci).unwrap_err();
    assert_eq!(err.message(), "float8_regr_sxx: expected 6-element float8 array");
}

// fnconf batch-1, OID 2467 (atanh): C calls platform libm atanh; Rust std's
// 0.5*ln_1p(2x/(1-x)) formula is one ulp off on some inputs, which the
// shortest-round-trip float8out then renders as different bytes.
// The exact bits differ per libm (macOS 0x...3ff, glibc-aarch64 0x...3fe),
// so the pin is the CALL: bit-equality vs this platform's own atanh — C on
// the same box returns the same bits by the same call.
#[test]
fn datanh_matches_platform_libm() {
    extern "C" {
        fn atanh(x: f64) -> f64;
    }
    let x = -1.3990760221756862e-5;
    // SAFETY: pure libm function, no preconditions.
    let expect = unsafe { atanh(x) };
    let r = funcs::datanh(x).unwrap();
    assert_eq!(r.to_bits(), expect.to_bits());
    // Endpoints and out-of-range behavior unchanged.
    assert_eq!(funcs::datanh(1.0).unwrap(), f64::INFINITY);
    assert_eq!(funcs::datanh(-1.0).unwrap(), f64::NEG_INFINITY);
    assert!(funcs::datanh(1.5).is_err());
    assert_eq!(funcs::datanh(0.0).unwrap(), 0.0);
}
