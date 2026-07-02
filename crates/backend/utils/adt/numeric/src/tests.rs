use crate::aggregates::*;
use crate::ops::*;
use crate::var::NumericImage;
use crate::*;

fn n(s: &str) -> NumericImage {
    io::numeric_in(s, -1, None).unwrap().unwrap()
}

fn out(img: &NumericImage) -> String {
    let mut buf = Vec::new();
    io::numeric_out_into(img.num(), &mut buf);
    String::from_utf8(buf).unwrap()
}

fn rt(s: &str) -> String {
    out(&n(s))
}

#[test]
fn in_out_round_trip() {
    assert_eq!(rt("0"), "0");
    assert_eq!(rt("0.0"), "0.0");
    assert_eq!(rt("-0.0"), "0.0");
    assert_eq!(rt("1"), "1");
    assert_eq!(rt("-1"), "-1");
    assert_eq!(rt("12345678901234567890"), "12345678901234567890");
    assert_eq!(rt("0.00001"), "0.00001");
    assert_eq!(rt("-0.00001"), "-0.00001");
    assert_eq!(rt("3.14159265358979323846"), "3.14159265358979323846");
    assert_eq!(rt("  42  "), "42");
    assert_eq!(rt("+7"), "7");
    assert_eq!(rt(".5"), "0.5");
    assert_eq!(rt("-.5"), "-0.5");
    assert_eq!(rt("00012.30400"), "12.30400");
}

#[test]
fn in_scientific_notation() {
    assert_eq!(rt("1e3"), "1000");
    assert_eq!(rt("1.23e2"), "123");
    assert_eq!(rt("1.23e-2"), "0.0123");
    assert_eq!(rt("1.23e+5"), "123000");
    assert_eq!(rt("5e-1"), "0.5");
    assert_eq!(rt("1.5e0"), "1.5");
}

#[test]
fn in_underscores_and_bases() {
    assert_eq!(rt("1_000_000"), "1000000");
    assert_eq!(rt("0x10"), "16");
    assert_eq!(rt("0XFF"), "255");
    assert_eq!(rt("0o17"), "15");
    assert_eq!(rt("0b101"), "5");
    assert_eq!(rt("-0xff"), "-255");
    assert_eq!(rt("0xffff_ffff_ffff_ffff_ffff"), "1208925819614629174706175");
}

#[test]
fn in_specials() {
    assert_eq!(rt("NaN"), "NaN");
    assert_eq!(rt("nan"), "NaN");
    assert_eq!(rt("Infinity"), "Infinity");
    assert_eq!(rt("-Infinity"), "-Infinity");
    assert_eq!(rt("inf"), "Infinity");
    assert_eq!(rt("-inf"), "-Infinity");
    assert_eq!(rt("  +inf  "), "Infinity");
}

#[test]
fn in_errors() {
    for bad in [
        "", " ", "abc", "1..2", "1e", "1e+", "0x", "_1", "1.2.3", "5 x", "- 1", "+NaN", "-NaN",
        "1._2",
    ] {
        assert!(io::numeric_in(bad, -1, None).is_err(), "accepted {bad:?}");
    }
    let e = io::numeric_in("junk", -1, None).unwrap_err();
    assert_eq!(
        e.message(),
        "invalid input syntax for type numeric: \"junk\""
    );
    let e = io::numeric_in("1e2000000000", -1, None).unwrap_err();
    assert_eq!(e.message(), "value overflows numeric format");
}

#[test]
fn short_header_packing() {
    let img = n("5");
    assert!(img.num().is_short());
    assert_eq!(img.as_bytes().len(), 4 + 2 + 2);
    assert!(n("1.5").num().is_short());
    assert_eq!(n("1.5").as_bytes().len(), 4 + 2 + 4);
    // dscale > 63 exceeds the short header's 6-bit dscale field.
    let img = n("0.0000000000000000000000000000000000000000000000000000000000000000123");
    assert!(!img.num().is_short());
    let img = n("1e260");
    assert!(!img.num().is_short(), "weight 64+ needs the long header");
}

#[test]
fn add_sub_mul_div() {
    let a = n("123.45");
    let b = n("0.55");
    assert_eq!(out(&numeric_add_common(a.num(), b.num()).unwrap()), "124.00");
    assert_eq!(out(&numeric_sub_common(a.num(), b.num()).unwrap()), "122.90");
    assert_eq!(
        out(&numeric_mul_common(a.num(), b.num()).unwrap()),
        "67.8975"
    );
    assert_eq!(
        out(&numeric_div_common(n("10").num(), n("4").num()).unwrap()),
        "2.5000000000000000"
    );
    assert_eq!(
        out(&numeric_div_common(n("1").num(), n("3").num()).unwrap()),
        "0.33333333333333333333"
    );
    let e = numeric_div_common(n("1").num(), n("0").num()).unwrap_err();
    assert_eq!(e.message(), "division by zero");
    assert_eq!(
        out(&numeric_div_trunc_common(n("10").num(), n("3").num()).unwrap()),
        "3"
    );
}

#[test]
fn cmp_family() {
    assert_eq!(cmp_numerics(n("1").num(), n("2").num()), -1);
    assert_eq!(cmp_numerics(n("2.50").num(), n("2.5").num()), 0);
    assert_eq!(cmp_numerics(n("-1").num(), n("-2").num()), 1);
    assert_eq!(cmp_numerics(n("0").num(), n("-0.0").num()), 0);
    // NaN > Inf > finite > -Inf.
    let nan = NumericImage::nan();
    let pinf = NumericImage::pinf();
    let ninf = NumericImage::ninf();
    assert_eq!(cmp_numerics(nan.num(), pinf.num()), 1);
    assert_eq!(cmp_numerics(nan.num(), nan.num()), 0);
    assert_eq!(cmp_numerics(pinf.num(), n("1e100").num()), 1);
    assert_eq!(cmp_numerics(ninf.num(), n("-1e100").num()), -1);
    assert!(numeric_eq(n("1.0").num(), n("1.000").num()));
    assert!(numeric_lt(n("1.1").num(), n("1.2").num()));
    assert!(numeric_ge(n("10000").num(), n("9999.9999").num()));
}

#[test]
fn arith_specials() {
    let nan = NumericImage::nan();
    let pinf = NumericImage::pinf();
    let ninf = NumericImage::ninf();
    let one = n("1");
    assert_eq!(out(&numeric_add_common(nan.num(), one.num()).unwrap()), "NaN");
    assert_eq!(
        out(&numeric_add_common(pinf.num(), ninf.num()).unwrap()),
        "NaN"
    );
    assert_eq!(
        out(&numeric_add_common(pinf.num(), one.num()).unwrap()),
        "Infinity"
    );
    assert_eq!(
        out(&numeric_sub_common(ninf.num(), ninf.num()).unwrap()),
        "NaN"
    );
    assert_eq!(
        out(&numeric_mul_common(pinf.num(), n("0").num()).unwrap()),
        "NaN"
    );
    assert_eq!(
        out(&numeric_mul_common(pinf.num(), n("-2").num()).unwrap()),
        "-Infinity"
    );
    assert_eq!(out(&numeric_div_common(one.num(), pinf.num()).unwrap()), "0");
    assert_eq!(
        out(&numeric_div_common(pinf.num(), n("-1").num()).unwrap()),
        "-Infinity"
    );
    assert!(numeric_div_common(pinf.num(), n("0").num()).is_err());
}

#[test]
fn typmod_coercion() {
    let t = make_numeric_typmod(5, 2);
    assert_eq!(
        out(&numeric_apply_typmod(n("123.456").num(), t).unwrap()),
        "123.46"
    );
    assert_eq!(out(&numeric_apply_typmod(n("1").num(), t).unwrap()), "1.00");
    let e = numeric_apply_typmod(n("1234").num(), t).unwrap_err();
    assert_eq!(e.message(), "numeric field overflow");
    assert_eq!(
        e.detail().unwrap(),
        "A field with precision 5, scale 2 must round to an absolute value less than 10^3."
    );
    assert!(numeric_apply_typmod(n("999.995").num(), t).is_err());
    assert_eq!(
        out(&numeric_apply_typmod(n("999.994").num(), t).unwrap()),
        "999.99"
    );
    assert_eq!(
        out(&numeric_apply_typmod(NumericImage::nan().num(), t).unwrap()),
        "NaN"
    );
    let e = numeric_apply_typmod(NumericImage::pinf().num(), t).unwrap_err();
    assert_eq!(
        e.detail().unwrap(),
        "A field with precision 5, scale 2 cannot hold an infinite value."
    );
    let t = make_numeric_typmod(2, -3);
    assert_eq!(
        out(&numeric_apply_typmod(n("12345").num(), t).unwrap()),
        "12000"
    );
    let img = io::numeric_in("123.456", make_numeric_typmod(5, 2), None)
        .unwrap()
        .unwrap();
    assert_eq!(out(&img), "123.46");
}

#[test]
fn round_trunc() {
    assert_eq!(
        out(&numeric_round_common(n("123.4567").num(), 2).unwrap()),
        "123.46"
    );
    assert_eq!(
        out(&numeric_round_common(n("123.4567").num(), 0).unwrap()),
        "123"
    );
    assert_eq!(out(&numeric_round_common(n("125").num(), -1).unwrap()), "130");
    assert_eq!(out(&numeric_round_common(n("-2.5").num(), 0).unwrap()), "-3");
    assert_eq!(
        out(&numeric_trunc_common(n("123.4567").num(), 2).unwrap()),
        "123.45"
    );
    assert_eq!(out(&numeric_trunc_common(n("-2.9").num(), 0).unwrap()), "-2");
    assert_eq!(
        out(&numeric_round_common(NumericImage::nan().num(), 2).unwrap()),
        "NaN"
    );
}

#[test]
fn sign_ops() {
    assert_eq!(out(&numeric_abs(n("-1.5").num())), "1.5");
    assert_eq!(out(&numeric_abs(n("1.5").num())), "1.5");
    assert_eq!(out(&numeric_abs(NumericImage::ninf().num())), "Infinity");
    assert_eq!(out(&numeric_uminus(n("1.5").num())), "-1.5");
    assert_eq!(out(&numeric_uminus(n("-1.5").num())), "1.5");
    assert_eq!(out(&numeric_uminus(n("0").num())), "0");
    assert_eq!(out(&numeric_uminus(NumericImage::pinf().num())), "-Infinity");
    assert_eq!(out(&numeric_uplus(n("-7").num())), "-7");
    let long = n("1e260");
    assert!(!long.num().is_short());
    assert_eq!(out(&numeric_uminus(long.num())), format!("-{}", out(&long)));
}

#[test]
fn int_conversions() {
    assert_eq!(out(&int4_numeric(42)), "42");
    assert_eq!(out(&int4_numeric(-2147483648)), "-2147483648");
    assert_eq!(out(&int8_numeric(i64::MIN)), "-9223372036854775808");
    assert_eq!(out(&int2_numeric(-32768)), "-32768");
    assert_eq!(numeric_int4(n("42.4").num()).unwrap(), 42);
    assert_eq!(numeric_int4(n("42.5").num()).unwrap(), 43);
    assert_eq!(numeric_int4(n("-42.5").num()).unwrap(), -43);
    assert_eq!(numeric_int4(n("2147483647.49").num()).unwrap(), i32::MAX);
    assert!(numeric_int4(n("2147483647.5").num()).is_err());
    assert_eq!(
        numeric_int8(n("-9223372036854775808").num()).unwrap(),
        i64::MIN
    );
    assert!(numeric_int8(n("9223372036854775808").num()).is_err());
    assert_eq!(numeric_int2(n("32767").num()).unwrap(), 32767);
    assert!(numeric_int2(n("32768").num()).is_err());
    let e = numeric_int4(NumericImage::nan().num()).unwrap_err();
    assert_eq!(e.message(), "cannot convert NaN to integer");
    let e = numeric_int8(NumericImage::pinf().num()).unwrap_err();
    assert_eq!(e.message(), "cannot convert infinity to bigint");
    assert_eq!(numeric_int4(n("0").num()).unwrap(), 0);
}

#[test]
fn float_conversions() {
    assert_eq!(out(&float8_numeric(1.5).unwrap()), "1.5");
    assert_eq!(out(&float8_numeric(0.0).unwrap()), "0");
    assert_eq!(out(&float8_numeric(-0.1).unwrap()), "-0.1");
    assert_eq!(
        out(&float8_numeric(1e100).unwrap()),
        format!("1{}", "0".repeat(100))
    );
    assert_eq!(out(&float8_numeric(f64::NAN).unwrap()), "NaN");
    assert_eq!(out(&float8_numeric(f64::INFINITY).unwrap()), "Infinity");
    assert_eq!(out(&float4_numeric(1.5).unwrap()), "1.5");
    assert_eq!(out(&float4_numeric(0.1).unwrap()), "0.1");
    assert_eq!(numeric_float8(n("1.5").num()).unwrap(), 1.5);
    assert_eq!(numeric_float8(n("1e300").num()).unwrap(), 1e300);
    assert!(numeric_float8(NumericImage::nan().num()).unwrap().is_nan());
    assert_eq!(
        numeric_float8(NumericImage::ninf().num()).unwrap(),
        f64::NEG_INFINITY
    );
    assert!(numeric_float8(n("1e400").num()).is_err());
    assert_eq!(numeric_float4(n("1.5").num()).unwrap(), 1.5f32);
    assert!(numeric_float4(n("1e50").num()).is_err());
}

#[test]
fn sum_accum_positive_negative_split() {
    let mut state = NumericAggState::new(false);
    for v in ["1.5", "-2.5", "1000000", "-999999", "0.001"] {
        do_numeric_accum(&mut state, n(v).num());
    }
    let sum = numeric_sum(Some(&mut state)).unwrap().unwrap();
    assert_eq!(out(&sum), "0.001");
    let avg = numeric_avg(Some(&mut state)).unwrap().unwrap();
    assert_eq!(out(&avg), "0.00020000000000000000");

    // Enough inputs to force lazy carry propagation (cap is NBASE-1).
    let mut state = NumericAggState::new(false);
    let v = n("9999.9999");
    for _ in 0..20000 {
        do_numeric_accum(&mut state, v.num());
    }
    let sum = numeric_sum(Some(&mut state)).unwrap().unwrap();
    assert_eq!(out(&sum), "199999998.0000");

    assert!(numeric_sum(None).unwrap().is_none());
    let mut empty = NumericAggState::new(false);
    assert!(numeric_sum(Some(&mut empty)).unwrap().is_none());
    assert!(numeric_avg(Some(&mut empty)).unwrap().is_none());
}

#[test]
fn sum_specials() {
    let mut state = NumericAggState::new(false);
    do_numeric_accum(&mut state, n("1").num());
    do_numeric_accum(&mut state, NumericImage::pinf().num());
    assert_eq!(
        out(&numeric_sum(Some(&mut state)).unwrap().unwrap()),
        "Infinity"
    );
    do_numeric_accum(&mut state, NumericImage::ninf().num());
    assert_eq!(out(&numeric_sum(Some(&mut state)).unwrap().unwrap()), "NaN");
    let mut state = NumericAggState::new(false);
    do_numeric_accum(&mut state, NumericImage::nan().num());
    assert_eq!(out(&numeric_sum(Some(&mut state)).unwrap().unwrap()), "NaN");
}

#[test]
fn discard_inverse_transition() {
    let mut state = NumericAggState::new(false);
    do_numeric_accum(&mut state, n("1.01").num());
    do_numeric_accum(&mut state, n("2").num());
    // Removing the only max-dscale input must fail (dscale unknowable).
    assert!(!do_numeric_discard(&mut state, n("1.01").num()));
    // Removing the dscale-0 input is fine.
    assert!(do_numeric_discard(&mut state, n("2").num()));
    assert_eq!(out(&numeric_sum(Some(&mut state)).unwrap().unwrap()), "1.01");
}

#[test]
fn int128_poly_aggregates() {
    let mut state = Int128AggState::new(false);
    do_int128_accum(&mut state, 5);
    do_int128_accum(&mut state, -3);
    do_int128_accum(&mut state, i64::MAX as i128);
    do_int128_accum(&mut state, i64::MAX as i128);
    let sum = numeric_poly_sum(Some(&state)).unwrap().unwrap();
    assert_eq!(out(&sum), "18446744073709551616");
    do_int128_discard(&mut state, 5);
    do_int128_discard(&mut state, -3);
    let sum = numeric_poly_sum(Some(&state)).unwrap().unwrap();
    assert_eq!(out(&sum), "18446744073709551614");
    // Live PG 18.3: avg of two int8 maxes prints with rscale 0.
    let avg = numeric_poly_avg(Some(&state)).unwrap().unwrap();
    assert_eq!(out(&avg), "9223372036854775807");
    assert!(numeric_poly_sum(None).unwrap().is_none());

    let mut x2 = Int128AggState::new(true);
    do_int128_accum(&mut x2, 4);
    assert_eq!(x2.sum_x2, 16);
}

#[test]
fn int64_div_fast() {
    assert_eq!(out(&int64_div_fast_to_numeric(123456, 2).unwrap()), "1234.56");
    assert_eq!(out(&int64_div_fast_to_numeric(123456, 0).unwrap()), "123456");
    assert_eq!(out(&int64_div_fast_to_numeric(1, 6).unwrap()), "0.000001");
    assert_eq!(
        out(&int64_div_fast_to_numeric(i64::MAX, 3).unwrap()),
        "9223372036854775.807"
    );
}

#[test]
fn maximum_size() {
    assert_eq!(numeric_maximum_size(-1), -1);
    assert_eq!(numeric_maximum_size(make_numeric_typmod(10, 2)), 8 + 4 * 2);
}
