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
fn binary_wire_round_trip() {
    use ::mcx::MemoryContext;
    use ::stringinfo::StringInfo;

    let ctx = MemoryContext::new("numeric-wire-test");
    let mcx = ctx.mcx();
    for s in ["0", "1", "-1", "12345678901234567890", "-0.00001",
              "3.14159265358979323846", "NaN", "Infinity", "-Infinity", "12.30400"] {
        let img = n(s);
        let bytea = io::numeric_send(mcx, img.num()).unwrap();
        let mut buf = StringInfo::new_in(mcx).unwrap();
        buf.append_bytes(bytea.data()).unwrap();
        let back = io::numeric_recv(&mut buf, -1).unwrap();
        assert_eq!(buf.cursor, buf.len(), "recv must consume the whole buffer for {s}");
        assert_eq!(img.as_bytes(), back.as_bytes(), "wire round-trip mismatch for {s}");
    }
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
    let ctx = ::mcx::MemoryContext::new_bump("agg-test");
    let mut state = NumericAggState::new(false);
    for v in ["1.5", "-2.5", "1000000", "-999999", "0.001"] {
        do_numeric_accum(&mut state, ctx.mcx(), n(v).num()).unwrap();
    }
    let sum = numeric_sum(Some(&mut state)).unwrap().unwrap();
    assert_eq!(out(&sum), "0.001");
    let avg = numeric_avg(Some(&mut state)).unwrap().unwrap();
    assert_eq!(out(&avg), "0.00020000000000000000");

    // Enough inputs to force lazy carry propagation (cap is NBASE-1).
    let mut state = NumericAggState::new(false);
    let v = n("9999.9999");
    for _ in 0..20000 {
        do_numeric_accum(&mut state, ctx.mcx(), v.num()).unwrap();
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
    let ctx = ::mcx::MemoryContext::new_bump("agg-test");
    let mut state = NumericAggState::new(false);
    do_numeric_accum(&mut state, ctx.mcx(), n("1").num()).unwrap();
    do_numeric_accum(&mut state, ctx.mcx(), NumericImage::pinf().num()).unwrap();
    assert_eq!(
        out(&numeric_sum(Some(&mut state)).unwrap().unwrap()),
        "Infinity"
    );
    do_numeric_accum(&mut state, ctx.mcx(), NumericImage::ninf().num()).unwrap();
    assert_eq!(out(&numeric_sum(Some(&mut state)).unwrap().unwrap()), "NaN");
    let mut state = NumericAggState::new(false);
    do_numeric_accum(&mut state, ctx.mcx(), NumericImage::nan().num()).unwrap();
    assert_eq!(out(&numeric_sum(Some(&mut state)).unwrap().unwrap()), "NaN");
}

#[test]
fn discard_inverse_transition() {
    let ctx = ::mcx::MemoryContext::new_bump("agg-test");
    let mut state = NumericAggState::new(false);
    do_numeric_accum(&mut state, ctx.mcx(), n("1.01").num()).unwrap();
    do_numeric_accum(&mut state, ctx.mcx(), n("2").num()).unwrap();
    // Removing the only max-dscale input must fail (dscale unknowable).
    assert!(!do_numeric_discard(&mut state, ctx.mcx(), n("1.01").num()).unwrap());
    // Removing the dscale-0 input is fine.
    assert!(do_numeric_discard(&mut state, ctx.mcx(), n("2").num()).unwrap());
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
fn int_avg_div_matches_numeric_avg_div_bytes() {
    // The finalize fast entries must be byte-identical to the materializing
    // composition they replace, across the dscale ladder (select_div_scale's
    // qweight steps), signs, exact/inexact quotients, and the i64/i128
    // boundaries — the fast path shares the division tail, so this pins the
    // operand-decomposition equivalence.
    let i128_slow = |s: i128, c: i64| -> NumericImage {
        let mut v = crate::var::NumericVar::new();
        crate::var::int128_to_var(s, &mut v);
        numeric_avg_div(crate::var::make_result(v.view()).unwrap().num(), c).unwrap()
    };
    let sums: &[i64] = &[
        0,
        1,
        -1,
        7,
        -7,
        59,
        100,
        -100,
        9999,
        10000,
        123456789,
        -123456789,
        1366120260,
        i64::MAX,
        i64::MIN,
        i64::MIN + 1,
    ];
    let counts: &[i64] = &[1, 2, 3, 7, 10, 59, 1000, 9999, 1_366_120, i64::MAX];
    for &s in sums {
        for &c in counts {
            let slow = numeric_avg_div(int64_to_numeric(s).num(), c).unwrap();
            let fast = int64_avg_div(s, c).unwrap();
            assert_eq!(fast.as_bytes(), slow.as_bytes(), "sum={s} count={c}");
            let fast128 = int128_avg_div(s as i128, c).unwrap();
            assert_eq!(
                fast128.as_bytes(),
                i128_slow(s as i128, c).as_bytes(),
                "i128 sum={s} count={c}"
            );
        }
    }
    // Beyond-i64 int128 sums (avg(int8) accumulations).
    for &s in &[i64::MAX as i128 * 37, i64::MIN as i128 * 1000 - 1, 10i128.pow(30)] {
        for &c in &[1i64, 3, 1_000_000_007] {
            let fast = int128_avg_div(s, c).unwrap();
            assert_eq!(fast.as_bytes(), i128_slow(s, c).as_bytes(), "i128 sum={s} count={c}");
        }
    }
    // Rounding-tie and near-tie corners (the integer quotient's half-away
    // carry vs round_var's), incl. exact halves, thirds, and carry-9999
    // propagation shapes.
    for &(s, c) in &[
        (1i64, 2i64),
        (-1, 2),
        (1, 3),
        (2, 3),
        (-2, 3),
        (1, 6),
        (5, 4),
        (-5, 4),
        (1, 7),
        (999999, 7),
        (49999, 10000),
        (50001, 10000),
        (9_999_999_999_999_999, 2),
        (-9_999_999_999_999_999, 2),
        (1, 20_000_000_000_000_000),
        (3, 20_000_000_000_000_000),
        (1, 3_000_000_000),
        (7, 9_999_999_999),
    ] {
        let slow = numeric_avg_div(int64_to_numeric(s).num(), c).unwrap();
        let fast = int64_avg_div(s, c).unwrap();
        assert_eq!(fast.as_bytes(), slow.as_bytes(), "tie sum={s} count={c}");
    }
    // Deterministic LCG sweep, biased toward realistic small counts half the
    // time (avg-of-groups shapes) and full-range the other half.
    let mut x: u64 = 0x9e3779b97f4a7c15;
    for i in 0..20_000u32 {
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let s = if i % 4 < 2 { (x as i64) % 2_000_000 } else { x as i64 };
        x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let c = if i % 2 == 0 {
            ((x % 1000) as i64).max(1)
        } else {
            (((x | 1) >> 1) as i64).max(1)
        };
        let slow = numeric_avg_div(int64_to_numeric(s).num(), c).unwrap();
        let fast = int64_avg_div(s, c).unwrap();
        assert_eq!(fast.as_bytes(), slow.as_bytes(), "sum={s} count={c}");
        // The i128 leg through the same pairs plus a widened sum.
        let s128 = (s as i128) * ((x as i8) as i128).max(1);
        let fast128 = int128_avg_div(s128, c).unwrap();
        assert_eq!(fast128.as_bytes(), i128_slow(s128, c).as_bytes(), "i128 sum={s128} count={c}");
    }
}

#[test]
fn sqrt_family() {
    assert_eq!(out(&numeric_sqrt(n("2").num()).unwrap()), "1.414213562373095");
    assert_eq!(out(&numeric_sqrt(n("0").num()).unwrap()), "0.000000000000000");
    assert_eq!(
        out(&numeric_sqrt(n("1e100").num()).unwrap()),
        format!("1{}", "0".repeat(50))
    );
    assert_eq!(
        out(&numeric_sqrt(n("0.5").num()).unwrap()),
        "0.70710678118654752"
    );
    // rounds at every requested digit: 30-digit perfect square
    let sq = numeric_mul_common(n("123456789.987654321").num(), n("123456789.987654321").num())
        .unwrap();
    assert_eq!(
        out(&numeric_sqrt(sq.num()).unwrap()),
        "123456789.987654321000000000"
    );
    let e = numeric_sqrt(n("-1").num()).unwrap_err();
    assert_eq!(e.message(), "cannot take square root of a negative number");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION);
    assert!(numeric_sqrt(NumericImage::ninf().num()).is_err());
    assert_eq!(out(&numeric_sqrt(NumericImage::pinf().num()).unwrap()), "Infinity");
    assert_eq!(out(&numeric_sqrt(NumericImage::nan().num()).unwrap()), "NaN");
}

#[test]
fn exp_ln_log() {
    assert_eq!(out(&numeric_exp(n("1").num()).unwrap()), "2.7182818284590452");
    assert_eq!(out(&numeric_exp(n("0").num()).unwrap()), "1.0000000000000000");
    assert_eq!(out(&numeric_exp(n("-1").num()).unwrap()), "0.3678794411714423");
    assert_eq!(out(&numeric_exp(n("10.5").num()).unwrap()), "36315.502674246638");
    // exp overflow threshold and the underflow-to-zero arm
    let e = numeric_exp(n("6000").num()).unwrap_err();
    assert_eq!(e.message(), "value overflows numeric format");
    let z = numeric_exp(n("-6000").num()).unwrap();
    assert_eq!(out(&z), format!("0.{}", "0".repeat(1000)));
    assert_eq!(out(&numeric_exp(NumericImage::ninf().num()).unwrap()), "0");
    assert_eq!(out(&numeric_exp(NumericImage::pinf().num()).unwrap()), "Infinity");

    assert_eq!(out(&numeric_ln(n("2").num()).unwrap()), "0.6931471805599453");
    assert_eq!(out(&numeric_ln(n("0.5").num()).unwrap()), "-0.6931471805599453");
    // ln(1) picks rscale off a zero estimate
    assert_eq!(out(&numeric_ln(n("1").num()).unwrap()), "0.0000000000000000");
    assert_eq!(out(&numeric_ln(n("1e100").num()).unwrap()), "230.25850929940457");
    let e = numeric_ln(n("0").num()).unwrap_err();
    assert_eq!(e.message(), "cannot take logarithm of zero");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_ARGUMENT_FOR_LOG);
    assert!(numeric_ln(n("-2").num()).is_err());
    assert!(numeric_ln(NumericImage::ninf().num()).is_err());

    assert_eq!(
        out(&numeric_log(n("2").num(), n("64").num()).unwrap()),
        "6.0000000000000000"
    );
    assert_eq!(
        out(&numeric_log(n("10").num(), n("100").num()).unwrap()),
        "2.0000000000000000"
    );
    assert!(numeric_log(n("0").num(), n("64").num()).is_err());
    assert!(numeric_log(n("-2").num(), n("64").num()).is_err());
    assert_eq!(
        out(&numeric_log(NumericImage::pinf().num(), n("2").num()).unwrap()),
        "0"
    );
    assert_eq!(
        out(&numeric_log(n("2").num(), NumericImage::pinf().num()).unwrap()),
        "Infinity"
    );
}

#[test]
fn power_family() {
    assert_eq!(
        out(&numeric_power(n("2").num(), n("10").num()).unwrap()),
        "1024.0000000000000"
    );
    assert_eq!(
        out(&numeric_power(n("2").num(), n("0.5").num()).unwrap()),
        "1.4142135623730950"
    );
    assert_eq!(
        out(&numeric_power(n("2.5").num(), n("-3.7").num()).unwrap()),
        "0.03369938443095648"
    );
    assert_eq!(
        out(&numeric_power(n("0").num(), n("0").num()).unwrap()),
        "1.0000000000000000"
    );
    assert_eq!(
        out(&numeric_power(n("-2").num(), n("3").num()).unwrap()),
        "-8.0000000000000000"
    );
    assert_eq!(
        out(&numeric_power(n("1.000000001").num(), n("10000000").num()).unwrap()),
        "1.0100501670791178"
    );
    let e = numeric_power(n("0").num(), n("-1").num()).unwrap_err();
    assert_eq!(e.message(), "zero raised to a negative power is undefined");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION);
    let e = numeric_power(n("-2").num(), n("0.5").num()).unwrap_err();
    assert_eq!(
        e.message(),
        "a negative number raised to a non-integer power yields a complex result"
    );
    // NaN^0 = 1 and 1^NaN = 1 per POSIX
    assert_eq!(
        out(&numeric_power(NumericImage::nan().num(), n("0").num()).unwrap()),
        "1"
    );
    assert_eq!(
        out(&numeric_power(n("1").num(), NumericImage::nan().num()).unwrap()),
        "1"
    );
    assert_eq!(
        out(&numeric_power(n("0.5").num(), NumericImage::ninf().num()).unwrap()),
        "Infinity"
    );
    assert_eq!(
        out(&numeric_power(NumericImage::ninf().num(), n("3").num()).unwrap()),
        "-Infinity"
    );
    assert_eq!(
        out(&numeric_power(NumericImage::ninf().num(), n("2").num()).unwrap()),
        "Infinity"
    );
}

#[test]
fn mod_gcd_lcm_fac() {
    assert_eq!(out(&numeric_mod_common(n("11").num(), n("4").num()).unwrap()), "3");
    assert_eq!(out(&numeric_mod_common(n("-11").num(), n("4").num()).unwrap()), "-3");
    assert_eq!(
        out(&numeric_mod_common(n("11.5").num(), n("4.2").num()).unwrap()),
        "3.1"
    );
    assert!(numeric_mod_common(n("1").num(), n("0").num()).is_err());
    assert!(numeric_mod_common(NumericImage::pinf().num(), n("0").num()).is_err());
    assert_eq!(
        out(&numeric_mod_common(NumericImage::pinf().num(), n("2").num()).unwrap()),
        "NaN"
    );
    assert_eq!(
        out(&numeric_mod_common(n("7").num(), NumericImage::ninf().num()).unwrap()),
        "7"
    );

    assert_eq!(out(&numeric_gcd_common(n("48").num(), n("18").num()).unwrap()), "6");
    assert_eq!(
        out(&numeric_gcd_common(n("4.8").num(), n("1.8").num()).unwrap()),
        "0.6"
    );
    assert_eq!(
        out(&numeric_gcd_common(NumericImage::pinf().num(), n("1").num()).unwrap()),
        "NaN"
    );
    assert_eq!(out(&numeric_lcm_common(n("4").num(), n("6").num()).unwrap()), "12");
    assert_eq!(out(&numeric_lcm_common(n("0").num(), n("6").num()).unwrap()), "0");

    assert_eq!(out(&numeric_fac(5).unwrap()), "120");
    assert_eq!(out(&numeric_fac(0).unwrap()), "1");
    assert_eq!(out(&numeric_fac(20).unwrap()), "2432902008176640000");
    let e = numeric_fac(-1).unwrap_err();
    assert_eq!(e.message(), "factorial of a negative number is undefined");
    assert!(numeric_fac(32178).is_err());
}

#[test]
fn out_sci_and_width_bucket() {
    let sci = |s: &str, scale: i32| {
        let mut buf = Vec::new();
        numeric_out_sci(n(s).num(), scale, &mut buf);
        String::from_utf8(buf).unwrap()
    };
    assert_eq!(sci("1234.5678", 3), "1.235e+03");
    assert_eq!(sci("0", 3), "0.000e+00");
    assert_eq!(sci("0.00001234", 2), "1.23e-05");
    assert_eq!(sci("-1234.5678", 3), "-1.235e+03");
    assert_eq!(sci("1e100", 1), "1.0e+100");
    let mut buf = Vec::new();
    numeric_out_sci(NumericImage::ninf().num(), 3, &mut buf);
    assert_eq!(buf, b"-Infinity");

    let wb = |op: &str, b1: &str, b2: &str, c: i32| {
        width_bucket_numeric(n(op).num(), n(b1).num(), n(b2).num(), c)
    };
    assert_eq!(wb("5.0", "0.0", "10.0", 5).unwrap(), 3);
    assert_eq!(wb("-1", "0.0", "10.0", 5).unwrap(), 0);
    assert_eq!(wb("11", "0.0", "10.0", 5).unwrap(), 6);
    assert_eq!(wb("9.99", "10.0", "0.0", 5).unwrap(), 1);
    let e = wb("1", "0", "10", 0).unwrap_err();
    assert_eq!(e.message(), "count must be greater than zero");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION);
    let e = wb("1", "5", "5", 3).unwrap_err();
    assert_eq!(e.message(), "lower bound cannot equal upper bound");
    assert!(width_bucket_numeric(
        NumericImage::nan().num(),
        n("0").num(),
        n("1").num(),
        2
    )
    .is_err());
    assert!(width_bucket_numeric(
        n("1").num(),
        NumericImage::pinf().num(),
        n("1").num(),
        2
    )
    .is_err());
    assert_eq!(
        width_bucket_numeric(NumericImage::pinf().num(), n("0").num(), n("1").num(), 2).unwrap(),
        3
    );
}

#[test]
fn maximum_size() {
    assert_eq!(numeric_maximum_size(-1), -1);
    assert_eq!(numeric_maximum_size(make_numeric_typmod(10, 2)), 8 + 4 * 2);
}

mod fc_results {
    use datum::Datum;
    use mcx::MemoryContext;
    use types_fmgr::{
        direct_function_call1_coll_in, direct_function_call2_coll_in,
        direct_function_call3_coll_in, FmgrInfo, LocalFcinfo,
    };

    use crate::builtins::*;
    use crate::{int64_to_numeric, Num};

    fn num_datum(img: &crate::NumericImage) -> Datum {
        Datum::from_usize(img.as_bytes().as_ptr() as usize)
    }

    fn result_num(d: Datum) -> Num<'static> {
        let p = d.as_usize() as *const u8;
        assert_eq!(p as usize % 8, 0);
        // SAFETY: results are live 4B-header numeric varlenas kept in the ctx.
        let r = unsafe { datum::VarlenaRef::from_ptr(p) };
        Num::from_payload(r.data())
    }

    #[test]
    fn arith_through_fc_frames() {
        let ctx = MemoryContext::new_bump("t");
        let a = int64_to_numeric(6);
        let b = int64_to_numeric(7);
        let d = direct_function_call2_coll_in(fc_numeric_mul, 0, ctx.mcx(), num_datum(&a), num_datum(&b))
            .unwrap();
        assert_eq!(crate::cmp_numerics(result_num(d), int64_to_numeric(42).num()), 0);
        let d = direct_function_call2_coll_in(fc_numeric_add, 0, ctx.mcx(), num_datum(&a), num_datum(&b))
            .unwrap();
        assert_eq!(crate::cmp_numerics(result_num(d), int64_to_numeric(13).num()), 0);
    }

    #[test]
    fn cmp_and_pointer_returning_rows() {
        let a = int64_to_numeric(1);
        let b = int64_to_numeric(2);
        let d = types_fmgr::direct_function_call2_coll(fc_numeric_cmp, 0, num_datum(&a), num_datum(&b))
            .unwrap();
        assert_eq!(d.as_i32(), -1);
        let d = types_fmgr::direct_function_call2_coll(fc_numeric_larger, 0, num_datum(&a), num_datum(&b))
            .unwrap();
        assert_eq!(d.as_usize(), num_datum(&b).as_usize());
    }

    // C keeps num2 on ties; the winner's identity is value-visible because
    // equal numerics can differ in dscale (proofs divergence #9).
    #[test]
    fn smaller_larger_tie_keeps_num2() {
        let ctx = MemoryContext::new_bump("t");
        let mk = |s: &'static [u8]| {
            direct_function_call3_coll_in(
                fc_numeric_in,
                0,
                ctx.mcx(),
                Datum::from_usize(s.as_ptr() as usize),
                Datum::from_oid(0),
                Datum::from_i32(-1),
            )
            .unwrap()
        };
        let one_0 = mk(b"1.0\0");
        let one_00 = mk(b"1.00\0");
        for fc in [fc_numeric_smaller, fc_numeric_larger] {
            let d = types_fmgr::direct_function_call2_coll(fc, 0, one_0, one_00).unwrap();
            assert_eq!(d.as_usize(), one_00.as_usize());
        }
    }

    #[test]
    fn in_out_round_trip() {
        let ctx = MemoryContext::new_bump("t");
        let d = direct_function_call3_coll_in(
            fc_numeric_in,
            0,
            ctx.mcx(),
            Datum::from_usize(b"12.75\0".as_ptr() as usize),
            Datum::from_oid(0),
            Datum::from_i32(-1),
        )
        .unwrap();
        // numeric_out needs a resolved FmgrInfo (retained cstring scratch).
        let mut flinfo = FmgrInfo::new(fc_numeric_out, 1702, 1, true, false);
        let mut fci = LocalFcinfo::<1>::fresh(0);
        fci.set_arg(0, d);
        let out = flinfo.invoke(&mut fci).unwrap();
        // SAFETY: numeric_out result is a live NUL-terminated cstring scratch.
        let s = unsafe { core::ffi::CStr::from_ptr((out.as_usize() as *const u8).cast()) };
        assert_eq!(s.to_bytes(), b"12.75");
    }

    #[test]
    fn short_header_arg_expands_aligned() {
        let ctx = MemoryContext::new_bump("t");
        let img = crate::int64_to_numeric(42);
        let payload = img.payload();
        let total = 1 + payload.len();
        // 2-aligned base puts the 1B-packed image's digits at an odd address.
        let mut buf = [0u16; 16];
        let p = buf.as_mut_ptr().cast::<u8>();
        // SAFETY: total <= 32; buf is live for the whole test.
        unsafe {
            *p = ((total as u8) << 1) | 1;
            core::ptr::copy_nonoverlapping(payload.as_ptr(), p.add(1), payload.len());
        }
        let d = Datum::from_usize(p as usize);

        let mut flinfo = FmgrInfo::new(fc_numeric_out, 1702, 1, true, false);
        let mut fci = LocalFcinfo::<1>::fresh(0);
        // SAFETY: ctx outlives every call through the frame.
        unsafe { fci.set_result_mcx(ctx.mcx()) };
        fci.set_arg(0, d);
        let out = flinfo.invoke(&mut fci).unwrap();
        // SAFETY: numeric_out result is a live NUL-terminated cstring scratch.
        let s = unsafe { core::ffi::CStr::from_ptr((out.as_usize() as *const u8).cast()) };
        assert_eq!(s.to_bytes(), b"42");

        let d2 = direct_function_call2_coll_in(fc_numeric_add, 0, ctx.mcx(), d, d).unwrap();
        assert_eq!(
            crate::cmp_numerics(result_num(d2), crate::int64_to_numeric(84).num()),
            0
        );
    }

    #[test]
    fn int_and_typmod_rows() {
        let ctx = MemoryContext::new_bump("t");
        let d = direct_function_call1_coll_in(fc_int8_numeric, 0, ctx.mcx(), Datum::from_i64(-9))
            .unwrap();
        assert_eq!(crate::cmp_numerics(result_num(d), int64_to_numeric(-9).num()), 0);
    }
}

// Live PG 18.3: stddev/variance over 1..5 (samp and pop, numeric + int128
// poly lanes agree).
#[test]
fn stddev_variance_finals() {
    let ctx = ::mcx::MemoryContext::new_bump("agg-test");
    let mut state = NumericAggState::new(true);
    for v in ["1", "2", "3", "4", "5"] {
        do_numeric_accum(&mut state, ctx.mcx(), n(v).num()).unwrap();
    }
    let f = |s: &mut NumericAggState, variance, sample| {
        out(&numeric_stddev_internal(Some(s), variance, sample).unwrap().unwrap())
    };
    assert_eq!(f(&mut state, false, true), "1.5811388300841897");
    assert_eq!(f(&mut state, true, true), "2.5000000000000000");
    assert_eq!(f(&mut state, false, false), "1.4142135623730950");
    assert_eq!(f(&mut state, true, false), "2.0000000000000000");

    let mut one = NumericAggState::new(true);
    do_numeric_accum(&mut one, ctx.mcx(), n("7").num()).unwrap();
    assert!(numeric_stddev_internal(Some(&mut one), false, true).unwrap().is_none());
    assert_eq!(
        out(&numeric_stddev_internal(Some(&mut one), true, false).unwrap().unwrap()),
        "0"
    );
    do_numeric_accum(&mut one, ctx.mcx(), NumericImage::nan().num()).unwrap();
    assert_eq!(
        out(&numeric_stddev_internal(Some(&mut one), false, true).unwrap().unwrap()),
        "NaN"
    );
    assert!(numeric_stddev_internal(None, false, true).unwrap().is_none());

    let mut poly = Int128AggState::new(true);
    for v in 1..=5i128 {
        do_int128_accum(&mut poly, v);
    }
    assert_eq!(
        out(&numeric_poly_stddev_internal(Some(&poly), false, true).unwrap().unwrap()),
        "1.5811388300841897"
    );
    assert_eq!(
        out(&numeric_poly_stddev_internal(Some(&poly), true, true).unwrap().unwrap()),
        "2.5000000000000000"
    );
}

// Alias-row cores diffed vs live C 18.3 (psql, 2026-07-03).
#[test]
fn scale_min_trim_int2() {
    assert_eq!(numeric_int2(n("1.5").num()).unwrap(), 2);
    assert_eq!(numeric_int2(n("-32768.4").num()).unwrap(), -32768);
    assert!(numeric_int2(n("32768").num()).is_err());
    assert_eq!(n("1.230").num().dscale(), 3);
    assert_eq!(numeric_min_scale(n("1.2300").num()), 2);
    assert_eq!(numeric_min_scale(n("1.5").num()), 1);
    assert_eq!(numeric_min_scale(n("100").num()), 0);
    assert_eq!(numeric_min_scale(n("0.000").num()), 0);
    assert_eq!(out(&numeric_trim_scale(n("1.2300").num()).unwrap()), "1.23");
    assert_eq!(out(&numeric_trim_scale(n("100.00").num()).unwrap()), "100");
    assert_eq!(out(&numeric_trim_scale(n("nan").num()).unwrap()), "NaN");
    assert_eq!(out(&numeric_div_trunc_common(n("10.5").num(), n("0.3").num()).unwrap()), "35");
    assert_eq!(out(&numeric_div_trunc_common(n("-10").num(), n("3").num()).unwrap()), "-3");
    assert_eq!(out(&numeric_round_common(n("nan").num(), 1).unwrap()), "NaN");
    assert_eq!(out(&numeric_trunc_common(n("-inf").num(), 1).unwrap()), "-Infinity");
}

#[test]
fn hash_numeric_scale_invariance_and_specials() {
    use crate::{hash_numeric, hash_numeric_extended};
    assert_eq!(hash_numeric(n("nan").num()), 0);
    assert_eq!(hash_numeric(n("inf").num()), 0);
    assert_eq!(hash_numeric(n("0").num()), u32::MAX);
    assert_eq!(hash_numeric(n("0.000").num()), u32::MAX);
    assert_eq!(hash_numeric(n("1").num()), hash_numeric(n("1.000").num()));
    assert_eq!(
        hash_numeric(n("12345.678").num()),
        hash_numeric(n("12345.67800000").num())
    );
    assert_ne!(hash_numeric(n("1").num()), hash_numeric(n("10").num()));
    assert_eq!(hash_numeric_extended(n("nan").num(), 42), 42);
    assert_eq!(hash_numeric_extended(n("0").num(), 42), 41);
    assert_eq!(
        hash_numeric_extended(n("7.5").num(), 11),
        hash_numeric_extended(n("7.50000").num(), 11)
    );
}

#[test]
fn in_range_numeric_cases() {
    use crate::in_range_numeric_numeric as ir;
    let v = |s: &str| n(s);
    assert!(ir(v("5").num(), v("10").num(), v("5").num(), true, true).unwrap());
    assert!(!ir(v("4.9").num(), v("10").num(), v("5").num(), true, false).unwrap());
    assert!(ir(v("15").num(), v("10").num(), v("5").num(), false, true).unwrap());
    let e = ir(v("1").num(), v("1").num(), v("-1").num(), true, true).unwrap_err();
    assert_eq!(e.message(), "invalid preceding or following size in window function");
    let e = ir(v("1").num(), v("1").num(), v("nan").num(), true, true).unwrap_err();
    assert_eq!(e.message(), "invalid preceding or following size in window function");
    assert!(ir(v("nan").num(), v("nan").num(), v("1").num(), true, true).unwrap());
    assert!(!ir(v("nan").num(), v("1").num(), v("1").num(), true, true).unwrap());
    assert!(ir(v("1").num(), v("nan").num(), v("1").num(), true, true).unwrap());
    assert!(ir(v("1").num(), v("1").num(), v("inf").num(), false, true).unwrap());
    assert!(!ir(v("1").num(), v("1").num(), v("inf").num(), true, true).unwrap());
    assert!(ir(v("-inf").num(), v("1").num(), v("inf").num(), true, true).unwrap());
    assert!(ir(v("5").num(), v("inf").num(), v("inf").num(), true, true).unwrap());
    assert!(ir(v("inf").num(), v("1").num(), v("2").num(), false, false).unwrap());
    assert!(!ir(v("inf").num(), v("1").num(), v("2").num(), false, true).unwrap());
    assert!(ir(v("-inf").num(), v("-inf").num(), v("0").num(), true, true).unwrap());
    assert!(ir(v("1").num(), v("inf").num(), v("0").num(), true, true).unwrap());
    assert!(ir(v("1").num(), v("-inf").num(), v("0").num(), true, false).unwrap());
}

#[test]
fn generate_series_numeric_walk() {
    use crate::series::GenerateSeriesNumeric;
    let mut g =
        GenerateSeriesNumeric::new(n("1").num(), n("2.2").num(), Some(n("0.4").num())).unwrap();
    let mut got = Vec::new();
    while let Some(img) = g.next().unwrap() {
        got.push(out(&img));
    }
    assert_eq!(got, ["1", "1.4", "1.8", "2.2"]);
    let mut g =
        GenerateSeriesNumeric::new(n("3").num(), n("1").num(), Some(n("-1").num())).unwrap();
    let mut got = Vec::new();
    while let Some(img) = g.next().unwrap() {
        got.push(out(&img));
    }
    assert_eq!(got, ["3", "2", "1"]);
    let mut g = GenerateSeriesNumeric::new(n("5").num(), n("4").num(), None).unwrap();
    assert!(g.next().unwrap().is_none());
    let e = GenerateSeriesNumeric::new(n("nan").num(), n("1").num(), None).err().unwrap();
    assert_eq!(e.message(), "start value cannot be NaN");
    let e = GenerateSeriesNumeric::new(n("inf").num(), n("1").num(), None).err().unwrap();
    assert_eq!(e.message(), "start value cannot be infinity");
    let e = GenerateSeriesNumeric::new(n("1").num(), n("nan").num(), None).err().unwrap();
    assert_eq!(e.message(), "stop value cannot be NaN");
    let e =
        GenerateSeriesNumeric::new(n("1").num(), n("2").num(), Some(n("0.0").num())).err().unwrap();
    assert_eq!(e.message(), "step size cannot equal zero");
    let e =
        GenerateSeriesNumeric::new(n("1").num(), n("2").num(), Some(n("-inf").num())).err().unwrap();
    assert_eq!(e.message(), "step size cannot be infinity");
}

#[test]
fn generate_series_numeric_rows_estimate() {
    use crate::series::generate_series_numeric_rows;
    let r = generate_series_numeric_rows(n("1").num(), n("10").num(), None).unwrap().unwrap();
    assert_eq!(r, 10.0);
    let r = generate_series_numeric_rows(n("1").num(), n("10").num(), Some(n("3").num()))
        .unwrap()
        .unwrap();
    assert_eq!(r, 4.0);
    let r = generate_series_numeric_rows(n("10").num(), n("1").num(), Some(n("3").num()))
        .unwrap()
        .unwrap();
    assert_eq!(r, 0.0);
    assert!(generate_series_numeric_rows(n("nan").num(), n("1").num(), None).unwrap().is_none());
    assert!(
        generate_series_numeric_rows(n("1").num(), n("2").num(), Some(n("0").num()))
            .unwrap()
            .is_none()
    );
}

#[test]
fn numeric_sign_and_inc() {
    assert_eq!(out(&numeric_sign(n("0.000").num()).unwrap()), "0");
    assert_eq!(out(&numeric_sign(n("-7.3").num()).unwrap()), "-1");
    assert_eq!(out(&numeric_sign(n("12").num()).unwrap()), "1");
    assert_eq!(out(&numeric_sign(n("nan").num()).unwrap()), "NaN");
    assert_eq!(out(&numeric_sign(n("inf").num()).unwrap()), "1");
    assert_eq!(out(&numeric_sign(n("-inf").num()).unwrap()), "-1");
    assert_eq!(out(&numeric_inc(n("41").num()).unwrap()), "42");
    assert_eq!(out(&numeric_inc(n("-1.5").num()).unwrap()), "-0.5");
    assert_eq!(out(&numeric_inc(n("nan").num()).unwrap()), "NaN");
    assert_eq!(out(&numeric_inc(n("-inf").num()).unwrap()), "-Infinity");
}

fn serialize_numeric_state(state: &mut NumericAggState, with_sum_x2: bool) -> Vec<u8> {
    use ::mcx::MemoryContext;
    use ::stringinfo::StringInfo;
    let ctx = MemoryContext::new("numeric-agg-serialize");
    let mut buf = StringInfo::new_in(ctx.mcx()).unwrap();
    numeric_agg_state_serialize(state, with_sum_x2, &mut buf).unwrap();
    buf.as_bytes().to_vec()
}

fn serialize_int128_state(state: &Int128AggState, with_sum_x2: bool) -> Vec<u8> {
    use ::mcx::MemoryContext;
    use ::stringinfo::StringInfo;
    let ctx = MemoryContext::new("int128-agg-serialize");
    let mut buf = StringInfo::new_in(ctx.mcx()).unwrap();
    int128_agg_state_serialize(state, with_sum_x2, &mut buf).unwrap();
    buf.as_bytes().to_vec()
}

#[test]
fn numeric_agg_serialize_pinned_bytes() {
    use ::mcx::MemoryContext;

    let ctx = MemoryContext::new("numeric-agg-pinned");
    let mcx = ctx.mcx();
    let mut state = NumericAggState::new(true);
    do_numeric_accum(&mut state, mcx, n("1.5").num()).unwrap();
    do_numeric_accum(&mut state, mcx, n("2.5").num()).unwrap();

    // Hand-computed C image: N=2; sumX 4.0 = {1, 0, POS, dscale 1, [4]};
    // sumX2 8.50 = {2, 0, POS, dscale 2, [8, 5000]}; maxScale 1,
    // maxScaleCount 2, NaN/pInf/nInf 0.
    let mut expected = Vec::new();
    expected.extend_from_slice(&2i64.to_be_bytes());
    for v in [1i32, 0, 0, 1] {
        expected.extend_from_slice(&v.to_be_bytes());
    }
    expected.extend_from_slice(&4i16.to_be_bytes());
    for v in [2i32, 0, 0, 2] {
        expected.extend_from_slice(&v.to_be_bytes());
    }
    expected.extend_from_slice(&8i16.to_be_bytes());
    expected.extend_from_slice(&5000i16.to_be_bytes());
    expected.extend_from_slice(&1i32.to_be_bytes());
    expected.extend_from_slice(&2i64.to_be_bytes());
    expected.extend_from_slice(&0i64.to_be_bytes());
    expected.extend_from_slice(&0i64.to_be_bytes());
    expected.extend_from_slice(&0i64.to_be_bytes());
    assert_eq!(serialize_numeric_state(&mut state, true), expected);

    // numeric_avg_serialize drops the sumX2 leg only.
    let mut avg_expected = Vec::new();
    avg_expected.extend_from_slice(&expected[..26]);
    avg_expected.extend_from_slice(&expected[46..]);
    assert_eq!(serialize_numeric_state(&mut state, false), avg_expected);
}

#[test]
fn numeric_agg_serialize_round_trip() {
    use ::mcx::MemoryContext;
    use ::stringinfo::StringInfo;

    let ctx = MemoryContext::new("numeric-agg-rt");
    let mcx = ctx.mcx();
    for with_sum_x2 in [true, false] {
        let mut state = NumericAggState::new(with_sum_x2);
        for v in ["1.5", "-2.25", "1000000.0001", "3", "NaN", "Infinity", "-Infinity", "0"] {
            do_numeric_accum(&mut state, mcx, n(v).num()).unwrap();
        }
        let bytes = serialize_numeric_state(&mut state, with_sum_x2);

        let mut buf = StringInfo::new_in(mcx).unwrap();
        buf.append_bytes(&bytes).unwrap();
        let mut back = numeric_agg_state_deserialize(&mut buf, mcx, with_sum_x2).unwrap();
        assert_eq!(back.n, state.n);
        assert_eq!(back.nan_count, 1);
        assert_eq!(back.pinf_count, 1);
        assert_eq!(back.ninf_count, 1);
        assert_eq!(back.max_scale, state.max_scale);
        assert_eq!(back.max_scale_count, state.max_scale_count);
        assert_eq!(serialize_numeric_state(&mut back, with_sum_x2), bytes);
    }
}

#[test]
fn numeric_agg_deserialize_errors() {
    use ::mcx::MemoryContext;
    use ::stringinfo::StringInfo;

    let ctx = MemoryContext::new("numeric-agg-deserr");
    let mcx = ctx.mcx();
    let mut state = NumericAggState::new(true);
    do_numeric_accum(&mut state, mcx, n("7").num()).unwrap();
    let bytes = serialize_numeric_state(&mut state, true);

    let mut buf = StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&bytes[..bytes.len() - 1]).unwrap();
    let e = numeric_agg_state_deserialize(&mut buf, mcx, true).map(|_| ()).unwrap_err();
    assert_eq!(e.message(), "insufficient data left in message");

    let mut buf = StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&bytes).unwrap();
    buf.append_bytes(&[0]).unwrap();
    let e = numeric_agg_state_deserialize(&mut buf, mcx, true).map(|_| ()).unwrap_err();
    assert_eq!(e.message(), "invalid message format");
}

#[test]
fn numeric_agg_combine_matches_serial() {
    use ::mcx::MemoryContext;

    let ctx = MemoryContext::new("numeric-agg-combine");
    let mcx = ctx.mcx();
    let vals = ["1.5", "-2.25", "1000000.0001", "3", "0.5", "-0.125"];
    for with_sum_x2 in [true, false] {
        let mut all = NumericAggState::new(with_sum_x2);
        for v in vals {
            do_numeric_accum(&mut all, mcx, n(v).num()).unwrap();
        }
        let mut s1 = NumericAggState::new(with_sum_x2);
        for v in &vals[..2] {
            do_numeric_accum(&mut s1, mcx, n(v).num()).unwrap();
        }
        let mut s2 = NumericAggState::new(with_sum_x2);
        for v in &vals[2..] {
            do_numeric_accum(&mut s2, mcx, n(v).num()).unwrap();
        }
        numeric_agg_combine(&mut s1, &mut s2, mcx, with_sum_x2).unwrap();
        assert_eq!(
            serialize_numeric_state(&mut s1, with_sum_x2),
            serialize_numeric_state(&mut all, with_sum_x2)
        );

        // The NULL-state1 arm's field copy.
        let mut copied = NumericAggState::new(with_sum_x2);
        numeric_agg_copy(&mut copied, &mut all, mcx, with_sum_x2).unwrap();
        assert_eq!(
            serialize_numeric_state(&mut copied, with_sum_x2),
            serialize_numeric_state(&mut all, with_sum_x2)
        );
    }
}

#[test]
fn int128_agg_serialize_pinned_bytes() {
    let mut state = Int128AggState::new(true);
    do_int128_accum(&mut state, -2);
    do_int128_accum(&mut state, -3);

    // Hand-computed C image: N=2; sumX -5 = {1, 0, NEG(0x4000), 0, [5]};
    // sumX2 13 = {1, 0, POS, 0, [13]}.
    let mut expected = Vec::new();
    expected.extend_from_slice(&2i64.to_be_bytes());
    for v in [1i32, 0, 0x4000, 0] {
        expected.extend_from_slice(&v.to_be_bytes());
    }
    expected.extend_from_slice(&5i16.to_be_bytes());
    for v in [1i32, 0, 0, 0] {
        expected.extend_from_slice(&v.to_be_bytes());
    }
    expected.extend_from_slice(&13i16.to_be_bytes());
    assert_eq!(serialize_int128_state(&state, true), expected);

    // int8_avg_serialize drops the sumX2 leg only.
    assert_eq!(serialize_int128_state(&state, false), expected[..26].to_vec());
}

#[test]
fn int128_agg_serialize_round_trip() {
    use ::mcx::MemoryContext;
    use ::stringinfo::StringInfo;

    let ctx = MemoryContext::new("int128-agg-rt");
    let mcx = ctx.mcx();
    for with_sum_x2 in [true, false] {
        let mut state = Int128AggState::new(with_sum_x2);
        for v in [1i128 << 62, -(1i128 << 62), -1, 123456789, 0, 1i128 << 61] {
            do_int128_accum(&mut state, v);
        }
        let bytes = serialize_int128_state(&state, with_sum_x2);

        let mut buf = StringInfo::new_in(mcx).unwrap();
        buf.append_bytes(&bytes).unwrap();
        let back = int128_agg_state_deserialize(&mut buf, with_sum_x2).unwrap();
        assert_eq!(back.n, state.n);
        assert_eq!(back.sum_x, state.sum_x);
        if with_sum_x2 {
            assert_eq!(back.sum_x2, state.sum_x2);
        } else {
            assert_eq!(back.sum_x2, 0);
        }
        assert_eq!(serialize_int128_state(&back, with_sum_x2), bytes[..bytes.len()].to_vec());
    }
}

#[test]
fn int128_agg_combine_matches_serial() {
    let vals: [i128; 5] = [5, -7, 1_000_000_007, 0, 42];
    let mut all = Int128AggState::new(true);
    for v in vals {
        do_int128_accum(&mut all, v);
    }
    let mut s1 = Int128AggState::new(true);
    for v in &vals[..2] {
        do_int128_accum(&mut s1, *v);
    }
    let mut s2 = Int128AggState::new(true);
    for v in &vals[2..] {
        do_int128_accum(&mut s2, *v);
    }
    // numeric_poly_combine's non-NULL arm.
    if s2.n > 0 {
        s1.n += s2.n;
        s1.sum_x += s2.sum_x;
        s1.sum_x2 += s2.sum_x2;
    }
    assert_eq!(s1.n, all.n);
    assert_eq!(s1.sum_x, all.sum_x);
    assert_eq!(s1.sum_x2, all.sum_x2);
}

// Stats arrays hand out packed images at arbitrary offsets; one of the two
// placements below puts the digits at an odd address (panicked pre-fix).
#[test]
fn float8_no_overflow_any_realigns_odd_payloads() {
    for s in ["123.456", "-0.38676990993745586", "12345678901234567890", "NaN", "Infinity"] {
        let img = n(s);
        let payload = img.num().as_bytes();
        let want = numeric_float8_no_overflow(img.num()).to_bits();
        let mut buf = vec![0u8; payload.len() + 1];
        for off in 0..2 {
            buf[off..off + payload.len()].copy_from_slice(payload);
            let got = numeric_float8_no_overflow_any(&buf[off..off + payload.len()]).to_bits();
            assert_eq!(got, want, "offset {off} for {s}");
        }
    }
}

// fnconf batch-1, OIDs 1840/1841: C's int2_sum/int4_sum accumulate with a
// bare int64 `+` and PostgreSQL builds with -fwrapv, so overflow WRAPS
// (C 18.3: SELECT int2_sum(9223372036854775807, 1) → -9223372036854775808).
// Red at base: debug add-with-overflow panic in the accumulation.
#[test]
fn int_sum_transitions_wrap_like_c() {
    use crate::builtins::{int2_sum, int4_sum};
    assert_eq!(int2_sum(Some(i64::MAX), Some(1)), Some(i64::MIN));
    assert_eq!(int4_sum(Some(i64::MAX), Some(1)), Some(i64::MIN));
    assert_eq!(int4_sum(Some(i64::MIN), Some(-1)), Some(i64::MAX));
    // Non-overflow behavior unchanged.
    assert_eq!(int2_sum(Some(40), Some(2)), Some(42));
    assert_eq!(int4_sum(None, Some(7)), Some(7));
    assert_eq!(int4_sum(Some(7), None), Some(7));
    assert_eq!(int2_sum(None, None), None);
}

// ---------------------------------------------------------------------------
// Fixed-buffer arithmetic mirrors (fixed.rs) — agreement with the allocating
// kernels. Proofs campaign / TRIAGE "numeric ARITHMETIC walls on
// DigitBuf::realloc_uninit": the fixed mirrors must be behavior-identical.
// ---------------------------------------------------------------------------

fn check_fixed_agree(op: &str, got: VarView<'_>, want: VarView<'_>, v1: VarView<'_>, v2: VarView<'_>) {
    let g = (got.ndigits, got.weight, got.sign, got.dscale, got.digits.to_vec());
    let w = (want.ndigits, want.weight, want.sign, want.dscale, want.digits.to_vec());
    assert_eq!(g, w, "{op} fixed-vs-allocating mismatch for {v1:?} x {v2:?}");
}

fn check_fixed_ops(v1: VarView<'_>, v2: VarView<'_>) {
    let mut f = FixedVar::<40>::new();
    assert!(add_var_fixed(v1, v2, &mut f).is_some(), "add fit {v1:?} {v2:?}");
    let mut a = NumericVar::new();
    add_var(v1, v2, &mut a);
    check_fixed_agree("add", f.view(), a.view(), v1, v2);

    let mut f = FixedVar::<40>::new();
    assert!(sub_var_fixed(v1, v2, &mut f).is_some(), "sub fit {v1:?} {v2:?}");
    let mut a = NumericVar::new();
    sub_var(v1, v2, &mut a);
    check_fixed_agree("sub", f.view(), a.view(), v1, v2);

    // mul: fixed path exists exactly when the shorter side has <= 6 digits
    // and rscale == dscale sum (mul_var's short-kernel route).
    let rscale = v1.dscale + v2.dscale;
    let mut f = FixedVar::<40>::new();
    if mul_var_fixed(v1, v2, &mut f, rscale).is_some() {
        let mut a = NumericVar::new();
        mul_var(v1, v2, &mut a, rscale);
        check_fixed_agree("mul", f.view(), a.view(), v1, v2);
    } else {
        assert!(
            v1.ndigits.min(v2.ndigits) > 6,
            "mul_var_fixed refused a short-kernel case {v1:?} {v2:?}"
        );
    }
}

fn digit_vecs(set: &[NumericDigit], maxlen: usize) -> Vec<Vec<NumericDigit>> {
    let mut out: Vec<Vec<NumericDigit>> = vec![vec![]];
    let mut prev: Vec<Vec<NumericDigit>> = vec![vec![]];
    for _ in 0..maxlen {
        let mut next = Vec::new();
        for p in &prev {
            for &d in set {
                let mut v = p.clone();
                v.push(d);
                next.push(v);
            }
        }
        out.extend(next.iter().cloned());
        prev = next;
    }
    out
}

// Exhaustive small domain: every digit pattern up to 3 digits/side over the
// carry/borrow boundary alphabet, crossed with weights (overlapping and
// disjoint digit ranges) and both signs, for add/sub/mul.
#[test]
fn fixed_kernels_agree_exhaustive_small() {
    let vecs = digit_vecs(&[0, 1, 5000, 9999], 3);
    let weights = [-2i32, 0, 3];
    let signs = [NUMERIC_POS, NUMERIC_NEG];
    for d1 in &vecs {
        for d2 in &vecs {
            for &w1 in &weights {
                for &w2 in &weights {
                    for &s1 in &signs {
                        for &s2 in &signs {
                            let v1 = VarView {
                                ndigits: d1.len() as i32,
                                weight: w1,
                                sign: s1,
                                dscale: 1,
                                digits: d1,
                            };
                            let v2 = VarView {
                                ndigits: d2.len() as i32,
                                weight: w2,
                                sign: s2,
                                dscale: 2,
                                digits: d2,
                            };
                            check_fixed_ops(v1, v2);
                        }
                    }
                }
            }
        }
    }
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

// Randomized wide cases: up to 12 digits/side (48 decimal digits), full
// digit range, wide weight spread.
#[test]
fn fixed_kernels_agree_randomized_wide() {
    let mut rng = Lcg(0x9e37_79b9_7f4a_7c15);
    for _ in 0..20_000 {
        let n1 = (rng.next() % 13) as usize;
        let n2 = (rng.next() % 13) as usize;
        let d1: Vec<NumericDigit> = (0..n1).map(|_| (rng.next() % 10000) as NumericDigit).collect();
        let d2: Vec<NumericDigit> = (0..n2).map(|_| (rng.next() % 10000) as NumericDigit).collect();
        let v1 = VarView {
            ndigits: n1 as i32,
            weight: (rng.next() % 31) as i32 - 15,
            sign: if rng.next() % 2 == 0 { NUMERIC_POS } else { NUMERIC_NEG },
            dscale: (rng.next() % 7) as i32,
            digits: &d1,
        };
        let v2 = VarView {
            ndigits: n2 as i32,
            weight: (rng.next() % 31) as i32 - 15,
            sign: if rng.next() % 2 == 0 { NUMERIC_POS } else { NUMERIC_NEG },
            dscale: (rng.next() % 7) as i32,
            digits: &d2,
        };

        let mut f = FixedVar::<64>::new();
        assert!(add_var_fixed(v1, v2, &mut f).is_some());
        let mut a = NumericVar::new();
        add_var(v1, v2, &mut a);
        check_fixed_agree("add", f.view(), a.view(), v1, v2);

        let mut f = FixedVar::<64>::new();
        assert!(sub_var_fixed(v1, v2, &mut f).is_some());
        let mut a = NumericVar::new();
        sub_var(v1, v2, &mut a);
        check_fixed_agree("sub", f.view(), a.view(), v1, v2);

        // mul with the shorter side truncated to <= 6 digits so the fixed
        // (short-kernel) route applies.
        let n1m = n1.min(6);
        let v1m = VarView {
            ndigits: n1m as i32,
            digits: &d1[..n1m],
            ..v1
        };
        let rscale = v1m.dscale + v2.dscale;
        let mut f = FixedVar::<64>::new();
        assert!(mul_var_fixed(v1m, v2, &mut f, rscale).is_some());
        let mut a = NumericVar::new();
        mul_var(v1m, v2, &mut a, rscale);
        check_fixed_agree("mul", f.view(), a.view(), v1m, v2);
    }
}

// None = allocating fallback: capacity misses and mul cases outside the
// short-kernel route must refuse, never produce a wrong value.
#[test]
fn fixed_kernels_fallback_cases() {
    let d5 = [1 as NumericDigit, 2, 3, 4, 5];
    let v5 = VarView {
        ndigits: 5,
        weight: 4,
        sign: NUMERIC_POS,
        dscale: 0,
        digits: &d5,
    };
    // Result (10 digits + spare) does not fit N=4.
    let mut f = FixedVar::<4>::new();
    assert!(add_var_fixed(v5, v5, &mut f).is_none());
    // (sub of EQUAL operands takes the cmp==0 zero shortcut with no alloc,
    // so it succeeds at any N — use unequal operands for the capacity miss.)
    let d5b = [1 as NumericDigit, 2, 3, 4, 6];
    let v5b = VarView { digits: &d5b, ..v5 };
    assert!(sub_var_fixed(v5, v5b, &mut f).is_none());
    assert!(mul_var_fixed(v5, v5, &mut f, 0).is_none());
    let mut f = FixedVar::<4>::new();
    assert!(sub_var_fixed(v5, v5, &mut f).is_some());
    let mut a = NumericVar::new();
    sub_var(v5, v5, &mut a);
    check_fixed_agree("sub-equal-zero", f.view(), a.view(), v5, v5);
    // Same inputs fit N=16.
    let mut f = FixedVar::<16>::new();
    assert!(add_var_fixed(v5, v5, &mut f).is_some());

    // mul: rscale != dscale sum -> allocating fallback (mul_var would take
    // the full pairwise kernel).
    let mut f = FixedVar::<40>::new();
    assert!(mul_var_fixed(v5, v5, &mut f, 1).is_none());

    // mul: shorter side > 6 digits -> allocating fallback.
    let d7 = [9999 as NumericDigit; 7];
    let v7 = VarView {
        ndigits: 7,
        weight: 6,
        sign: NUMERIC_POS,
        dscale: 0,
        digits: &d7,
    };
    let mut f = FixedVar::<40>::new();
    assert!(mul_var_fixed(v7, v7, &mut f, 0).is_none());

    // mul: zero operand short-circuits regardless of rscale (as mul_var).
    let mut f = FixedVar::<8>::new();
    let z = VarView {
        ndigits: 0,
        weight: 0,
        sign: NUMERIC_POS,
        dscale: 0,
        digits: &[],
    };
    assert!(mul_var_fixed(z, v7, &mut f, 5).is_some());
    let mut a = NumericVar::new();
    mul_var(z, v7, &mut a, 5);
    check_fixed_agree("mul-zero", f.view(), a.view(), z, v7);
}
