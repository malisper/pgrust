//! Tests for the `money` datatype port.
//!
//! `cash.c`'s genuine externals (`PGLC_localeconv`, `float8_mul`/`float8_div`)
//! are installed here with faithful stand-ins: the C-locale lconv snapshot
//! (`lc_monetary = 'C'`, the configuration the `money` regression test assumes)
//! and the IEEE float ops with PostgreSQL's overflow detection. The `numeric`
//! arithmetic cores are the real owner, installed via its `init_seams()`.

use super::*;
use mcx::MemoryContext;
use std::sync::Once;
use types_cash::CashLconv;

static INIT: Once = Once::new();

fn float8_mul_impl(a: f64, b: f64) -> PgResult<f64> {
    let r = a * b;
    if r.is_infinite() && a.is_finite() && b.is_finite() {
        return Err(PgError::error("value out of range: overflow")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }
    Ok(r)
}

fn float8_div_impl(a: f64, b: f64) -> PgResult<f64> {
    if b == 0.0 {
        return Err(division_by_zero());
    }
    let r = a / b;
    if r.is_infinite() && a.is_finite() && b.is_finite() {
        return Err(PgError::error("value out of range: overflow")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }
    Ok(r)
}

fn install() {
    INIT.call_once(|| {
        backend_utils_adt_pg_locale_seams::pglc_localeconv::set(CashLconv::c_locale);
        float_seam::float8_mul::set(float8_mul_impl);
        float_seam::float8_div::set(float8_div_impl);
        backend_utils_adt_numeric::init_seams();
    });
}

// ---- pure comparison / arithmetic cores ---------------------------------

#[test]
fn comparisons() {
    assert!(cash_eq(5, 5));
    assert!(cash_ne(5, 6));
    assert!(cash_lt(5, 6));
    assert!(cash_le(5, 5));
    assert!(cash_gt(6, 5));
    assert!(cash_ge(5, 5));
    assert_eq!(cash_cmp(6, 5), 1);
    assert_eq!(cash_cmp(5, 5), 0);
    assert_eq!(cash_cmp(5, 6), -1);
    assert_eq!(cashlarger(5, 6), 6);
    assert_eq!(cashsmaller(5, 6), 5);
}

#[test]
fn add_sub_overflow() {
    assert_eq!(cash_pl(100, 23).unwrap(), 123);
    assert_eq!(cash_mi(100, 23).unwrap(), 77);
    assert!(cash_pl(i64::MAX, 1).is_err());
    assert!(cash_mi(i64::MIN, 1).is_err());
}

#[test]
fn int_mul_div() {
    assert_eq!(cash_mul_int8(100, 3).unwrap(), 300);
    assert_eq!(int8_mul_cash(3, 100).unwrap(), 300);
    assert_eq!(cash_div_int8(100, 4).unwrap(), 25);
    assert_eq!(cash_mul_int4(100, 3).unwrap(), 300);
    assert_eq!(cash_div_int4(100, 4).unwrap(), 25);
    assert_eq!(cash_mul_int2(100, 3).unwrap(), 300);
    assert_eq!(cash_div_int2(100, 4).unwrap(), 25);
    assert!(cash_div_int8(100, 0).is_err());
    assert!(cash_mul_int8(i64::MAX, 2).is_err());
}

#[test]
fn float_mul_div() {
    install();
    assert_eq!(cash_mul_flt8(100, 2.5).unwrap(), 250);
    assert_eq!(flt8_mul_cash(2.5, 100).unwrap(), 250);
    assert_eq!(cash_div_flt8(100, 4.0).unwrap(), 25);
    assert_eq!(cash_mul_flt4(100, 2.5).unwrap(), 250);
    assert_eq!(cash_div_flt4(100, 4.0).unwrap(), 25);
    assert_eq!(cash_div_cash(100, 4).unwrap(), 25.0);
    assert!(cash_div_cash(100, 0).is_err());
}

// ---- cash_in / cash_out under the C locale ------------------------------

#[test]
fn cash_in_basic() {
    install();
    // Under C locale fpoint=2, so "$123.45" -> 12345 cents.
    assert_eq!(cash_in(b"$123.45", None).unwrap(), 12345);
    assert_eq!(cash_in(b"123.45", None).unwrap(), 12345);
    assert_eq!(cash_in(b"123", None).unwrap(), 12300);
    assert_eq!(cash_in(b"-123.45", None).unwrap(), -12345);
    assert_eq!(cash_in(b"(123.45)", None).unwrap(), -12345);
    assert_eq!(cash_in(b"$1,234.56", None).unwrap(), 123456);
    // rounding: a trailing >=5 digit rounds the last kept place.
    assert_eq!(cash_in(b"1.005", None).unwrap(), 101);
    assert_eq!(cash_in(b"1.004", None).unwrap(), 100);
}

#[test]
fn cash_in_invalid() {
    install();
    assert!(cash_in(b"abc", None).is_err());
    assert!(cash_in(b"12.3x", None).is_err());
}

#[test]
fn cash_out_basic() {
    install();
    assert_eq!(cash_out(12345), "$123.45");
    assert_eq!(cash_out(0), "$0.00");
    assert_eq!(cash_out(-12345), "-$123.45");
    assert_eq!(cash_out(123456), "$1,234.56");
}

#[test]
fn roundtrip_in_out() {
    install();
    for v in [0i64, 1, 99, 100, 12345, -12345, 100000000] {
        let s = cash_out(v);
        let back = cash_in(s.as_bytes(), None).unwrap();
        assert_eq!(back, v, "roundtrip {v} via {s}");
    }
}

// ---- cash_words ----------------------------------------------------------

#[test]
fn words() {
    assert_eq!(cash_words(0), "Zero dollars and zero cents");
    assert_eq!(cash_words(100), "One dollar and zero cents");
    assert_eq!(cash_words(101), "One dollar and one cent");
    assert_eq!(
        cash_words(12345),
        "One hundred twenty three dollars and forty five cents"
    );
    // The "and" branch only fires for tu < 20 (e.g. 11305 -> 113 dollars 05).
    assert_eq!(
        cash_words(11305),
        "One hundred and thirteen dollars and five cents"
    );
    assert_eq!(cash_words(-100), "Minus one dollar and zero cents");
}

#[test]
fn words_min_value() {
    // INT64_MIN must not panic (the wrapping-neg + (uint64) cast path).
    let s = cash_words(i64::MIN);
    assert!(s.starts_with("Minus "), "got {s}");
}

// ---- numeric / integer conversions --------------------------------------

#[test]
fn int_conversions() {
    install();
    assert_eq!(int4_cash(5).unwrap(), 500);
    assert_eq!(int8_cash(5).unwrap(), 500);
    assert_eq!(int4_cash(-5).unwrap(), -500);
    assert!(int8_cash(i64::MAX).is_err());
}

#[test]
fn cash_numeric_roundtrip() {
    install();
    let ctx = MemoryContext::new("cash-test");
    let mcx = ctx.mcx();

    let num = cash_numeric(mcx, 12345).unwrap();
    let back = numeric_cash(mcx, &num).unwrap();
    assert_eq!(back, 12345);

    let num = cash_numeric(mcx, -100).unwrap();
    let back = numeric_cash(mcx, &num).unwrap();
    assert_eq!(back, -100);
}

#[test]
fn numeric_cash_rejects_nan() {
    install();
    let ctx = MemoryContext::new("cash-test");
    let mcx = ctx.mcx();
    let nan = backend_utils_adt_numeric::io::numeric_in(mcx, "NaN", -1).unwrap();
    assert!(numeric_cash(mcx, &nan).is_err());
}
