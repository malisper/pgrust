use mcx::MemoryContext;
use types_error::{
    SoftErrorContext, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use super::*;

fn parse(s: &str) -> Cash {
    cash_in(s, None).unwrap()
}

fn out(value: Cash) -> String {
    let mut buf = [0u8; CASH_OUT_BUFLEN];
    let len = cash_out_into(value, &mut buf);
    String::from_utf8(buf[..len].to_vec()).unwrap()
}

#[test]
fn cash_in_c_locale_forms() {
    assert_eq!(parse("123.45"), 12345);
    assert_eq!(parse("$123.45"), 12345);
    assert_eq!(parse("$123,456.78"), 12345678);
    assert_eq!(parse("  $  123"), 12300);
    assert_eq!(parse("(1.23)"), -123);
    assert_eq!(parse("-1.23"), -123);
    assert_eq!(parse("+1.23"), 123);
    assert_eq!(parse("123.45-"), -12345);
    assert_eq!(parse("123.45 $"), 12345);
    assert_eq!(parse("1"), 100);
    assert_eq!(parse("1."), 100);
    assert_eq!(parse(".5"), 50);
    assert_eq!(parse("0.056"), 6);
    assert_eq!(parse("0.054"), 5);
    assert_eq!(parse(""), 0);
}

#[test]
fn cash_in_range_corners() {
    assert_eq!(parse("92233720368547758.07"), i64::MAX);
    assert_eq!(parse("-92233720368547758.08"), i64::MIN);

    let err = cash_in("92233720368547758.08", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "value \"92233720368547758.08\" is out of range for type money"
    );
    let err = cash_in("-92233720368547758.09", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
}

#[test]
fn cash_in_bad_syntax() {
    let err = cash_in("123.45x", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(
        err.message(),
        "invalid input syntax for type money: \"123.45x\""
    );

    let mut soft = SoftErrorContext::new(true);
    assert_eq!(cash_in("bogus$$", Some(&mut soft)).unwrap(), 0);
    assert!(soft.error_occurred());
}

#[test]
fn cash_out_c_locale_forms() {
    assert_eq!(out(12345), "$123.45");
    assert_eq!(out(1234567), "$12,345.67");
    assert_eq!(out(-1234567), "-$12,345.67");
    assert_eq!(out(0), "$0.00");
    assert_eq!(out(5), "$0.05");
    assert_eq!(out(i64::MAX), "$92,233,720,368,547,758.07");
    assert_eq!(out(i64::MIN), "-$92,233,720,368,547,758.08");
}

#[test]
fn comparisons_and_extremes() {
    assert!(cash_eq(5, 5) && cash_ne(5, 6));
    assert!(cash_lt(5, 6) && cash_le(5, 5) && cash_gt(6, 5) && cash_ge(6, 6));
    assert_eq!(cash_cmp(1, 2), -1);
    assert_eq!(cash_cmp(2, 2), 0);
    assert_eq!(cash_cmp(3, 2), 1);
    assert_eq!(cashlarger(3, 2), 3);
    assert_eq!(cashsmaller(3, 2), 2);
}

#[test]
fn arithmetic_matches_c() {
    assert_eq!(cash_pl(100, 23).unwrap(), 123);
    assert_eq!(cash_mi(100, 23).unwrap(), 77);
    let err = cash_pl(i64::MAX, 1).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "money out of range");
    assert_eq!(cash_mi(i64::MIN, 1).unwrap_err().message(), "money out of range");

    assert_eq!(cash_mul_int64(12345, 2).unwrap(), 24690);
    assert_eq!(
        cash_mul_int64(i64::MAX, 2).unwrap_err().message(),
        "money out of range"
    );
    assert_eq!(cash_div_int64(24690, 2).unwrap(), 12345);
    assert_eq!(cash_div_int64(7, 2).unwrap(), 3);
    let err = cash_div_int64(1, 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_DIVISION_BY_ZERO);
    assert_eq!(err.message(), "division by zero");

    assert_eq!(cash_mul_float8(100, 2.5).unwrap(), 250);
    assert_eq!(cash_mul_float8(5, 0.5).unwrap(), 2); // rint ties-to-even
    assert_eq!(cash_mul_float8(7, 0.5).unwrap(), 4);
    assert_eq!(cash_div_float8(250, 2.5).unwrap(), 100);
    assert_eq!(
        cash_mul_float8(i64::MAX, f64::MAX).unwrap_err().message(),
        "value out of range: overflow"
    );
    assert_eq!(
        cash_mul_float8(i64::MAX, 4.0).unwrap_err().message(),
        "money out of range"
    );
    assert_eq!(
        cash_div_float8(1, 0.0).unwrap_err().sqlstate(),
        ERRCODE_DIVISION_BY_ZERO
    );

    assert_eq!(cash_div_cash(500, 250).unwrap(), 2.0);
    assert_eq!(
        cash_div_cash(1, 0).unwrap_err().sqlstate(),
        ERRCODE_DIVISION_BY_ZERO
    );
}

#[test]
fn int_conversions_scale_by_fpoint() {
    assert_eq!(int4_cash(123).unwrap(), 12300);
    assert_eq!(int4_cash(-1).unwrap(), -100);
    assert_eq!(int8_cash(92233720368547758).unwrap(), 9223372036854775800);
    assert_eq!(
        int8_cash(92233720368547759).unwrap_err().message(),
        "bigint out of range"
    );
}

#[test]
fn cash_words_matches_c() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let words = |v: Cash| {
        let t = cash_words(mcx, v).unwrap();
        String::from_utf8(t.data().to_vec()).unwrap()
    };
    assert_eq!(words(0), "Zero dollars and zero cents");
    assert_eq!(words(100), "One dollar and zero cents");
    assert_eq!(words(101), "One dollar and one cent");
    assert_eq!(words(123), "One dollar and twenty three cents");
    assert_eq!(
        words(12345),
        "One hundred twenty three dollars and forty five cents"
    );
    assert_eq!(
        words(11305),
        "One hundred and thirteen dollars and five cents"
    );
    assert_eq!(words(-100), "Minus one dollar and zero cents");
    assert_eq!(
        words(120000),
        "One thousand two hundred dollars and zero cents"
    );
    assert_eq!(
        words(i64::MIN),
        "Minus ninety two quadrillion two hundred thirty three trillion seven \
         hundred twenty billion three hundred sixty eight million five hundred \
         forty seven thousand seven hundred fifty eight dollars and eight cents"
    );
}

#[test]
fn wire_roundtrip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let image = cash_send(mcx, -424242).unwrap();
    let mut si = stringinfo::StringInfo::new_in(mcx).unwrap();
    si.append_bytes(image.data()).unwrap();
    assert_eq!(cash_recv(&mut si).unwrap(), -424242);
}
