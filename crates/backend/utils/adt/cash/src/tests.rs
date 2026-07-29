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
    let len = cash_out_into(value, &mut buf).unwrap();
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

// Differential corpus captured from live PostgreSQL 18.3 (Homebrew, aarch64),
// lc_monetary=C, 2026-07-03: every (input, output) below is the live server's
// byte-exact answer.
#[test]
fn live_pg_in_out_corpus() {
    let pairs: &[(&str, &str)] = &[
        ("123.45", "$123.45"),
        ("$123.45", "$123.45"),
        ("$123,456.78", "$123,456.78"),
        ("  $  123", "$123.00"),
        ("(1.23)", "-$1.23"),
        ("-1.23", "-$1.23"),
        ("+1.23", "$1.23"),
        ("123.45-", "-$123.45"),
        ("123.45 $", "$123.45"),
        ("1", "$1.00"),
        ("1.", "$1.00"),
        (".5", "$0.50"),
        ("0.056", "$0.06"),
        ("0.054", "$0.05"),
        ("", "$0.00"),
        ("92233720368547758.07", "$92,233,720,368,547,758.07"),
        ("-92233720368547758.08", "-$92,233,720,368,547,758.08"),
        ("(92233720368547758.07)", "-$92,233,720,368,547,758.07"),
        ("$0.00", "$0.00"),
        ("0", "$0.00"),
        ("-0", "$0.00"),
        ("(0)", "$0.00"),
        ("- 1.23", "-$1.23"),
        ("$ -1.23", "-$1.23"),
        ("-$1.23", "-$1.23"),
        ("($1.23)", "-$1.23"),
        ("1,2,3.45", "$123.45"),
        ("1,,2", "$12.00"),
        (",1", "$1.00"),
        (".", "$0.00"),
        ("-.", "$0.00"),
        ("$", "$0.00"),
        ("()", "$0.00"),
        ("(1.23", "-$1.23"),
        ("1.23)", "$1.23"),
        ("1.23--", "-$1.23"),
        ("123.456", "$123.46"),
        ("123.454", "$123.45"),
        ("123.4549", "$123.45"),
        ("123.455", "$123.46"),
        ("-123.456", "-$123.46"),
        ("-123.454", "-$123.45"),
        (".005", "$0.01"),
        (".004", "$0.00"),
        ("1234567890.12", "$1,234,567,890.12"),
        ("(  1.23  )", "-$1.23"),
        ("( 1.23 ) -", "-$1.23"),
    ];
    for (input, expected) in pairs {
        assert_eq!(&out(parse(input)), expected, "input {input:?}");
    }

    let syntax_errs = ["--1.23", "1.2.3", "1e5", "0x10", "abc", "12abc", "$abc", "12$34", "1 2"];
    for input in syntax_errs {
        let err = cash_in(input, None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION, "input {input:?}");
        assert_eq!(
            err.message(),
            format!("invalid input syntax for type money: \"{input}\""),
            "input {input:?}"
        );
    }
    let range_errs = [
        "9223372036854775807",
        "92233720368547758.08",
        "-92233720368547758.09",
        "999999999999999999999",
    ];
    for input in range_errs {
        let err = cash_in(input, None).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, "input {input:?}");
        assert_eq!(
            err.message(),
            format!("value \"{input}\" is out of range for type money"),
            "input {input:?}"
        );
    }
}

#[test]
fn live_pg_arithmetic_corpus() {
    assert_eq!(out(cash_pl(parse("1.23"), parse("2.77")).unwrap()), "$4.00");
    assert_eq!(out(cash_mi(parse("5.00"), parse("7.25")).unwrap()), "-$2.25");
    assert_eq!(out(cash_mul_int64(parse("3.00"), 2).unwrap()), "$6.00");
    assert_eq!(out(cash_mul_float8(parse("3.00"), 2.5).unwrap()), "$7.50");
    assert_eq!(out(cash_mul_float8(parse("3.00"), 2.5f32 as f64).unwrap()), "$7.50");
    assert_eq!(out(cash_div_int64(parse("7.00"), 2).unwrap()), "$3.50");
    assert_eq!(out(cash_div_float8(parse("7.00"), 2.0).unwrap()), "$3.50");
    assert_eq!(cash_div_cash(parse("7.00"), parse("2.00")).unwrap(), 3.5);

    for err in [
        cash_div_int64(parse("7.00"), 0).unwrap_err(),
        cash_div_float8(parse("7.00"), 0.0).unwrap_err(),
        cash_div_cash(parse("7.00"), 0).unwrap_err(),
    ] {
        assert_eq!(err.sqlstate(), ERRCODE_DIVISION_BY_ZERO);
        assert_eq!(err.message(), "division by zero");
    }
    for err in [
        cash_pl(i64::MAX, 1).unwrap_err(),
        cash_mi(i64::MIN, 1).unwrap_err(),
        cash_mul_int64(i64::MAX, 2).unwrap_err(),
    ] {
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(err.message(), "money out of range");
    }

    assert_eq!(out(int4_cash(123).unwrap()), "$123.00");
    assert_eq!(out(int4_cash(-123).unwrap()), "-$123.00");
    assert_eq!(out(int8_cash(123).unwrap()), "$123.00");
    assert_eq!(out(int4_cash(2147483647).unwrap()), "$2,147,483,647.00");

    let ctx = MemoryContext::new("t");
    let words = |v: Cash| {
        String::from_utf8(cash_words(ctx.mcx(), v).unwrap().data().to_vec()).unwrap()
    };
    assert_eq!(words(parse("0.05")), "Zero dollars and five cents");
    assert_eq!(
        words(parse("-12345678.90")),
        "Minus twelve million three hundred forty five thousand six hundred \
         seventy eight dollars and ninety cents"
    );
    assert_eq!(
        words(parse("92233720368547758.07")),
        "Ninety two quadrillion two hundred thirty three trillion seven hundred \
         twenty billion three hundred sixty eight million five hundred forty \
         seven thousand seven hundred fifty eight dollars and seven cents"
    );
}

fn numeric_str(v: Cash) -> String {
    let img = cash_numeric(v).unwrap();
    let mut out = Vec::new();
    adt_numeric::numeric_out_into(img.num(), &mut out);
    String::from_utf8(out).unwrap()
}

fn parse_numeric(s: &str) -> adt_numeric::NumericImage {
    adt_numeric::numeric_in(s, -1, None).unwrap().unwrap()
}

#[test]
fn cash_numeric_matches_c() {
    assert_eq!(numeric_str(0), "0.00");
    assert_eq!(numeric_str(12345), "123.45");
    assert_eq!(numeric_str(-12345), "-123.45");
    assert_eq!(numeric_str(100), "1.00");
    assert_eq!(numeric_str(5), "0.05");
    assert_eq!(numeric_str(i64::MAX), "92233720368547758.07");
    assert_eq!(numeric_str(i64::MIN), "-92233720368547758.08");
}

#[test]
fn numeric_cash_matches_c() {
    assert_eq!(numeric_cash(parse_numeric("0").num()).unwrap(), 0);
    assert_eq!(numeric_cash(parse_numeric("123.45").num()).unwrap(), 12345);
    assert_eq!(numeric_cash(parse_numeric("-123.45").num()).unwrap(), -12345);
    // numeric_int8 rounds to the nearest integer after scaling.
    assert_eq!(numeric_cash(parse_numeric("1.005").num()).unwrap(), 101);
    assert_eq!(numeric_cash(parse_numeric("1.004").num()).unwrap(), 100);

    let err = numeric_cash(parse_numeric("999999999999999999").num()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "bigint out of range");
}

#[test]
fn cash_numeric_numeric_cash_roundtrip() {
    for v in [0i64, 1, -1, 12345, -12345, 100, i64::MAX, i64::MIN] {
        let img = cash_numeric(v).unwrap();
        assert_eq!(numeric_cash(img.num()).unwrap(), v);
    }
}

#[test]
fn div_min_by_neg_one_errors() {
    // Ruling 2026-07-29: MIN/-1 raises 22003 like int8div, not panic/wrap.
    let e = cash_div_int64(super::Cash::MIN, -1).unwrap_err();
    assert_eq!(e.sqlstate, types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    // neighbours still work
    assert_eq!(cash_div_int64(super::Cash::MIN, 1).unwrap(), super::Cash::MIN);
    assert_eq!(cash_div_int64(super::Cash::MIN + 1, -1).unwrap(), super::Cash::MAX);
}
