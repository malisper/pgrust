use numutils::*;
use types_error::{
    SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

#[test]
fn parse_boundaries_all_widths() {
    assert_eq!(pg_strtoint16("32767").unwrap(), i16::MAX);
    assert_eq!(pg_strtoint16("-32768").unwrap(), i16::MIN);
    assert!(pg_strtoint16("32768").is_err());
    assert!(pg_strtoint16("-32769").is_err());
    assert_eq!(pg_strtoint32("2147483647").unwrap(), i32::MAX);
    assert_eq!(pg_strtoint32("-2147483648").unwrap(), i32::MIN);
    assert!(pg_strtoint32("2147483648").is_err());
    assert!(pg_strtoint32("-2147483649").is_err());
    assert_eq!(pg_strtoint64("9223372036854775807").unwrap(), i64::MAX);
    assert_eq!(pg_strtoint64("-9223372036854775808").unwrap(), i64::MIN);
    assert!(pg_strtoint64("9223372036854775808").is_err());
    assert!(pg_strtoint64("-9223372036854775809").is_err());
    assert_eq!(pg_strtoint32("0").unwrap(), 0);
    assert_eq!(pg_strtoint32("-0").unwrap(), 0);
}

#[test]
fn parse_error_surface_matches_c() {
    let e = pg_strtoint16("32768").unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(e.message(), "value \"32768\" is out of range for type smallint");

    let e = pg_strtoint32("-2147483649").unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        e.message(),
        "value \"-2147483649\" is out of range for type integer"
    );

    let e = pg_strtoint64("junk").unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(e.message(), "invalid input syntax for type bigint: \"junk\"");

    // Guard trips before the trailing-junk check: overflow-then-junk is
    // out_of_range in C, not invalid_syntax.
    let e = pg_strtoint32("9999999999x").unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    let e = pg_strtoint16("32768x").unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    let e = pg_strtoint16("327690x").unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
}

#[test]
fn parse_whitespace_sign_and_bases() {
    assert_eq!(pg_strtoint32("  42  ").unwrap(), 42);
    assert_eq!(pg_strtoint32("\t\n\x0b\x0c\r42").unwrap(), 42);
    assert_eq!(pg_strtoint32("+42").unwrap(), 42);
    assert_eq!(pg_strtoint32(" -42 ").unwrap(), -42);
    assert_eq!(pg_strtoint32("0x7fffffff").unwrap(), i32::MAX);
    assert_eq!(pg_strtoint32("-0x80000000").unwrap(), i32::MIN);
    assert_eq!(pg_strtoint32("0X1F").unwrap(), 31);
    assert_eq!(pg_strtoint32("0o17").unwrap(), 15);
    assert_eq!(pg_strtoint32("0O17").unwrap(), 15);
    assert_eq!(pg_strtoint32("0b101").unwrap(), 5);
    assert_eq!(pg_strtoint32("0B101").unwrap(), 5);
    assert_eq!(pg_strtoint16("-0x8000").unwrap(), i16::MIN);
    assert_eq!(pg_strtoint64("-0x8000000000000000").unwrap(), i64::MIN);
    assert!(pg_strtoint64("0x10000000000000000").is_err());
    assert!(pg_strtoint32("").is_err());
    assert!(pg_strtoint32("-").is_err());
    assert!(pg_strtoint32("+").is_err());
    assert!(pg_strtoint32("   ").is_err());
    assert!(pg_strtoint32("42 x").is_err());
    assert!(pg_strtoint32("4 2").is_err());
    assert!(pg_strtoint32("0x").is_err());
    assert!(pg_strtoint32("0o").is_err());
    assert!(pg_strtoint32("0b").is_err());
}

#[test]
fn parse_underscores_match_c_per_branch() {
    assert_eq!(pg_strtoint32("1_000_000").unwrap(), 1_000_000);
    assert_eq!(pg_strtoint64("9_223_372_036_854_775_807").unwrap(), i64::MAX);
    assert!(pg_strtoint32("_1").is_err());
    assert!(pg_strtoint32("1_").is_err());
    assert!(pg_strtoint32("1__0").is_err());
    // hex/octal/binary branches allow a leading underscore after the prefix.
    assert_eq!(pg_strtoint32("0x_1").unwrap(), 1);
    assert_eq!(pg_strtoint32("0o_17").unwrap(), 15);
    assert_eq!(pg_strtoint32("0b_101").unwrap(), 5);
    assert!(pg_strtoint32("0x_").is_err());
    assert!(pg_strtoint32("0xff_").is_err());
    assert!(pg_strtoint32("0x1_g").is_err());
}

#[test]
fn parse_leading_zeros_never_false_overflow() {
    let s = format!("{}2147483647", "0".repeat(40));
    assert_eq!(pg_strtoint32(&s).unwrap(), i32::MAX);
    let s = format!("-{}32768", "0".repeat(40));
    assert_eq!(pg_strtoint16(&s).unwrap(), i16::MIN);
    let s = format!("{}9223372036854775807", "0".repeat(40));
    assert_eq!(pg_strtoint64(&s).unwrap(), i64::MAX);
    let s = format!("{}9223372036854775808", "0".repeat(40));
    assert!(pg_strtoint64(&s).is_err());
    assert!(pg_strtoint32("99999999999999999999999999").is_err());
}

#[test]
fn parse_safe_soft_errors() {
    let mut cx = SoftErrorContext::new(true);
    assert_eq!(pg_strtoint32_safe("bad", Some(&mut cx)).unwrap(), 0);
    assert!(cx.error_occurred());
    assert_eq!(
        cx.error().unwrap().message(),
        "invalid input syntax for type integer: \"bad\""
    );
    let mut cx = SoftErrorContext::new(false);
    assert_eq!(pg_strtoint64_safe("9999999999999999999999", Some(&mut cx)).unwrap(), 0);
    assert!(cx.error_occurred());
    assert!(cx.error().is_none());
}

#[test]
fn unsigned_subr_strtoul_base0() {
    assert_eq!(uint32in_subr("42", false, "oid", None).unwrap(), (42, ""));
    assert_eq!(uint32in_subr("42 rest", true, "oid", None).unwrap(), (42, " rest"));
    assert_eq!(uint32in_subr("010", false, "oid", None).unwrap(), (8, ""));
    assert_eq!(uint32in_subr("0xffffffff", false, "oid", None).unwrap(), (u32::MAX, ""));
    assert_eq!(uint32in_subr("-1", false, "oid", None).unwrap(), (u32::MAX, ""));
    assert_eq!(
        uint64in_subr("18446744073709551615  ", false, "xid8", None).unwrap(),
        (u64::MAX, "  ")
    );
    assert_eq!(uint64in_subr("-1", false, "xid8", None).unwrap(), (u64::MAX, ""));
    // bare 0x backtracks: 0 parses, 'x' is the tail.
    assert_eq!(uint32in_subr("0x", true, "oid", None).unwrap(), (0, "x"));
    assert!(uint32in_subr("0x", false, "oid", None).is_err());
    assert!(uint32in_subr("0o17", false, "oid", None).is_err());
    assert!(uint32in_subr("08", false, "oid", None).is_err());
    assert!(uint32in_subr("4294967296", false, "oid", None).is_err());
    assert!(uint64in_subr("18446744073709551616", false, "xid8", None).is_err());
    let e = uint32in_subr("12x", false, "oid", None).unwrap_err();
    assert_eq!(e.message(), "invalid input syntax for type oid: \"12x\"");
    let e = uint32in_subr("4294967296", false, "oid", None).unwrap_err();
    assert_eq!(e.message(), "value \"4294967296\" is out of range for type oid");
}

fn fmt_u32(v: u32) -> String {
    let mut buf = [0u8; 16];
    let n = pg_ultoa_n(v, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

fn fmt_i32(v: i32) -> String {
    let mut buf = [0u8; 16];
    let n = pg_ltoa(v, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

fn fmt_u64(v: u64) -> String {
    let mut buf = [0u8; MAXINT8LEN];
    let n = pg_ulltoa_n(v, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

fn fmt_i64(v: i64) -> String {
    let mut buf = [0u8; MAXINT8LEN + 1];
    let n = pg_lltoa(v, &mut buf);
    String::from_utf8(buf[..n].to_vec()).unwrap()
}

#[test]
fn format_matches_decimal_over_boundaries() {
    for v in [
        0u32, 1, 9, 10, 99, 100, 999, 1000, 9999, 10000, 99999, 100000, 999999, 1000000,
        99999999, 100000000, 999999999, 1000000000, u32::MAX,
    ] {
        assert_eq!(fmt_u32(v), v.to_string());
    }
    for v in [0i32, 1, -1, 12345, -12345, i32::MAX, i32::MIN, i32::MIN + 1] {
        assert_eq!(fmt_i32(v), v.to_string());
    }
    let mut v = 1u64;
    for _ in 0..20 {
        assert_eq!(fmt_u64(v), v.to_string());
        assert_eq!(fmt_u64(v - 1), (v - 1).to_string());
        v = v.wrapping_mul(10);
    }
    for v in [0u64, 99999999, 100000000, 10000000000000000, u64::MAX] {
        assert_eq!(fmt_u64(v), v.to_string());
    }
    for v in [0i64, -1, 1, i64::MAX, i64::MIN, i64::MIN + 1, -99999999999] {
        assert_eq!(fmt_i64(v), v.to_string());
    }
}

#[test]
fn format_parse_roundtrip_pseudorandom() {
    let mut s = 0x243F_6A88_85A3_08D3u64;
    for _ in 0..10000 {
        s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let v32 = s as i32;
        assert_eq!(pg_strtoint32(&fmt_i32(v32)).unwrap(), v32);
        let v64 = s as i64;
        assert_eq!(pg_strtoint64(&fmt_i64(v64)).unwrap(), v64);
        let v16 = s as i16;
        let mut buf = [0u8; 16];
        let n = pg_itoa(v16, &mut buf);
        assert_eq!(
            pg_strtoint16(std::str::from_utf8(&buf[..n]).unwrap()).unwrap(),
            v16
        );
    }
}

#[test]
fn ultostr_and_zeropad() {
    let mut buf = [0u8; 32];
    assert_eq!(pg_ultostr_zeropad(&mut buf, 7, 2), 2);
    assert_eq!(&buf[..2], b"07");
    assert_eq!(pg_ultostr_zeropad(&mut buf, 0, 2), 2);
    assert_eq!(&buf[..2], b"00");
    assert_eq!(pg_ultostr_zeropad(&mut buf, 42, 5), 5);
    assert_eq!(&buf[..5], b"00042");
    assert_eq!(pg_ultostr_zeropad(&mut buf, 12345, 3), 5);
    assert_eq!(&buf[..5], b"12345");
    assert_eq!(pg_ultostr_zeropad(&mut buf, 100, 2), 3);
    assert_eq!(&buf[..3], b"100");
    assert_eq!(pg_ultostr(&mut buf, 2026), 4);
    assert_eq!(&buf[..4], b"2026");

    // datetime.c piecewise use: hh:mm:ss into one buffer.
    let mut pos = 0;
    pos += pg_ultostr_zeropad(&mut buf[pos..], 9, 2);
    buf[pos] = b':';
    pos += 1;
    pos += pg_ultostr_zeropad(&mut buf[pos..], 5, 2);
    buf[pos] = b':';
    pos += 1;
    pos += pg_ultostr_zeropad(&mut buf[pos..], 42, 2);
    assert_eq!(&buf[..pos], b"09:05:42");
}
