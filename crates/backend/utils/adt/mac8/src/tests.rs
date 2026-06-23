//! Tests for the idiomatic `mac8.c` port. Golden values cross-checked against
//! PostgreSQL's `src/test/regress/expected/macaddr8.out` canonical I/O.

extern crate std;

use super::*;

#[allow(clippy::too_many_arguments)]
fn mac8(a: u8, b: u8, c: u8, d: u8, e: u8, f: u8, g: u8, h: u8) -> macaddr8 {
    macaddr8 {
        a,
        b,
        c,
        d,
        e,
        f,
        g,
        h,
    }
}

fn mac6(a: u8, b: u8, c: u8, d: u8, e: u8, f: u8) -> macaddr {
    macaddr { a, b, c, d, e, f }
}

fn parse(s: &str) -> macaddr8 {
    macaddr8_in(s.as_bytes(), None).unwrap().unwrap()
}

fn out_str(addr: &macaddr8) -> String {
    String::from_utf8(macaddr8_out(addr)).unwrap()
}

// ---------------------------------------------------------------------------
// macaddr8_in / macaddr8_out
// ---------------------------------------------------------------------------

#[test]
fn macaddr8_in_accepts_eui64_notations() {
    let expected = mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05);

    let inputs = [
        "08:00:2b:01:02:03:04:05",
        "08-00-2b-01-02-03-04-05",
        "08.00.2b.01.02.03.04.05",
        "08002b0102030405",
    ];
    for input in inputs {
        assert_eq!(parse(input), expected, "input {input:?}");
    }
}

#[test]
fn macaddr8_in_widens_eui48_input() {
    // A 6-byte address is stored in EUI-64 form with FF FE injected as the
    // 4th and 5th bytes (mac8.c:198-206).
    assert_eq!(
        parse("08:00:2b:01:02:03"),
        mac8(0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03)
    );
}

#[test]
fn macaddr8_in_rejects_bad_syntax() {
    // Trailing two-char garbage forms a 9th "byte" -> count overflows -> fail.
    let err = macaddr8_in(b"08:00:2b:01:02:03:04:05zz", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(
        err.message(),
        "invalid input syntax for type macaddr8: \"08:00:2b:01:02:03:04:05zz\""
    );

    // mixed spacers
    let err = macaddr8_in(b"08:00-2b:01:02:03", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

    // not hex
    let err = macaddr8_in(b"zz:00:2b:01:02:03", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

    // wrong byte count (7 bytes = 14 hex digits)
    let err = macaddr8_in(b"08:00:2b:01:02:03:04", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
}

#[test]
fn macaddr8_in_trailing_whitespace_ok() {
    assert_eq!(
        parse("08:00:2b:01:02:03:04:05   "),
        mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05)
    );
    // 6-byte with trailing whitespace, then widened.
    assert_eq!(
        parse("08:00:2b:01:02:03 "),
        mac8(0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03)
    );
}

#[test]
fn macaddr8_in_leading_whitespace_ok() {
    assert_eq!(
        parse("   08:00:2b:01:02:03:04:05"),
        mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05)
    );
}

#[test]
fn macaddr8_in_space_then_nonspace_invalid() {
    let err = macaddr8_in(b"08:00:2b:01:02:03 zz", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
}

#[test]
fn macaddr8_in_soft_error_uses_escontext() {
    let mut soft = SoftErrorContext::new(true);
    let result = macaddr8_in(b"bad", Some(&mut soft)).unwrap();
    assert!(result.is_none());
    assert!(soft.error_occurred());
    assert_eq!(
        soft.error().unwrap().sqlstate(),
        ERRCODE_INVALID_TEXT_REPRESENTATION
    );
}

#[test]
fn macaddr8_out_is_lowercase_zero_padded() {
    assert_eq!(
        out_str(&mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05)),
        "08:00:2b:01:02:03:04:05"
    );
    assert_eq!(
        out_str(&mac8(0xff, 0x00, 0xab, 0xcd, 0x01, 0x9f, 0x00, 0xfe)),
        "ff:00:ab:cd:01:9f:00:fe"
    );
}

// ---------------------------------------------------------------------------
// comparisons / hashing / bitwise / trunc / set7bit
// ---------------------------------------------------------------------------

#[test]
fn comparisons_match_hibits_lobits_ordering() {
    let lo = mac8(0, 0, 0, 0, 0, 0, 0, 1);
    let hi = mac8(0, 0, 0, 1, 0, 0, 0, 0);

    assert_eq!(macaddr8_cmp(&lo, &hi), -1);
    assert_eq!(macaddr8_cmp(&hi, &lo), 1);
    assert_eq!(macaddr8_cmp(&lo, &lo), 0);
    assert!(macaddr8_lt(&lo, &hi));
    assert!(macaddr8_le(&lo, &lo));
    assert!(macaddr8_gt(&hi, &lo));
    assert!(macaddr8_ge(&hi, &hi));
    assert!(macaddr8_eq(&lo, &lo));
    assert!(macaddr8_ne(&lo, &hi));

    // High byte >= 0x80 must still sort above a lower-magnitude address.
    let a = mac8(0x7f, 0, 0, 0, 0, 0, 0, 0);
    let b = mac8(0x80, 0, 0, 0, 0, 0, 0, 0);
    assert_eq!(macaddr8_cmp(&a, &b), -1);
}

#[test]
fn hashes_use_eight_raw_bytes() {
    let key = mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05);
    let bytes = [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05];
    assert_eq!(hashmacaddr8(&key), hash_bytes(&bytes));
    assert_eq!(
        hashmacaddr8extended(&key, 42),
        hash_bytes_extended(&bytes, 42)
    );
}

#[test]
fn bitwise_and_or_not() {
    let a = mac8(0xf0, 0x0f, 0xaa, 0x55, 0x00, 0xff, 0xf0, 0x0f);
    let b = mac8(0x0f, 0xf0, 0x55, 0xaa, 0xff, 0x00, 0x0f, 0xf0);

    assert_eq!(
        macaddr8_not(&a),
        mac8(0x0f, 0xf0, 0x55, 0xaa, 0xff, 0x00, 0x0f, 0xf0)
    );
    assert_eq!(macaddr8_and(&a, &b), mac8(0, 0, 0, 0, 0, 0, 0, 0));
    assert_eq!(
        macaddr8_or(&a, &b),
        mac8(0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
    );
}

#[test]
fn trunc_zeros_low_five_bytes() {
    let addr = mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05);
    assert_eq!(macaddr8_trunc(&addr), mac8(0x08, 0x00, 0x2b, 0, 0, 0, 0, 0));
}

#[test]
fn set7bit_sets_0x02_in_first_byte() {
    let addr = mac8(0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77);
    assert_eq!(
        macaddr8_set7bit(&addr),
        mac8(0x02, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77)
    );
}

// ---------------------------------------------------------------------------
// conversions
// ---------------------------------------------------------------------------

#[test]
fn macaddrtomacaddr8_inserts_ff_fe() {
    let addr6 = mac6(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03);
    assert_eq!(
        macaddrtomacaddr8(&addr6),
        mac8(0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03)
    );
}

#[test]
fn macaddr8tomacaddr_ok_when_ff_fe() {
    let addr = mac8(0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03);
    assert_eq!(
        macaddr8tomacaddr(&addr).unwrap(),
        mac6(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03)
    );
}

#[test]
fn macaddr8tomacaddr_rejects_non_ff_fe() {
    let addr = mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05);
    let err = macaddr8tomacaddr(&addr).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "macaddr8 data out of range to convert to macaddr"
    );
    assert!(err.hint().is_some());
}

// ---------------------------------------------------------------------------
// recv / send
// ---------------------------------------------------------------------------

#[test]
fn recv_reads_eight_bytes_msb_first() {
    let addr = macaddr8_recv(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]).unwrap();
    assert_eq!(addr, mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05));
}

#[test]
fn recv_six_byte_message_injects_ff_fe() {
    // A 6-byte external message gets FF FE injected as the 4th/5th bytes.
    let addr = macaddr8_recv(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]).unwrap();
    assert_eq!(addr, mac8(0x08, 0x00, 0x2b, 0xff, 0xfe, 0x01, 0x02, 0x03));
}

#[test]
fn recv_short_message_errors() {
    let err = macaddr8_recv(&[0x08, 0x00, 0x2b, 0x01]).unwrap_err();
    assert_eq!(
        err.sqlstate(),
        types_error::ERRCODE_INVALID_BINARY_REPRESENTATION
    );
}

#[test]
fn send_emits_eight_bytes_msb_first() {
    let addr = mac8(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05);
    assert_eq!(
        macaddr8_send(&addr),
        [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03, 0x04, 0x05]
    );
}
