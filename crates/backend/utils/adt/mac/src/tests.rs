//! Tests for the idiomatic `mac.c` port. Golden values cross-checked against
//! PostgreSQL's `src/test/regress/expected/macaddr.out` canonical I/O.

extern crate std;

use super::*;

fn mac(a: u8, b: u8, c: u8, d: u8, e: u8, f: u8) -> macaddr {
    macaddr { a, b, c, d, e, f }
}

fn parse(s: &str) -> macaddr {
    macaddr_in(s.as_bytes(), None).unwrap().unwrap()
}

fn out_str(addr: &macaddr) -> String {
    String::from_utf8(macaddr_out(addr)).unwrap()
}

// ---------------------------------------------------------------------------
// macaddr_in / macaddr_out
// ---------------------------------------------------------------------------

#[test]
fn macaddr_in_accepts_all_notations() {
    let expected = mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03);

    let inputs = [
        "08:00:2b:01:02:03",
        "08-00-2b-01-02-03",
        "08002b:010203",
        "08002b-010203",
        "0800.2b01.0203",
        "0800-2b01-0203",
        "08002b010203",
    ];
    for input in inputs {
        assert_eq!(parse(input), expected, "input {input:?}");
    }
}

#[test]
fn macaddr_in_rejects_trailing_junk_and_bad_syntax() {
    let err = macaddr_in(b"08:00:2b:01:02:03z", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(
        err.message(),
        "invalid input syntax for type macaddr: \"08:00:2b:01:02:03z\""
    );

    let err = macaddr_in(b"08:00:2b", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

    let err = macaddr_in(b"not-a-mac", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

    let err = macaddr_in(b"0800:2b01:0203", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(
        err.message(),
        "invalid input syntax for type macaddr: \"0800:2b01:0203\""
    );
    let err = macaddr_in(b"not even close", None).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid input syntax for type macaddr: \"not even close\""
    );
}

#[test]
fn macaddr_in_rejects_out_of_range_octets() {
    // The colon form uses %x (unbounded), so an octet can exceed 0xff.
    let err = macaddr_in(b"08:00:2b:01:02:100", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(
        err.message(),
        "invalid octet value in \"macaddr\" value: \"08:00:2b:01:02:100\""
    );
}

#[test]
fn macaddr_in_trailing_whitespace_ok() {
    // %1s skips whitespace and finds nothing -> count stays 6 -> accepted.
    assert_eq!(
        parse("08:00:2b:01:02:03   "),
        mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03)
    );
}

#[test]
fn macaddr_in_soft_error_uses_escontext() {
    let mut soft = SoftErrorContext::new(true);
    let result = macaddr_in(b"bad", Some(&mut soft)).unwrap();
    assert!(result.is_none());
    assert!(soft.error_occurred());
    assert_eq!(
        soft.error().unwrap().sqlstate(),
        ERRCODE_INVALID_TEXT_REPRESENTATION
    );
}

#[test]
fn macaddr_out_is_lowercase_zero_padded() {
    assert_eq!(out_str(&mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03)), "08:00:2b:01:02:03");
    assert_eq!(out_str(&mac(0xff, 0x00, 0xab, 0xcd, 0x01, 0x9f)), "ff:00:ab:cd:01:9f");
}

#[test]
fn macaddr_in_honors_glibc_0x_prefix() {
    assert_eq!(
        parse("0x8:0x0:0x2b:0x1:0x2:0x3"),
        mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03)
    );
}

#[test]
fn macaddr_in_0x_prefix_rollback_rejects() {
    let err = macaddr_in(b"0x:0:0:0:0:0", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(
        err.message(),
        "invalid input syntax for type macaddr: \"0x:0:0:0:0:0\""
    );
}

#[test]
fn scan_hex_0x_prefix_value_and_rollback() {
    let mut s = Scanner::new(b"0x2b");
    assert_eq!(s.scan_hex(None), Some(0x2b));
    assert_eq!(s.pos, 4);

    let mut s = Scanner::new(b"0x:");
    assert_eq!(s.scan_hex(None), Some(0));
    assert_eq!(s.peek(), Some(b'x'));

    let mut s = Scanner::new(b"0xff");
    assert_eq!(s.scan_hex(Some(2)), Some(0));
    assert_eq!(s.peek(), Some(b'x'));

    let mut s = Scanner::new(b"0X1F");
    assert_eq!(s.scan_hex(None), Some(0x1f));
}

// ---------------------------------------------------------------------------
// comparisons / hashing / bitwise / trunc
// ---------------------------------------------------------------------------

#[test]
fn comparisons_match_hibits_lobits_ordering() {
    let lo = mac(0x00, 0x00, 0x00, 0x00, 0x00, 0x01);
    let hi = mac(0x00, 0x00, 0x01, 0x00, 0x00, 0x00);

    assert_eq!(macaddr_cmp(&lo, &hi), -1);
    assert_eq!(macaddr_cmp(&hi, &lo), 1);
    assert_eq!(macaddr_cmp(&lo, &lo), 0);
    assert!(macaddr_lt(&lo, &hi));
    assert!(macaddr_le(&lo, &lo));
    assert!(macaddr_gt(&hi, &lo));
    assert!(macaddr_ge(&hi, &hi));
    assert!(macaddr_eq(&lo, &lo));
    assert!(macaddr_ne(&lo, &hi));
    let a = mac(1, 2, 3, 0, 0, 1);
    let b = mac(1, 2, 3, 0, 0, 2);
    assert_eq!(macaddr_cmp(&a, &b), -1);
}

#[test]
fn hashes_use_six_raw_bytes() {
    let key = mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03);
    assert_eq!(hashmacaddr(&key), hash_bytes(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]));
    assert_eq!(
        hashmacaddrextended(&key, 42),
        hash_bytes_extended(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03], 42)
    );
}

#[test]
fn bitwise_and_or_not() {
    let a = mac(0xf0, 0x0f, 0xaa, 0x55, 0x00, 0xff);
    let b = mac(0x0f, 0xf0, 0x55, 0xaa, 0xff, 0x00);

    assert_eq!(macaddr_not(&a), mac(0x0f, 0xf0, 0x55, 0xaa, 0xff, 0x00));
    assert_eq!(macaddr_and(&a, &b), mac(0, 0, 0, 0, 0, 0));
    assert_eq!(macaddr_or(&a, &b), mac(0xff, 0xff, 0xff, 0xff, 0xff, 0xff));
}

#[test]
fn trunc_zeros_low_three_bytes() {
    let addr = mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03);
    assert_eq!(macaddr_trunc(&addr), mac(0x08, 0x00, 0x2b, 0, 0, 0));
}

#[test]
fn fast_cmp_matches_cmp_internal() {
    let lo = mac(0, 0, 0, 0, 0, 1);
    let hi = mac(0, 0, 1, 0, 0, 0);
    assert_eq!(macaddr_fast_cmp(&lo, &hi), macaddr_cmp_internal(&lo, &hi));
}

#[test]
fn abbrev_convert_packs_and_byteswaps() {
    let addr = mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03);
    let res = macaddr_abbrev_convert_bits(&addr);

    // Recompute the expected value the same way the C code does.
    let mut bytes = [0u8; SIZEOF_DATUM];
    let src = [0x08u8, 0x00, 0x2b, 0x01, 0x02, 0x03];
    if SIZEOF_DATUM == 8 {
        bytes[..6].copy_from_slice(&src);
    } else {
        bytes.copy_from_slice(&src[..SIZEOF_DATUM]);
    }
    let mut expected = usize::from_ne_bytes(bytes);
    if cfg!(target_endian = "little") {
        expected = expected.swap_bytes();
    }
    assert_eq!(res, expected);
}

// ---------------------------------------------------------------------------
// recv / send
// ---------------------------------------------------------------------------

#[test]
fn recv_reads_six_bytes_msb_first() {
    let addr = macaddr_recv(&[0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]).unwrap();
    assert_eq!(addr, mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03));
}

#[test]
fn recv_short_message_errors() {
    let err = macaddr_recv(&[0x08, 0x00, 0x2b]).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_BINARY_REPRESENTATION);
}

#[test]
fn send_emits_six_bytes_msb_first() {
    let addr = mac(0x08, 0x00, 0x2b, 0x01, 0x02, 0x03);
    let body = macaddr_send(&addr);
    assert_eq!(body, [0x08, 0x00, 0x2b, 0x01, 0x02, 0x03]);
}
