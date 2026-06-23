#![cfg(test)]

use super::*;

#[test]
fn single_byte_db_mbstrlen() {
    // SQL_ASCII (default) is single-byte: pg_mbstrlen == strlen.
    assert_eq!(pg_mbstrlen(b"hello\0world").unwrap(), 5);
    assert_eq!(pg_mbstrlen_with_len(b"hello", 5).unwrap(), 5);
}

#[test]
fn cliplen_single_byte() {
    // SQL_ASCII single-byte: clip honors the byte limit and stops at NUL.
    assert_eq!(pg_mbcliplen(b"abcdef", 6, 3), 3);
    assert_eq!(pg_mbcliplen(b"ab\0def", 6, 5), 2);
}

#[test]
fn byte_sequence_render() {
    assert_eq!(byte_sequence(&[0x80, 0x81, 0x82], 3, 3), "0x80 0x81 0x82");
    assert_eq!(byte_sequence(&[0xff], 1, 1), "0xff");
}

#[test]
fn unicode_to_utf8_roundtrip() {
    let mut buf = [0u8; 5];
    unicode_to_utf8(0x41, &mut buf);
    assert_eq!(&buf[..1], b"A");
    unicode_to_utf8(0x20AC, &mut buf); // euro sign U+20AC
    assert_eq!(&buf[..3], &[0xE2, 0x82, 0xAC]);
    assert_eq!(pg_utf_mblen(&buf), 3);
}

#[test]
fn utf8_increment_ascii() {
    let mut c = [0x41u8];
    assert!(pg_utf8_increment(&mut c));
    assert_eq!(c[0], 0x42);
    let mut c = [0x7Fu8];
    assert!(!pg_utf8_increment(&mut c));
}

#[test]
fn utf8_increment_lengths_5_6_rejected() {
    // C `default` arm: lengths 5 and 6 are rejected without touching the bytes.
    let mut c = [0xF8, 0x80, 0x80, 0x80, 0x80];
    let before = c;
    assert!(!pg_utf8_increment(&mut c));
    assert_eq!(c, before);
}

#[test]
fn utf8_increment_multibyte_last_byte() {
    // 2-byte sequence, last byte below 0xBF: increment the trailing byte.
    let mut c = [0xC3, 0xA0]; // U+00E0
    assert!(pg_utf8_increment(&mut c));
    assert_eq!(c, [0xC3, 0xA1]);
}
