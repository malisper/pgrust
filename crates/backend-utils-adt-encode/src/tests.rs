//! Tests for `backend-utils-adt-encode`.
//!
//! The codec cores are tested directly on byte slices (matching the faithful
//! port's coverage). The dispatch entry points are exercised via the `*_bytes`
//! cores against an owned [`MemoryContext`]. The `pg_mblen_range` seam is
//! installed once with a single-byte-encoding (SQL_ASCII-style) implementation
//! so the hex/base64 error-snippet tests run deterministically: every byte is a
//! 1-byte character (the value those error messages assume for ASCII bytes).

use super::*;
use mcx::MemoryContext;
use std::sync::Once;
use std::vec;

static INSTALL: Once = Once::new();

/// Install the `pg_mblen_range` seam with a single-byte-encoding behavior: the
/// leading byte is always a 1-byte character (SQL_ASCII / any single-byte server
/// encoding), which is what the `%.*s` error-snippet tests (all ASCII offending
/// bytes) expect.
fn install_mblen_single_byte() {
    INSTALL.call_once(|| {
        pg_mblen_range::set(|_mbstr| 1);
    });
}

// ---- hex ----

#[test]
fn hex_encode_roundtrip() {
    let src = b"\x00\x01\xab\xff";
    let mut dst = vec![0u8; hex_enc_len(src) as usize];
    let n = hex_encode(src, &mut dst);
    assert_eq!(&dst[..n as usize], b"0001abff");
}

#[test]
fn hex_decode_basic_and_whitespace() {
    let src = b"00 01\nab\tff\r";
    let mut dst = vec![0u8; hex_dec_len(src) as usize + 1];
    let n = hex_decode(src, &mut dst).unwrap();
    assert_eq!(&dst[..n as usize], b"\x00\x01\xab\xff");
}

#[test]
fn hex_decode_invalid_digit_hard_error() {
    install_mblen_single_byte();
    let src = b"0g";
    let mut dst = vec![0u8; 8];
    let err = hex_decode(src, &mut dst).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(err.message(), "invalid hexadecimal digit: \"g\"");
}

#[test]
fn hex_decode_odd_digits() {
    let src = b"abc";
    let mut dst = vec![0u8; 8];
    let err = hex_decode(src, &mut dst).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(
        err.message(),
        "invalid hexadecimal data: odd number of digits"
    );
}

#[test]
fn hex_decode_safe_collects_soft_error() {
    install_mblen_single_byte();
    let src = b"0z";
    let mut dst = vec![0u8; 8];
    let mut ctx = SoftErrorContext::new(true);
    let n = hex_decode_safe(src, &mut dst, Some(&mut ctx)).unwrap();
    assert_eq!(n, 0);
    assert!(ctx.error_occurred());
    let err = ctx.take_error().unwrap();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(err.message(), "invalid hexadecimal digit: \"z\"");
}

// ---- base64 ----

#[test]
fn base64_encode_known_vectors() {
    let cases: &[(&[u8], &[u8])] = &[
        (b"", b""),
        (b"f", b"Zg=="),
        (b"fo", b"Zm8="),
        (b"foo", b"Zm9v"),
        (b"foob", b"Zm9vYg=="),
        (b"fooba", b"Zm9vYmE="),
        (b"foobar", b"Zm9vYmFy"),
    ];
    for (src, expected) in cases {
        let mut dst = vec![0u8; pg_base64_enc_len(src) as usize + 4];
        let n = pg_base64_encode(src, &mut dst).unwrap();
        assert_eq!(&dst[..n as usize], *expected, "encoding {src:?}");
    }
}

#[test]
fn base64_decode_known_vectors() {
    let cases: &[(&[u8], &[u8])] = &[
        (b"", b""),
        (b"Zg==", b"f"),
        (b"Zm8=", b"fo"),
        (b"Zm9v", b"foo"),
        (b"Zm9vYg==", b"foob"),
        (b"Zm9vYmE=", b"fooba"),
        (b"Zm9vYmFy", b"foobar"),
    ];
    for (src, expected) in cases {
        let mut dst = vec![0u8; pg_base64_dec_len(src) as usize + 4];
        let n = pg_base64_decode(src, &mut dst).unwrap();
        assert_eq!(&dst[..n as usize], *expected, "decoding {src:?}");
    }
}

#[test]
fn base64_encode_wraps_at_76() {
    let src = vec![b'A'; 60]; // 60 bytes -> 80 base64 chars + a newline
    let mut dst = vec![0u8; pg_base64_enc_len(&src) as usize + 4];
    let n = pg_base64_encode(&src, &mut dst).unwrap();
    let out = &dst[..n as usize];
    assert_eq!(out.iter().filter(|&&c| c == b'\n').count(), 1);
    assert_eq!(out[76], b'\n');
}

#[test]
fn base64_decode_invalid_symbol() {
    install_mblen_single_byte();
    let src = b"Zm9v*g==";
    let mut dst = vec![0u8; 16];
    let err = pg_base64_decode(src, &mut dst).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(
        err.message(),
        "invalid symbol \"*\" found while decoding base64 sequence"
    );
}

#[test]
fn base64_decode_unexpected_equals() {
    let src = b"Z=";
    let mut dst = vec![0u8; 16];
    let err = pg_base64_decode(src, &mut dst).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(
        err.message(),
        "unexpected \"=\" while decoding base64 sequence"
    );
}

#[test]
fn base64_decode_invalid_end() {
    let src = b"Zm9"; // pos != 0 at the end
    let mut dst = vec![0u8; 16];
    let err = pg_base64_decode(src, &mut dst).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(err.message(), "invalid base64 end sequence");
}

// ---- escape ----

#[test]
fn escape_encode_special_bytes() {
    let src = b"a\x00b\\c\xff";
    let mut dst = vec![0u8; esc_enc_len(src) as usize];
    let n = esc_encode(src, &mut dst).unwrap();
    assert_eq!(&dst[..n as usize], b"a\\000b\\\\c\\377");
}

#[test]
fn escape_decode_roundtrip() {
    let encoded = b"a\\000b\\\\c\\377";
    let mut dst = vec![0u8; esc_dec_len(encoded).unwrap() as usize];
    let n = esc_decode(encoded, &mut dst).unwrap();
    assert_eq!(&dst[..n as usize], b"a\x00b\\c\xff");
}

#[test]
fn escape_decode_lone_backslash_errors() {
    let src = b"a\\b";
    let err = esc_dec_len(src).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.message(), "invalid input syntax for type bytea");
}

// ---- dispatch ----

#[test]
fn find_encoding_is_case_insensitive() {
    assert!(pg_find_encoding("hex").is_some());
    assert!(pg_find_encoding("HEX").is_some());
    assert!(pg_find_encoding("Base64").is_some());
    assert!(pg_find_encoding("ESCAPE").is_some());
    assert!(pg_find_encoding("nope").is_none());
    assert!(pg_find_encoding("hexx").is_none());
}

// ---- dispatch entry points (binary_encode/binary_decode cores) ----

#[test]
fn binary_encode_hex_via_bytes_core() {
    let ctx = MemoryContext::new("encode-test");
    assert_eq!(
        binary_encode_bytes(ctx.mcx(), b"\xde\xad\xbe\xef", "hex").unwrap()[..],
        b"deadbeef"[..]
    );
    assert_eq!(
        binary_decode_bytes(ctx.mcx(), b"Zm9vYmFy", "base64").unwrap()[..],
        b"foobar"[..]
    );
}

#[test]
fn binary_encode_unrecognized_encoding() {
    let ctx = MemoryContext::new("encode-test");
    let err = binary_encode_bytes(ctx.mcx(), b"abc", "rot13").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(err.message(), "unrecognized encoding: \"rot13\"");
}

#[test]
fn binary_decode_escape_lone_backslash_errors_before_alloc() {
    // The `escape` decode_len (`esc_dec_len`) errors on a lone backslash before
    // any allocation, surfacing through `binary_decode`.
    let ctx = MemoryContext::new("encode-test");
    let err = binary_decode_bytes(ctx.mcx(), b"a\\b", "escape").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(err.message(), "invalid input syntax for type bytea");
}

#[test]
fn binary_encode_escape_roundtrips_through_decode() {
    let ctx = MemoryContext::new("encode-test");
    let raw: &[u8] = b"x\x00y\xfez";
    let encoded = binary_encode_bytes(ctx.mcx(), raw, "escape").unwrap();
    let decoded = binary_decode_bytes(ctx.mcx(), &encoded, "escape").unwrap();
    assert_eq!(&decoded[..], raw);
}

// ---- installed seams ----

#[test]
fn seam_bodies_match_codec_cores() {
    let ctx = MemoryContext::new("encode-test");
    let hex = seam_hex_encode(ctx.mcx(), b"\x00\x01\xab\xff").unwrap();
    assert_eq!(&hex[..], b"0001abff");
    let raw = seam_hex_decode_safe(ctx.mcx(), b"0001abff", false)
        .unwrap()
        .unwrap();
    assert_eq!(&raw[..], b"\x00\x01\xab\xff");
}
