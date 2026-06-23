//! Tests for the `ascii.c` port.
//!
//! Run single-threaded (`--test-threads=1`): the encnames / mbutils seams are
//! process-global install-once slots, so the tests install deterministic mocks
//! once and must not race. The harness gates this crate's tests under the
//! workspace `cargo test`, where these mocks would collide with the real
//! owners' installs; the mock installs below are therefore guarded so they run
//! at most once regardless of which install wins.

use super::*;
use std::sync::Once;
use ::types_wchar::encoding::{PG_LATIN1, PG_LATIN2, PG_UTF8, PG_WIN1250, PG_SQL_ASCII};

static INSTALL: Once = Once::new();

/// Install the cross-crate seams this crate calls, with deterministic mocks, so
/// the `to_ascii_*` entry points can run in isolation.
fn install_seams() {
    INSTALL.call_once(|| {
        encnames_seams::pg_char_to_encoding::set(|name| match name {
            "LATIN1" => PG_LATIN1,
            "LATIN2" => PG_LATIN2,
            "WIN1250" => PG_WIN1250,
            "UTF8" => PG_UTF8,
            "SQL_ASCII" => PG_SQL_ASCII,
            _ => -1,
        });
        encnames_seams::pg_encoding_to_char::set(|enc| match enc {
            PG_LATIN1 => "LATIN1",
            PG_LATIN2 => "LATIN2",
            PG_WIN1250 => "WIN1250",
            PG_UTF8 => "UTF8",
            _ => "",
        });
        mbutils_seams::get_database_encoding::set(|| PG_LATIN2);
    });
}

/// Wrap content bytes as the content-only `text` payload an `FmgrArg::Ref`
/// carries.
fn text_payload(content: &[u8]) -> RefPayload {
    RefPayload::Varlena(content.to_vec())
}

/// Extract the content bytes from a by-reference `text` result.
fn out_content<'a>(out: &'a FmgrOut) -> &'a [u8] {
    match out {
        FmgrOut::Ref(RefPayload::Varlena(b)) => b.as_slice(),
        _ => panic!("to_ascii returns a by-reference text"),
    }
}

#[test]
fn converts_supported_latin1_bytes_to_ascii() {
    // 'A' passes through; 0x80 (in [128,160)) -> ' '; 0xa0 (==range) -> table[0]
    // == ' '; 0xc0 -> 'A'; 0xe9 -> 'e'; 0xff -> 'y'.
    assert_eq!(
        pg_to_ascii(&[b'A', 0x80, 0xa0, 0xc0, 0xe9, 0xff], PG_LATIN1).unwrap(),
        b"A  Aey"
    );
}

#[test]
fn converts_win1250_range_128_bytes() {
    // WIN1250 uses range 128, so high bytes index directly into the table.
    assert_eq!(
        pg_to_ascii(&[0x80, 0x82, 0xa5, 0xb9, b'Z'], PG_WIN1250).unwrap(),
        b" 'AaZ"
    );
}

#[test]
fn output_length_equals_input_length() {
    let input = [b'x', 0x80, 0xc0, 0xff, b'y'];
    assert_eq!(pg_to_ascii(&input, PG_LATIN1).unwrap().len(), input.len());
}

#[test]
fn empty_input_yields_empty_output() {
    assert_eq!(pg_to_ascii(&[], PG_LATIN1).unwrap(), b"");
}

#[test]
fn unsupported_encoding_matches_postgres_error() {
    install_seams();
    let error = pg_to_ascii(b"abc", PG_UTF8).unwrap_err();

    assert_eq!(error.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(
        error.message(),
        "encoding conversion from UTF8 to ASCII not supported"
    );
}

#[test]
fn encname_error_matches_postgres() {
    install_seams();
    let payload = text_payload(b"abc");
    let err = to_ascii_encname(FmgrArg::Ref(&payload), "not-an-encoding").unwrap_err();

    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
    assert_eq!(err.message(), "not-an-encoding is not a valid encoding name");
}

#[test]
fn encoding_code_error_matches_postgres() {
    let payload = text_payload(b"abc");
    let err = to_ascii_enc(FmgrArg::Ref(&payload), -1).unwrap_err();

    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
    assert_eq!(err.message(), "-1 is not a valid encoding code");
}

#[test]
fn to_ascii_enc_returns_text_content() {
    let payload = text_payload(&[0xc0, 0xe9]);
    let out = to_ascii_enc(FmgrArg::Ref(&payload), PG_LATIN1).unwrap();
    assert_eq!(out_content(&out), b"Ae");
}

#[test]
fn to_ascii_encname_returns_text_content() {
    install_seams();
    let payload = text_payload(&[0xc0, 0xe9]);
    let out = to_ascii_encname(FmgrArg::Ref(&payload), "LATIN1").unwrap();
    assert_eq!(out_content(&out), b"Ae");
}

#[test]
fn to_ascii_default_uses_database_encoding() {
    install_seams(); // mock GetDatabaseEncoding reports LATIN2
    let payload = text_payload(&[0xa1]); // LATIN2: range 160, table[0xa1-160] == 'A'
    let out = to_ascii_default(FmgrArg::Ref(&payload)).unwrap();
    assert_eq!(out_content(&out), b"A");
}

#[test]
fn ascii_safe_strlcpy_sanitizes_and_nul_terminates() {
    let mut dest = [b'X'; 8];
    ascii_safe_strlcpy(&mut dest, b"a\n\x01\xffbcdef");

    // 'a' and '\n' pass; 0x01 and 0xff -> '?'; then 'bcd' (room for 7 + NUL).
    assert_eq!(&dest, b"a\n??bcd\0");
}

#[test]
fn ascii_safe_strlcpy_stops_at_embedded_nul() {
    let mut dest = [b'X'; 8];
    ascii_safe_strlcpy(&mut dest, b"ab\0cd");
    assert_eq!(&dest[..3], b"ab\0");
}

#[test]
fn ascii_safe_strlcpy_handles_zero_length_destination() {
    let mut dest: [u8; 0] = [];
    ascii_safe_strlcpy(&mut dest, b"abc");
    assert!(dest.is_empty());
}
