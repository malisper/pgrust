//! Tests for parser.c's own logic: `str_udeescape`, `check_uescapechar`, the
//! surrogate helpers, `mode_token`, and `scanner_errposition`.

use super::*;
use ::mcx::MemoryContext;
use std::sync::Once;

/// Install the `pg_unicode_to_server` seam with a UTF-8 stand-in (the test
/// server encoding is UTF-8), and `pg_mbstrlen_with_len` with a UTF-8 char
/// count. Both are global OnceLocks, so install at most once per test binary.
fn install_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        mb::pg_unicode_to_server::set(|mcx, c| {
            let ch = char::from_u32(c).ok_or_else(|| {
                types_error::PgError::error("invalid Unicode code point")
                    .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR)
            })?;
            let mut buf = [0u8; 4];
            let s = ch.encode_utf8(&mut buf);
            ::mcx::slice_in(mcx, s.as_bytes())
        });
        mb::pg_mbstrlen_with_len::set(|s, limit| {
            let lim = (limit.max(0) as usize).min(s.len());
            core::str::from_utf8(&s[..lim])
                .map(|t| t.chars().count() as i32)
                .unwrap_or(lim as i32)
        });
    });
}

fn deescape(input: &str, escape: u8) -> Result<Vec<u8>, types_error::PgError> {
    install_seams();
    let ctx = MemoryContext::new("udeescape");
    str_udeescape(ctx.mcx(), input.as_bytes(), escape, 0).map(|v| v.to_vec())
}

#[test]
fn plain_string_passes_through() {
    assert_eq!(deescape("hello", b'\\').unwrap(), b"hello");
}

#[test]
fn doubled_escape_is_literal() {
    // `\\` -> a single backslash.
    assert_eq!(deescape(r"a\\b", b'\\').unwrap(), b"a\\b");
}

#[test]
fn four_hex_escape() {
    // \0041 -> 'A'.
    assert_eq!(deescape(r"\0041", b'\\').unwrap(), b"A");
}

#[test]
fn six_hex_escape() {
    // \+000041 -> 'A'.
    assert_eq!(deescape(r"\+000041", b'\\').unwrap(), b"A");
}

#[test]
fn custom_escape_char() {
    // With '!' as the escape character, !0041 -> 'A'.
    assert_eq!(deescape("!0041", b'!').unwrap(), b"A");
}

#[test]
fn surrogate_pair_combines() {
    // U+1F600 as a UTF-16 surrogate pair: \D83D\DE00 -> 4-byte UTF-8.
    let out = deescape(r"\D83D\DE00", b'\\').unwrap();
    assert_eq!(out, "\u{1F600}".as_bytes());
}

#[test]
fn lone_first_surrogate_errors() {
    let err = deescape(r"\D83Dx", b'\\').unwrap_err();
    assert!(err.message().contains("surrogate pair"));
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
}

#[test]
fn lone_second_surrogate_errors() {
    let err = deescape(r"\DE00", b'\\').unwrap_err();
    assert!(err.message().contains("surrogate pair"));
}

#[test]
fn unfinished_surrogate_at_end_errors() {
    let err = deescape(r"\D83D", b'\\').unwrap_err();
    assert!(err.message().contains("surrogate pair"));
}

#[test]
fn invalid_escape_errors_with_hint() {
    // `\zz` is neither doubled nor 4/6 hex digits.
    let err = deescape(r"\zz", b'\\').unwrap_err();
    assert!(err.message().contains("invalid Unicode escape"));
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert!(err.hint().is_some());
}

#[test]
fn zero_codepoint_is_invalid() {
    // \0000 is not a valid Unicode code point (must be > 0).
    let err = deescape(r"\0000", b'\\').unwrap_err();
    assert!(err.message().contains("invalid Unicode escape value"));
}

#[test]
fn error_cursor_is_byte_offset_of_escape() {
    // The error cursor is in - str + position + 3; here position=0, the escape
    // is at byte 2 ("ab\zz"), so the byte cursor is 2 + 0 + 3 = 5.
    install_seams();
    let ctx = MemoryContext::new("c");
    let err = str_udeescape(ctx.mcx(), b"ab\\zz", b'\\', 0).unwrap_err();
    assert_eq!(err.cursor_position(), Some(5));
}

#[test]
fn check_uescapechar_accepts_and_rejects() {
    // Rejected: hex digits, '+', quotes, whitespace.
    assert!(!check_uescapechar(b'a'));
    assert!(!check_uescapechar(b'F'));
    assert!(!check_uescapechar(b'9'));
    assert!(!check_uescapechar(b'+'));
    assert!(!check_uescapechar(b'\''));
    assert!(!check_uescapechar(b'"'));
    assert!(!check_uescapechar(b' '));
    assert!(!check_uescapechar(b'\t'));
    // Accepted: a non-hex letter, punctuation.
    assert!(check_uescapechar(b'!'));
    assert!(check_uescapechar(b'g'));
    assert!(check_uescapechar(b'#'));
}

#[test]
fn surrogate_helpers_match_pg_wchar_ranges() {
    assert!(is_utf16_surrogate_first(0xD800));
    assert!(is_utf16_surrogate_first(0xDBFF));
    assert!(!is_utf16_surrogate_first(0xDC00));
    assert!(is_utf16_surrogate_second(0xDC00));
    assert!(is_utf16_surrogate_second(0xDFFF));
    assert!(!is_valid_unicode_codepoint(0));
    assert!(is_valid_unicode_codepoint(0x41));
    assert!(is_valid_unicode_codepoint(0x10FFFF));
    assert!(!is_valid_unicode_codepoint(0x110000));
    // 😀 -> U+1F600.
    assert_eq!(surrogate_pair_to_codepoint(0xD83D, 0xDE00), 0x1F600);
}

#[test]
fn mode_token_maps_each_mode() {
    use ::parsenodes::RawParseMode::*;
    assert_eq!(mode_token(RAW_PARSE_DEFAULT), None);
    assert_eq!(mode_token(RAW_PARSE_TYPE_NAME), Some(tokens::MODE_TYPE_NAME));
    assert_eq!(mode_token(RAW_PARSE_PLPGSQL_EXPR), Some(tokens::MODE_PLPGSQL_EXPR));
    assert_eq!(mode_token(RAW_PARSE_PLPGSQL_ASSIGN1), Some(tokens::MODE_PLPGSQL_ASSIGN1));
    assert_eq!(mode_token(RAW_PARSE_PLPGSQL_ASSIGN2), Some(tokens::MODE_PLPGSQL_ASSIGN2));
    assert_eq!(mode_token(RAW_PARSE_PLPGSQL_ASSIGN3), Some(tokens::MODE_PLPGSQL_ASSIGN3));
}

#[test]
fn scanner_errposition_converts_byte_to_char_cursor() {
    install_seams();
    // Negative location is a no-op.
    assert_eq!(scanner_errposition(-1, b"abc"), 0);
    // ASCII: byte offset N -> char cursor N+1.
    assert_eq!(scanner_errposition(0, b"abc"), 1);
    assert_eq!(scanner_errposition(2, b"abc"), 3);
    // Multibyte: a 2-byte UTF-8 char before the cursor counts as one char.
    // "é" is 2 bytes; a cursor at byte 2 is the 2nd character.
    assert_eq!(scanner_errposition(2, "éx".as_bytes()), 2);
}
