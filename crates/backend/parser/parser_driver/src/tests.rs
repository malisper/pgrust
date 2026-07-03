use mcx::MemoryContext;
use wchar::PG_UTF8;

use crate::{check_uescapechar, str_udeescape, UdeescapeError};

fn de(s: &[u8], escape: u8) -> Result<alloc::vec::Vec<u8>, UdeescapeError> {
    let ctx = MemoryContext::new("t");
    str_udeescape(ctx.mcx(), s, escape, 0, PG_UTF8).map(|v| v[..].to_vec())
}

#[test]
fn plain_text_and_doubled_escape() {
    assert_eq!(de(b"data", b'\\').unwrap(), b"data");
    assert_eq!(de(br"d\\ata", b'\\').unwrap(), br"d\ata");
    assert_eq!(de(b"d!!ata", b'!').unwrap(), b"d!ata");
}

#[test]
fn four_and_six_digit_escapes() {
    assert_eq!(de(br"\0041", b'\\').unwrap(), b"A");
    assert_eq!(de(br"\+000041", b'\\').unwrap(), b"A");
    assert_eq!(de(br"\00e9x", b'\\').unwrap(), "\u{e9}x".as_bytes());
    assert_eq!(de(br"\+01F600", b'\\').unwrap(), "\u{1F600}".as_bytes());
}

#[test]
fn surrogate_pairs_combine() {
    // U+1D5B4 as UTF-16 pair D835/DDB4, in both escape widths.
    assert_eq!(de(br"\d835\ddb4", b'\\').unwrap(), "\u{1D5B4}".as_bytes());
    assert_eq!(de(br"\d835\+00ddb4", b'\\').unwrap(), "\u{1D5B4}".as_bytes());
}

#[test]
fn invalid_escape_reports_hint_and_location() {
    let err = de(br"ab\00zz", b'\\').unwrap_err();
    assert_eq!(err.message, "invalid Unicode escape");
    assert_eq!(err.hint, Some("Unicode escapes must be \\XXXX or \\+XXXXXX."));
    // in - str + position + 3, position = 0.
    assert_eq!(err.location, 2 + 3);
}

#[test]
fn invalid_value_and_pairs() {
    let err = de(br"\+110000", b'\\').unwrap_err();
    assert_eq!(err.message, "invalid Unicode escape value");

    for bad in [
        br"\d835\0041".as_slice(), // pair first + non-second
        br"\ddb4",                 // bare second half
        br"\d835x",                // pair first + plain char
        br"\d835",                 // unfinished pair at end
        br"\d835\\",               // pair first + doubled escape
    ] {
        let err = de(bad, b'\\').unwrap_err();
        assert_eq!(err.message, "invalid Unicode surrogate pair", "input {bad:?}");
    }
}

#[test]
fn uescapechar_rejects_hex_quote_space() {
    for c in [b'a', b'F', b'0', b'+', b'\'', b'"', b' ', b'\t'] {
        assert!(!check_uescapechar(c));
    }
    for c in [b'!', b'*', b'x', b'~', b'y'] {
        assert!(check_uescapechar(c));
    }
}

#[test]
fn raw_parser_parses_select_1() {
    crate::init_seams();
    let ctx = MemoryContext::new("t");
    let stmts = parser_seams::raw_parser::call(
        ctx.mcx(),
        "select 1",
        parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
    )
    .unwrap();
    assert_eq!(stmts.len(), 1);
}

#[test]
#[should_panic(expected = "gram")]
fn base_yylex_is_deferred() {
    crate::base_yylex();
}
