use mcx::MemoryContext;
use wchar::PG_UTF8;

use crate::{check_uescapechar, str_udeescape, UdeescapeError, UdeescapeFailure};

fn de(s: &[u8], escape: u8) -> Result<alloc::vec::Vec<u8>, UdeescapeError> {
    let ctx = MemoryContext::new("t");
    str_udeescape(ctx.mcx(), s, escape, 0, PG_UTF8).map(|v| v[..].to_vec()).map_err(
        |e| match e {
            UdeescapeFailure::Escape(e) => e,
            UdeescapeFailure::Hard { error, .. } => {
                panic!("unexpected hard failure: {}", error.message())
            }
        },
    )
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

// parser.c:42 raw_parser accepts every RawParseMode; under RAW_PARSE_TYPE_NAME
// gram.y's parse_toplevel yields a TypeName list (not RawStmt). The RawStmt-typed
// seam cannot return that shape; a caller asking for it must get a catchable
// elog(ERROR)-style XX000, not a process panic (audit-18.6
// a186-candidate-fp-parser-parser-3927db86cf686806eaf9-1).
#[test]
fn raw_parser_type_name_mode_is_a_catchable_error_not_a_panic() {
    let ctx = MemoryContext::new("t");
    let err = crate::raw_parser(ctx.mcx(), "int", parser_seams::RawParseMode::RAW_PARSE_TYPE_NAME)
        .err()
        .expect("TYPE_NAME through the RawStmt seam is an error");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    assert_eq!(err.message(), "unexpected node type: T_TypeName");
}

// The TYPE_NAME consumer (parse_type.c typeStringToTypeName) reads the TypeName
// list straight from gram_core::raw_parser — the C-exact port of parser.c:42
// for every mode.
#[test]
fn gram_core_raw_parser_serves_type_name_mode() {
    let ctx = MemoryContext::new("t");
    let list = gram_core::raw_parser(
        ctx.mcx(),
        "int",
        parser_seams::RawParseMode::RAW_PARSE_TYPE_NAME,
    )
    .unwrap();
    assert_eq!(list.len(), 1);
    assert!(list.first().unwrap().as_type_name().is_some());
}

// parser.c:111 base_yylex is the lookahead filter between scanner and grammar;
// its port lives inside gram_core's Parser and is reached through raw_parser.
// Every merge arm (parser.c:172-372: FORMAT_LA, NOT_LA, NULLS_LA, WITH_LA,
// WITHOUT_LA, UIDENT/USCONST + UESCAPE) is exercised here through the seam
// (audit-18.6 a186-candidate-fp-parser-parser-6cee8c22c54827d13e8b-1).
#[test]
fn base_yylex_lookahead_merges_are_reached_through_raw_parser() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let ok = |q: &str| {
        let stmts = crate::raw_parser(mcx, q, parser_seams::RawParseMode::RAW_PARSE_DEFAULT)
            .unwrap_or_else(|e| panic!("{q}: {}", e.message()));
        assert_eq!(stmts.len(), 1, "{q}");
    };
    ok("select json_serialize('1' format json)");
    ok("select 1 where 'a' not like 'b' and 1 not in (2) and 1 not between 0 and 2");
    ok("select 1 order by 1 nulls first");
    ok("create table t (a timestamp with time zone, b time without time zone)");
    ok("select * from unnest(array[1]) with ordinality");
    ok(r"select U&'d\0061t\+000061', U&'d!0061t!+000061' uescape '!'");
    ok(r#"select U&"d\0061t\+000061" from t"#);

    // parser.c:272-280: the UESCAPE tail errors are scanner_yyerror syntax errors
    // positioned at the third token.
    let err = crate::raw_parser(
        mcx,
        r"select U&'\0041' uescape '+'",
        parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
    )
    .err()
    .unwrap();
    assert_eq!(err.message(), "invalid Unicode escape character at or near \"'+'\"");
    assert_eq!(err.cursor_position(), Some(26));
    let err = crate::raw_parser(
        mcx,
        r"select U&'\0041' uescape 1",
        parser_seams::RawParseMode::RAW_PARSE_DEFAULT,
    )
    .err()
    .unwrap();
    assert_eq!(err.message(), "UESCAPE must be followed by a simple string literal at or near \"1\"");
}
