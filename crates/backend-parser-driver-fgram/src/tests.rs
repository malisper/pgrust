//! Tests for `base_yylex` token merging, the mode-token seed, and
//! `str_udeescape` Unicode de-escaping.

use super::*;
use backend_parser_scan::Utf8UnicodeSeam;
use backend_utils_mb::SetDatabaseEncoding;
use std::sync::Mutex;

static ENC_LOCK: Mutex<()> = Mutex::new(());

fn lex(input: &str) -> Vec<Token> {
    let _g = ENC_LOCK.lock().unwrap();
    SetDatabaseEncoding(pgrust_pg_ffi::PG_UTF8).unwrap();
    lex_tokens(input).expect("lex ok")
}

fn codes(input: &str) -> Vec<i32> {
    lex(input).into_iter().map(|t| t.token).collect()
}

#[test]
fn not_like_merges_to_not_la() {
    let c = codes("a NOT LIKE b");
    // IDENT NOT_LA LIKE IDENT
    assert_eq!(c[1], tokens::NOT_LA);
    assert_eq!(c[2], tokens::LIKE);
}

#[test]
fn not_in_merges_to_not_la() {
    let c = codes("a NOT IN (1)");
    assert_eq!(c[1], tokens::NOT_LA);
}

#[test]
fn plain_not_stays_not() {
    // NOT not followed by BETWEEN/IN/LIKE/ILIKE/SIMILAR stays NOT.
    let c = codes("NOT a");
    assert_eq!(c[0], tokens::NOT);
    assert_eq!(c[1], tokens::IDENT);
}

#[test]
fn nulls_first_merges_to_nulls_la() {
    let c = codes("NULLS FIRST");
    assert_eq!(c[0], tokens::NULLS_LA);
    assert_eq!(c[1], tokens::FIRST_P);
}

#[test]
fn with_time_merges_to_with_la() {
    let c = codes("WITH TIME");
    assert_eq!(c[0], tokens::WITH_LA);
    assert_eq!(c[1], tokens::TIME);
}

#[test]
fn with_ordinality_merges_to_with_la() {
    let c = codes("WITH ORDINALITY");
    assert_eq!(c[0], tokens::WITH_LA);
}

#[test]
fn plain_with_stays_with() {
    let c = codes("WITH cte AS");
    assert_eq!(c[0], tokens::WITH);
}

#[test]
fn without_time_merges_to_without_la() {
    let c = codes("WITHOUT TIME");
    assert_eq!(c[0], tokens::WITHOUT_LA);
}

#[test]
fn format_json_merges_to_format_la() {
    let c = codes("FORMAT JSON");
    assert_eq!(c[0], tokens::FORMAT_LA);
    assert_eq!(c[1], tokens::JSON);
}

#[test]
fn lookahead_token_is_not_lost() {
    // After a merge, the lookahead token must still be returned next.
    let toks = lex("NULLS FIRST x");
    let c: Vec<i32> = toks.iter().map(|t| t.token).collect();
    assert_eq!(c[0], tokens::NULLS_LA);
    assert_eq!(c[1], tokens::FIRST_P);
    assert_eq!(c[2], tokens::IDENT);
    assert_eq!(toks[2].value, CoreYYSTYPE::Str(b"x".to_vec()));
}

#[test]
fn locations_preserved_through_filter() {
    // "a NOT LIKE b": a@0, NOT@2, LIKE@6, b@11
    let toks = lex("a NOT LIKE b");
    assert_eq!(toks[0].location, 0);
    assert_eq!(toks[1].location, 2); // NOT_LA points at NOT
    assert_eq!(toks[2].location, 6); // LIKE
    assert_eq!(toks[3].location, 11);
}

#[test]
fn uescape_default_unicode_string() {
    // U&'d\0061t\0061' with default escape '\' -> 'data'
    let toks = lex(r"U&'d\0061t\0061'");
    assert_eq!(toks.len(), 1);
    assert_eq!(toks[0].token, tokens::SCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"data".to_vec()));
}

#[test]
fn uescape_custom_escape_char() {
    // U&'d!0061t!0061' UESCAPE '!' -> 'data'
    let toks = lex(r"U&'d!0061t!0061' UESCAPE '!'");
    assert_eq!(toks.len(), 1);
    assert_eq!(toks[0].token, tokens::SCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"data".to_vec()));
}

#[test]
fn uescape_doubled_escape_is_literal() {
    // U&'a\\b' -> a\b  (doubled escape produces a literal backslash)
    let toks = lex(r"U&'a\\b'");
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"a\\b".to_vec()));
}

#[test]
fn uident_unicode_identifier() {
    // U&"d\0061ta" -> identifier "data"
    let toks = lex(r#"U&"d\0061ta""#);
    assert_eq!(toks.len(), 1);
    assert_eq!(toks[0].token, tokens::IDENT);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"data".to_vec()));
}

#[test]
fn str_udeescape_six_digit_form() {
    // \+000061 -> 'a'
    let out = str_udeescape(br"\+000061", b'\\', 0, &Utf8UnicodeSeam).unwrap();
    assert_eq!(out, b"a");
}

#[test]
fn str_udeescape_surrogate_pair() {
    // U+1F600 via surrogate pair \D83D\DE00 -> 4-byte UTF-8.
    let out = str_udeescape(br"\D83D\DE00", b'\\', 0, &Utf8UnicodeSeam).unwrap();
    assert_eq!(out, vec![0xF0, 0x9F, 0x98, 0x80]);
}

#[test]
fn str_udeescape_lone_surrogate_errors() {
    let err = str_udeescape(br"\DE00", b'\\', 0, &Utf8UnicodeSeam);
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().message, "invalid Unicode surrogate pair");
}

#[test]
fn check_uescapechar_rules() {
    assert!(!check_uescapechar(b'+'));
    assert!(!check_uescapechar(b'\''));
    assert!(!check_uescapechar(b'"'));
    assert!(!check_uescapechar(b'A')); // hex digit
    assert!(!check_uescapechar(b' '));
    assert!(check_uescapechar(b'!'));
    assert!(check_uescapechar(b'#'));
}

#[test]
fn mode_token_seeds_lookahead() {
    // RAW_PARSE_TYPE_NAME injects MODE_TYPE_NAME as the first token.
    let _g = ENC_LOCK.lock().unwrap();
    SetDatabaseEncoding(pgrust_pg_ffi::PG_UTF8).unwrap();
    let scanner = Scanner::with_unicode_seam(b"int", ScannerSettings::default(), &Utf8UnicodeSeam);
    let seed = Some(Token {
        token: tokens::MODE_TYPE_NAME,
        value: CoreYYSTYPE::None,
        location: 0,
    });
    let mut lexer = BaseLexer::new(scanner, seed, &Utf8UnicodeSeam);
    let first = lexer.base_yylex().unwrap();
    assert_eq!(first.token, tokens::MODE_TYPE_NAME);
    let second = lexer.base_yylex().unwrap();
    // "int" -> INT_P keyword token
    assert_ne!(second.token, tokens::IDENT);
}
