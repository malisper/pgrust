//! Golden tests for the core scanner: assert the token stream (code + value +
//! location) matches what PostgreSQL's flex scanner produces.

use super::*;
use backend_utils_mb::SetDatabaseEncoding;
use std::sync::Mutex;

/// Serialize tests touching process-global database encoding.
static ENC_LOCK: Mutex<()> = Mutex::new(());

/// Lex `input` to completion, returning every token up to (and excluding) the
/// terminating end-of-input token.
fn lex_all(input: &str) -> Vec<Token> {
    let _g = ENC_LOCK.lock().unwrap();
    SetDatabaseEncoding(pgrust_pg_ffi::PG_UTF8).unwrap();
    let mut sc = Scanner::new(input.as_bytes(), ScannerSettings::default());
    let mut out = Vec::new();
    loop {
        match sc.core_yylex() {
            Ok(tok) if tok.token == YY_NULL => break,
            Ok(tok) => out.push(tok),
            Err(e) => panic!("unexpected lex error: {e:?}"),
        }
        if out.len() > 10_000 {
            panic!("runaway scanner");
        }
    }
    out
}

fn ttok(input: &str) -> Vec<i32> {
    lex_all(input).into_iter().map(|t| t.token).collect()
}

#[test]
fn ident_511_is_258() {
    // scanner.h promises IDENT = 258.
    assert_eq!(tokens::IDENT, 258);
    assert_eq!(tokens::SCONST, 261);
    assert_eq!(tokens::ICONST, 266);
}

#[test]
fn simple_identifier() {
    let toks = lex_all("hello");
    assert_eq!(toks.len(), 1);
    assert_eq!(toks[0].token, tokens::IDENT);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"hello".to_vec()));
    assert_eq!(toks[0].location, 0);
}

#[test]
fn identifier_downcased() {
    let toks = lex_all("HeLLo");
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"hello".to_vec()));
}

#[test]
fn keyword_select() {
    let toks = lex_all("select");
    // SELECT is a reserved keyword; its token is its bison code, not IDENT.
    assert_ne!(toks[0].token, tokens::IDENT);
    assert_eq!(toks[0].value, CoreYYSTYPE::Keyword("select"));
    // SELECT's token code from the generated table.
    let kwnum = common_keywords::ScanKeywordLookup("select", &common_keywords::ScanKeywords);
    assert_eq!(toks[0].token, SCAN_KEYWORD_TOKENS[kwnum as usize] as i32);
}

#[test]
fn keyword_is_case_insensitive() {
    assert_eq!(ttok("SELECT"), ttok("select"));
    assert_eq!(ttok("SeLeCt"), ttok("select"));
}

#[test]
fn integer_literal() {
    let toks = lex_all("12345");
    assert_eq!(toks[0].token, tokens::ICONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Ival(12345));
}

#[test]
fn big_integer_becomes_fconst() {
    // Larger than int32 -> FCONST with original text.
    let toks = lex_all("99999999999");
    assert_eq!(toks[0].token, tokens::FCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"99999999999".to_vec()));
}

#[test]
fn hex_oct_bin_integers() {
    assert_eq!(lex_all("0x1A")[0].value, CoreYYSTYPE::Ival(26));
    assert_eq!(lex_all("0o17")[0].value, CoreYYSTYPE::Ival(15));
    assert_eq!(lex_all("0b101")[0].value, CoreYYSTYPE::Ival(5));
    assert_eq!(lex_all("1_000")[0].value, CoreYYSTYPE::Ival(1000));
}

#[test]
fn float_literal() {
    let toks = lex_all("3.14");
    assert_eq!(toks[0].token, tokens::FCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"3.14".to_vec()));
    let e = lex_all("1e10");
    assert_eq!(e[0].token, tokens::FCONST);
}

#[test]
fn dotdot_splits_integer() {
    // "1..10" lexes as ICONST(1), DOT_DOT, ICONST(10).
    let toks = lex_all("1..10");
    assert_eq!(toks.len(), 3);
    assert_eq!(toks[0].token, tokens::ICONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Ival(1));
    assert_eq!(toks[1].token, tokens::DOT_DOT);
    assert_eq!(toks[2].token, tokens::ICONST);
    assert_eq!(toks[2].value, CoreYYSTYPE::Ival(10));
}

#[test]
fn string_literal() {
    let toks = lex_all("'hello world'");
    assert_eq!(toks.len(), 1);
    assert_eq!(toks[0].token, tokens::SCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"hello world".to_vec()));
}

#[test]
fn string_with_doubled_quote() {
    let toks = lex_all("'it''s'");
    assert_eq!(toks[0].token, tokens::SCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"it's".to_vec()));
}

#[test]
fn string_continuation_across_newline() {
    // SQL string continuation: two literals joined by whitespace+newline.
    let toks = lex_all("'foo'\n'bar'");
    assert_eq!(toks.len(), 1);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"foobar".to_vec()));
}

#[test]
fn two_strings_no_newline_are_separate() {
    // Without a newline between them, the second quote starts a new literal.
    let toks = lex_all("'foo' 'bar'");
    assert_eq!(toks.len(), 2);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"foo".to_vec()));
    assert_eq!(toks[1].value, CoreYYSTYPE::Str(b"bar".to_vec()));
}

#[test]
fn extended_string_escapes() {
    let toks = lex_all(r"E'a\tb\n'");
    assert_eq!(toks[0].token, tokens::SCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"a\tb\n".to_vec()));
}

#[test]
fn dollar_quoted_string() {
    let toks = lex_all("$$body$$");
    assert_eq!(toks[0].token, tokens::SCONST);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"body".to_vec()));
    let tagged = lex_all("$tag$a$b$tag$");
    assert_eq!(tagged[0].value, CoreYYSTYPE::Str(b"a$b".to_vec()));
}

#[test]
fn delimited_identifier() {
    let toks = lex_all("\"MixedCase\"");
    assert_eq!(toks[0].token, tokens::IDENT);
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"MixedCase".to_vec()));
}

#[test]
fn delimited_identifier_doubled_quote() {
    let toks = lex_all("\"a\"\"b\"");
    assert_eq!(toks[0].value, CoreYYSTYPE::Str(b"a\"b".to_vec()));
}

#[test]
fn bit_and_hex_strings() {
    let b = lex_all("B'101'");
    assert_eq!(b[0].token, tokens::BCONST);
    assert_eq!(b[0].value, CoreYYSTYPE::Str(b"b101".to_vec()));
    let x = lex_all("X'1F'");
    assert_eq!(x[0].token, tokens::XCONST);
    assert_eq!(x[0].value, CoreYYSTYPE::Str(b"x1F".to_vec()));
}

#[test]
fn operators_and_self_chars() {
    let toks = lex_all("a + b");
    assert_eq!(toks.len(), 3);
    assert_eq!(toks[0].token, tokens::IDENT);
    assert_eq!(toks[1].token, b'+' as i32);
    assert_eq!(toks[2].token, tokens::IDENT);

    let cmp = lex_all("a <= b");
    assert_eq!(cmp[1].token, tokens::LESS_EQUALS);

    let user_op = lex_all("a @> b");
    assert_eq!(user_op[1].token, tokens::Op);
    assert_eq!(user_op[1].value, CoreYYSTYPE::Str(b"@>".to_vec()));
}

#[test]
fn typecast_operator() {
    let toks = lex_all("x::int");
    assert_eq!(toks[0].token, tokens::IDENT);
    assert_eq!(toks[1].token, tokens::TYPECAST);
    // int is a keyword? "int" -> INT_P keyword
    assert_eq!(toks.len(), 3);
}

#[test]
fn parameter() {
    let toks = lex_all("$1");
    assert_eq!(toks[0].token, tokens::PARAM);
    assert_eq!(toks[0].value, CoreYYSTYPE::Ival(1));
}

#[test]
fn comments_are_whitespace() {
    let toks = lex_all("a -- comment\n+ b");
    assert_eq!(toks.len(), 3);
    assert_eq!(toks[1].token, b'+' as i32);

    let c = lex_all("a /* x /* nested */ y */ + b");
    assert_eq!(c.len(), 3);
    assert_eq!(c[1].token, b'+' as i32);
}

#[test]
fn locations_are_byte_offsets() {
    // "a + bb" -> a@0, +@2, bb@4
    let toks = lex_all("a + bb");
    assert_eq!(toks[0].location, 0);
    assert_eq!(toks[1].location, 2);
    assert_eq!(toks[2].location, 4);
}

#[test]
fn operator_trims_trailing_plus_minus() {
    // "=-" lexes as '=' then '-' (SQL: trailing +/- split off).
    let toks = lex_all("a=-1");
    let codes: Vec<i32> = toks.iter().map(|t| t.token).collect();
    assert_eq!(
        codes,
        vec![tokens::IDENT, b'=' as i32, b'-' as i32, tokens::ICONST]
    );
}

#[test]
fn slashstar_in_operator_stops_at_comment() {
    // "a+/* c */b" : '+' operator, then comment, then b.
    let toks = lex_all("a+/* c */b");
    let codes: Vec<i32> = toks.iter().map(|t| t.token).collect();
    assert_eq!(codes, vec![tokens::IDENT, b'+' as i32, tokens::IDENT]);
}

#[test]
fn select_statement_token_stream() {
    // A representative statement: SELECT a, b FROM t WHERE a = 1;
    let toks = lex_all("SELECT a, b FROM t WHERE a = 1;");
    let codes: Vec<i32> = toks.iter().map(|t| t.token).collect();
    // SELECT kw, IDENT a, ',', IDENT b, FROM kw, IDENT t, WHERE kw, IDENT a,
    // '=', ICONST 1, ';'
    assert_eq!(codes.len(), 11);
    assert_eq!(codes[1], tokens::IDENT);
    assert_eq!(codes[2], b',' as i32);
    assert_eq!(codes[8], b'=' as i32);
    assert_eq!(codes[9], tokens::ICONST);
    assert_eq!(codes[10], b';' as i32);
}

#[test]
fn numeric_after_a_prior_token_is_one_fconst() {
    // Regression: pick_initial_rule used to compare the stored *end* offset
    // against the new candidate's *length*, which only agrees when p == 0.  For
    // a multi-byte token (here "2.5") that does not start at offset 0 it wrongly
    // preferred the shorter {decinteger} match, splitting "2.5" into 2 . 5.
    let toks = lex_all("a 2.5");
    assert_eq!(toks.len(), 2);
    assert_eq!(toks[0].token, tokens::IDENT);
    assert_eq!(toks[1].token, tokens::FCONST);
    assert_eq!(toks[1].location, 2);
    assert_eq!(toks[1].value, CoreYYSTYPE::Str(b"2.5".to_vec()));
}
