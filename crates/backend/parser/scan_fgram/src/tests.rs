use super::*;
use mcx::MemoryContext;

// One leaked context per test thread: mcx's ACCT_POOL assumes one backend =
// one thread, and concurrent context drops on test threads race it.
fn test_ctx() -> &'static MemoryContext {
    thread_local! {
        static CTX: &'static MemoryContext =
            Box::leak(Box::new(MemoryContext::new("scan-test")));
    }
    CTX.with(|c| *c)
}

fn lex<'a>(sc: &mut Scanner<'a>) -> types_error::PgResult<Token<'a>> {
    let mut value = CoreYYSTYPE::None;
    let mut location = 0;
    let token = sc.core_yylex(&mut value, &mut location)?;
    Ok(Token { token, value, location })
}

fn lex_all_with(input: &[u8], settings: ScannerSettings) -> Vec<(i32, String, i32)> {
    let ctx = test_ctx();
    let mut sc = Scanner::new(input, ctx.mcx(), settings);
    let mut out = Vec::new();
    loop {
        let tok = lex(&mut sc).expect("unexpected lex error");
        if tok.token == YY_NULL {
            break;
        }
        let val = match tok.value.get() {
            CoreVal::None => String::new(),
            CoreVal::Ival(v) => format!("#{v}"),
            CoreVal::Str(s) => format!("={}", String::from_utf8_lossy(s)),
            CoreVal::Keyword(k) => format!("kw:{k}"),
        };
        out.push((tok.token, val, tok.location));
        assert!(out.len() < 10_000, "runaway scanner");
    }
    out
}

fn lex_all(input: &str) -> Vec<(i32, String, i32)> {
    lex_all_with(input.as_bytes(), ScannerSettings::default())
}

fn lex_err(input: &str) -> Box<types_error::PgError> {
    let ctx = test_ctx();
    let mut sc = Scanner::new(input.as_bytes(), ctx.mcx(), ScannerSettings::default());
    loop {
        match lex(&mut sc) {
            Ok(tok) if tok.token == YY_NULL => panic!("no error for {input:?}"),
            Ok(_) => {}
            Err(e) => return e,
        }
    }
}

fn codes(input: &str) -> Vec<i32> {
    lex_all(input).into_iter().map(|t| t.0).collect()
}

#[test]
fn token_numbering_matches_scanner_h() {
    assert_eq!(tokens::IDENT, 258);
    assert_eq!(tokens::UIDENT, 259);
    assert_eq!(tokens::FCONST, 260);
    assert_eq!(tokens::SCONST, 261);
    assert_eq!(tokens::ICONST, 266);
    assert_eq!(tokens::PARAM, 267);
    assert_eq!(tokens::NOT_EQUALS, 274);
}

#[test]
fn identifiers_and_keywords() {
    assert_eq!(lex_all("hello"), vec![(tokens::IDENT, "=hello".into(), 0)]);
    assert_eq!(lex_all("HeLLo")[0].1, "=hello");
    let sel = lex_all("select");
    let kwnum = keywords::ScanKeywordLookup(b"select", &keywords::ScanKeywords) as usize;
    assert_eq!(sel[0].0, SCAN_KEYWORD_TOKENS[kwnum] as i32);
    assert_eq!(sel[0].1, "kw:select");
    assert_eq!(codes("SELECT"), codes("select"));
    assert_eq!(codes("SeLeCt"), codes("select"));
}

#[test]
fn integer_literals() {
    assert_eq!(lex_all("12345")[0], (tokens::ICONST, "#12345".into(), 0));
    assert_eq!(
        lex_all("99999999999")[0],
        (tokens::FCONST, "=99999999999".into(), 0)
    );
    assert_eq!(lex_all("0x1A")[0].1, "#26");
    assert_eq!(lex_all("0o17")[0].1, "#15");
    assert_eq!(lex_all("0b101")[0].1, "#5");
    assert_eq!(lex_all("1_000")[0].1, "#1000");
}

#[test]
fn float_literals() {
    assert_eq!(lex_all("3.14")[0], (tokens::FCONST, "=3.14".into(), 0));
    assert_eq!(lex_all("1e10")[0].0, tokens::FCONST);
    assert_eq!(lex_all(".5")[0].0, tokens::FCONST);
    assert_eq!(lex_all("1.")[0].0, tokens::FCONST);
}

#[test]
fn dotdot_splits_integer() {
    assert_eq!(
        codes("1..10"),
        vec![tokens::ICONST, tokens::DOT_DOT, tokens::ICONST]
    );
}

#[test]
fn string_literals() {
    assert_eq!(
        lex_all("'hello world'"),
        vec![(tokens::SCONST, "=hello world".into(), 0)]
    );
    assert_eq!(lex_all("'it''s'")[0].1, "=it's");
    assert_eq!(lex_all("'foo'\n'bar'"), vec![(tokens::SCONST, "=foobar".into(), 0)]);
    let sep = lex_all("'foo' 'bar'");
    assert_eq!(sep.len(), 2);
    assert_eq!(sep[1], (tokens::SCONST, "=bar".into(), 6));
}

#[test]
fn extended_string_escapes() {
    assert_eq!(lex_all(r"E'a\tb\n'")[0], (tokens::SCONST, "=a\tb\n".into(), 0));
    assert_eq!(lex_all(r"E'\x41'")[0].1, "=A");
    assert_eq!(lex_all(r"E'\101'")[0].1, "=A");
    assert_eq!(lex_all(r"E'A'")[0].1, "=A");
    assert_eq!(lex_all(r"E'é'")[0].1, "=\u{e9}");
    assert_eq!(lex_all(r"E'😄'")[0].1, "=\u{1f604}");
}

#[test]
fn dollar_quoted_strings() {
    assert_eq!(lex_all("$$body$$")[0], (tokens::SCONST, "=body".into(), 0));
    assert_eq!(lex_all("$tag$a$b$tag$")[0].1, "=a$b");
    assert_eq!(lex_all("$tag$a$other$z$tag$")[0].1, "=a$other$z");
}

#[test]
fn delimited_identifiers() {
    assert_eq!(
        lex_all("\"MixedCase\"")[0],
        (tokens::IDENT, "=MixedCase".into(), 0)
    );
    assert_eq!(lex_all("\"a\"\"b\"")[0].1, "=a\"b");
    assert_eq!(lex_all("U&\"d!0061t\"")[0].0, tokens::UIDENT);
    assert_eq!(lex_all("U&'d!0061t'")[0].0, tokens::USCONST);
}

#[test]
fn bit_and_hex_strings() {
    assert_eq!(lex_all("B'101'")[0], (tokens::BCONST, "=b101".into(), 0));
    assert_eq!(lex_all("X'1F'")[0], (tokens::XCONST, "=x1F".into(), 0));
}

#[test]
fn operators_and_self_chars() {
    assert_eq!(
        codes("a + b"),
        vec![tokens::IDENT, b'+' as i32, tokens::IDENT]
    );
    assert_eq!(codes("a <= b")[1], tokens::LESS_EQUALS);
    let op = lex_all("a @> b");
    assert_eq!(op[1], (tokens::Op, "=@>".into(), 2));
    assert_eq!(codes("x::int")[1], tokens::TYPECAST);
    assert_eq!(
        codes("a=-1"),
        vec![tokens::IDENT, b'=' as i32, b'-' as i32, tokens::ICONST]
    );
    assert_eq!(
        codes("a+/* c */b"),
        vec![tokens::IDENT, b'+' as i32, tokens::IDENT]
    );
}

#[test]
fn parameters() {
    assert_eq!(lex_all("$1")[0], (tokens::PARAM, "#1".into(), 0));
    assert_eq!(lex_all("$1,$2").len(), 3);
}

#[test]
fn comments_are_whitespace() {
    assert_eq!(codes("a -- comment\n+ b").len(), 3);
    assert_eq!(codes("a /* x /* nested */ y */ + b").len(), 3);
}

#[test]
fn locations_are_byte_offsets() {
    let toks = lex_all("a + bb");
    assert_eq!(
        toks.iter().map(|t| t.2).collect::<Vec<_>>(),
        vec![0, 2, 4]
    );
}

#[test]
fn select_statement_stream() {
    let toks = lex_all("SELECT a, b FROM t WHERE a = 1;");
    let c: Vec<i32> = toks.iter().map(|t| t.0).collect();
    assert_eq!(c.len(), 11);
    assert_eq!(c[1], tokens::IDENT);
    assert_eq!(c[2], b',' as i32);
    assert_eq!(c[8], b'=' as i32);
    assert_eq!(c[9], tokens::ICONST);
    assert_eq!(c[10], b';' as i32);
}

#[test]
fn nchar_prefix() {
    let toks = lex_all("N'abc'");
    let kwnum = keywords::ScanKeywordLookup(b"nchar", &keywords::ScanKeywords) as usize;
    assert_eq!(toks[0].0, SCAN_KEYWORD_TOKENS[kwnum] as i32);
    assert_eq!(toks[1].0, tokens::SCONST);
}

#[test]
fn xufailed_returns_u_ident() {
    let toks = lex_all("u&x");
    assert_eq!(toks[0], (tokens::IDENT, "=u".into(), 0));
}

#[test]
fn non_scs_quote_becomes_xe() {
    let settings = ScannerSettings {
        standard_conforming_strings: false,
        escape_string_warning: false,
        ..Default::default()
    };
    let toks = lex_all_with(br"'a\nb'", settings);
    assert_eq!(toks[0].1, "=a\nb");
}

#[test]
fn errors_carry_sqlstate_and_message() {
    let e = lex_err("/* unterminated");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_SYNTAX_ERROR);
    assert!(e.message().starts_with("unterminated /* comment"));
    let e = lex_err("'unterminated");
    assert!(e.message().starts_with("unterminated quoted string"));
    let e = lex_err("123abc");
    assert!(e.message().starts_with("trailing junk after numeric literal"));
    let e = lex_err("0x");
    assert!(e.message().starts_with("invalid hexadecimal integer"));
    let e = lex_err("$1x");
    assert!(e.message().starts_with("trailing junk after parameter"));
    let e = lex_err("\"\"");
    assert!(e.message().starts_with("zero-length delimited identifier"));
    let e = lex_err(r"E'\ud83d'");
    assert!(e.message().starts_with("invalid Unicode surrogate pair"));
    let e = lex_err(r"E'\u12'");
    assert_eq!(e.sqlstate(), types_error::ERRCODE_INVALID_ESCAPE_SEQUENCE);
}

#[test]
fn every_keyword_lexes_to_its_gram_token() {
    assert_eq!(crate::dfa::YY_NUM_RULES, 73);
    for n in 0..keywords::SCANKEYWORDS_NUM_KEYWORDS {
        let kw = keywords::keyword_text(n).unwrap();
        let toks = lex_all(kw);
        assert_eq!(toks[0].0, SCAN_KEYWORD_TOKENS[n] as i32, "{kw}");
        assert_eq!(toks[0].1, format!("kw:{kw}"));
    }
}

#[test]
fn eof_token_location() {
    let ctx = test_ctx();
    let mut sc = Scanner::new(b"ab", ctx.mcx(), ScannerSettings::default());
    lex(&mut sc).unwrap();
    let eof = lex(&mut sc).unwrap();
    assert_eq!(eof.token, YY_NULL);
    assert_eq!(eof.location, 2);
    assert_eq!(lex(&mut sc).unwrap().token, YY_NULL);
}

#[test]
fn embedded_nul_ends_token_stream() {
    // {other} matches NUL (flex class 256) and returns yytext[0] == 0, which
    // reads as end-of-input — same stream a NUL-terminated C buffer yields.
    let toks = lex_all_with(b"ab\0cd", ScannerSettings::default());
    assert_eq!(toks, vec![(tokens::IDENT, "=ab".into(), 0)]);
}
