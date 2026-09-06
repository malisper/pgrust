use crate::config::*;

fn p(s: &str) -> SyncRepConfigData {
    parse_synchronous_standby_names(s).unwrap()
}

#[test]
fn parse_bare_list() {
    let c = p("s1");
    assert_eq!((c.num_sync, c.syncrep_method), (1, SYNC_REP_PRIORITY));
    assert_eq!(c.members, ["s1"]);

    let c = p(" s1 , s2,s3 ");
    assert_eq!(c.num_sync, 1);
    assert_eq!(c.members, ["s1", "s2", "s3"]);
}

#[test]
fn parse_num_paren() {
    let c = p("2 (s1, s2, s3)");
    assert_eq!((c.num_sync, c.syncrep_method), (2, SYNC_REP_PRIORITY));
    assert_eq!(c.members, ["s1", "s2", "s3"]);
}

#[test]
fn parse_first_and_any() {
    let c = p("FIRST 2 (s1, s2)");
    assert_eq!((c.num_sync, c.syncrep_method), (2, SYNC_REP_PRIORITY));
    let c = p("any 1 (s1, s2)");
    assert_eq!((c.num_sync, c.syncrep_method), (1, SYNC_REP_QUORUM));
    // Keywords are case-insensitive (scanner brute-forces case).
    let c = p("AnY 2(a,b,c)");
    assert_eq!((c.num_sync, c.syncrep_method), (2, SYNC_REP_QUORUM));
}

#[test]
fn parse_quoted_and_star() {
    let c = p("\"node one\", \"say \"\"hi\"\"\"");
    assert_eq!(c.members, ["node one", "say \"hi\""]);
    let c = p("*");
    assert_eq!(c.members, ["*"]);
    // A quoted keyword is a plain name.
    let c = p("\"any\"");
    assert_eq!(c.members, ["any"]);
}

#[test]
fn parse_numeric_standby_name() {
    // standby_name: NUM — a bare number is a name when not followed by '('.
    let c = p("12");
    assert_eq!((c.num_sync, &c.members[..]), (1, &["12".to_string()][..]));
    let c = p("s1, 33");
    assert_eq!(c.members, ["s1", "33"]);
}

#[test]
fn parse_errors() {
    for bad in ["", "2 (", "()", "any (s1)", "first s1", "s1,", "\"unterminated", "2 (s1) x"] {
        assert!(
            parse_synchronous_standby_names(bad).is_err(),
            "expected parse failure for {bad:?}"
        );
    }
    // C reports the offending token.
    let e = parse_synchronous_standby_names("s1,").unwrap_err();
    assert_eq!(e, "syntax error at end of input");
}

#[test]
fn parse_num_sync_is_c_atoi() {
    // syncrep_gram.y:106 config->num_sync = atoi(num_sync), i.e. (int) strtol:
    // a value that fits a 64-bit long wraps modulo 2^32 on the narrowing cast,
    // and strtol clamps to LONG_MAX beyond that (low 32 bits all set -> -1).
    // check_synchronous_standby_names (syncrep.c:1087) then rejects
    // num_sync <= 0 quoting the wrapped value.
    assert_eq!(p("2147483647 (s1)").num_sync, i32::MAX);
    assert_eq!(p("2147483648 (s1)").num_sync, i32::MIN);
    assert_eq!(p("4294967296 (s1)").num_sync, 0);
    assert_eq!(p("4294967297 (s1)").num_sync, 1);
    assert_eq!(p("99999999999999999999999 (s1)").num_sync, -1);
    assert_eq!(p("any 9223372036854775807 (s1)").num_sync, -1);
    assert_eq!(p("first 9223372036854775808 (s1)").num_sync, -1);
    assert_eq!(p("0 (s1)").num_sync, 0);
}

#[test]
fn syntax_error_reports_yytext() {
    // syncrep_scanner.l:165 syncrep_yyerror formats yytext — the offending
    // token's raw text as typed: keywords keep the user's casing, a quoted
    // name errors on its closing dquote (the <xd>{xdstop} token), "*" is "*".
    for (input, detail) in [
        ("s1, any", "syntax error at or near \"any\""),
        ("s1, First", "syntax error at or near \"First\""),
        ("s1 aNy", "syntax error at or near \"aNy\""),
        ("Any (s1)", "syntax error at or near \"(\""),
        ("s1 \"x\"", "syntax error at or near \"\"\""),
        ("s1 *", "syntax error at or near \"*\""),
        ("s1 ; s2", "syntax error at or near \";\""),
        ("2 (s1) x", "syntax error at or near \"x\""),
        ("2 (s1) 33", "syntax error at or near \"33\""),
        ("\"unterminated", "unterminated quoted identifier at end of input"),
        ("any 2", "syntax error at end of input"),
        ("any", "syntax error at end of input"),
    ] {
        assert_eq!(parse_synchronous_standby_names(input).unwrap_err(), detail, "input {input:?}");
    }
}
