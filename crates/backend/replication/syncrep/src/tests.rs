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
