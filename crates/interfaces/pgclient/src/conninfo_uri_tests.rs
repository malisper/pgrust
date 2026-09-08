// fe-connect.c conninfo_uri_parse witness (audit-18.6 w2-037 row 2): the
// URI connection-string grammar PQconninfoParse accepts, reached through
// dblink_connect / dblink_connstr_has_pw / libpqrcv_check_conninfo. Error
// strings are libpq's (pchomp'd: no trailing newline, as pgclient reports).
use crate::{opt, parse_conninfo, resolve_conninfo};

fn get(opts: &[(String, String)], key: &str) -> Option<String> {
    opt(opts, key).map(str::to_string)
}

// conninfo_uri_parse_options: the documented full form with credentials,
// port, dbname and query parameters; the short designator; empty
// components are not stored at all.
#[test]
fn uri_full_form_parses_like_c() {
    let o = parse_conninfo(
        "postgresql://uri-user:secret@host:12345/mydb?connect_timeout=10&application_name=myapp",
    )
    .unwrap();
    assert_eq!(get(&o, "user").as_deref(), Some("uri-user"));
    assert_eq!(get(&o, "password").as_deref(), Some("secret"));
    assert_eq!(get(&o, "host").as_deref(), Some("host"));
    assert_eq!(get(&o, "port").as_deref(), Some("12345"));
    assert_eq!(get(&o, "dbname").as_deref(), Some("mydb"));
    assert_eq!(get(&o, "connect_timeout").as_deref(), Some("10"));
    assert_eq!(get(&o, "application_name").as_deref(), Some("myapp"));

    let o = parse_conninfo("postgres:///mydb").unwrap();
    assert_eq!(get(&o, "dbname").as_deref(), Some("mydb"));
    assert!(get(&o, "host").is_none());
    assert!(get(&o, "user").is_none());

    // dblink's socket-directory form (audit b045 repro): no netloc, host and
    // port as query parameters.
    let o = parse_conninfo("postgresql:///postgres?host=/tmp/fp/slots/20&port=56201&user=postgres")
        .unwrap();
    assert_eq!(get(&o, "host").as_deref(), Some("/tmp/fp/slots/20"));
    assert_eq!(get(&o, "port").as_deref(), Some("56201"));
    assert_eq!(get(&o, "user").as_deref(), Some("postgres"));
    assert_eq!(get(&o, "dbname").as_deref(), Some("postgres"));

    // user without password; password without user; bare designator.
    let o = parse_conninfo("postgresql://alice@h/d").unwrap();
    assert_eq!(get(&o, "user").as_deref(), Some("alice"));
    assert!(get(&o, "password").is_none());
    let o = parse_conninfo("postgresql://:pw@h/d").unwrap();
    assert!(get(&o, "user").is_none());
    assert_eq!(get(&o, "password").as_deref(), Some("pw"));
    assert!(parse_conninfo("postgresql://").unwrap().is_empty());
    // A '@' after the first '/' is not a credentials designator.
    let o = parse_conninfo("postgresql://h/d@b").unwrap();
    assert!(get(&o, "user").is_none());
    assert_eq!(get(&o, "dbname").as_deref(), Some("d@b"));
    // An empty dbname component is not set (it would force the default).
    let o = parse_conninfo("postgresql://h:1/?user=u").unwrap();
    assert!(get(&o, "dbname").is_none());
    assert_eq!(get(&o, "user").as_deref(), Some("u"));
}

// The parsed options feed resolve_conninfo's defaults ladder unchanged.
#[test]
fn uri_resolves_with_defaults() {
    let o = resolve_conninfo("postgresql://u:p@localhost:5433/db").unwrap();
    assert_eq!(get(&o, "user").as_deref(), Some("u"));
    assert_eq!(get(&o, "password").as_deref(), Some("p"));
    assert_eq!(get(&o, "port").as_deref(), Some("5433"));
    assert_eq!(get(&o, "dbname").as_deref(), Some("db"));
    assert_eq!(get(&o, "sslmode").as_deref(), Some("prefer"));
    assert_eq!(get(&o, "gssencmode").as_deref(), Some("prefer"));
}

// conninfo_uri_decode: %xy in every component, %00 forbidden, bad tokens
// and embedded spaces are errors with libpq's messages.
#[test]
fn uri_percent_encoding_matches_c() {
    let o = parse_conninfo("postgresql://uri%2Duser:p%40ss@host/db%2Fname").unwrap();
    assert_eq!(get(&o, "user").as_deref(), Some("uri-user"));
    assert_eq!(get(&o, "password").as_deref(), Some("p@ss"));
    assert_eq!(get(&o, "dbname").as_deref(), Some("db/name"));
    let o = parse_conninfo("postgresql://host/db?application_name=%41%62c").unwrap();
    assert_eq!(get(&o, "application_name").as_deref(), Some("Abc"));
    // Leading/trailing spaces are skipped; an inner space is rejected.
    let o = parse_conninfo("postgresql://host/db?application_name=%20x%20").unwrap();
    assert_eq!(get(&o, "application_name").as_deref(), Some(" x "));
    let o = parse_conninfo("postgresql://host/db?application_name=  x  ").unwrap();
    assert_eq!(get(&o, "application_name").as_deref(), Some("x"));

    let e = parse_conninfo("postgresql://host/db?application_name=a%zzb").unwrap_err();
    assert_eq!(e, "invalid percent-encoded token: \"a%zzb\"");
    let e = parse_conninfo("postgresql://host/db?application_name=a%4").unwrap_err();
    assert_eq!(e, "invalid percent-encoded token: \"a%4\"");
    let e = parse_conninfo("postgresql://host/db?application_name=a%").unwrap_err();
    assert_eq!(e, "invalid percent-encoded token: \"a%\"");
    let e = parse_conninfo("postgresql://host/db?application_name=a%00b").unwrap_err();
    assert_eq!(e, "forbidden value %00 in percent-encoded value: \"a%00b\"");
    let e = parse_conninfo("postgresql://host/db?application_name=a b").unwrap_err();
    assert_eq!(
        e,
        "unexpected spaces found in \"a b\", use percent-encoded spaces (%20) instead"
    );
    // The host component is decoded too (and its error names the raw text).
    let e = parse_conninfo("postgresql://ho%zzst/db").unwrap_err();
    assert_eq!(e, "invalid percent-encoded token: \"ho%zzst\"");
}

// Bracketed IPv6 netlocs and comma-separated multi-host lists build the
// comma-joined host/port values C produces, with the exact diagnostics.
#[test]
fn uri_ipv6_and_multihost_match_c() {
    let o = parse_conninfo("postgresql://[::1]:5433/db").unwrap();
    assert_eq!(get(&o, "host").as_deref(), Some("::1"));
    assert_eq!(get(&o, "port").as_deref(), Some("5433"));
    let o = parse_conninfo("postgresql://[::1]/db").unwrap();
    assert_eq!(get(&o, "host").as_deref(), Some("::1"));
    assert!(get(&o, "port").is_none());
    let o = parse_conninfo("postgresql://[::1]?dbname=x").unwrap();
    assert_eq!(get(&o, "host").as_deref(), Some("::1"));
    assert_eq!(get(&o, "dbname").as_deref(), Some("x"));

    let o = parse_conninfo("postgresql://h1:5432,h2:5433/db").unwrap();
    assert_eq!(get(&o, "host").as_deref(), Some("h1,h2"));
    assert_eq!(get(&o, "port").as_deref(), Some("5432,5433"));
    let o = parse_conninfo("postgresql://h1,h2:5433/db").unwrap();
    assert_eq!(get(&o, "host").as_deref(), Some("h1,h2"));
    assert_eq!(get(&o, "port").as_deref(), Some(",5433"));
    let o = parse_conninfo("postgresql://[::1]:1,h2/db").unwrap();
    assert_eq!(get(&o, "host").as_deref(), Some("::1,h2"));
    assert_eq!(get(&o, "port").as_deref(), Some("1,"));

    let e = parse_conninfo("postgresql://[::1/db").unwrap_err();
    assert_eq!(
        e,
        "end of string reached when looking for matching \"]\" in IPv6 host address in URI: \"postgresql://[::1/db\""
    );
    let e = parse_conninfo("postgresql://[]/db").unwrap_err();
    assert_eq!(e, "IPv6 host address may not be empty in URI: \"postgresql://[]/db\"");
    let e = parse_conninfo("postgresql://[::1]x/db").unwrap_err();
    assert_eq!(
        e,
        "unexpected character \"x\" at position 19 in URI (expected \":\" or \"/\"): \"postgresql://[::1]x/db\""
    );
}

// Query-parameter quirks: ssl=true / requiressl JDBC rewrites, unknown
// parameters, the =-separator diagnostics, and last-wins duplicates.
#[test]
fn uri_query_parameter_quirks_match_c() {
    let o = parse_conninfo("postgresql://host/db?ssl=true").unwrap();
    assert_eq!(get(&o, "sslmode").as_deref(), Some("require"));
    let o = parse_conninfo("postgresql://host/db?ssl=false").unwrap_err();
    assert_eq!(o, "invalid URI query parameter: \"ssl\"");
    let o = parse_conninfo("postgresql://host/db?requiressl=1").unwrap();
    assert_eq!(get(&o, "sslmode").as_deref(), Some("require"));
    let o = parse_conninfo("postgresql://host/db?requiressl=0").unwrap();
    assert_eq!(get(&o, "sslmode").as_deref(), Some("prefer"));
    // Percent-encoded keyword.
    let o = parse_conninfo("postgresql://host/db?%75ser=u").unwrap();
    assert_eq!(get(&o, "user").as_deref(), Some("u"));
    // Later occurrences replace earlier ones (one entry per keyword).
    let o = parse_conninfo("postgresql://u1@host/db?user=u2&port=1&port=2").unwrap();
    assert_eq!(get(&o, "user").as_deref(), Some("u2"));
    assert_eq!(get(&o, "port").as_deref(), Some("2"));
    assert_eq!(o.iter().filter(|(k, _)| k == "port").count(), 1);
    // Empty value is stored (not the same as absent).
    let o = parse_conninfo("postgresql://host/db?application_name=").unwrap();
    assert_eq!(get(&o, "application_name").as_deref(), Some(""));

    let e = parse_conninfo("postgresql://host/db?bogus=x").unwrap_err();
    assert_eq!(e, "invalid URI query parameter: \"bogus\"");
    let e = parse_conninfo("postgresql://host/db?foo").unwrap_err();
    assert_eq!(e, "missing key/value separator \"=\" in URI query parameter: \"foo\"");
    let e = parse_conninfo("postgresql://host/db?foo=a=b").unwrap_err();
    assert_eq!(e, "extra key/value separator \"=\" in URI query parameter: \"foo\"");
    let e = parse_conninfo("postgresql://host/db?user=u&").unwrap();
    assert_eq!(get(&e, "user").as_deref(), Some("u"));
    let e = parse_conninfo("postgresql://host/db?").unwrap();
    assert_eq!(get(&e, "dbname").as_deref(), Some("db"));
}

// dblink_connstr_has_pw's view: a URI password is a connstr password.
#[test]
fn uri_password_is_visible_to_connstr_scans() {
    let o = parse_conninfo("postgresql://b045_nsu:secret@localhost:1/postgres").unwrap();
    assert_eq!(get(&o, "password").as_deref(), Some("secret"));
    let o = parse_conninfo("postgresql://b045_nsu@localhost:1/postgres?password=via_query").unwrap();
    assert_eq!(get(&o, "password").as_deref(), Some("via_query"));
    let o = parse_conninfo("postgresql://b045_nsu:@localhost:1/postgres").unwrap();
    assert!(get(&o, "password").is_none());
}
