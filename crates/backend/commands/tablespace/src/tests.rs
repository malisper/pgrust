use super::*;

/// varlena.c `SplitIdentifierString` — comma list, quoting, lower-casing,
/// whitespace, and syntax-error cases.
#[test]
fn split_identifier_string_basic() {
    assert_eq!(SplitIdentifierString("", ','), Ok(vec![]));
    assert_eq!(
        SplitIdentifierString("Foo, BAR", ','),
        Ok(vec!["foo".to_string(), "bar".to_string()])
    );
    // Quoted preserves case; "" is an embedded quote.
    assert_eq!(
        SplitIdentifierString("\"MixedCase\",plain", ','),
        Ok(vec!["MixedCase".to_string(), "plain".to_string()])
    );
    assert_eq!(
        SplitIdentifierString("\"a\"\"b\"", ','),
        Ok(vec!["a\"b".to_string()])
    );
    // A leading/trailing empty element from a stray separator is a syntax error
    // (an unquoted empty identifier).
    assert!(SplitIdentifierString(",", ',').is_err());
    // Trailing junk after a closing quote is a syntax error.
    assert!(SplitIdentifierString("\"x\"y", ',').is_err());
}

/// scansup.c `scanner_isspace` includes \v and \f (which `is_ascii_whitespace`
/// omits for \v).
#[test]
fn scanner_isspace_matches_c() {
    for c in [' ', '\t', '\n', '\r', '\x0b', '\x0c'] {
        assert!(scanner_isspace(c), "{c:?} should be space");
    }
    assert!(!scanner_isspace('x'));
    assert!(!scanner_isspace('\u{00a0}'));
}

/// scansup.c `truncate_identifier` clamps to NAMEDATALEN-1 on a char boundary.
#[test]
fn truncate_identifier_clamps() {
    let mut s = "a".repeat(100);
    truncate_identifier(&mut s);
    assert_eq!(s.len(), NAMEDATALEN - 1);

    let mut short = "abc".to_string();
    truncate_identifier(&mut short);
    assert_eq!(short, "abc");
}

/// The CreateTableSpace XLOG record layout: `ts_id` (4-byte Oid) then the
/// NUL-terminated path; `decode_oid`/`decode_cstr` round-trip it.
#[test]
fn wal_payload_round_trip() {
    let oid: Oid = 20981;
    let mut payload = oid.to_ne_bytes().to_vec();
    payload.extend_from_slice(b"/mnt/space\0trailing-garbage");
    assert_eq!(decode_oid(&payload[0..4]), oid);
    assert_eq!(decode_cstr(&payload[4..]), "/mnt/space");
}
