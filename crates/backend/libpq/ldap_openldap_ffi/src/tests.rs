//! Unit tests for the LDAP helpers that don't require a live server: the
//! `FormatSearchFilter` placeholder substitution and the simple-bind DN
//! construction (`ldapprefix` + user + `ldapsuffix`), plus the default-filter
//! selection rules. These mirror auth.c's `FormatSearchFilter` (auth.c:2413)
//! and `CheckLDAPAuth`'s `fulluser`/`filter` construction (auth.c:2552/2630).

use crate::format_search_filter;

#[test]
fn format_search_filter_substitutes_username() {
    // Single placeholder.
    assert_eq!(
        format_search_filter("(uid=$username)", "alice"),
        "(uid=alice)"
    );
    // No placeholder -> unchanged.
    assert_eq!(
        format_search_filter("(objectClass=person)", "bob"),
        "(objectClass=person)"
    );
    // Multiple placeholders are all replaced (C's strncmp loop replaces each).
    assert_eq!(
        format_search_filter("(|(uid=$username)(cn=$username))", "carol"),
        "(|(uid=carol)(cn=carol))"
    );
    // Empty pattern.
    assert_eq!(format_search_filter("", "dave"), "");
    // Placeholder at the very start/end.
    assert_eq!(format_search_filter("$username", "eve"), "eve");
    assert_eq!(format_search_filter("x$username", "eve"), "xeve");
    assert_eq!(format_search_filter("$usernamey", "eve"), "evey");
}

/// Replicate the simple-bind `fulluser` rule (auth.c:2630):
///   psprintf("%s%s%s", ldapprefix ?: "", user, ldapsuffix ?: "")
fn simple_bind_dn(prefix: Option<&str>, user: &str, suffix: Option<&str>) -> String {
    format!(
        "{}{}{}",
        prefix.unwrap_or(""),
        user,
        suffix.unwrap_or("")
    )
}

#[test]
fn simple_bind_dn_construction() {
    assert_eq!(
        simple_bind_dn(Some("uid="), "alice", Some(",dc=example,dc=net")),
        "uid=alice,dc=example,dc=net"
    );
    // Prefix only.
    assert_eq!(simple_bind_dn(Some("CN="), "alice", None), "CN=alice");
    // Suffix only.
    assert_eq!(
        simple_bind_dn(None, "alice", Some("@example.com")),
        "alice@example.com"
    );
    // Neither -> bare user (degenerate, but matches C).
    assert_eq!(simple_bind_dn(None, "alice", None), "alice");
}

/// Replicate the search-filter selection (auth.c:2551-2557):
///   ldapsearchfilter -> FormatSearchFilter
///   else ldapsearchattribute -> "(attr=user)"
///   else "(uid=user)"
fn search_filter(
    searchfilter: Option<&str>,
    searchattribute: Option<&str>,
    user: &str,
) -> String {
    if let Some(f) = searchfilter {
        format_search_filter(f, user)
    } else if let Some(a) = searchattribute {
        format!("({a}={user})")
    } else {
        format!("(uid={user})")
    }
}

#[test]
fn search_filter_selection() {
    // Custom filter wins.
    assert_eq!(
        search_filter(Some("(mail=$username)"), Some("uid"), "x"),
        "(mail=x)"
    );
    // Attribute filter.
    assert_eq!(search_filter(None, Some("cn"), "x"), "(cn=x)");
    // Default.
    assert_eq!(search_filter(None, None, "x"), "(uid=x)");
}

#[test]
fn parse_ldap_url_basic() {
    // ldapurl="ldap://localhost:3389/dc=example,dc=net?uid?sub" — the form the
    // src/test/ldap suite uses (hba.c:2168 / 001_auth.pl).
    let u = crate::parse_ldap_url("ldap://localhost:3389/dc=example,dc=net?uid?sub")
        .expect("valid LDAP URL");
    assert_eq!(u.scheme.as_deref(), Some("ldap"));
    assert_eq!(u.host.as_deref(), Some("localhost"));
    assert_eq!(u.port, 3389);
    assert_eq!(u.basedn.as_deref(), Some("dc=example,dc=net"));
    assert_eq!(u.searchattribute.as_deref(), Some("uid"));
    // LDAP_SCOPE_SUBTREE == 2.
    assert_eq!(u.scope, 2);
}

#[test]
fn parse_ldap_url_ldaps_and_filter() {
    let u = crate::parse_ldap_url(
        "ldaps://host:636/dc=example,dc=net??sub?(|(uid=$username)(mail=$username))",
    )
    .expect("valid LDAPS URL");
    assert_eq!(u.scheme.as_deref(), Some("ldaps"));
    assert_eq!(u.port, 636);
    assert_eq!(u.scope, 2);
    assert_eq!(
        u.filter.as_deref(),
        Some("(|(uid=$username)(mail=$username))")
    );
    // No attribute given (??) -> none.
    assert!(u.searchattribute.is_none());
}

#[test]
fn parse_ldap_url_rejects_bad_scheme() {
    // A non-ldap(s) scheme is rejected with the hba.c message.
    let e = crate::parse_ldap_url("http://localhost/dc=x").unwrap_err();
    assert!(e.contains("unsupported LDAP URL scheme") || e.contains("could not parse"), "{e}");
}
