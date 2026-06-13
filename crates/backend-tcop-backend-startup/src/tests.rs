//! Unit tests for the in-crate pure logic (the parts that do not cross a
//! seam): the protocol-version macros, the log_connections validator, and the
//! libc-equivalent byte/string helpers.

use super::*;

#[test]
fn protocol_version_macros() {
    assert_eq!(pg_protocol(3, 0), 0x0003_0000);
    assert_eq!(pg_protocol(3, 2), 0x0003_0002);
    assert_eq!(PG_PROTOCOL_EARLIEST, 0x0003_0000);
    assert_eq!(PG_PROTOCOL_LATEST, 0x0003_0002);
    assert_eq!(pg_protocol_major(PG_PROTOCOL_LATEST), 3);
    assert_eq!(pg_protocol_minor(PG_PROTOCOL_LATEST), 2);
    assert_eq!(CANCEL_REQUEST_CODE, pg_protocol(1234, 5678));
    assert_eq!(NEGOTIATE_SSL_CODE, pg_protocol(1234, 5679));
    assert_eq!(NEGOTIATE_GSS_CODE, pg_protocol(1234, 5680));
}

#[test]
fn log_connection_flag_values() {
    assert_eq!(LOG_CONNECTION_RECEIPT, 1);
    assert_eq!(LOG_CONNECTION_AUTHENTICATION, 2);
    assert_eq!(LOG_CONNECTION_AUTHORIZATION, 4);
    assert_eq!(LOG_CONNECTION_SETUP_DURATIONS, 8);
    assert_eq!(LOG_CONNECTION_ON, 1 | 2 | 4);
    assert_eq!(LOG_CONNECTION_ALL, 1 | 2 | 4 | 8);
}

#[test]
fn validate_empty_is_zero() {
    assert_eq!(validate_log_connections_options(&[]), Ok(0));
}

#[test]
fn validate_compat_options() {
    for (name, val) in [
        ("off", 0u32),
        ("false", 0),
        ("no", 0),
        ("0", 0),
        ("on", LOG_CONNECTION_ON),
        ("true", LOG_CONNECTION_ON),
        ("yes", LOG_CONNECTION_ON),
        ("1", LOG_CONNECTION_ON),
    ] {
        assert_eq!(
            validate_log_connections_options(&[name.to_string()]),
            Ok(val),
            "compat option {name}"
        );
    }
}

#[test]
fn validate_compat_is_case_insensitive() {
    assert_eq!(
        validate_log_connections_options(&["ON".to_string()]),
        Ok(LOG_CONNECTION_ON)
    );
}

#[test]
fn validate_compat_must_be_alone() {
    let err = validate_log_connections_options(&["on".to_string(), "receipt".to_string()]);
    assert_eq!(
        err,
        Err("Cannot specify log_connections option \"on\" in a list with other options.".to_string())
    );
}

#[test]
fn validate_aspect_options_combine() {
    let flags = validate_log_connections_options(&[
        "receipt".to_string(),
        "authorization".to_string(),
    ])
    .unwrap();
    assert_eq!(flags, LOG_CONNECTION_RECEIPT | LOG_CONNECTION_AUTHORIZATION);
}

#[test]
fn validate_all_aspect() {
    assert_eq!(
        validate_log_connections_options(&["all".to_string()]),
        Ok(LOG_CONNECTION_ALL)
    );
}

#[test]
fn validate_rejects_unknown() {
    assert_eq!(
        validate_log_connections_options(&["bogus".to_string()]),
        Err("Invalid option \"bogus\".".to_string())
    );
}

#[test]
fn pg_strcasecmp_matches_c_sign() {
    assert_eq!(pg_strcasecmp("abc", "abc"), 0);
    assert_eq!(pg_strcasecmp("ABC", "abc"), 0);
    assert!(pg_strcasecmp("a", "b") < 0);
    assert!(pg_strcasecmp("b", "a") > 0);
    assert!(pg_strcasecmp("ab", "abc") < 0);
}

#[test]
fn cstr_helpers() {
    let buf = b"database\0postgres\0";
    assert_eq!(cstr_len(buf, 0), 8);
    assert!(cstr_eq(buf, 0, b"database"));
    assert!(!cstr_eq(buf, 0, b"user"));
    assert!(cstr_starts_with(buf, 9, b"post"));
    assert_eq!(cstr_str(buf, 9, cstr_len(buf, 9)), "postgres");
}

#[test]
fn truncate_to_namedatalen() {
    let mut s = Some("x".repeat(NAMEDATALEN + 5));
    truncate_namedatalen(&mut s);
    assert_eq!(s.as_deref().unwrap().len(), NAMEDATALEN - 1);

    let mut short = Some("short".to_string());
    truncate_namedatalen(&mut short);
    assert_eq!(short.as_deref(), Some("short"));
}

#[test]
fn strspn_matches_c() {
    assert_eq!(strspn("127.0.0.1", b"0123456789."), 9);
    assert_eq!(strspn("host.example", b"0123456789."), 0);
    assert_eq!(strspn("12ab", b"0123456789"), 2);
}

#[test]
fn read_be_u32_network_order() {
    let buf = [0x00, 0x03, 0x00, 0x02, 0xff];
    assert_eq!(read_be_u32(&buf, 0), 0x0003_0002);
}

#[test]
fn min_u32_picks_smaller() {
    assert_eq!(min_u32(PG_PROTOCOL_LATEST, pg_protocol(3, 5)), PG_PROTOCOL_LATEST);
    assert_eq!(min_u32(pg_protocol(3, 1), PG_PROTOCOL_LATEST), pg_protocol(3, 1));
}
