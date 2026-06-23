//! Unit tests for the `backend-utils-adt-pseudotypes` port.
//!
//! The error-throwing dummies, `cstring` working I/O, and `void` working I/O
//! are self-contained (no foreign seam installs needed). The delegating
//! output/send functions forward to real type I/O in other adt units and are
//! exercised by those owners' own tests, so they are not re-tested here.

extern crate std;

use super::*;
use ::mcx::MemoryContext;
use ::stringinfo::StringInfo;

use std::sync::Once;

/// `pq_getmsgtext`/`pq_sendtext` route through the mbutils encoding-conversion
/// seams (a real cross-subsystem dependency). For the ASCII paths under test we
/// install identity ("no conversion needed", C's `None`) mocks once.
static INSTALL_MB: Once = Once::new();

fn mock_no_conversion<'mcx>(
    _mcx: Mcx<'mcx>,
    _s: &[u8],
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    Ok(None)
}

fn install_mb_mocks() {
    INSTALL_MB.call_once(|| {
        mbutils_seams::pg_client_to_server::set(mock_no_conversion);
        mbutils_seams::pg_server_to_client::set(mock_no_conversion);
    });
}

// ----- error dummies: message text + SQLSTATE -----------------------------

#[test]
fn input_dummies_say_cannot_accept_with_sqlstate() {
    let cases: &[(fn(&str) -> PgResult<Datum>, &str)] = &[
        (anyarray_in, "anyarray"),
        (anycompatiblearray_in, "anycompatiblearray"),
        (anyenum_in, "anyenum"),
        (anyrange_in, "anyrange"),
        (anycompatiblerange_in, "anycompatiblerange"),
        (anymultirange_in, "anymultirange"),
        (anycompatiblemultirange_in, "anycompatiblemultirange"),
        (pg_node_tree_in, "pg_node_tree"),
        (pg_ddl_command_in, "pg_ddl_command"),
        (any_in, "any"),
        (trigger_in, "trigger"),
        (event_trigger_in, "event_trigger"),
        (language_handler_in, "language_handler"),
        (fdw_handler_in, "fdw_handler"),
        (table_am_handler_in, "table_am_handler"),
        (index_am_handler_in, "index_am_handler"),
        (tsm_handler_in, "tsm_handler"),
        (internal_in, "internal"),
        (anyelement_in, "anyelement"),
        (anynonarray_in, "anynonarray"),
        (anycompatible_in, "anycompatible"),
        (anycompatiblenonarray_in, "anycompatiblenonarray"),
    ];
    for (f, typname) in cases {
        let err = f("anything").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(
            err.message(),
            alloc::format!("cannot accept a value of type {typname}")
        );
    }
}

#[test]
fn recv_dummies_say_cannot_accept_with_sqlstate() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let cases: &[(fn(&mut StringInfo<'_>) -> PgResult<Datum>, &str)] = &[
        (anyarray_recv, "anyarray"),
        (anycompatiblearray_recv, "anycompatiblearray"),
        (pg_node_tree_recv, "pg_node_tree"),
        (pg_ddl_command_recv, "pg_ddl_command"),
    ];
    for (f, typname) in cases {
        let mut buf = StringInfo::from_vec(::mcx::slice_in(mcx, b"").unwrap());
        let err = f(&mut buf).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(
            err.message(),
            alloc::format!("cannot accept a value of type {typname}")
        );
    }
}

#[test]
fn output_dummies_say_cannot_display_with_sqlstate() {
    let cases: &[(fn(Datum) -> PgResult<PgString<'static>>, &str)] = &[
        (pg_ddl_command_out, "pg_ddl_command"),
        (any_out, "any"),
        (trigger_out, "trigger"),
        (event_trigger_out, "event_trigger"),
        (language_handler_out, "language_handler"),
        (fdw_handler_out, "fdw_handler"),
        (table_am_handler_out, "table_am_handler"),
        (index_am_handler_out, "index_am_handler"),
        (tsm_handler_out, "tsm_handler"),
        (internal_out, "internal"),
        (anyelement_out, "anyelement"),
        (anynonarray_out, "anynonarray"),
        (anycompatible_out, "anycompatible"),
        (anycompatiblenonarray_out, "anycompatiblenonarray"),
    ];
    for (f, typname) in cases {
        let err = f(Datum::null()).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
        assert_eq!(
            err.message(),
            alloc::format!("cannot display a value of type {typname}")
        );
    }
}

#[test]
fn send_dummy_says_cannot_display() {
    let err = pg_ddl_command_send(Datum::null()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot display a value of type pg_ddl_command");
}

#[test]
fn shell_dummies_say_shell_type() {
    let err = shell_in("x").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot accept a value of a shell type");

    let err = shell_out(Datum::null()).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "cannot display a value of a shell type");
}

// ----- cstring: working I/O -----------------------------------------------

#[test]
fn cstring_in_out_echo_input() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    assert_eq!(cstring_in(mcx, "hello").unwrap().as_str(), "hello");
    assert_eq!(cstring_out(mcx, "world").unwrap().as_str(), "world");
}

#[test]
fn cstring_recv_reads_remaining_message_text() {
    install_mb_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let mut buf = StringInfo::from_vec(::mcx::slice_in(mcx, b"hello").unwrap());
    let out = cstring_recv(mcx, &mut buf).unwrap();
    assert_eq!(&out[..], b"hello");
}

#[test]
fn cstring_send_payload_is_the_string() {
    install_mb_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let bytea = cstring_send(mcx, "abc").unwrap();
    // The bytea body is a varlena: 4-byte header + payload.
    assert!(bytea.as_bytes().ends_with(b"abc"));
}

// ----- void: working I/O --------------------------------------------------

#[test]
fn void_in_recv_are_null() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    assert_eq!(void_in("anything").unwrap(), Datum::null());
    let mut buf = StringInfo::from_vec(::mcx::slice_in(mcx, b"").unwrap());
    assert_eq!(void_recv(&mut buf).unwrap(), Datum::null());
}

#[test]
fn void_out_is_empty_cstring() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    assert_eq!(void_out(mcx).unwrap().as_str(), "");
}

#[test]
fn void_send_is_empty_payload() {
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    let bytea = void_send(mcx).unwrap();
    // Empty string => varlena header only (no payload bytes).
    assert_eq!(bytea.as_bytes().len(), datum::VARHDRSZ);
}
