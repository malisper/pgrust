// The full client-encoding lane over real parts: mbutils framework -> real
// fmgr_core resolution -> real conversion procs. Only the pg_conversion
// catalog read is faked (the pg_conversion.dat default rows for these pairs).
use std::sync::Once;

use types_core::{InvalidOid, Oid};
use wchar::{pg_enc, PG_LATIN1, PG_LATIN9, PG_SQL_ASCII, PG_UTF8, PG_WIN1252};

fn default_conversion(for_enc: i32, to_enc: i32) -> Oid {
    match (for_enc, to_enc) {
        (PG_LATIN1, PG_UTF8) => 4374,
        (PG_UTF8, PG_LATIN1) => 4375,
        (PG_WIN1252, PG_UTF8) => 4359,
        (PG_UTF8, PG_WIN1252) => 4358,
        (PG_LATIN9, PG_UTF8) => 4373,
        (PG_UTF8, PG_LATIN9) => 4372,
        _ => InvalidOid,
    }
}

static INSTALL: Once = Once::new();

fn boot(client: pg_enc) -> mcx::MemoryContext {
    INSTALL.call_once(|| {
        xact_seams::is_transaction_state::set(|| true);
        namespace_seams::find_default_conversion_proc::set(|f, t| Ok(default_conversion(f, t)));
        fmgr_seams::fmgr_info::set(fmgr_core::fmgr_info);
    });
    mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
    mbutils::InitializeClientEncoding().unwrap();
    assert_eq!(mbutils::PrepareClientEncoding(client).unwrap(), 0);
    assert_eq!(mbutils::SetClientEncoding(client).unwrap(), 0);
    mcx::MemoryContext::new("t")
}

fn to_client(ctx: &mcx::MemoryContext, s: &[u8]) -> Option<Vec<u8>> {
    mbutils::pg_server_to_client(ctx.mcx(), s)
        .unwrap()
        .map(|v| v.to_vec())
}

fn to_server(ctx: &mcx::MemoryContext, s: &[u8]) -> Option<Vec<u8>> {
    mbutils::pg_client_to_server(ctx.mcx(), s)
        .unwrap()
        .map(|v| v.to_vec())
}

#[test]
fn latin1_client_round_trip_and_22p05() {
    let ctx = boot(PG_LATIN1);
    assert!(mbutils::server_to_client_conversion_needed());

    assert_eq!(
        to_client(&ctx, "caf\u{e9} \u{c9}L\u{c8}VE".as_bytes()).unwrap(),
        b"caf\xe9 \xc9L\xc8VE"
    );
    assert_eq!(
        to_server(&ctx, b"caf\xe9").unwrap(),
        "caf\u{e9}".as_bytes()
    );
    // Identity when no high-bit conversion output differs? No: conversion runs
    // regardless; pure-ASCII still round-trips through the proc.
    assert_eq!(to_client(&ctx, b"plain").unwrap(), b"plain");

    let e = mbutils::pg_server_to_client(ctx.mcx(), "\u{6c34}".as_bytes()).unwrap_err();
    assert_eq!(e.sqlstate(), types_error::ERRCODE_UNTRANSLATABLE_CHARACTER);
    assert_eq!(
        e.message(),
        "character with byte sequence 0xe6 0xb0 0xb4 in encoding \"UTF8\" has no equivalent in encoding \"LATIN1\""
    );

    // Every LATIN1 byte converts; only an embedded NUL is invalid.
    let e = mbutils::pg_client_to_server(ctx.mcx(), b"a\x00b").unwrap_err();
    assert_eq!(
        e.message(),
        "invalid byte sequence for encoding \"LATIN1\": 0x00"
    );
}

#[test]
fn win1252_client_euro_and_quotes() {
    let ctx = boot(PG_WIN1252);
    assert_eq!(
        to_client(&ctx, "\u{20ac}99 \u{201c}hi\u{201d}".as_bytes()).unwrap(),
        b"\x8099 \x93hi\x94"
    );
    assert_eq!(
        to_server(&ctx, b"\x8099").unwrap(),
        "\u{20ac}99".as_bytes()
    );
}

#[test]
fn latin9_client_euro() {
    let ctx = boot(PG_LATIN9);
    assert_eq!(
        to_client(&ctx, "\u{20ac}9 \u{e9}".as_bytes()).unwrap(),
        b"\xa49 \xe9"
    );
    assert_eq!(to_server(&ctx, b"\xa49").unwrap(), "\u{20ac}9".as_bytes());
}

#[test]
fn sql_ascii_client_is_passthrough() {
    let ctx = boot(PG_SQL_ASCII);
    assert!(!mbutils::server_to_client_conversion_needed());
    // Server-to-client: no conversion, no validation (C pg_server_to_any).
    assert_eq!(to_client(&ctx, "caf\u{e9}".as_bytes()), None);
    // Client-to-server: validated against the server encoding, bytes stand.
    assert_eq!(to_server(&ctx, "caf\u{e9}".as_bytes()), None);
    let e = mbutils::pg_client_to_server(ctx.mcx(), b"a\xe9b").unwrap_err();
    assert_eq!(
        e.sqlstate(),
        types_error::ERRCODE_CHARACTER_NOT_IN_REPERTOIRE
    );
}

#[test]
fn same_encoding_client_is_identity() {
    let ctx = boot(PG_UTF8);
    assert!(!mbutils::server_to_client_conversion_needed());
    assert_eq!(to_client(&ctx, "caf\u{e9}".as_bytes()), None);
    assert_eq!(to_server(&ctx, "caf\u{e9}".as_bytes()), None);
}
