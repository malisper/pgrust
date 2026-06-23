//! Tests for `backend-libpq-crypt`.
//!
//! Seams are process-wide `OnceLock` slots, so each is installed exactly once
//! with a deterministic mock. The mocks that must vary per test
//! (`fetch_role_password`, `get_current_timestamp`) read a `thread_local`
//! script that the individual tests set before calling.
//!
//! The mock MD5 is *not* the real RFC algorithm — it only needs to be a
//! stable, length-correct (`"md5"` + 32 hex) transform so the classification /
//! verify control flow can be exercised.

use super::*;
use std::cell::RefCell;
use std::sync::Once;

use crate::{crypt_seams, syscache_seams, timestamp_seams, RolePasswordLookup};
use ::types_error::PgResult;

/// Deterministic fake of `pg_md5_encrypt`: `"md5" + 32-hex` from a tiny rolling
/// hash over `passwd || salt`. Stable for equal inputs; classifies as MD5.
fn mock_md5(passwd: &[u8], salt: &[u8]) -> Result<String, String> {
    let mut h: u64 = 1469598103934665603; // FNV offset basis
    for &b in passwd.iter().chain(salt.iter()) {
        h ^= b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    let half = format!("{h:016x}");
    Ok(format!("md5{half}{half}"))
}

fn mock_md5_seam(passwd: &[u8], salt: &[u8]) -> PgResult<Result<String, String>> {
    Ok(mock_md5(passwd, salt))
}

fn mock_parse_scram_secret(secret: &str) -> PgResult<bool> {
    Ok(secret.starts_with("SCRAM-SHA-256$"))
}

fn mock_build_secret(password: &str) -> PgResult<String> {
    Ok(format!("SCRAM-SHA-256$4096:c2FsdA==${password}:server"))
}

fn mock_scram_verify(_user: &str, password: &str, secret: &str) -> PgResult<bool> {
    Ok(secret.contains(&format!("${password}:")))
}

thread_local! {
    static ROLE_LOOKUP: RefCell<RolePasswordLookup> = RefCell::new(RolePasswordLookup::NoSuchRole);
    static NOW: RefCell<TimestampTz> = const { RefCell::new(0) };
}

fn set_role_lookup(r: RolePasswordLookup) {
    ROLE_LOOKUP.with(|c| *c.borrow_mut() = r);
}

fn set_now(now: TimestampTz) {
    NOW.with(|c| *c.borrow_mut() = now);
}

fn mock_fetch_role_password(_role: &str) -> PgResult<RolePasswordLookup> {
    Ok(ROLE_LOOKUP.with(|c| c.borrow().clone()))
}

fn mock_get_current_timestamp() -> TimestampTz {
    NOW.with(|c| *c.borrow())
}

static INIT: Once = Once::new();

fn install_mocks() {
    INIT.call_once(|| {
        crypt_seams::pg_md5_encrypt::set(mock_md5_seam);
        crypt_seams::parse_scram_secret::set(mock_parse_scram_secret);
        crypt_seams::pg_be_scram_build_secret::set(mock_build_secret);
        crypt_seams::scram_verify_plain_password::set(mock_scram_verify);
        syscache_seams::fetch_role_password::set(mock_fetch_role_password);
        timestamp_seams::get_current_timestamp::set(mock_get_current_timestamp);
    });
}

#[test]
fn get_password_type_md5() {
    install_mocks();
    let pw = mock_md5(b"secret", b"alice").unwrap();
    assert_eq!(get_password_type(&pw).unwrap(), PasswordType::Md5);
}

#[test]
fn get_password_type_md5_rejects_wrong_charset() {
    install_mocks();
    let bad = format!("md5{}", "z".repeat(32));
    assert_eq!(get_password_type(&bad).unwrap(), PasswordType::Plaintext);
}

#[test]
fn get_password_type_md5_rejects_wrong_length() {
    install_mocks();
    let short = format!("md5{}", "a".repeat(31)); // 34 chars
    assert_eq!(get_password_type(&short).unwrap(), PasswordType::Plaintext);
}

#[test]
fn get_password_type_scram() {
    install_mocks();
    assert_eq!(
        get_password_type("SCRAM-SHA-256$4096:c2FsdA==$stored:server").unwrap(),
        PasswordType::ScramSha256
    );
}

#[test]
fn get_password_type_plaintext() {
    install_mocks();
    assert_eq!(
        get_password_type("just a password").unwrap(),
        PasswordType::Plaintext
    );
}

#[test]
fn encrypt_password_already_encrypted_passthrough() {
    install_mocks();
    let md5pw = mock_md5(b"x", b"y").unwrap();
    let out = encrypt_password(PasswordType::ScramSha256, "role", &md5pw).unwrap();
    assert_eq!(out, md5pw);
}

#[test]
fn encrypt_password_to_md5() {
    install_mocks();
    let out = encrypt_password(PasswordType::Md5, "alice", "secret").unwrap();
    assert_eq!(out, mock_md5(b"secret", b"alice").unwrap());
    assert_eq!(get_password_type(&out).unwrap(), PasswordType::Md5);
}

#[test]
fn encrypt_password_to_scram() {
    install_mocks();
    let out = encrypt_password(PasswordType::ScramSha256, "alice", "secret").unwrap();
    assert!(out.starts_with("SCRAM-SHA-256$"));
}

#[test]
fn encrypt_password_plaintext_target_errors() {
    install_mocks();
    let err = encrypt_password(PasswordType::Plaintext, "alice", "secret").unwrap_err();
    assert!(err.message().contains("cannot encrypt password with 'plaintext'"));
}

#[test]
fn encrypt_password_too_long_errors() {
    install_mocks();
    let long = format!("SCRAM-SHA-256${}", "a".repeat(600));
    let err = encrypt_password(PasswordType::Md5, "role", &long).unwrap_err();
    assert!(err.message().contains("encrypted password is too long"));
}

#[test]
fn md5_crypt_verify_ok() {
    install_mocks();
    let role = "alice";
    let salt = [1u8, 2, 3, 4];
    let shadow = mock_md5(b"secret", role.as_bytes()).unwrap();
    let client = mock_md5(&shadow.as_bytes()[3..], &salt).unwrap();
    let mut logdetail = None;
    let rc =
        md5_crypt_verify(role, &shadow, &client, &salt, salt.len() as i32, &mut logdetail).unwrap();
    assert_eq!(rc, STATUS_OK);
    assert!(logdetail.is_none());
}

#[test]
fn md5_crypt_verify_mismatch() {
    install_mocks();
    let salt = [9u8, 9, 9, 9];
    let shadow = mock_md5(b"secret", b"alice").unwrap();
    let mut logdetail = None;
    let rc = md5_crypt_verify("alice", &shadow, "wrong", &salt, salt.len() as i32, &mut logdetail)
        .unwrap();
    assert_eq!(rc, STATUS_ERROR);
    assert!(logdetail.unwrap().contains("Password does not match"));
}

#[test]
fn md5_crypt_verify_non_md5_shadow() {
    install_mocks();
    let salt = [1u8];
    let mut logdetail = None;
    let rc = md5_crypt_verify(
        "alice",
        "SCRAM-SHA-256$4096:x$y:z",
        "resp",
        &salt,
        1,
        &mut logdetail,
    )
    .unwrap();
    assert_eq!(rc, STATUS_ERROR);
    assert!(logdetail
        .unwrap()
        .contains("cannot be used with MD5 authentication"));
}

#[test]
fn plain_crypt_verify_md5_ok() {
    install_mocks();
    let shadow = mock_md5(b"secret", b"alice").unwrap();
    let mut logdetail = None;
    let rc = plain_crypt_verify("alice", &shadow, "secret", &mut logdetail).unwrap();
    assert_eq!(rc, STATUS_OK);
}

#[test]
fn plain_crypt_verify_md5_mismatch() {
    install_mocks();
    let shadow = mock_md5(b"secret", b"alice").unwrap();
    let mut logdetail = None;
    let rc = plain_crypt_verify("alice", &shadow, "WRONG", &mut logdetail).unwrap();
    assert_eq!(rc, STATUS_ERROR);
    assert!(logdetail.unwrap().contains("Password does not match"));
}

#[test]
fn plain_crypt_verify_scram_ok() {
    install_mocks();
    let secret = mock_build_secret("secret").unwrap();
    let mut logdetail = None;
    let rc = plain_crypt_verify("alice", &secret, "secret", &mut logdetail).unwrap();
    assert_eq!(rc, STATUS_OK);
}

#[test]
fn get_role_password_no_such_role() {
    install_mocks();
    set_role_lookup(RolePasswordLookup::NoSuchRole);
    let mut logdetail = None;
    let r = get_role_password("nobody", &mut logdetail).unwrap();
    assert!(r.is_none());
    assert!(logdetail.unwrap().contains("does not exist"));
}

#[test]
fn get_role_password_no_password() {
    install_mocks();
    set_role_lookup(RolePasswordLookup::NoPassword);
    let mut logdetail = None;
    let r = get_role_password("alice", &mut logdetail).unwrap();
    assert!(r.is_none());
    assert!(logdetail.unwrap().contains("no password assigned"));
}

#[test]
fn get_role_password_found_unexpired() {
    install_mocks();
    set_role_lookup(RolePasswordLookup::Found {
        shadow_pass: "md5deadbeefdeadbeefdeadbeefdeadbeef".to_string(),
        valid_until: None,
    });
    let mut logdetail = None;
    let r = get_role_password("alice", &mut logdetail).unwrap();
    assert_eq!(r.as_deref(), Some("md5deadbeefdeadbeefdeadbeefdeadbeef"));
}

#[test]
fn get_role_password_expired() {
    install_mocks();
    set_now(0);
    set_role_lookup(RolePasswordLookup::Found {
        shadow_pass: "secret".to_string(),
        valid_until: Some(-1000), // before "now" (0)
    });
    let mut logdetail = None;
    let r = get_role_password("alice", &mut logdetail).unwrap();
    assert!(r.is_none());
    assert!(logdetail.unwrap().contains("expired password"));
}
