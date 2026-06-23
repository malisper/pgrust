//! Tests for `backend-libpq-auth-scram`.
//!
//! The in-crate parsers (`parse_scram_secret`, the message-syntax helpers, the
//! C string primitives) are crypto-free and tested directly. The crypto/RNG/
//! normalization seams are installed with deterministic mocks where a test
//! exercises a function that crosses them.

use super::*;
use std::sync::Once;

/// `pg_b64_encode` convenience wrapper returning a `String`.
fn b64(src: &[u8]) -> String {
    let cap = pg_b64_encode_len(src.len());
    let mut dst = vec![0u8; cap];
    let n = pg_b64_encode(src, src.len() as i32, &mut dst, cap as i32);
    assert!(n >= 0);
    dst.truncate(n as usize);
    String::from_utf8(dst).unwrap()
}

fn pg_b64_encode_len(srclen: usize) -> usize {
    pg_b64_enc_len(srclen as i32) as usize
}

/// Build a syntactically-valid SCRAM-SHA-256 secret string.
fn make_secret(iterations: i32, salt: &[u8], stored: &[u8], server: &[u8]) -> String {
    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        iterations,
        b64(salt),
        b64(stored),
        b64(server),
    )
}

#[test]
fn parse_scram_secret_valid() {
    let salt = [0xABu8; 16];
    let stored = [0x11u8; 32];
    let server = [0x22u8; 32];
    let secret = make_secret(4096, &salt, &stored, &server);

    let mut iterations = 0;
    let mut hash_type = PG_SHA256;
    let mut key_length = 0;
    let mut out_salt = None;
    let mut stored_key = [0u8; SCRAM_MAX_KEY_LEN];
    let mut server_key = [0u8; SCRAM_MAX_KEY_LEN];

    let ok = parse_scram_secret(
        secret.as_bytes(),
        &mut iterations,
        &mut hash_type,
        &mut key_length,
        &mut out_salt,
        &mut stored_key,
        &mut server_key,
    )
    .unwrap();

    assert!(ok);
    assert_eq!(iterations, 4096);
    assert_eq!(hash_type, PG_SHA256);
    assert_eq!(key_length, 32);
    assert_eq!(out_salt.unwrap(), b64(&salt));
    assert_eq!(&stored_key[..], &stored[..]);
    assert_eq!(&server_key[..], &server[..]);
}

fn parse_fail(secret: &str) -> bool {
    let mut iterations = 0;
    let mut hash_type = PG_SHA256;
    let mut key_length = 0;
    let mut out_salt = Some("sentinel".to_string());
    let mut stored_key = [0u8; SCRAM_MAX_KEY_LEN];
    let mut server_key = [0u8; SCRAM_MAX_KEY_LEN];
    let ok = parse_scram_secret(
        secret.as_bytes(),
        &mut iterations,
        &mut hash_type,
        &mut key_length,
        &mut out_salt,
        &mut stored_key,
        &mut server_key,
    )
    .unwrap();
    assert!(out_salt.is_none() || ok);
    ok
}

#[test]
fn parse_scram_secret_rejects_wrong_scheme() {
    let salt = [1u8; 16];
    let key = [2u8; 32];
    let secret = make_secret(4096, &salt, &key, &key);
    let bad = secret.replacen("SCRAM-SHA-256", "SCRAM-SHA-512", 1);
    assert!(!parse_fail(&bad));
}

#[test]
fn parse_scram_secret_rejects_missing_delimiters() {
    assert!(!parse_fail("SCRAM-SHA-256")); // no '$'
    assert!(!parse_fail("SCRAM-SHA-256$4096")); // no ':'
    assert!(!parse_fail("SCRAM-SHA-256$4096:c2FsdA==")); // no second '$'
    assert!(!parse_fail("SCRAM-SHA-256$4096:c2FsdA==$stored")); // no second ':'
}

#[test]
fn parse_scram_secret_rejects_non_numeric_iterations() {
    let salt = [1u8; 16];
    let key = [2u8; 32];
    let mut secret = make_secret(4096, &salt, &key, &key);
    secret = secret.replacen("4096", "12ab", 1);
    assert!(!parse_fail(&secret));
}

#[test]
fn parse_scram_secret_rejects_wrong_key_length() {
    let salt = [1u8; 16];
    let shortkey = [2u8; 16];
    let secret = make_secret(4096, &salt, &shortkey, &shortkey);
    assert!(!parse_fail(&secret));
}

#[test]
fn is_scram_printable_works() {
    assert!(is_scram_printable(b"abcXYZ123-._~"));
    assert!(!is_scram_printable(b"has,comma"));
    assert!(!is_scram_printable(b"has space"));
    assert!(!is_scram_printable(&[0x7f])); // DEL is out of range
    assert!(is_scram_printable(b"ok\0,not-checked"));
}

#[test]
fn sanitize_char_works() {
    assert_eq!(sanitize_char(b'A'), "'A'");
    assert_eq!(sanitize_char(b' '), "0x20");
    assert_eq!(sanitize_char(0x00), "0x00");
    assert_eq!(sanitize_char(0xff), "0xff");
}

#[test]
fn sanitize_str_works() {
    assert_eq!(sanitize_str(b"clean"), "clean");
    assert_eq!(sanitize_str(b"a\tb c"), "a?b?c");
    let long = vec![b'x'; 50];
    assert_eq!(sanitize_str(&long).len(), 30);
    assert_eq!(sanitize_str(b"ab\0cd"), "ab");
}

#[test]
fn read_attr_value_works() {
    let mut buf = b"n=user,r=nonce\0".to_vec();
    let mut input = 0usize;
    let (start, end) = read_attr_value(&mut buf, &mut input, b'n').unwrap();
    assert_eq!(&buf[start..end], b"user");
    let (s2, e2) = read_attr_value(&mut buf, &mut input, b'r').unwrap();
    assert_eq!(&buf[s2..e2], b"nonce");
}

#[test]
fn read_attr_value_rejects_wrong_attr() {
    let mut buf = b"n=user\0".to_vec();
    let mut input = 0usize;
    let err = read_attr_value(&mut buf, &mut input, b'r').unwrap_err();
    assert!(format!("{err:?}").contains("malformed SCRAM message"));
}

#[test]
fn read_any_attr_works() {
    let mut buf = b"r=somenonce,p=proof\0".to_vec();
    let mut input = 0usize;
    let mut attr = 0u8;
    let (start, end) = read_any_attr(&mut buf, &mut input, Some(&mut attr)).unwrap();
    assert_eq!(attr, b'r');
    assert_eq!(&buf[start..end], b"somenonce");
}

#[test]
fn read_any_attr_rejects_non_alpha() {
    let mut buf = b"9=bad\0".to_vec();
    let mut input = 0usize;
    let err = read_any_attr(&mut buf, &mut input, None).unwrap_err();
    assert!(format!("{err:?}").contains("malformed SCRAM message"));
}

#[test]
fn strtol_base10_full_works() {
    assert_eq!(strtol_base10_full(b"4096"), Some(4096));
    assert_eq!(strtol_base10_full(b"  +12"), Some(12));
    assert_eq!(strtol_base10_full(b"-7"), Some(-7));
    assert_eq!(strtol_base10_full(b"12ab"), None); // trailing chars
    assert_eq!(strtol_base10_full(b""), None); // no digits
    assert_eq!(strtol_base10_full(b"99999999999999999999"), None); // overflow
}

// --- seam-crossing functions (deterministic mocks) --------------------------

static INIT: Once = Once::new();

fn install_mocks() {
    INIT.call_once(|| {
        // Deterministic "SASLprep": passthrough (raw bytes used).
        scram_seams::pg_saslprep::set(|_p| None);
        // Deterministic "strong random": fixed bytes.
        pg_strong_random::pg_strong_random::set(|buf| {
            for b in buf.iter_mut() {
                *b = 0x5A;
            }
            true
        });
        // Deterministic "build secret".
        scram_seams::scram_build_secret::set(|salt, iter, password| {
            Ok(format!(
                "SCRAM-SHA-256${iter}:{}$len{}:len{}",
                b64(&salt),
                password.len(),
                password.len()
            ))
        });
        // Deterministic SaltedPassword: 32 bytes from password+salt+iter.
        scram_seams::scram_salted_password::set(|password, salt, iter| {
            let mut out = [0u8; 32];
            for (i, b) in out.iter_mut().enumerate() {
                let p = password.get(i % password.len().max(1)).copied().unwrap_or(0);
                let s = salt.get(i % salt.len().max(1)).copied().unwrap_or(0);
                *b = p ^ s ^ (iter as u8) ^ (i as u8);
            }
            Ok(out)
        });
        // Deterministic ServerKey: identity over the salted password.
        scram_seams::scram_server_key::set(|salted| {
            let mut out = [0u8; 32];
            out.copy_from_slice(&salted[..32]);
            Ok(out)
        });
        // Deterministic mock salt: cluster nonce of fixed bytes.
        scram_seams::get_mock_authentication_nonce::set(|| Some(vec![0x33u8; MOCK_AUTH_NONCE_LEN]));
        // Deterministic cryptohash producing a fixed digest. The opaque ctx is
        // never dereferenced by the consumer, so a non-null sentinel suffices.
        cryptohash_seams::pg_cryptohash_create::set(|_t| {
            core::ptr::NonNull::<crypto::pg_cryptohash_ctx>::dangling().as_ptr()
        });
        cryptohash_seams::pg_cryptohash_init::set(|_c| 0);
        cryptohash_seams::pg_cryptohash_update::set(|_c, _d, _l| 0);
        cryptohash_seams::pg_cryptohash_final::set(|_c, dest, len| {
            // SAFETY: test-only; dest points to a `len`-byte buffer.
            unsafe {
                for i in 0..len {
                    *dest.add(i) = 0x44;
                }
            }
            0
        });
        cryptohash_seams::pg_cryptohash_free::set(|_c| {});
        // get_password_type: classify by the scheme prefix.
        user_seams::get_password_type::set(|p| {
            if p.starts_with("SCRAM-SHA-256$") {
                Ok(authid::PasswordType::ScramSha256)
            } else if p.starts_with("md5") {
                Ok(authid::PasswordType::Md5)
            } else {
                Ok(authid::PasswordType::Plaintext)
            }
        });
    });
}

#[test]
fn pg_be_scram_build_secret_works() {
    install_mocks();
    let out = pg_be_scram_build_secret(b"hunter2").unwrap();
    assert!(out.starts_with("SCRAM-SHA-256$"));
}

#[test]
fn scram_verify_plain_password_matches() {
    install_mocks();
    let salt = [0xABu8; 16];
    // Mirror the mock derivation: ServerKey == SaltedPassword identity.
    let password = b"pw";
    let mut server = [0u8; 32];
    for (i, b) in server.iter_mut().enumerate() {
        let p = password[i % password.len()];
        let s = salt[i % salt.len()];
        *b = p ^ s ^ (4096u32 as u8) ^ (i as u8);
    }
    let stored = [0u8; 32]; // unused by verify
    let secret = make_secret(4096, &salt, &stored, &server);
    let ok = scram_verify_plain_password("alice", b"pw", secret.as_bytes()).unwrap();
    assert!(ok);
}

#[test]
fn scram_verify_plain_password_rejects_wrong_password() {
    install_mocks();
    let salt = [0xABu8; 16];
    let server = [0x00u8; 32];
    let stored = [0u8; 32];
    let secret = make_secret(4096, &salt, &stored, &server);
    let ok = scram_verify_plain_password("alice", b"wrong", secret.as_bytes()).unwrap();
    assert!(!ok);
}

#[test]
fn mock_scram_secret_produces_parseable_salt() {
    install_mocks();
    let mut hash_type = PG_SHA256;
    let mut iterations = 0;
    let mut key_length = 0;
    let mut salt = None;
    let mut stored_key = [9u8; SCRAM_MAX_KEY_LEN];
    let mut server_key = [9u8; SCRAM_MAX_KEY_LEN];
    mock_scram_secret(
        "alice",
        &mut hash_type,
        &mut iterations,
        &mut key_length,
        &mut salt,
        &mut stored_key,
        &mut server_key,
    )
    .unwrap();
    assert_eq!(hash_type, PG_SHA256);
    assert_eq!(iterations, SCRAM_SHA_256_DEFAULT_ITERATIONS);
    assert!(salt.is_some());
    assert_eq!(&stored_key[..], &[0u8; SCRAM_MAX_KEY_LEN][..]);
    assert_eq!(&server_key[..], &[0u8; SCRAM_MAX_KEY_LEN][..]);
}
