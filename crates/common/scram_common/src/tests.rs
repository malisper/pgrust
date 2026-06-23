//! Known-answer tests for the SCRAM-SHA-256 crypto kernel.
//!
//! The SHA-256 / HMAC-SHA256 / PBKDF2 leaves and the verifier format are pinned
//! against published vectors (NIST SHA-256, RFC 4231 HMAC-SHA256, and the
//! RFC 7677 §3 SCRAM-SHA-256 worked example) so a constant-table or
//! byte-ordering regression is caught here.

use super::*;

/// Decode a base64 string to bytes (test helper; uses the ported decoder).
fn b64_decode(s: &str) -> alloc::vec::Vec<u8> {
    let src = s.as_bytes();
    let dstlen = prng_base64::base64::pg_b64_dec_len(src.len() as i32);
    let mut dst = alloc::vec![0u8; dstlen as usize];
    let n = prng_base64::base64::pg_b64_decode(src, src.len() as i32, &mut dst, dstlen);
    assert!(n >= 0, "b64 decode failed");
    dst.truncate(n as usize);
    dst
}

fn hex(bytes: &[u8]) -> alloc::string::String {
    use core::fmt::Write;
    let mut s = alloc::string::String::new();
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[test]
fn sha256_nist_vector() {
    // NIST: SHA-256("abc").
    assert_eq!(
        hex(&sha256(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    // SHA-256("") empty.
    assert_eq!(
        hex(&sha256(b"")),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn hmac_sha256_rfc4231_vector() {
    // RFC 4231 Test Case 1: key = 0x0b * 20, data = "Hi There".
    let key = [0x0bu8; 20];
    let mac = hmac_sha256(&key, b"Hi There");
    assert_eq!(
        hex(&mac),
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
}

#[test]
fn hmac_sha256_long_key_vector() {
    // RFC 4231 Test Case 6: key = 0xaa * 131 (> block size, gets SHA-256 shrunk).
    let key = [0xaau8; 131];
    let mac = hmac_sha256(
        &key,
        b"Test Using Larger Than Block-Size Key - Hash Key First",
    );
    assert_eq!(
        hex(&mac),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

#[test]
fn client_server_stored_key_rfc7677() {
    // RFC 7677 §3: with the worked example the derived keys are:
    //   ClientKey  = HMAC(SaltedPassword, "Client Key")
    //   StoredKey  = H(ClientKey)
    //   ServerKey  = HMAC(SaltedPassword, "Server Key")
    // The wire example carries StoredKey/ServerKey via the client-proof and
    // server-signature; we pin StoredKey and ServerKey directly.
    let salt = b64_decode("W22ZaJ0SNY7soEsUEjb6gQ==");
    let salted = scram_salted_password(b"pencil", &salt, 4096).unwrap();

    let client_key = scram_client_key(&salted).unwrap();
    let stored_key = scram_h(&client_key).unwrap();
    let server_key = scram_server_key(&salted).unwrap();

    // StoredKey base64 from RFC 7677 server-side stored value.
    assert_eq!(b64_encode(&stored_key).unwrap(), "WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=");
    // ServerKey base64 from RFC 7677.
    assert_eq!(b64_encode(&server_key).unwrap(), "wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=");
}

#[test]
fn build_secret_format_and_roundtrip() {
    let salt = b64_decode("W22ZaJ0SNY7soEsUEjb6gQ==");
    let secret = scram_build_secret(&salt, 4096, b"pencil").unwrap();

    // Format: SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>
    assert!(secret.starts_with("SCRAM-SHA-256$4096:"));
    let body = secret.strip_prefix("SCRAM-SHA-256$4096:").unwrap();
    let (encoded_salt, rest) = body.split_once('$').unwrap();
    let (encoded_stored, encoded_server) = rest.split_once(':').unwrap();

    assert_eq!(encoded_salt, "W22ZaJ0SNY7soEsUEjb6gQ==");
    assert_eq!(encoded_stored, "WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=");
    assert_eq!(encoded_server, "wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=");
}

#[test]
fn build_secret_rejects_nonpositive_iterations() {
    let salt = [0u8; 16];
    assert!(scram_build_secret(&salt, 0, b"pw").is_err());
    assert!(scram_build_secret(&salt, -1, b"pw").is_err());
}

#[test]
fn constants_match_header() {
    assert_eq!(SCRAM_SHA_256_NAME, "SCRAM-SHA-256");
    assert_eq!(SCRAM_SHA_256_PLUS_NAME, "SCRAM-SHA-256-PLUS");
    assert_eq!(SCRAM_SHA_256_KEY_LEN, 32);
    assert_eq!(SCRAM_MAX_KEY_LEN, 32);
    assert_eq!(SCRAM_RAW_NONCE_LEN, 18);
    assert_eq!(SCRAM_DEFAULT_SALT_LEN, 16);
    assert_eq!(SCRAM_SHA_256_DEFAULT_ITERATIONS, 4096);
}
