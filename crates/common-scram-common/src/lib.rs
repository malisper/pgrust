//! Port of PostgreSQL's `src/common/scram-common.c` — the shared
//! frontend/backend low-level SCRAM-SHA-256 (Salted Challenge Response
//! Authentication Mechanism, RFC 5802) crypto helpers.
//!
//! Reference: `postgres-18.3/src/common/scram-common.c`,
//! `postgres-18.3/src/include/common/scram-common.h`.
//!
//! Every function in `scram-common.c` is ported with behavior identical to
//! PostgreSQL 18.3:
//!
//!   * [`scram_salted_password`] — `scram_SaltedPassword`: PBKDF2 (RFC 2898)
//!     with HMAC-SHA256 as the pseudorandom function.
//!   * [`scram_h`] — `scram_H`: the bare SHA-256 cryptohash of a key-length
//!     input (`H()` in RFC 5802).
//!   * [`scram_client_key`] / [`scram_server_key`] — `scram_ClientKey` /
//!     `scram_ServerKey`: `HMAC(SaltedPassword, "Client Key" / "Server Key")`.
//!   * [`scram_build_secret`] — assemble the stored verifier
//!     `SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>`.
//!
//! # Seam shape (always SHA-256 / 32-byte)
//!
//! The C functions are parameterized by `pg_cryptohash_type hash_type` /
//! `int key_length`, but SCRAM only ever uses SHA-256 (`scram_build_secret`
//! asserts `hash_type == PG_SHA256`). The `backend-libpq-auth-scram-seams`
//! contract therefore specializes the surface to SHA-256: inputs are owned
//! `Vec<u8>`, outputs are owned `[u8; 32]` / `String`, and a crypto failure is
//! reported as `Err(String)` (the C `-1` + `*errstr`). This crate installs
//! those seams from [`init_seams`].
//!
//! # The crypto leaves
//!
//! `scram-common.c` drives `pg_cryptohash_*` (`common/cryptohash.c`) for the
//! plain hash and `pg_hmac_*` (`common/hmac.c`) for the keyed MAC. The
//! cryptohash dispatcher / hmac context machinery are not separately ported
//! here; instead the SHA-256 leg is computed directly over the already-ported
//! software SHA-256 ([`common_sha2`]) — bit-identical to the non-OpenSSL
//! `cryptohash.c`/`hmac.c` build path SCRAM uses — and base64 over the ported
//! [`common_prng_base64`]. The folded HMAC-SHA256 also backs the
//! `pg_hmac_sha256` seam consumed by `auth-scram.c`, installed here too.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::format;
use alloc::string::String;

use common_prng_base64::base64::{pg_b64_enc_len, pg_b64_encode};
use common_sha2::{
    pg_sha256_ctx, pg_sha256_final, pg_sha256_init, pg_sha256_update, PG_SHA256_BLOCK_LENGTH,
    PG_SHA256_DIGEST_LENGTH,
};

// ---------------------------------------------------------------------------
// Constants — `src/include/common/scram-common.h`.
// ---------------------------------------------------------------------------

/// `SCRAM_SHA_256_NAME` — name of the SCRAM mechanism per IANA.
pub const SCRAM_SHA_256_NAME: &str = "SCRAM-SHA-256";
/// `SCRAM_SHA_256_PLUS_NAME` — the channel-binding variant.
pub const SCRAM_SHA_256_PLUS_NAME: &str = "SCRAM-SHA-256-PLUS";

/// `SCRAM_SHA_256_KEY_LEN` — length of SCRAM keys (client and server).
/// (`= PG_SHA256_DIGEST_LENGTH`.)
pub const SCRAM_SHA_256_KEY_LEN: usize = PG_SHA256_DIGEST_LENGTH;

/// `SCRAM_MAX_KEY_LEN` — size of buffers used internally by SCRAM routines, the
/// maximum `SCRAM_SHA_*_KEY_LEN` among supported hash methods.
pub const SCRAM_MAX_KEY_LEN: usize = SCRAM_SHA_256_KEY_LEN;

/// `SCRAM_RAW_NONCE_LEN` — size of the random nonce generated in the exchange.
pub const SCRAM_RAW_NONCE_LEN: usize = 18;

/// `SCRAM_DEFAULT_SALT_LEN` — salt length when generating new secrets, in bytes.
pub const SCRAM_DEFAULT_SALT_LEN: usize = 16;

/// `SCRAM_SHA_256_DEFAULT_ITERATIONS` — default iteration count when generating
/// a secret (at least 4096 per RFC 7677).
pub const SCRAM_SHA_256_DEFAULT_ITERATIONS: i32 = 4096;

// ---------------------------------------------------------------------------
// SHA-256 cryptohash leaf (the `pg_cryptohash_*` SHA-256 path).
// ---------------------------------------------------------------------------

/// A fresh, initialized SHA-256 context (`pg_cryptohash_create(PG_SHA256)` +
/// `pg_cryptohash_init`).
fn sha256_new() -> pg_sha256_ctx {
    let mut ctx = pg_sha256_ctx {
        state: [0; 8],
        bitcount: 0,
        buffer: [0; PG_SHA256_BLOCK_LENGTH],
    };
    pg_sha256_init(&mut ctx);
    ctx
}

/// `H(input)` over SHA-256 — the single-shot cryptohash of `input`. The C
/// software `cryptohash.c` SHA-256 path cannot fail for a correctly sized
/// destination, so this is infallible; it returns the 32-byte digest.
fn sha256(input: &[u8]) -> [u8; PG_SHA256_DIGEST_LENGTH] {
    let mut ctx = sha256_new();
    pg_sha256_update(&mut ctx, input, input.len());
    let mut digest = [0u8; PG_SHA256_DIGEST_LENGTH];
    pg_sha256_final(&mut ctx, &mut digest);
    digest
}

// ---------------------------------------------------------------------------
// HMAC-SHA256 leaf (the `pg_hmac_*` SHA-256 path, RFC 2104 / `common/hmac.c`).
// ---------------------------------------------------------------------------

const HMAC_IPAD: u8 = 0x36;
const HMAC_OPAD: u8 = 0x5C;

/// `HMAC-SHA256(key, msg)` — the in-tree (non-OpenSSL) `pg_hmac_*` SHA-256
/// sequence (`pg_hmac_create(PG_SHA256)` → `pg_hmac_init(key)` →
/// `pg_hmac_update(msg)` → `pg_hmac_final`) folded into one call over the
/// software SHA-256. Pure RFC 2104 arithmetic; cannot fail for SHA-256, so it
/// returns the 32-byte MAC directly.
///
/// Mirrors `hmac.c`: the pads are sized to the block length, the key is
/// SHA-256-shrunk if longer than the block, XOR'd into `k_ipad`/`k_opad`, and
/// the MAC is `H(K XOR opad, H(K XOR ipad, msg))`.
fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; PG_SHA256_DIGEST_LENGTH] {
    let block_size = PG_SHA256_BLOCK_LENGTH;

    let mut k_ipad = [HMAC_IPAD; PG_SHA256_BLOCK_LENGTH];
    let mut k_opad = [HMAC_OPAD; PG_SHA256_BLOCK_LENGTH];

    // If the key is longer than the block size, hash it once to shrink it.
    let shrunk;
    let key: &[u8] = if key.len() > block_size {
        shrunk = sha256(key);
        &shrunk[..]
    } else {
        key
    };

    // for (i = 0; i < len; i++) { k_ipad[i] ^= key[i]; k_opad[i] ^= key[i]; }
    #[allow(clippy::needless_range_loop)]
    for i in 0..key.len() {
        k_ipad[i] ^= key[i];
        k_opad[i] ^= key[i];
    }

    // tmp = H(K XOR ipad, text)
    let mut inner = sha256_new();
    pg_sha256_update(&mut inner, &k_ipad, block_size);
    pg_sha256_update(&mut inner, msg, msg.len());
    let mut h = [0u8; PG_SHA256_DIGEST_LENGTH];
    pg_sha256_final(&mut inner, &mut h);

    // result = H(K XOR opad, tmp)
    let mut outer = sha256_new();
    pg_sha256_update(&mut outer, &k_opad, block_size);
    pg_sha256_update(&mut outer, &h, h.len());
    let mut result = [0u8; PG_SHA256_DIGEST_LENGTH];
    pg_sha256_final(&mut outer, &mut result);

    result
}

// ---------------------------------------------------------------------------
// scram_SaltedPassword — PBKDF2(HMAC-SHA256) per RFC 2898.
// ---------------------------------------------------------------------------

/// `scram_SaltedPassword(password, PG_SHA256, SCRAM_SHA_256_KEY_LEN, salt,
/// saltlen, iterations, result, &errstr)`.
///
/// Calculate `SaltedPassword`. The password should already be normalized by
/// SASLprep. Returns the 32-byte salted password, or `Err` on failure (the C
/// `-1` + `*errstr`).
pub fn scram_salted_password(
    password: &[u8],
    salt: &[u8],
    iterations: i32,
) -> Result<[u8; PG_SHA256_DIGEST_LENGTH], String> {
    let key_length = PG_SHA256_DIGEST_LENGTH;

    // C: `uint32 one = pg_hton32(1)` then feeds `(uint8 *) &one, sizeof(uint32)`.
    // `pg_hton32(1)` stores 1 big-endian, so the 4 bytes fed are [0,0,0,1] on
    // any host — exactly `1u32.to_be_bytes()`.
    let one_bytes = 1u32.to_be_bytes();

    let mut Ui = [0u8; PG_SHA256_DIGEST_LENGTH];
    let mut Ui_prev = [0u8; PG_SHA256_DIGEST_LENGTH];
    let mut result = [0u8; PG_SHA256_DIGEST_LENGTH];

    // Iterate hash calculation of HMAC entry using given salt. This is
    // essentially PBKDF2 (RFC 2898) with HMAC() as the pseudorandom function.

    // First iteration: Ui_prev = HMAC(password, salt || INT(1)).
    {
        // The C drives pg_hmac_init(password) + update(salt) + update(&one) +
        // final. HMAC over the concatenation salt||one is bit-identical.
        let mut msg = alloc::vec::Vec::with_capacity(salt.len() + one_bytes.len());
        msg.extend_from_slice(salt);
        msg.extend_from_slice(&one_bytes);
        Ui_prev.copy_from_slice(&hmac_sha256(password, &msg));
    }

    result.copy_from_slice(&Ui_prev);

    // Subsequent iterations.
    let mut i = 1;
    while i < iterations {
        // Make sure that this is interruptible as scram_iterations could be set
        // to a large value (C backend `CHECK_FOR_INTERRUPTS()`). Interrupt
        // servicing is the not-yet-ported miscadmin runtime; this PBKDF2 leaf
        // does not reach it, matching the frontend `#ifndef FRONTEND` elision.

        Ui.copy_from_slice(&hmac_sha256(password, &Ui_prev[..key_length]));

        for j in 0..key_length {
            result[j] ^= Ui[j];
        }
        Ui_prev.copy_from_slice(&Ui);

        i += 1;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// scram_H — H(input).
// ---------------------------------------------------------------------------

/// `scram_H(input, PG_SHA256, SCRAM_SHA_256_KEY_LEN, result, &errstr)`.
///
/// Calculate the SHA-256 hash of the key-length `input`. Returns the 32-byte
/// digest, or `Err` on failure.
pub fn scram_h(input: &[u8]) -> Result<[u8; PG_SHA256_DIGEST_LENGTH], String> {
    let key_length = PG_SHA256_DIGEST_LENGTH;
    Ok(sha256(&input[..key_length]))
}

// ---------------------------------------------------------------------------
// scram_ClientKey / scram_ServerKey — HMAC(SaltedPassword, label).
// ---------------------------------------------------------------------------

/// `scram_ClientKey(salted_password, PG_SHA256, key_length, result, &errstr)` —
/// `HMAC(SaltedPassword, "Client Key")`.
pub fn scram_client_key(
    salted_password: &[u8],
) -> Result<[u8; PG_SHA256_DIGEST_LENGTH], String> {
    scram_key(salted_password, b"Client Key")
}

/// `scram_ServerKey(salted_password, PG_SHA256, key_length, result, &errstr)` —
/// `HMAC(SaltedPassword, "Server Key")`.
pub fn scram_server_key(
    salted_password: &[u8],
) -> Result<[u8; PG_SHA256_DIGEST_LENGTH], String> {
    scram_key(salted_password, b"Server Key")
}

/// Shared body of `scram_ClientKey` / `scram_ServerKey` — the two C functions
/// are byte-identical aside from the HMAC message label.
fn scram_key(
    salted_password: &[u8],
    label: &[u8],
) -> Result<[u8; PG_SHA256_DIGEST_LENGTH], String> {
    let key_length = PG_SHA256_DIGEST_LENGTH;
    Ok(hmac_sha256(&salted_password[..key_length], label))
}

// ---------------------------------------------------------------------------
// scram_build_secret — assemble the stored verifier string.
// ---------------------------------------------------------------------------

/// `scram_build_secret(PG_SHA256, key_length, salt, saltlen, iterations,
/// password, &errstr)`.
///
/// Construct a SCRAM secret for storing in `pg_authid.rolpassword`. The
/// password should already have been processed with SASLprep. Returns the
/// owned verifier string `SCRAM-SHA-256$<iter>:<salt>$<StoredKey>:<ServerKey>`,
/// or `Err` on failure.
///
/// In C the backend path `elog(ERROR)`s on a crypto failure (no return) while
/// the frontend path returns `NULL`; both surface here as `Err`.
pub fn scram_build_secret(
    salt: &[u8],
    iterations: i32,
    password: &[u8],
) -> Result<String, String> {
    let key_length = PG_SHA256_DIGEST_LENGTH;

    // C `Assert(hash_type == PG_SHA256)`: this seam is SHA-256-only by contract.
    // C `Assert(iterations > 0)`.
    if iterations <= 0 {
        return Err(String::from("scram_build_secret: iterations must be > 0"));
    }

    // Calculate StoredKey and ServerKey.
    let salted_password =
        scram_salted_password(password, salt, iterations).map_err(stored_key_err)?;
    let stored_key = scram_client_key(&salted_password).map_err(stored_key_err)?;
    let stored_key = scram_h(&stored_key).map_err(stored_key_err)?;
    let server_key = scram_server_key(&salted_password).map_err(stored_key_err)?;

    // The format is:
    //   SCRAM-SHA-256$<iteration count>:<salt>$<StoredKey>:<ServerKey>
    let encoded_salt = b64_encode(salt).ok_or_else(|| String::from("could not encode salt"))?;
    let encoded_stored = b64_encode(&stored_key[..key_length])
        .ok_or_else(|| String::from("could not encode stored key"))?;
    let encoded_server = b64_encode(&server_key[..key_length])
        .ok_or_else(|| String::from("could not encode server key"))?;

    Ok(format!(
        "{SCRAM_SHA_256_NAME}${iterations}:{encoded_salt}${encoded_stored}:{encoded_server}"
    ))
}

/// `pg_b64_encode(src, len, dst, dstlen)` over an owned destination buffer
/// sized with `pg_b64_enc_len`, returning the encoded string. `None` mirrors
/// the C `pg_b64_encode(...) < 0` failure path.
fn b64_encode(src: &[u8]) -> Option<String> {
    let dstlen = pg_b64_enc_len(src.len() as i32);
    let mut dst = alloc::vec![0u8; dstlen as usize];
    let n = pg_b64_encode(src, src.len() as i32, &mut dst, dstlen);
    if n < 0 {
        return None;
    }
    dst.truncate(n as usize);
    String::from_utf8(dst).ok()
}

/// The C `scram_build_secret` error path: on any crypto-leaf failure it reports
/// `"could not calculate stored key and server key: %s"` (backend `elog(ERROR)`)
/// with the leaf's `*errstr`. Reproduce that wrapping message here.
fn stored_key_err(error: String) -> String {
    format!("could not calculate stored key and server key: {error}")
}

// ---------------------------------------------------------------------------
// Seam installation — retire the scram-common.c (and folded hmac.c) outward
// seams declared by `backend-libpq-auth-scram-seams`.
// ---------------------------------------------------------------------------

/// Install the SCRAM crypto-kernel seams consumed by `auth-scram.c`.
///
/// These are *outward* seams declared in `backend-libpq-auth-scram-seams` whose
/// owner (`common/scram-common.c`, here) had not landed; `auth-scram.c`
/// loud-panicked on them until now. With the kernel ported they resolve to the
/// real functions:
///
///   * `scram_h`, `scram_salted_password`, `scram_server_key`,
///     `scram_build_secret` — `common/scram-common.c`.
///   * `pg_hmac_sha256` — the `common/hmac.c` SHA-256 leg, folded to a single
///     call (HMAC over the concatenation of the C `update` sequence is
///     bit-identical); ported here as the in-tree HMAC-SHA256 leaf.
pub fn init_seams() {
    backend_libpq_auth_scram_seams::scram_h::set(|input| scram_h(&input));
    backend_libpq_auth_scram_seams::scram_salted_password::set(|password, salt, iterations| {
        scram_salted_password(&password, &salt, iterations)
    });
    backend_libpq_auth_scram_seams::scram_server_key::set(|salted_password| {
        scram_server_key(&salted_password)
    });
    backend_libpq_auth_scram_seams::scram_build_secret::set(|salt, iterations, password| {
        scram_build_secret(&salt, iterations, &password)
    });
    backend_libpq_auth_scram_seams::pg_hmac_sha256::set(|key, msg| Ok(hmac_sha256(&key, &msg)));
}

#[cfg(test)]
mod tests;
