//! Seam declarations for `src/backend/libpq/auth-scram.c` — the genuinely
//! external/unported leaves the server-side SCRAM-SHA-256 mechanism reaches.
//!
//! All of these are *outward* seams: declared here, **installed by their own
//! owners** when those land. Until then a call panics loudly (no silent
//! fallback). The owner crate `backend-libpq-auth-scram` consumes them.
//!
//! The absent owners are:
//!
//!   * `common/scram-common.c` — the SCRAM crypto kernel (`scram_H`,
//!     `scram_SaltedPassword`, `scram_ServerKey`, `scram_build_secret`).
//!   * `common/hmac.c` — keyed HMAC. The C drives `pg_hmac_create/init/
//!     update*/final`; the multi-`update` sequence is folded into a single
//!     `pg_hmac_sha256(key, msg)` here (HMAC over the concatenation is
//!     bit-identical to HMAC fed the same bytes across several updates).
//!   * `common/saslprep.c` — SASLprep (RFC 4013) Unicode normalization.
//!   * `port/pg_strong_random.c` — CSPRNG (declared by
//!     `port-pg-strong-random-seams`, re-used; not redeclared here).
//!   * `access/xlog.c` — the cluster mock-authentication nonce
//!     (`GetMockAuthenticationNonce`).
//!
//! The low-level `pg_cryptohash_*` primitive is already declared by
//! `common-cryptohash-seams`; `scram_mock_salt` is implemented in the owner
//! crate over those plus `get_mock_authentication_nonce`, so it crosses no seam
//! here.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

// ---------------------------------------------------------------------------
// common/scram-common.c — the SCRAM crypto kernel.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `scram_H(input, hash_type, key_length, result, &errstr)`
    /// (`common/scram-common.c`): a single plain hash (`H()` in RFC 5802) of
    /// `input` with SHA-256. `Ok(digest)` on success, `Err(errstr)` on a crypto
    /// failure. Always SHA-256 / 32-byte output here.
    pub fn scram_h(input: Vec<u8>) -> Result<[u8; 32], String>
);

seam_core::seam!(
    /// `scram_SaltedPassword(password, hash_type, key_length, salt, saltlen,
    /// iterations, result, &errstr)` (`common/scram-common.c`): PBKDF2-HMAC of
    /// `password` with `salt` for `iterations` rounds (`SaltedPassword` in
    /// RFC 5802). `Ok(32-byte key)` / `Err(errstr)`.
    pub fn scram_salted_password(
        password: Vec<u8>,
        salt: Vec<u8>,
        iterations: i32,
    ) -> Result<[u8; 32], String>
);

seam_core::seam!(
    /// `scram_ServerKey(salted_password, hash_type, key_length, result,
    /// &errstr)` (`common/scram-common.c`): HMAC(SaltedPassword, "Server Key").
    /// `Ok(32-byte ServerKey)` / `Err(errstr)`.
    pub fn scram_server_key(salted_password: Vec<u8>) -> Result<[u8; 32], String>
);

seam_core::seam!(
    /// `scram_build_secret(hash_type, key_length, salt, saltlen, iterations,
    /// password, &errstr)` (`common/scram-common.c`): assemble the full
    /// `SCRAM-SHA-256$<iter>:<salt>$<storedkey>:<serverkey>` verifier string
    /// from a (SASLprep'd) password and raw salt. `Ok(secret)` / `Err(errstr)`.
    pub fn scram_build_secret(
        salt: Vec<u8>,
        iterations: i32,
        password: Vec<u8>,
    ) -> Result<String, String>
);

// ---------------------------------------------------------------------------
// common/hmac.c — keyed-hash MAC (SHA-256).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// The `pg_hmac_create(SHA256)` → `pg_hmac_init(key)` →
    /// `pg_hmac_update(msg)`* → `pg_hmac_final` → `pg_hmac_free` sequence
    /// (`common/hmac.c`), folded to a single call. `Ok(32-byte MAC)` on
    /// success; `Err(errstr)` carries `pg_hmac_error(ctx)`.
    pub fn pg_hmac_sha256(key: Vec<u8>, msg: Vec<u8>) -> Result<[u8; 32], String>
);

// ---------------------------------------------------------------------------
// common/saslprep.c — SASLprep (RFC 4013).
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `pg_saslprep(input, &output)` (`common/saslprep.c`): normalize `input`
    /// per the SASLprep profile. `Some(normalized)` when `rc ==
    /// SASLPREP_SUCCESS`; `None` for any other return (invalid UTF-8 /
    /// prohibited chars / OOM), in which case the caller uses the raw bytes.
    pub fn pg_saslprep(input: Vec<u8>) -> Option<Vec<u8>>
);

// ---------------------------------------------------------------------------
// access/xlog.c — the cluster mock-authentication nonce.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `GetMockAuthenticationNonce()` (`access/xlog.c`): the control file's
    /// `MOCK_AUTH_NONCE_LEN`-byte cluster nonce, used to derive a deterministic
    /// fake salt for nonexistent roles. `None` only if the control file is not
    /// available.
    pub fn get_mock_authentication_nonce() -> Option<Vec<u8>>
);
