//! Outward seams for `backend-libpq-crypt` (`src/backend/libpq/crypt.c`) into
//! subsystems whose owners are not yet ported and therefore have no seam crate
//! of their own to host the declaration.
//!
//! These are installed by the owning unit once it lands; until then a call
//! panics loudly (no silent fallback). The seams are:
//!
//! * **`common/md5.c`** — `pg_md5_encrypt`. The C signature is
//!   `pg_md5_encrypt(passwd, salt, salt_len, buf, &errstr)`; here it returns
//!   the formatted `"md5"`-prefixed string on success, or the OpenSSL error
//!   string (`Ok(Err(errstr))`) on failure, mirroring the C `bool` + `*errstr`.
//! * **`libpq/auth-scram.c` / `common/scram-common.c`** — the three SCRAM
//!   routines `parse_scram_secret`, `pg_be_scram_build_secret`,
//!   `scram_verify_plain_password`. They depend on crypto primitives
//!   (SHA-256/HMAC/PBKDF2, `pg_strong_random`, SASLprep) absent here, so they
//!   stay behind this seam to keep crypt's build self-contained.

use ::types_error::PgResult;

seam_core::seam!(
    /// `pg_md5_encrypt(passwd, salt, salt_len, buf, &errstr)` (`common/md5.c`):
    /// compute the MD5 password hash of `passwd` salted with `salt`, formatted
    /// as `"md5" + 32 hex digits`. `Ok(Ok(hash))` on success; `Ok(Err(errstr))`
    /// (the OpenSSL error string) on failure, mirroring the C `bool` return +
    /// `*errstr` out-param.
    pub fn pg_md5_encrypt(passwd: &[u8], salt: &[u8]) -> PgResult<Result<String, String>>
);

seam_core::seam!(
    /// `parse_scram_secret(secret, ...)` (`common/scram-common.c`): does
    /// `secret` parse as a valid SCRAM-SHA-256 stored secret? `crypt.c` only
    /// consumes the boolean result (for `get_password_type` classification).
    pub fn parse_scram_secret(secret: &str) -> PgResult<bool>
);

seam_core::seam!(
    /// `pg_be_scram_build_secret(password)` (`libpq/auth-scram.c`): build a
    /// SCRAM-SHA-256 verifier from a plaintext password (random salt, default
    /// iteration count, SASLprep-normalized).
    pub fn pg_be_scram_build_secret(password: &str) -> PgResult<String>
);

seam_core::seam!(
    /// `scram_verify_plain_password(user, password, secret)`
    /// (`libpq/auth-scram.c`): verify a plaintext `password` against a stored
    /// SCRAM secret by recomputing the verifier.
    pub fn scram_verify_plain_password(
        user: &str,
        password: &str,
        secret: &str,
    ) -> PgResult<bool>
);
