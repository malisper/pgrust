//! Idiomatic 1:1 port of `src/backend/utils/adt/cryptohashfuncs.c` — the
//! SQL-callable cryptographic-hash functions `md5(text)` / `md5(bytea)` and
//! `sha224`/`sha256`/`sha384`/`sha512` over `bytea`.
//!
//! Reference: `postgres-18.3/src/backend/utils/adt/cryptohashfuncs.c`.
//!
//! # Faithfulness notes
//!
//! * **MD5** (`md5_text`, `md5_bytea`): the C calls `pg_md5_hash(VARDATA_ANY,
//!   len, hexsum, &errstr)` (from `common/md5.c`) and returns `cstring_to_text`
//!   of the 32-char lowercase-hex digest.  This port calls
//!   [`common_md5::pg_md5_hash`] and returns the hex string as a `text`
//!   varlena.  The C `ereport(ERROR, ERRCODE_OUT_OF_MEMORY, "could not compute
//!   %s hash: %s")` on the OpenSSL-backend failure path is reproduced through a
//!   `PgError`, though the software fallback never fails.
//!
//! * **SHA** (`sha224_bytea` … `sha512_bytea`): the C runs the input through
//!   the `pg_cryptohash_*` abstraction (`cryptohash_internal`), which on a
//!   non-OpenSSL build is the in-tree `common/sha2.c` reference.  This port
//!   calls [`common_sha2`] directly — the same software fallback — initializing
//!   the matching context, feeding the detoasted `VARDATA_ANY` payload, and
//!   emitting the fixed-length raw digest as a `bytea` varlena.  The C
//!   `elog(ERROR, "could not {initialize,update,finalize} %s context")` paths
//!   cannot be reached by the software implementation (no per-call failure
//!   mode), matching upstream behavior.
//!
//! Both families produce a by-ref `text`/`bytea` result, which crosses the
//! current fmgr boundary on the `RefPayload::Varlena` lane, so all six builtins
//! are registered (see [`fmgr_builtins`]).

pub mod fmgr_builtins;

use common_sha2::{
    pg_sha224_ctx, pg_sha224_final, pg_sha224_init, pg_sha224_update, pg_sha256_ctx,
    pg_sha256_final, pg_sha256_init, pg_sha256_update, pg_sha384_ctx, pg_sha384_final,
    pg_sha384_init, pg_sha384_update, pg_sha512_ctx, pg_sha512_final, pg_sha512_init,
    pg_sha512_update, PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH, PG_SHA384_DIGEST_LENGTH,
    PG_SHA512_DIGEST_LENGTH,
};
use types_error::{PgError, PgResult, ERRCODE_OUT_OF_MEMORY};

/// `pg_cryptohash_type` selector (`common/cryptohash.h`) — only the SHA-2
/// members are used here.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CryptohashType {
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

// ---------------------------------------------------------------------------
// MD5 (`md5_text` / `md5_bytea`).
// ---------------------------------------------------------------------------

/// `md5_text(PG_FUNCTION_ARGS)`: compute the MD5 hex digest of a `text` input.
///
/// `in_bytes` is the detoasted `VARDATA_ANY(in_text)` payload (`len =
/// VARSIZE_ANY_EXHDR`); the result is the 32-char lowercase-hex digest as a
/// `text` value (its `VARDATA` bytes), matching `cstring_to_text(hexsum)`.
pub fn md5_text(in_bytes: &[u8]) -> PgResult<Vec<u8>> {
    md5_hash_to_text("MD5", in_bytes)
}

/// `md5_bytea(PG_FUNCTION_ARGS)`: compute the MD5 hex digest of a `bytea`
/// input (same logic as [`md5_text`], differing only in the C arg macro).
pub fn md5_bytea(in_bytes: &[u8]) -> PgResult<Vec<u8>> {
    md5_hash_to_text("MD5", in_bytes)
}

/// Shared body of `md5_text`/`md5_bytea`: `pg_md5_hash` + `cstring_to_text`.
fn md5_hash_to_text(name: &str, in_bytes: &[u8]) -> PgResult<Vec<u8>> {
    match common_md5::pg_md5_hash(in_bytes) {
        Ok(hexsum) => Ok(hexsum.into_bytes()),
        Err(errstr) => {
            /*
             * C: ereport(ERROR, errcode(ERRCODE_OUT_OF_MEMORY),
             *           errmsg("could not compute %s hash: %s", "MD5", errstr))
             */
            Err(PgError::error(format!("could not compute {name} hash: {errstr}"))
                .with_sqlstate(ERRCODE_OUT_OF_MEMORY))
        }
    }
}

// ---------------------------------------------------------------------------
// SHA-2 (`sha224_bytea` … `sha512_bytea`).
// ---------------------------------------------------------------------------

/// `cryptohash_internal(type, input)` (`cryptohashfuncs.c`): run `input`'s
/// detoasted `VARDATA_ANY` payload through the selected SHA-2 digest and return
/// the fixed-length raw digest as the `VARDATA` of a `bytea` (the caller adds
/// the varlena header at the boundary).
fn cryptohash_internal(type_: CryptohashType, data: &[u8]) -> Vec<u8> {
    /*
     * The C selects the per-type digest length, palloc0's a result of
     * digest_len + VARHDRSZ, then drives pg_cryptohash_{create,init,update,
     * final,free}.  On a software (non-OpenSSL) build that abstraction is the
     * common/sha2.c reference, which this port calls directly.  The
     * "could not {initialize,update,finalize} %s context" elog() paths are
     * unreachable for the software implementation.
     */
    match type_ {
        CryptohashType::Sha224 => {
            let mut ctx = pg_sha224_ctx::default();
            pg_sha224_init(&mut ctx);
            pg_sha224_update(&mut ctx, data, data.len());
            let mut digest = vec![0u8; PG_SHA224_DIGEST_LENGTH];
            pg_sha224_final(&mut ctx, &mut digest);
            digest
        }
        CryptohashType::Sha256 => {
            let mut ctx = pg_sha256_ctx::default();
            pg_sha256_init(&mut ctx);
            pg_sha256_update(&mut ctx, data, data.len());
            let mut digest = vec![0u8; PG_SHA256_DIGEST_LENGTH];
            pg_sha256_final(&mut ctx, &mut digest);
            digest
        }
        CryptohashType::Sha384 => {
            let mut ctx = pg_sha384_ctx::default();
            pg_sha384_init(&mut ctx);
            pg_sha384_update(&mut ctx, data, data.len());
            let mut digest = vec![0u8; PG_SHA384_DIGEST_LENGTH];
            pg_sha384_final(&mut ctx, &mut digest);
            digest
        }
        CryptohashType::Sha512 => {
            let mut ctx = pg_sha512_ctx::default();
            pg_sha512_init(&mut ctx);
            pg_sha512_update(&mut ctx, data, data.len());
            let mut digest = vec![0u8; PG_SHA512_DIGEST_LENGTH];
            pg_sha512_final(&mut ctx, &mut digest);
            digest
        }
    }
}

/// `sha224_bytea(PG_FUNCTION_ARGS)`: SHA-224 of a `bytea`, returned as a
/// `bytea` (the 28-byte raw digest).
pub fn sha224_bytea(in_bytes: &[u8]) -> Vec<u8> {
    cryptohash_internal(CryptohashType::Sha224, in_bytes)
}

/// `sha256_bytea(PG_FUNCTION_ARGS)`: SHA-256 of a `bytea` (32-byte digest).
pub fn sha256_bytea(in_bytes: &[u8]) -> Vec<u8> {
    cryptohash_internal(CryptohashType::Sha256, in_bytes)
}

/// `sha384_bytea(PG_FUNCTION_ARGS)`: SHA-384 of a `bytea` (48-byte digest).
pub fn sha384_bytea(in_bytes: &[u8]) -> Vec<u8> {
    cryptohash_internal(CryptohashType::Sha384, in_bytes)
}

/// `sha512_bytea(PG_FUNCTION_ARGS)`: SHA-512 of a `bytea` (64-byte digest).
pub fn sha512_bytea(in_bytes: &[u8]) -> Vec<u8> {
    cryptohash_internal(CryptohashType::Sha512, in_bytes)
}

// ---------------------------------------------------------------------------
// Seam installation.
// ---------------------------------------------------------------------------

/// Register this crate's six SQL-callable fmgr builtins. Called from the
/// startup aggregator (`seams-init`). `cryptohashfuncs.c` declares no outward
/// seams of its own.
pub fn init_seams() {
    fmgr_builtins::register_cryptohashfuncs_builtins();
}

#[cfg(test)]
mod tests;
