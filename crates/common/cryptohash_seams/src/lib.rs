//! Seam declarations for the cryptographic hash subsystem
//! (`common/cryptohash.c` / `common/cryptohash_openssl.c`), exposing the
//! `pg_cryptohash_*` entry points declared in `common/cryptohash.h`.
//!
//! This is the genuinely external cryptographic primitive — backed by
//! OpenSSL's `EVP_*` digests when PostgreSQL is built `--with-ssl`, or by the
//! in-tree `common/sha2.c` software fallback otherwise. The owning unit
//! installs these from its `init_seams()` when it lands; until then a call
//! panics loudly. There is no silent fallback.
//!
//! The C `pg_cryptohash_ctx` is opaque (private to each implementation); it
//! crosses the seam as the raw `*mut pg_cryptohash_ctx` pointer C holds, never
//! dereferenced by consumers. The C functions report failure through the `int`
//! return convention (`< 0` on error) rather than `ereport(ERROR)`, so these
//! seams return that `int` / the raw pointer directly rather than a
//! `PgResult`.
#![allow(non_camel_case_types)]

use types_core::{uint8, Size};
use crypto::{pg_cryptohash_ctx, pg_cryptohash_type};

seam_core::seam!(
    /// `pg_cryptohash_ctx *pg_cryptohash_create(pg_cryptohash_type type)` —
    /// allocate and return a hash context for `type`, or NULL on failure.
    pub fn pg_cryptohash_create(type_: pg_cryptohash_type) -> *mut pg_cryptohash_ctx
);

seam_core::seam!(
    /// `int pg_cryptohash_init(pg_cryptohash_ctx *ctx)` — initialize the
    /// context. Returns 0 on success, -1 on failure.
    pub fn pg_cryptohash_init(ctx: *mut pg_cryptohash_ctx) -> i32
);

seam_core::seam!(
    /// `int pg_cryptohash_update(pg_cryptohash_ctx *ctx, const uint8 *data,
    /// size_t len)` — feed `data` into the running hash. Returns 0 on success,
    /// -1 on failure.
    pub fn pg_cryptohash_update(ctx: *mut pg_cryptohash_ctx, data: *const uint8, len: Size) -> i32
);

seam_core::seam!(
    /// `int pg_cryptohash_final(pg_cryptohash_ctx *ctx, uint8 *dest,
    /// size_t len)` — finalize and write the digest into `dest` (`len` is the
    /// caller's buffer size). Returns 0 on success, -1 on failure.
    pub fn pg_cryptohash_final(ctx: *mut pg_cryptohash_ctx, dest: *mut uint8, len: Size) -> i32
);

seam_core::seam!(
    /// `void pg_cryptohash_free(pg_cryptohash_ctx *ctx)` — release the context.
    pub fn pg_cryptohash_free(ctx: *mut pg_cryptohash_ctx)
);
