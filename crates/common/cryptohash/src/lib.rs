//! Idiomatic port of PostgreSQL's `src/common/cryptohash.c` — the fallback
//! (non-OpenSSL) cryptographic-hash dispatcher, and the owner of the
//! `pg_cryptohash_*` seams declared in [`cryptohash_seams`].
//!
//! Reference: `postgres-18.3/src/common/cryptohash.c`,
//! `postgres-18.3/src/include/common/cryptohash.h`.
//!
//! This is the set of in-core functions PostgreSQL uses when there is no
//! OpenSSL backend (`cryptohash.c`, as opposed to `cryptohash_openssl.c`). It
//! is a thin dispatcher: each `pg_cryptohash_*` entry point selects, by
//! [`pg_cryptohash_type`], one of the in-tree reference hash implementations and
//! drives its block functions:
//!
//! * MD5 — [`md5`] (port of `common/md5.c`),
//! * SHA-1 — the in-crate [`sha1`] module (port of `common/sha1.c`; there is no
//!   separate `common-sha1` crate, so the SHA-1 reference impl this dispatcher
//!   references via `sha1_int.h` is ported here, its only fallback-build caller),
//! * SHA-224/256/384/512 — [`sha2`] (port of `common/sha2.c`).
//!
//! # Faithfulness notes — the raw-pointer seam contract
//!
//! The seam crate models the C API exactly: `pg_cryptohash_create` returns the
//! opaque `*mut pg_cryptohash_ctx` (NULL on OOM); `init`/`update`/`final` take
//! that pointer and return the `int` `0`/`-1` convention; `free` releases it.
//! Consumers (`backup_manifest.c`, `auth-scram.c`, `checksum_helper.c`) hold the
//! raw pointer and never dereference it. This owner mints the pointer by boxing
//! the concrete [`CryptoHashCtx`] and leaking it, and reconstitutes the box in
//! `free`. The C struct is `palloc`'d (backend) / `malloc`'d (frontend) and
//! `memset` to zero; here the box is heap-allocated and the variant is
//! `Uninit` until `pg_cryptohash_init` selects an algorithm.
//!
//! The only per-call failure mode in this pure (no-OpenSSL) build is
//! `pg_cryptohash_final`'s destination-length check, which sets
//! `error = PG_CRYPTOHASH_ERROR_DEST_LEN` and returns `-1` (matching the C
//! `len < *_DIGEST_LENGTH` checks). `pg_cryptohash_create`'s C OOM branch
//! returns NULL in the frontend and `ereport`s in the backend; the in-tree
//! allocation here aborts on OOM exactly as the backend `palloc` does, so the
//! NULL-return path is preserved only structurally (a successful box is always
//! returned).

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

#[cfg(test)]
mod tests;

pub mod sha1;

use md5::{md5_ctxt, md5_init, md5_loop, md5_pad, md5_result, MD5_DIGEST_LENGTH};
use sha2::{
    pg_sha224_ctx, pg_sha224_final, pg_sha224_init, pg_sha224_update, pg_sha256_ctx,
    pg_sha256_final, pg_sha256_init, pg_sha256_update, pg_sha384_ctx, pg_sha384_final,
    pg_sha384_init, pg_sha384_update, pg_sha512_ctx, pg_sha512_final, pg_sha512_init,
    pg_sha512_update, PG_SHA224_DIGEST_LENGTH, PG_SHA256_DIGEST_LENGTH, PG_SHA384_DIGEST_LENGTH,
    PG_SHA512_DIGEST_LENGTH,
};
use sha1::{pg_sha1_ctx, pg_sha1_final, pg_sha1_init, pg_sha1_update, SHA1_DIGEST_LENGTH};
use types_core::{uint8, Size};
use crypto::pg_cryptohash_type::{
    self, PG_MD5, PG_SHA1, PG_SHA224, PG_SHA256, PG_SHA384, PG_SHA512,
};

/// `pg_cryptohash_errno` (cryptohash.c) — the set of error states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
enum pg_cryptohash_errno {
    /// `PG_CRYPTOHASH_ERROR_NONE = 0`.
    PG_CRYPTOHASH_ERROR_NONE = 0,
    /// `PG_CRYPTOHASH_ERROR_DEST_LEN`.
    PG_CRYPTOHASH_ERROR_DEST_LEN,
}

/// The C `union { pg_md5_ctx; pg_sha1_ctx; pg_sha224_ctx; ...; }` payload.
///
/// In C the union is uninitialized after `pg_cryptohash_create` (the whole
/// struct is `memset` to 0); the variant becomes meaningful once
/// `pg_cryptohash_init` runs. This port holds the live per-algorithm context as
/// the matching enum variant, populated by `pg_cryptohash_init`.
enum CryptoHashData {
    /// No context has been initialized yet (post-`create`, pre-`init`).
    Uninit,
    Md5(md5_ctxt),
    Sha1(pg_sha1_ctx),
    Sha224(pg_sha224_ctx),
    Sha256(pg_sha256_ctx),
    Sha384(pg_sha384_ctx),
    Sha512(pg_sha512_ctx),
}

/// Internal `pg_cryptohash_ctx` structure (cryptohash.c).
///
/// The seam crate exposes this only as the opaque `*mut pg_cryptohash_ctx`
/// pointer (`crypto::pg_cryptohash_ctx`); this concrete struct is the
/// box this owner mints behind that pointer.
struct CryptoHashCtx {
    type_: pg_cryptohash_type,
    error: pg_cryptohash_errno,
    data: CryptoHashData,
}

/// Cast the opaque seam pointer back to this owner's concrete box pointer.
#[inline]
fn as_ctx<'a>(ctx: *mut crypto::pg_cryptohash_ctx) -> Option<&'a mut CryptoHashCtx> {
    if ctx.is_null() {
        None
    } else {
        // SAFETY: every non-NULL `*mut pg_cryptohash_ctx` crossing these seams
        // was minted by `pg_cryptohash_create` as a leaked `Box<CryptoHashCtx>`
        // and is not freed until `pg_cryptohash_free`, mirroring the C ownership
        // of the opaque context pointer.
        Some(unsafe { &mut *(ctx as *mut CryptoHashCtx) })
    }
}

/// `pg_cryptohash_create`
///
/// Allocate a hash context. Returns NULL on failure for an OOM (the backend's
/// `palloc` issues an error without returning; the in-tree allocation here
/// aborts on OOM the same way). The whole struct is zeroed (`memset(ctx, 0,
/// ...)`), `type` is set, and `error` is `PG_CRYPTOHASH_ERROR_NONE`.
pub fn pg_cryptohash_create(type_: pg_cryptohash_type) -> *mut crypto::pg_cryptohash_ctx {
    let ctx = Box::new(CryptoHashCtx {
        type_,
        error: pg_cryptohash_errno::PG_CRYPTOHASH_ERROR_NONE,
        data: CryptoHashData::Uninit,
    });
    Box::into_raw(ctx) as *mut crypto::pg_cryptohash_ctx
}

/// `pg_cryptohash_init`
///
/// Initialize a hash context. Returns 0 on success, and -1 on failure
/// (`ctx == NULL`).
pub fn pg_cryptohash_init(ctx: *mut crypto::pg_cryptohash_ctx) -> i32 {
    let Some(ctx) = as_ctx(ctx) else {
        return -1;
    };

    match ctx.type_ {
        PG_MD5 => {
            let mut c = md5_ctxt::default();
            md5_init(&mut c);
            ctx.data = CryptoHashData::Md5(c);
        }
        PG_SHA1 => {
            let mut c = pg_sha1_ctx::default();
            pg_sha1_init(&mut c);
            ctx.data = CryptoHashData::Sha1(c);
        }
        PG_SHA224 => {
            let mut c = pg_sha224_ctx::default();
            pg_sha224_init(&mut c);
            ctx.data = CryptoHashData::Sha224(c);
        }
        PG_SHA256 => {
            let mut c = pg_sha256_ctx::default();
            pg_sha256_init(&mut c);
            ctx.data = CryptoHashData::Sha256(c);
        }
        PG_SHA384 => {
            let mut c = pg_sha384_ctx::default();
            pg_sha384_init(&mut c);
            ctx.data = CryptoHashData::Sha384(c);
        }
        PG_SHA512 => {
            let mut c = pg_sha512_ctx::default();
            pg_sha512_init(&mut c);
            ctx.data = CryptoHashData::Sha512(c);
        }
    }

    0
}

/// `pg_cryptohash_update`
///
/// Update a hash context. Returns 0 on success, and -1 on failure
/// (`ctx == NULL`).
///
/// # Safety
///
/// `data` must point to `len` readable bytes (or be NULL with `len == 0`), as
/// the C `const uint8 *data` contract requires.
pub fn pg_cryptohash_update(
    ctx: *mut crypto::pg_cryptohash_ctx,
    data: *const uint8,
    len: Size,
) -> i32 {
    let Some(ctx) = as_ctx(ctx) else {
        return -1;
    };

    // SAFETY: `data`/`len` mirror the C `const uint8 *data, size_t len` pair.
    let bytes: &[uint8] = if len == 0 {
        &[]
    } else {
        unsafe { core::slice::from_raw_parts(data, len) }
    };

    match &mut ctx.data {
        CryptoHashData::Md5(c) => md5_loop(c, bytes, len),
        CryptoHashData::Sha1(c) => pg_sha1_update(c, bytes, len),
        CryptoHashData::Sha224(c) => pg_sha224_update(c, bytes, len),
        CryptoHashData::Sha256(c) => pg_sha256_update(c, bytes, len),
        CryptoHashData::Sha384(c) => pg_sha384_update(c, bytes, len),
        CryptoHashData::Sha512(c) => pg_sha512_update(c, bytes, len),
        // In C the union is read according to ctx->type regardless of init; a
        // caller that updates before init is already undefined there. Callers
        // always init first.
        CryptoHashData::Uninit => {}
    }

    0
}

/// `pg_cryptohash_final`
///
/// Finalize a hash context, writing the digest into `dest` (`len` is the
/// caller's buffer size). Returns 0 on success, and -1 on failure (`ctx ==
/// NULL`, or a destination buffer shorter than the algorithm's digest length,
/// which also sets `error = PG_CRYPTOHASH_ERROR_DEST_LEN`).
///
/// # Safety
///
/// `dest` must point to `len` writable bytes, as the C `uint8 *dest, size_t
/// len` contract requires.
pub fn pg_cryptohash_final(
    ctx: *mut crypto::pg_cryptohash_ctx,
    dest: *mut uint8,
    len: Size,
) -> i32 {
    let Some(ctx) = as_ctx(ctx) else {
        return -1;
    };

    // Establish the destination slice only after the length check passes, since
    // a short buffer must report the error without writing.
    macro_rules! check_len {
        ($digest_len:expr) => {
            if len < $digest_len {
                ctx.error = pg_cryptohash_errno::PG_CRYPTOHASH_ERROR_DEST_LEN;
                return -1;
            }
        };
    }

    // SAFETY: `dest`/`len` mirror the C `uint8 *dest, size_t len` pair; the
    // per-arm length check below guarantees the digest fits.
    let dest_slice = |n: usize| -> &mut [uint8] { unsafe { core::slice::from_raw_parts_mut(dest, n) } };

    match &mut ctx.data {
        CryptoHashData::Md5(c) => {
            check_len!(MD5_DIGEST_LENGTH);
            md5_pad(c);
            let mut digest = [0u8; 16];
            md5_result(&mut digest, c);
            dest_slice(MD5_DIGEST_LENGTH).copy_from_slice(&digest);
        }
        CryptoHashData::Sha1(c) => {
            check_len!(SHA1_DIGEST_LENGTH);
            pg_sha1_final(c, dest_slice(SHA1_DIGEST_LENGTH));
        }
        CryptoHashData::Sha224(c) => {
            check_len!(PG_SHA224_DIGEST_LENGTH);
            pg_sha224_final(c, dest_slice(PG_SHA224_DIGEST_LENGTH));
        }
        CryptoHashData::Sha256(c) => {
            check_len!(PG_SHA256_DIGEST_LENGTH);
            pg_sha256_final(c, dest_slice(PG_SHA256_DIGEST_LENGTH));
        }
        CryptoHashData::Sha384(c) => {
            check_len!(PG_SHA384_DIGEST_LENGTH);
            pg_sha384_final(c, dest_slice(PG_SHA384_DIGEST_LENGTH));
        }
        CryptoHashData::Sha512(c) => {
            check_len!(PG_SHA512_DIGEST_LENGTH);
            pg_sha512_final(c, dest_slice(PG_SHA512_DIGEST_LENGTH));
        }
        CryptoHashData::Uninit => {
            // Per the C, `final` before `init` reads a zeroed union according to
            // ctx->type. Callers always init first.
        }
    }

    0
}

/// `pg_cryptohash_free`
///
/// Free a hash context. In C this `explicit_bzero`s the context then frees it.
/// Reconstituting the box and dropping it reclaims the storage; the box is
/// scrubbed first to mirror `explicit_bzero`.
pub fn pg_cryptohash_free(ctx: *mut crypto::pg_cryptohash_ctx) {
    if ctx.is_null() {
        return;
    }
    // SAFETY: `ctx` was minted by `pg_cryptohash_create` as a leaked
    // `Box<CryptoHashCtx>` and is freed exactly once here, as the C contract
    // requires (`pg_cryptohash_free` on a non-NULL context).
    let mut boxed = unsafe { Box::from_raw(ctx as *mut CryptoHashCtx) };
    // explicit_bzero(ctx, sizeof(pg_cryptohash_ctx)): scrub the live key
    // material before releasing the allocation.
    boxed.data = CryptoHashData::Uninit;
    boxed.error = pg_cryptohash_errno::PG_CRYPTOHASH_ERROR_NONE;
    drop(boxed);
}

/// Install the five `pg_cryptohash_*` seams declared by
/// [`cryptohash_seams`]. Called from the startup aggregator
/// (`seams-init`).
pub fn init_seams() {
    cryptohash_seams::pg_cryptohash_create::set(pg_cryptohash_create);
    cryptohash_seams::pg_cryptohash_init::set(pg_cryptohash_init);
    cryptohash_seams::pg_cryptohash_update::set(pg_cryptohash_update);
    cryptohash_seams::pg_cryptohash_final::set(pg_cryptohash_final);
    cryptohash_seams::pg_cryptohash_free::set(pg_cryptohash_free);
}
