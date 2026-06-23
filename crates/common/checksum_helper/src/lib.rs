#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]

//! Idiomatic port of PostgreSQL's `src/common/checksum_helper.c` — compute a
//! checksum of any of the supported types (`NONE`, `CRC32C`, and the four SHA-2
//! variants) through one common interface.
//!
//! Every function in `checksum_helper.c` is ported with identical branches:
//! `pg_checksum_parse_type`, `pg_checksum_type_name`, `pg_checksum_init`,
//! `pg_checksum_update`, and `pg_checksum_final`.
//!
//! # What is in-crate vs. seamed
//!
//! * **CRC-32C** routes to the real owner: `INIT_CRC32C` / `FIN_CRC32C` are the
//!   trivial header macros (`0xFFFFFFFF` / XOR `0xFFFFFFFF`) done at the call
//!   site, and `COMP_CRC32C` calls [`crc32c::pg_comp_crc32c_sb8`].
//! * **SHA-2** (`SHA224`/`SHA256`/`SHA384`/`SHA512`) is a thin wrapper over the
//!   genuinely external cryptographic primitive `common/cryptohash*.c`
//!   (`pg_cryptohash_*`), which has no idiomatic crate in this workspace yet,
//!   so it crosses [`cryptohash_seams`]. Each call panics loudly until
//!   the cryptohash owner installs the real implementation.
//!
//! # Idiomatic shape
//!
//! The C `pg_checksum_context` is a `(type, union { c_crc32c, c_sha2 })`. The
//! [`PgChecksumContext`] keeps the same discriminant + payload via a Rust enum
//! body, so there is no `unsafe` union access. The SHA-2 arm carries the real
//! `*mut pg_cryptohash_ctx` that the C union member `c_sha2` is — the opaque
//! context pointer minted by `pg_cryptohash_create`. The integer-`int` 0/-1
//! return convention of `init`/`update`/`final` becomes a `Result`; `-1`
//! (failure) maps to `Err`. A NULL/`NONE` checksum is fully supported and
//! produces a zero-length digest.

use ::crypto::{pg_cryptohash_ctx, pg_cryptohash_type};

// ---------------------------------------------------------------------------
// pg_checksum_type (common/checksum_helper.h)
// ---------------------------------------------------------------------------

/// `typedef enum pg_checksum_type` — the supported checksum types.
///
/// The discriminant values match the C enumeration order
/// (`CHECKSUM_TYPE_NONE = 0` .. `CHECKSUM_TYPE_SHA512 = 5`), which the
/// statistics / backup tooling persists, so the numeric mapping is preserved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum pg_checksum_type {
    /// `CHECKSUM_TYPE_NONE`
    None = 0,
    /// `CHECKSUM_TYPE_CRC32C`
    Crc32c = 1,
    /// `CHECKSUM_TYPE_SHA224`
    Sha224 = 2,
    /// `CHECKSUM_TYPE_SHA256`
    Sha256 = 3,
    /// `CHECKSUM_TYPE_SHA384`
    Sha384 = 4,
    /// `CHECKSUM_TYPE_SHA512`
    Sha512 = 5,
}

pub use pg_checksum_type::Crc32c as CHECKSUM_TYPE_CRC32C;
pub use pg_checksum_type::None as CHECKSUM_TYPE_NONE;
pub use pg_checksum_type::Sha224 as CHECKSUM_TYPE_SHA224;
pub use pg_checksum_type::Sha256 as CHECKSUM_TYPE_SHA256;
pub use pg_checksum_type::Sha384 as CHECKSUM_TYPE_SHA384;
pub use pg_checksum_type::Sha512 as CHECKSUM_TYPE_SHA512;

/// Digest lengths in bytes (`common/sha2.h` `PG_SHA*_DIGEST_LENGTH`).
const PG_SHA224_DIGEST_LENGTH: usize = 28;
const PG_SHA256_DIGEST_LENGTH: usize = 32;
const PG_SHA384_DIGEST_LENGTH: usize = 48;
const PG_SHA512_DIGEST_LENGTH: usize = 64;

/// `#define PG_CHECKSUM_MAX_LENGTH PG_SHA512_DIGEST_LENGTH` — the longest
/// possible output for any supported checksum algorithm.
pub const PG_CHECKSUM_MAX_LENGTH: usize = PG_SHA512_DIGEST_LENGTH;

/// Width of a `pg_crc32c` (`uint32`) in bytes — the CRC-32C digest length.
const SIZEOF_CRC32C: usize = 4;

/// `INIT_CRC32C(crc)` (`port/pg_crc32c.h`): seed value for a CRC-32C run.
const INIT_CRC32C: u32 = 0xFFFF_FFFF;

/// The error returned by the fallible checksum routines, replacing C's `-1`
/// `int` return convention with a typed failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChecksumError {
    /// A `pg_cryptohash_*` step failed (C's `< 0` / `NULL` return). This is the
    /// `-1` returned by `pg_checksum_init` / `_update` / `_final` for a SHA-2
    /// context.
    Cryptohash,
    /// `pg_checksum_final` was handed an output buffer shorter than the digest.
    /// C documents that the caller MUST supply a `PG_CHECKSUM_MAX_LENGTH` buffer
    /// and writes without bounds-checking; the idiomatic port surfaces an
    /// undersized buffer as this error instead of an out-of-bounds write.
    OutputTooSmall { needed: usize, actual: usize },
}

// ---------------------------------------------------------------------------
// pg_checksum_context (the (type, union) pair, as a safe enum)
// ---------------------------------------------------------------------------

/// In-progress payload for the active checksum type. Mirrors the C
/// `pg_checksum_raw_context` union without `unsafe` union access: the SHA-2 arm
/// carries the real `pg_cryptohash_ctx *` (the C union member `c_sha2`).
#[derive(Clone, Copy, Debug)]
enum RawContext {
    /// `union.c_crc32c` — the running CRC value.
    Crc32c(u32),
    /// `union.c_sha2` — the opaque cryptohash context pointer.
    Sha2(*mut pg_cryptohash_ctx),
    /// `CHECKSUM_TYPE_NONE` carries no running state.
    None,
}

/// `typedef struct pg_checksum_context { pg_checksum_type type; ... }`.
///
/// Pairs the checksum type with the running raw context, exactly like the C
/// struct, so callers thread one value through `update`/`final`.
#[derive(Clone, Copy, Debug)]
pub struct PgChecksumContext {
    type_: pg_checksum_type,
    raw_context: RawContext,
}

impl PgChecksumContext {
    /// The checksum type this context was initialized for (C: `context->type`).
    #[inline]
    pub fn checksum_type(&self) -> pg_checksum_type {
        self.type_
    }
}

// ---------------------------------------------------------------------------
// pg_checksum_parse_type (checksum_helper.c:27-50)
// ---------------------------------------------------------------------------

/// `pg_checksum_parse_type(name, type)`.
///
/// If `name` is a recognized checksum type, return `Some(type)`; otherwise
/// return `None` (the C function's `false`, with `*type` set to
/// `CHECKSUM_TYPE_NONE`). The match is ASCII case-insensitive via the real
/// `pg_strcasecmp`. `name` must be NUL-terminated, as the C `char *` is.
pub fn pg_checksum_parse_type(name: &[u8]) -> Option<pg_checksum_type> {
    if strcaseeq(name, b"none\0") {
        Some(CHECKSUM_TYPE_NONE)
    } else if strcaseeq(name, b"crc32c\0") {
        Some(CHECKSUM_TYPE_CRC32C)
    } else if strcaseeq(name, b"sha224\0") {
        Some(CHECKSUM_TYPE_SHA224)
    } else if strcaseeq(name, b"sha256\0") {
        Some(CHECKSUM_TYPE_SHA256)
    } else if strcaseeq(name, b"sha384\0") {
        Some(CHECKSUM_TYPE_SHA384)
    } else if strcaseeq(name, b"sha512\0") {
        Some(CHECKSUM_TYPE_SHA512)
    } else {
        None
    }
}

/// `pg_strcasecmp(a, b) == 0` — routed to the real owner (`port-pgstrcasecmp`).
#[inline]
fn strcaseeq(a: &[u8], b: &[u8]) -> bool {
    pgstrcasecmp::pg_strcasecmp(a, b) == 0
}

// ---------------------------------------------------------------------------
// pg_checksum_type_name (checksum_helper.c:55-76)
// ---------------------------------------------------------------------------

/// `pg_checksum_type_name(type)` — the canonical human-readable name.
///
/// Every enum value has a name; the C `Assert(false); return "???"` fall-through
/// is unreachable here because `pg_checksum_type` is a closed enum.
pub fn pg_checksum_type_name(type_: pg_checksum_type) -> &'static str {
    match type_ {
        CHECKSUM_TYPE_NONE => "NONE",
        CHECKSUM_TYPE_CRC32C => "CRC32C",
        CHECKSUM_TYPE_SHA224 => "SHA224",
        CHECKSUM_TYPE_SHA256 => "SHA256",
        CHECKSUM_TYPE_SHA384 => "SHA384",
        CHECKSUM_TYPE_SHA512 => "SHA512",
    }
}

// ---------------------------------------------------------------------------
// pg_checksum_init (checksum_helper.c:82-138)
// ---------------------------------------------------------------------------

/// `pg_checksum_init(context, type)`.
///
/// Initialize a checksum context for checksums of the given type. Returns the
/// initialized [`PgChecksumContext`] (C's "0 for success"), or
/// [`ChecksumError::Cryptohash`] for the SHA-2 create/init failure (C's "-1").
pub fn pg_checksum_init(type_: pg_checksum_type) -> Result<PgChecksumContext, ChecksumError> {
    let raw_context = match type_ {
        CHECKSUM_TYPE_NONE => {
            // do nothing
            RawContext::None
        }
        CHECKSUM_TYPE_CRC32C => {
            // INIT_CRC32C(context->raw_context.c_crc32c);
            RawContext::Crc32c(INIT_CRC32C)
        }
        // The four SHA-2 variants share an identical body in C, differing only
        // by the pg_cryptohash_type selector handed to pg_cryptohash_create.
        CHECKSUM_TYPE_SHA224 => RawContext::Sha2(create_sha2(pg_cryptohash_type::PG_SHA224)?),
        CHECKSUM_TYPE_SHA256 => RawContext::Sha2(create_sha2(pg_cryptohash_type::PG_SHA256)?),
        CHECKSUM_TYPE_SHA384 => RawContext::Sha2(create_sha2(pg_cryptohash_type::PG_SHA384)?),
        CHECKSUM_TYPE_SHA512 => RawContext::Sha2(create_sha2(pg_cryptohash_type::PG_SHA512)?),
    };

    Ok(PgChecksumContext { type_, raw_context })
}

/// `pg_cryptohash_create(type)` + `pg_cryptohash_init(ctx)` with C's exact
/// failure handling: NULL from create `return -1`s; an init failure frees the
/// context and `return -1`s.
fn create_sha2(type_: pg_cryptohash_type) -> Result<*mut pg_cryptohash_ctx, ChecksumError> {
    let ctx = cryptohash_seams::pg_cryptohash_create::call(type_);
    if ctx.is_null() {
        return Err(ChecksumError::Cryptohash);
    }
    if cryptohash_seams::pg_cryptohash_init::call(ctx) < 0 {
        cryptohash_seams::pg_cryptohash_free::call(ctx);
        return Err(ChecksumError::Cryptohash);
    }
    Ok(ctx)
}

// ---------------------------------------------------------------------------
// pg_checksum_update (checksum_helper.c:144-166)
// ---------------------------------------------------------------------------

/// `pg_checksum_update(context, input, len)`.
///
/// Update a checksum context with new data. Returns `Ok(())` (C's "0"), or
/// [`ChecksumError::Cryptohash`] for the SHA-2 update failure (C's "-1").
pub fn pg_checksum_update(
    context: &mut PgChecksumContext,
    input: &[u8],
) -> Result<(), ChecksumError> {
    match &mut context.raw_context {
        // do nothing
        RawContext::None => Ok(()),
        RawContext::Crc32c(crc) => {
            // COMP_CRC32C(context->raw_context.c_crc32c, input, len);
            *crc = crc32c::pg_comp_crc32c_sb8(*crc, input);
            Ok(())
        }
        RawContext::Sha2(ctx) => {
            // if (pg_cryptohash_update(context->raw_context.c_sha2, input, len) < 0)
            //     return -1;
            if cryptohash_seams::pg_cryptohash_update::call(*ctx, input.as_ptr(), input.len())
                < 0
            {
                Err(ChecksumError::Cryptohash)
            } else {
                Ok(())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// pg_checksum_final (checksum_helper.c:175-232)
// ---------------------------------------------------------------------------

/// `pg_checksum_final(context, output)`.
///
/// Finalize a checksum computation and write the result to `output`. Returns
/// the number of bytes written (C's non-negative return), or an error. The
/// caller must supply a buffer of at least [`PG_CHECKSUM_MAX_LENGTH`] bytes; an
/// undersized buffer is reported as [`ChecksumError::OutputTooSmall`] rather
/// than performing C's unchecked write.
///
/// For the SHA-2 variants this consumes the provider context (issuing
/// `pg_cryptohash_free` exactly as C does on the success path); the context is
/// left in the `None` raw state so a double-final does not re-free.
pub fn pg_checksum_final(
    context: &mut PgChecksumContext,
    output: &mut [u8],
) -> Result<usize, ChecksumError> {
    match context.raw_context {
        RawContext::None => Ok(0),
        RawContext::Crc32c(crc) => {
            // FIN_CRC32C(context->raw_context.c_crc32c);
            // retval = sizeof(pg_crc32c);
            // memcpy(output, &context->raw_context.c_crc32c, retval);
            let retval = SIZEOF_CRC32C;
            require_output(output, retval)?;
            let finalized = crc ^ INIT_CRC32C;
            // C `memcpy(&crc)` writes the host-endian bytes of the uint32.
            output[..retval].copy_from_slice(&finalized.to_ne_bytes());
            Ok(retval)
        }
        RawContext::Sha2(ctx) => {
            let retval = sha2_digest_length(context.type_);
            require_output(output, retval)?;
            // if (pg_cryptohash_final(context->raw_context.c_sha2, output, retval) < 0)
            //     return -1;
            if cryptohash_seams::pg_cryptohash_final::call(
                ctx,
                output.as_mut_ptr(),
                retval,
            ) < 0
            {
                // C does not free on the failure path; mirror that.
                return Err(ChecksumError::Cryptohash);
            }
            // pg_cryptohash_free(context->raw_context.c_sha2);
            cryptohash_seams::pg_cryptohash_free::call(ctx);
            context.raw_context = RawContext::None;
            Ok(retval)
        }
    }
}

/// `output.len() < needed` → C's "caller must supply PG_CHECKSUM_MAX_LENGTH".
#[inline]
fn require_output(output: &[u8], needed: usize) -> Result<(), ChecksumError> {
    if output.len() < needed {
        Err(ChecksumError::OutputTooSmall {
            needed,
            actual: output.len(),
        })
    } else {
        Ok(())
    }
}

/// The digest length for a SHA-2 checksum type. Only the four SHA-2 variants
/// reach this; the non-SHA-2 arms of `pg_checksum_final` never call it.
fn sha2_digest_length(type_: pg_checksum_type) -> usize {
    match type_ {
        CHECKSUM_TYPE_SHA224 => PG_SHA224_DIGEST_LENGTH,
        CHECKSUM_TYPE_SHA256 => PG_SHA256_DIGEST_LENGTH,
        CHECKSUM_TYPE_SHA384 => PG_SHA384_DIGEST_LENGTH,
        CHECKSUM_TYPE_SHA512 => PG_SHA512_DIGEST_LENGTH,
        // Unreachable: a Sha2 raw context is only ever paired with a SHA-2 type.
        _ => PG_SHA512_DIGEST_LENGTH,
    }
}

/// This crate installs no seams of its own: CRC-32C routes directly to
/// `port-crc32c` and the SHA-2 path crosses the cryptohash owner's seam crate
/// (`common-cryptohash-seams`), which that owner installs when it lands. The
/// no-op keeps the uniform `init_all()` wiring contract.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
