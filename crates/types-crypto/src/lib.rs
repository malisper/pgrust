//! Shared types for PostgreSQL's cryptographic hash subsystem
//! (`src/include/common/cryptohash.h`).
//!
//! These live in their own small types crate because the cryptohash provider
//! (`common/cryptohash.c` / `common/cryptohash_openssl.c`) is not yet ported;
//! its consumers (e.g. `common/checksum_helper.c`) thread these types across
//! the provider's seam crate until it lands.
#![no_std]
#![allow(non_camel_case_types)]

/// `typedef enum pg_cryptohash_type` (`common/cryptohash.h`) — selects the
/// cryptographic hash algorithm a context computes. Discriminants match the C
/// enumeration order exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum pg_cryptohash_type {
    /// `PG_MD5`
    PG_MD5 = 0,
    /// `PG_SHA1`
    PG_SHA1 = 1,
    /// `PG_SHA224`
    PG_SHA224 = 2,
    /// `PG_SHA256`
    PG_SHA256 = 3,
    /// `PG_SHA384`
    PG_SHA384 = 4,
    /// `PG_SHA512`
    PG_SHA512 = 5,
}

/// `typedef struct pg_cryptohash_ctx pg_cryptohash_ctx;` — the opaque context,
/// private to each cryptohash implementation. C only ever holds a
/// `pg_cryptohash_ctx *` to it; consumers never see its fields, so it stays an
/// opaque (zero-field, never-constructed) struct named only by raw pointer.
pub struct pg_cryptohash_ctx {
    _opaque: [u8; 0],
}
