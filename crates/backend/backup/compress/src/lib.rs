//! Server-side base-backup compression sinks: ports of `basebackup_gzip.c`,
//! `basebackup_lz4.c`, and `basebackup_zstd.c` (PostgreSQL 18.3).
//!
//! Each sink owns its input buffer (the driver writes uncompressed archive
//! bytes into it) and stages compressed output directly in the successor
//! sink's buffer, forwarding a chunk whenever it fills — the same shape as
//! C's bbsink_gzip / bbsink_lz4 / bbsink_zstd. Manifest contents are never
//! compressed; they are copied into the successor's buffer and forwarded.
//!
//! Codec backends (see Cargo.toml): gzip = miniz_oxide raw-DEFLATE plus a
//! hand-rolled RFC 1952 member header/trailer (byte-compatible with what
//! zlib's deflateInit2(15+16) emits, up to non-semantic header fields);
//! lz4 = lz4_flex's LZ4 frame encoder; zstd = the same libzstd C binding
//! Postgres links.

#![allow(non_snake_case)]

use types_error::ErrorLocation;

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report OUR source site (call site via track_caller).
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

mod gzip;
mod lz4;
mod zstd_sink;

pub use gzip::bbsink_gzip_new;
pub use lz4::bbsink_lz4_new;
pub use zstd_sink::bbsink_zstd_new;

/// Round `n` up to a positive multiple of BLCKSZ (the C compression sinks'
/// `output_buffer_bound + BLCKSZ - (output_buffer_bound % BLCKSZ)`).
fn round_up_blcksz(n: usize) -> usize {
    let blcksz = types_core::BLCKSZ;
    n + blcksz - (n % blcksz)
}

#[cfg(test)]
mod tests;
