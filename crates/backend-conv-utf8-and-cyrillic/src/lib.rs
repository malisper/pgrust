//! Port of `src/backend/utils/mb/conversion_procs/utf8_and_cyrillic/utf8_and_cyrillic.c` —
//! the UTF-8 <-> local-encoding conversion procedures.
//!
//! The `PG_FUNCTION_ARGS` entry points become plain Rust functions taking the
//! source/destination encoding ids, the source bytes, and the `no_error` flag.
//! Each validates the encodings with `check_encoding_conversion_args`
//! (`CHECK_ENCODING_CONVERSION_ARGS`) and delegates to the shared radix-tree
//! engine (`UtfToLocal` / `LocalToUtf` in `backend_utils_mb_conv_string_helpers`,
//! a 1:1 port of `conv.c`). The conversion tables are ported byte-for-byte from
//! the generated `Unicode/*.map` files (see `tables`).
//!
//! Like the C module, this crate owns no inward seams: the conversion procedures
//! are reached through the `pg_conversion` catalog, not a seam. `init_seams()` is
//! a no-op.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

mod tables;

use backend_utils_error::PgResult;
use backend_utils_mb_conv_string_helpers::{ConversionResult, LocalToUtf, UtfToLocal};
use backend_utils_mb_mbutils_seams::check_encoding_conversion_args;
use types_wchar::encoding::{pg_enc, PG_KOI8R, PG_KOI8U, PG_UTF8};

/// `utf8_to_koi8r` (utf8_and_cyrillic.c): convert PG_UTF8 <-> PG_KOI8R.
pub fn utf8_to_koi8r(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, PG_KOI8R)?;
    UtfToLocal(
        src,
        Some(&tables::koi8r_from_unicode_tree()),
        &[],
        None,
        PG_KOI8R,
        no_error,
    )
}
/// `koi8r_to_utf8` (utf8_and_cyrillic.c): convert PG_KOI8R <-> PG_UTF8.
pub fn koi8r_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_KOI8R, PG_UTF8)?;
    LocalToUtf(
        src,
        Some(&tables::koi8r_to_unicode_tree()),
        &[],
        None,
        PG_KOI8R,
        no_error,
    )
}
/// `utf8_to_koi8u` (utf8_and_cyrillic.c): convert PG_UTF8 <-> PG_KOI8U.
pub fn utf8_to_koi8u(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, PG_KOI8U)?;
    UtfToLocal(
        src,
        Some(&tables::koi8u_from_unicode_tree()),
        &[],
        None,
        PG_KOI8U,
        no_error,
    )
}
/// `koi8u_to_utf8` (utf8_and_cyrillic.c): convert PG_KOI8U <-> PG_UTF8.
pub fn koi8u_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(src_encoding, dest_encoding, src.len() as i32, PG_KOI8U, PG_UTF8)?;
    LocalToUtf(
        src,
        Some(&tables::koi8u_to_unicode_tree()),
        &[],
        None,
        PG_KOI8U,
        no_error,
    )
}

/// This crate owns no inward seams (conversions are dispatched via the
/// `pg_conversion` catalog, mirroring the C module).
pub fn init_seams() {}

#[cfg(test)]
mod tests;
