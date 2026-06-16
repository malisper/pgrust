//! Port of `utf8_and_johab.c` — the `JOHAB <--> UTF8`
//! encoding-conversion procedures.
//!
//! Faithful 1:1 translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_johab/utf8_and_johab.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`johab_to_utf8`, `utf8_to_johab`)
//! become plain Rust functions over `&[u8]`. Each validates its
//! source/destination encodings with the faithful
//! [`check_encoding_conversion_args`] (the C `CHECK_ENCODING_CONVERSION_ARGS`
//! macro) and then delegates to the radix-tree engine
//! ([`LocalToUtf`]/[`UtfToLocal`]), a 1:1 port of `conv.c`.
//!
//! The two JOHAB <-> Unicode radix tables (generated from the `.map` files) are
//! ported as `const` arrays in [`tables`], byte-for-byte identical to the
//! upstream generated tables.

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use backend_utils_mb_conv_string_helpers::{
    check_encoding_conversion_args, ConversionResult, LocalToUtf, UtfToLocal,
};
use types_error::PgResult;
use types_wchar::encoding::{pg_enc, PG_JOHAB, PG_UTF8};

/// `johab_to_utf8` — convert a JOHAB string to UTF-8.
pub fn johab_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_JOHAB,
        PG_UTF8,
    )?;
    LocalToUtf(
        src,
        Some(&tables::johab_to_unicode_tree()),
        &[],
        None,
        PG_JOHAB,
        no_error,
    )
}

/// `utf8_to_johab` — convert a UTF-8 string to JOHAB.
pub fn utf8_to_johab(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_UTF8,
        PG_JOHAB,
    )?;
    UtfToLocal(
        src,
        Some(&tables::johab_from_unicode_tree()),
        &[],
        None,
        PG_JOHAB,
        no_error,
    )
}

/// Wires this crate's seams. It declares none of its own, so this is a no-op
/// kept for the uniform `seams-init` startup convention.
pub fn init_seams() {}
