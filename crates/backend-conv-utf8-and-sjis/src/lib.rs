//! Port of `utf8_and_sjis.c` — the `SJIS <--> UTF8` encoding-conversion
//! procedures.
//!
//! Faithful translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_sjis/utf8_and_sjis.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`sjis_to_utf8`, `utf8_to_sjis`)
//! become plain Rust functions over `&[u8]`. Each validates its
//! source/destination encodings with the faithful
//! [`check_encoding_conversion_args`] (the C `CHECK_ENCODING_CONVERSION_ARGS`
//! macro) and then delegates to the merged radix-tree engine
//! ([`LocalToUtf`] / [`UtfToLocal`]), a 1:1 port of `conv.c`.
//!
//! The two SJIS <-> Unicode radix tables (generated from the `.map` files) are
//! ported as `const` arrays in [`tables`].

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use backend_utils_mb_conv_string_helpers::{
    check_encoding_conversion_args, ConversionResult, LocalToUtf, UtfToLocal,
};
use types_error::PgResult;
use types_wchar::encoding::{pg_enc, PG_SJIS, PG_UTF8};

/// `sjis_to_utf8` — convert an SJIS string to UTF-8.
pub fn sjis_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, PG_SJIS, PG_UTF8)?;
    LocalToUtf(
        src,
        Some(&tables::sjis_to_unicode_tree()),
        &[],
        None,
        PG_SJIS,
        no_error,
    )
}

/// `utf8_to_sjis` — convert a UTF-8 string to SJIS.
pub fn utf8_to_sjis(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, PG_SJIS)?;
    UtfToLocal(
        src,
        Some(&tables::sjis_from_unicode_tree()),
        &[],
        None,
        PG_SJIS,
        no_error,
    )
}

/// Wires this crate's seams. It declares none of its own, so this is a no-op
/// kept for the uniform `seams-init` startup convention.
pub fn init_seams() {}
