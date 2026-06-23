//! Port of `utf8_and_uhc.c` — the `UHC <--> UTF8` encoding-conversion
//! procedures.
//!
//! Faithful translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_uhc/utf8_and_uhc.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`uhc_to_utf8`, `utf8_to_uhc`) become
//! plain Rust functions over `&[u8]`. Each validates its source/destination
//! encodings with the faithful [`check_encoding_conversion_args`] (the C
//! `CHECK_ENCODING_CONVERSION_ARGS` macro) and then delegates to the merged
//! radix-tree engine ([`LocalToUtf`] / [`UtfToLocal`]), a 1:1 port of `conv.c`.
//!
//! The two UHC <-> Unicode radix tables (generated from the `.map` files) are
//! ported as `const` arrays in [`tables`].

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use ::conv_string_helpers::{
    check_encoding_conversion_args, ConversionResult, LocalToUtf, UtfToLocal,
};
use ::conv_string_helpers::make_conversion_builtin;
use ::types_error::PgResult;
use ::types_wchar::encoding::{pg_enc, PG_UHC, PG_UTF8};

/// `uhc_to_utf8` — convert a UHC string to UTF-8.
pub fn uhc_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, PG_UHC, PG_UTF8)?;
    LocalToUtf(
        src,
        Some(&tables::uhc_to_unicode_tree()),
        &[],
        None,
        PG_UHC,
        no_error,
    )
}

/// `utf8_to_uhc` — convert a UTF-8 string to UHC.
pub fn utf8_to_uhc(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, PG_UHC)?;
    UtfToLocal(
        src,
        Some(&tables::uhc_from_unicode_tree()),
        &[],
        None,
        PG_UHC,
        no_error,
    )
}

/// Registers this crate's ported conversion procedures as fmgr builtins so
/// their `pg_proc` OIDs resolve to the in-process Rust bodies (no `dlopen`).
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4380, "uhc_to_utf8", uhc_to_utf8),
        make_conversion_builtin(4381, "utf8_to_uhc", utf8_to_uhc),
    ]);
}
