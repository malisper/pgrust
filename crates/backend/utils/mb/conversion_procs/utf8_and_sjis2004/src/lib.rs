//! Port of `utf8_and_sjis2004.c` — the `SHIFT_JIS_2004 <--> UTF8`
//! encoding-conversion procedures.
//!
//! Faithful translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_sjis2004/utf8_and_sjis2004.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`shift_jis_2004_to_utf8`,
//! `utf8_to_shift_jis_2004`) become plain Rust functions over `&[u8]`. Each
//! validates its source/destination encodings with the faithful
//! [`check_encoding_conversion_args`] (the C `CHECK_ENCODING_CONVERSION_ARGS`
//! macro) and then delegates to the merged radix-tree engine ([`LocalToUtf`] /
//! [`UtfToLocal`]), a 1:1 port of `conv.c`.
//!
//! SHIFT_JIS_2004 has combining sequences, so each direction also passes a
//! combined-character map ([`tables::LUmapSHIFT_JIS_2004_combined`] /
//! [`tables::ULmapSHIFT_JIS_2004_combined`]) to the engine, exactly as the C
//! `LocalToUtf` / `UtfToLocal` calls do.
//!
//! The two SHIFT_JIS_2004 <-> Unicode radix tables and the two combined maps
//! (generated from the `.map` files) are ported as `const` arrays in [`tables`].

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use ::conv_string_helpers::{
    check_encoding_conversion_args, ConversionResult, LocalToUtf, UtfToLocal,
};
use ::conv_string_helpers::make_conversion_builtin;
use ::types_error::PgResult;
use ::types_wchar::encoding::{pg_enc, PG_SHIFT_JIS_2004, PG_UTF8};

/// `shift_jis_2004_to_utf8` — convert a SHIFT_JIS_2004 string to UTF-8.
pub fn shift_jis_2004_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_SHIFT_JIS_2004,
        PG_UTF8,
    )?;
    LocalToUtf(
        src,
        Some(&tables::shift_jis_2004_to_unicode_tree()),
        &tables::LUmapSHIFT_JIS_2004_combined,
        None,
        PG_SHIFT_JIS_2004,
        no_error,
    )
}

/// `utf8_to_shift_jis_2004` — convert a UTF-8 string to SHIFT_JIS_2004.
pub fn utf8_to_shift_jis_2004(
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
        PG_SHIFT_JIS_2004,
    )?;
    UtfToLocal(
        src,
        Some(&tables::shift_jis_2004_from_unicode_tree()),
        &tables::ULmapSHIFT_JIS_2004_combined,
        None,
        PG_SHIFT_JIS_2004,
        no_error,
    )
}

/// Registers this crate's ported conversion procedures as fmgr builtins so
/// their `pg_proc` OIDs resolve to the in-process Rust bodies (no `dlopen`).
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4384, "shift_jis_2004_to_utf8", shift_jis_2004_to_utf8),
        make_conversion_builtin(4385, "utf8_to_shift_jis_2004", utf8_to_shift_jis_2004),
    ]);
}
