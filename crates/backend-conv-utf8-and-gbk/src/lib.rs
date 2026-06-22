//! Port of `utf8_and_gbk.c` — the `GBK <--> UTF8` encoding-conversion
//! procedures.
//!
//! Faithful translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_gbk/utf8_and_gbk.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`gbk_to_utf8`, `utf8_to_gbk`) become
//! plain Rust functions over `&[u8]`. Each validates its source/destination
//! encodings with the faithful [`check_encoding_conversion_args`] (the C
//! `CHECK_ENCODING_CONVERSION_ARGS` macro) and then delegates to the merged
//! radix-tree engine ([`LocalToUtf`] / [`UtfToLocal`]), a 1:1 port of `conv.c`.
//!
//! The two GBK <-> Unicode radix tables (generated from the `.map` files) are
//! ported as `const` arrays in [`tables`].

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use backend_utils_mb_conv_string_helpers::{
    check_encoding_conversion_args, ConversionResult, LocalToUtf, UtfToLocal,
};
use backend_utils_mb_conv_string_helpers::make_conversion_builtin;
use types_error::PgResult;
use types_wchar::encoding::{pg_enc, PG_GBK, PG_UTF8};

/// `gbk_to_utf8` — convert a GBK string to UTF-8.
pub fn gbk_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, PG_GBK, PG_UTF8)?;
    LocalToUtf(
        src,
        Some(&tables::gbk_to_unicode_tree()),
        &[],
        None,
        PG_GBK,
        no_error,
    )
}

/// `utf8_to_gbk` — convert a UTF-8 string to GBK.
pub fn utf8_to_gbk(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, PG_GBK)?;
    UtfToLocal(
        src,
        Some(&tables::gbk_from_unicode_tree()),
        &[],
        None,
        PG_GBK,
        no_error,
    )
}

/// Registers this crate's ported conversion procedures as fmgr builtins so
/// their `pg_proc` OIDs resolve to the in-process Rust bodies (no `dlopen`).
pub fn init_seams() {
    backend_utils_fmgr_core::register_builtins_native([
        make_conversion_builtin(4370, "gbk_to_utf8", gbk_to_utf8),
        make_conversion_builtin(4371, "utf8_to_gbk", utf8_to_gbk),
    ]);
}
