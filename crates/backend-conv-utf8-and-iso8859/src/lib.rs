//! Port of `utf8_and_iso8859.c` — the `ISO-8859-x <--> UTF8`
//! encoding-conversion procedures.
//!
//! Faithful translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_iso8859/utf8_and_iso8859.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`iso8859_to_utf8`,
//! `utf8_to_iso8859`) become plain Rust functions over `&[u8]`. Each validates
//! its source/destination encodings with the faithful
//! [`check_encoding_conversion_args`] (the C `CHECK_ENCODING_CONVERSION_ARGS`
//! macro), selects the per-encoding radix map from the ISO-8859 family table
//! (the C `for (i = 0; i < lengthof(maps); i++)` dispatch), and then delegates
//! to the merged radix-tree engine ([`LocalToUtf`] / [`UtfToLocal`]), a 1:1 port
//! of `conv.c`.
//!
//! The 13 ISO-8859 family <-> Unicode radix tables (generated from the `.map`
//! files) are ported as `const` arrays in [`tables`].

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use backend_utils_error::ereport;
use backend_utils_mb_conv_string_helpers::{
    check_encoding_conversion_args, make_conversion_builtin, ConversionResult, LocalToUtf,
    UtfToLocal,
};
use types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use types_wchar::encoding::{
    pg_enc, PG_ISO_8859_5, PG_ISO_8859_6, PG_ISO_8859_7, PG_ISO_8859_8, PG_LATIN10, PG_LATIN2,
    PG_LATIN3, PG_LATIN4, PG_LATIN5, PG_LATIN6, PG_LATIN7, PG_LATIN8, PG_LATIN9, PG_UTF8,
};
use types_wchar::pg_mb_radix_tree;

/// `iso8859_to_utf8` — convert an ISO-8859-x string to UTF-8.
pub fn iso8859_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, -1, PG_UTF8)?;
    let Some(map) = to_utf8_map(src_encoding) else {
        return unexpected_encoding(src_encoding);
    };
    LocalToUtf(src, Some(&map), &[], None, src_encoding, no_error)
}

/// `utf8_to_iso8859` — convert a UTF-8 string to ISO-8859-x.
pub fn utf8_to_iso8859(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(src_encoding, dest_encoding, src.len() as i32, PG_UTF8, -1)?;
    let Some(map) = from_utf8_map(dest_encoding) else {
        return unexpected_encoding(dest_encoding);
    };
    UtfToLocal(src, Some(&map), &[], None, dest_encoding, no_error)
}

fn to_utf8_map(encoding: pg_enc) -> Option<pg_mb_radix_tree> {
    match encoding {
        PG_LATIN2 => Some(tables::iso8859_2_to_unicode_tree()),
        PG_LATIN3 => Some(tables::iso8859_3_to_unicode_tree()),
        PG_LATIN4 => Some(tables::iso8859_4_to_unicode_tree()),
        PG_LATIN5 => Some(tables::iso8859_9_to_unicode_tree()),
        PG_LATIN6 => Some(tables::iso8859_10_to_unicode_tree()),
        PG_LATIN7 => Some(tables::iso8859_13_to_unicode_tree()),
        PG_LATIN8 => Some(tables::iso8859_14_to_unicode_tree()),
        PG_LATIN9 => Some(tables::iso8859_15_to_unicode_tree()),
        PG_LATIN10 => Some(tables::iso8859_16_to_unicode_tree()),
        PG_ISO_8859_5 => Some(tables::iso8859_5_to_unicode_tree()),
        PG_ISO_8859_6 => Some(tables::iso8859_6_to_unicode_tree()),
        PG_ISO_8859_7 => Some(tables::iso8859_7_to_unicode_tree()),
        PG_ISO_8859_8 => Some(tables::iso8859_8_to_unicode_tree()),
        _ => None,
    }
}

fn from_utf8_map(encoding: pg_enc) -> Option<pg_mb_radix_tree> {
    match encoding {
        PG_LATIN2 => Some(tables::iso8859_2_from_unicode_tree()),
        PG_LATIN3 => Some(tables::iso8859_3_from_unicode_tree()),
        PG_LATIN4 => Some(tables::iso8859_4_from_unicode_tree()),
        PG_LATIN5 => Some(tables::iso8859_9_from_unicode_tree()),
        PG_LATIN6 => Some(tables::iso8859_10_from_unicode_tree()),
        PG_LATIN7 => Some(tables::iso8859_13_from_unicode_tree()),
        PG_LATIN8 => Some(tables::iso8859_14_from_unicode_tree()),
        PG_LATIN9 => Some(tables::iso8859_15_from_unicode_tree()),
        PG_LATIN10 => Some(tables::iso8859_16_from_unicode_tree()),
        PG_ISO_8859_5 => Some(tables::iso8859_5_from_unicode_tree()),
        PG_ISO_8859_6 => Some(tables::iso8859_6_from_unicode_tree()),
        PG_ISO_8859_7 => Some(tables::iso8859_7_from_unicode_tree()),
        PG_ISO_8859_8 => Some(tables::iso8859_8_from_unicode_tree()),
        _ => None,
    }
}

fn unexpected_encoding<T>(encoding: pg_enc) -> PgResult<T> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg(format!(
            "unexpected encoding ID {encoding} for ISO 8859 character sets"
        ))
        .into_error())
}

/// Wires this crate's seams. It declares none of its own, so this is a no-op
/// kept for the uniform `seams-init` startup convention.
pub fn init_seams() {
    // Register the two ported conversion procedures as fmgr builtins
    // (utf8_and_iso8859, pg_proc.dat OIDs) so they resolve in-process.
    backend_utils_fmgr_core::register_builtins([
        make_conversion_builtin(4372, "utf8_to_iso8859", utf8_to_iso8859),
        make_conversion_builtin(4373, "iso8859_to_utf8", iso8859_to_utf8),
    ]);
}
