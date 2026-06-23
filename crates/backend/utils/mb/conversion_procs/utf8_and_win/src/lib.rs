//! Port of `utf8_and_win.c` — the `WIN* <--> UTF8` encoding-conversion
//! procedures (WIN866, WIN874, WIN1250..WIN1258).
//!
//! Faithful translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_win/utf8_and_win.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`win_to_utf8`, `utf8_to_win`) become
//! plain Rust functions over `&[u8]`. Each validates its source/destination
//! encodings with the faithful [`check_encoding_conversion_args`] (the C
//! `CHECK_ENCODING_CONVERSION_ARGS` macro), selects the per-encoding radix map
//! (the C `switch` over the WIN code-page family, expressed via the `maps[]`
//! table), and then delegates to the merged radix-tree engine ([`LocalToUtf`] /
//! [`UtfToLocal`]), a 1:1 port of `conv.c`.
//!
//! The C code reads the encoding to dispatch on from a different `fcinfo`
//! argument per direction: `win_to_utf8` looks up the map by the *source*
//! encoding (`PG_GETARG_INT32(0)`), while `utf8_to_win` looks it up by the
//! *destination* encoding (`PG_GETARG_INT32(1)`). Both directions are mirrored
//! faithfully here.
//!
//! The eleven WIN <-> Unicode radix tables (generated from the `.map` files,
//! one to-Unicode and one from-Unicode tree per code page = 22 trees) are
//! ported as `const` arrays in [`tables`].

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use ::utils_error::ereport;
use ::conv_string_helpers::{
    check_encoding_conversion_args, ConversionResult, LocalToUtf, UtfToLocal,
};
use ::conv_string_helpers::make_conversion_builtin;
use ::types_error::{PgResult, ERRCODE_INTERNAL_ERROR, ERROR};
use ::types_wchar::encoding::{
    pg_enc, PG_UTF8, PG_WIN1250, PG_WIN1251, PG_WIN1252, PG_WIN1253, PG_WIN1254, PG_WIN1255,
    PG_WIN1256, PG_WIN1257, PG_WIN1258, PG_WIN866, PG_WIN874,
};
use ::types_wchar::pg_mb_radix_tree;

/// `win_to_utf8` — convert a WIN* string to UTF-8.
///
/// The C function dispatches on the *source* encoding (`PG_GETARG_INT32(0)`).
pub fn win_to_utf8(
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

/// `utf8_to_win` — convert a UTF-8 string to WIN*.
///
/// The C function dispatches on the *destination* encoding
/// (`PG_GETARG_INT32(1)`).
pub fn utf8_to_win(
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

/// Selects the WIN-code-page → Unicode radix map, mirroring the to_unicode
/// column of the C `maps[]` table.
fn to_utf8_map(encoding: pg_enc) -> Option<pg_mb_radix_tree> {
    match encoding {
        PG_WIN866 => Some(tables::win866_to_unicode_tree()),
        PG_WIN874 => Some(tables::win874_to_unicode_tree()),
        PG_WIN1250 => Some(tables::win1250_to_unicode_tree()),
        PG_WIN1251 => Some(tables::win1251_to_unicode_tree()),
        PG_WIN1252 => Some(tables::win1252_to_unicode_tree()),
        PG_WIN1253 => Some(tables::win1253_to_unicode_tree()),
        PG_WIN1254 => Some(tables::win1254_to_unicode_tree()),
        PG_WIN1255 => Some(tables::win1255_to_unicode_tree()),
        PG_WIN1256 => Some(tables::win1256_to_unicode_tree()),
        PG_WIN1257 => Some(tables::win1257_to_unicode_tree()),
        PG_WIN1258 => Some(tables::win1258_to_unicode_tree()),
        _ => None,
    }
}

/// Selects the Unicode → WIN-code-page radix map, mirroring the from_unicode
/// column of the C `maps[]` table.
fn from_utf8_map(encoding: pg_enc) -> Option<pg_mb_radix_tree> {
    match encoding {
        PG_WIN866 => Some(tables::win866_from_unicode_tree()),
        PG_WIN874 => Some(tables::win874_from_unicode_tree()),
        PG_WIN1250 => Some(tables::win1250_from_unicode_tree()),
        PG_WIN1251 => Some(tables::win1251_from_unicode_tree()),
        PG_WIN1252 => Some(tables::win1252_from_unicode_tree()),
        PG_WIN1253 => Some(tables::win1253_from_unicode_tree()),
        PG_WIN1254 => Some(tables::win1254_from_unicode_tree()),
        PG_WIN1255 => Some(tables::win1255_from_unicode_tree()),
        PG_WIN1256 => Some(tables::win1256_from_unicode_tree()),
        PG_WIN1257 => Some(tables::win1257_from_unicode_tree()),
        PG_WIN1258 => Some(tables::win1258_from_unicode_tree()),
        _ => None,
    }
}

/// The C `elog(ERROR, "unexpected encoding ID %d for WIN character sets", ...)`
/// reached when the dispatched encoding id is outside the WIN family.
fn unexpected_encoding<T>(encoding: pg_enc) -> PgResult<T> {
    Err(ereport(ERROR)
        .errcode(ERRCODE_INTERNAL_ERROR)
        .errmsg(format!(
            "unexpected encoding ID {encoding} for WIN character sets"
        ))
        .into_error())
}

/// Registers this crate's ported conversion procedures as fmgr builtins so
/// their `pg_proc` OIDs resolve to the in-process Rust bodies (no `dlopen`).
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4358, "utf8_to_win", utf8_to_win),
        make_conversion_builtin(4359, "win_to_utf8", win_to_utf8),
    ]);
}
