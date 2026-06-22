//! Port of
//! `src/backend/utils/mb/conversion_procs/latin2_and_win1250/latin2_and_win1250.c` —
//! the `LATIN2 <-> WIN1250 <-> MULE_INTERNAL` encoding-conversion procedures.
//!
//! The six `PG_FUNCTION_ARGS` entry points become plain Rust functions over the
//! source/destination encoding ids, the source bytes, and the `no_error` flag.
//! Each validates its encodings with `check_encoding_conversion_args`
//! (`CHECK_ENCODING_CONVERSION_ARGS`) and delegates to the shared engine in
//! `backend_utils_mb_conv_string_helpers` (`conv.c`):
//!
//! - LATIN2 <-> MULE: `latin2mic` / `mic2latin` (pure high-bit prefix/strip).
//! - WIN1250 <-> MULE: `latin2mic_with_table` / `mic2latin_with_table` driven
//!   by the WIN1250<->ISO-8859-2 byte tables.
//! - LATIN2 <-> WIN1250: `local2local` driven by the same byte tables.
//!
//! The conversion tables in [`tables`] are ported byte-for-byte from the C
//! `static const unsigned char` arrays.
//!
//! Like the C module, this crate owns no inward seams: the conversion
//! procedures are reached through the `pg_conversion` catalog, not a seam.
//! `init_seams()` is a no-op.

#![allow(clippy::result_large_err)]

mod tables;

use backend_utils_error::PgResult;
use backend_utils_mb_conv_string_helpers::{
    latin2mic, latin2mic_with_table, local2local, mic2latin, mic2latin_with_table, ConversionResult,
};
use backend_utils_mb_conv_string_helpers::make_conversion_builtin;
use backend_utils_mb_mbutils_seams::check_encoding_conversion_args;
use tables::{ISO88592_2_WIN1250, WIN1250_2_ISO88592};
use types_wchar::encoding::{pg_enc, PG_LATIN2, PG_MULE_INTERNAL, PG_WIN1250};
use types_wchar::wchar::LC_ISO8859_2;

/// `latin2_to_mic` (latin2_and_win1250.c): convert LATIN2 -> MULE_INTERNAL.
pub fn latin2_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_LATIN2,
        PG_MULE_INTERNAL,
    )?;
    latin2mic(src, LC_ISO8859_2, PG_LATIN2, no_error)
}

/// `mic_to_latin2` (latin2_and_win1250.c): convert MULE_INTERNAL -> LATIN2.
pub fn mic_to_latin2(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_MULE_INTERNAL,
        PG_LATIN2,
    )?;
    mic2latin(src, LC_ISO8859_2, PG_LATIN2, no_error)
}

/// `win1250_to_mic` (latin2_and_win1250.c): convert WIN1250 -> MULE_INTERNAL.
pub fn win1250_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN1250,
        PG_MULE_INTERNAL,
    )?;
    latin2mic_with_table(src, LC_ISO8859_2, PG_WIN1250, &WIN1250_2_ISO88592, no_error)
}

/// `mic_to_win1250` (latin2_and_win1250.c): convert MULE_INTERNAL -> WIN1250.
pub fn mic_to_win1250(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_MULE_INTERNAL,
        PG_WIN1250,
    )?;
    mic2latin_with_table(src, LC_ISO8859_2, PG_WIN1250, &ISO88592_2_WIN1250, no_error)
}

/// `latin2_to_win1250` (latin2_and_win1250.c): convert LATIN2 -> WIN1250.
pub fn latin2_to_win1250(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_LATIN2,
        PG_WIN1250,
    )?;
    local2local(src, PG_LATIN2, PG_WIN1250, &ISO88592_2_WIN1250, no_error)
}

/// `win1250_to_latin2` (latin2_and_win1250.c): convert WIN1250 -> LATIN2.
pub fn win1250_to_latin2(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN1250,
        PG_LATIN2,
    )?;
    local2local(src, PG_WIN1250, PG_LATIN2, &WIN1250_2_ISO88592, no_error)
}

/// This crate owns no inward seams (conversions are dispatched via the
/// `pg_conversion` catalog, mirroring the C module).
pub fn init_seams() {
    backend_utils_fmgr_core::register_builtins_native([
        make_conversion_builtin(4338, "latin2_to_mic", latin2_to_mic),
        make_conversion_builtin(4339, "mic_to_latin2", mic_to_latin2),
        make_conversion_builtin(4340, "win1250_to_mic", win1250_to_mic),
        make_conversion_builtin(4341, "mic_to_win1250", mic_to_win1250),
        make_conversion_builtin(4342, "latin2_to_win1250", latin2_to_win1250),
        make_conversion_builtin(4343, "win1250_to_latin2", win1250_to_latin2),
    ]);
}

#[cfg(test)]
mod tests;
