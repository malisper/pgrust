//! Port of `src/backend/utils/mb/conversion_procs/latin_and_mic/latin_and_mic.c` —
//! the `LATIN1/3/4 <-> MULE_INTERNAL` encoding-conversion procedures.
//!
//! The six `PG_FUNCTION_ARGS` entry points (`latin1_to_mic`, `mic_to_latin1`,
//! `latin3_to_mic`, `mic_to_latin3`, `latin4_to_mic`, `mic_to_latin4`) become
//! plain Rust functions taking the source/destination encoding ids, the source
//! bytes, and the `no_error` flag. Each validates its encodings with
//! `check_encoding_conversion_args` (`CHECK_ENCODING_CONVERSION_ARGS`) and
//! delegates to the shared engine `latin2mic` / `mic2latin` in
//! `conv_string_helpers` (a 1:1 port of the identically named
//! `conv.c` functions).
//!
//! Unlike the table-driven Latin conversions, `latin_and_mic.c` has no
//! conversion tables: the ISO-8859-1/3/4 single-byte values map directly into
//! the trailing byte of a two-byte MULE_INTERNAL character whose leading byte is
//! the charset code (`LC_ISO8859_1/3/4`), so the engine is a pure high-bit
//! prefix/strip.
//!
//! Like the C module, this crate owns no inward seams: the conversion
//! procedures are reached through the `pg_conversion` catalog, not a seam.
//! `init_seams()` is a no-op.

#![allow(clippy::result_large_err)]

use ::utils_error::PgResult;
use ::conv_string_helpers::{latin2mic, mic2latin, ConversionResult};
use ::conv_string_helpers::make_conversion_builtin;
use ::mbutils_seams::check_encoding_conversion_args;
use ::types_wchar::encoding::{pg_enc, PG_LATIN1, PG_LATIN3, PG_LATIN4, PG_MULE_INTERNAL};
use ::types_wchar::wchar::{LC_ISO8859_1, LC_ISO8859_3, LC_ISO8859_4};

/// `latin1_to_mic` (latin_and_mic.c): convert a LATIN1 (ISO-8859-1) string to
/// MULE_INTERNAL.
pub fn latin1_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_LATIN1,
        PG_MULE_INTERNAL,
    )?;
    latin2mic(src, LC_ISO8859_1, PG_LATIN1, no_error)
}

/// `mic_to_latin1` (latin_and_mic.c): convert a MULE_INTERNAL string to LATIN1
/// (ISO-8859-1).
pub fn mic_to_latin1(
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
        PG_LATIN1,
    )?;
    mic2latin(src, LC_ISO8859_1, PG_LATIN1, no_error)
}

/// `latin3_to_mic` (latin_and_mic.c): convert a LATIN3 (ISO-8859-3) string to
/// MULE_INTERNAL.
pub fn latin3_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_LATIN3,
        PG_MULE_INTERNAL,
    )?;
    latin2mic(src, LC_ISO8859_3, PG_LATIN3, no_error)
}

/// `mic_to_latin3` (latin_and_mic.c): convert a MULE_INTERNAL string to LATIN3
/// (ISO-8859-3).
pub fn mic_to_latin3(
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
        PG_LATIN3,
    )?;
    mic2latin(src, LC_ISO8859_3, PG_LATIN3, no_error)
}

/// `latin4_to_mic` (latin_and_mic.c): convert a LATIN4 (ISO-8859-4) string to
/// MULE_INTERNAL.
pub fn latin4_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_LATIN4,
        PG_MULE_INTERNAL,
    )?;
    latin2mic(src, LC_ISO8859_4, PG_LATIN4, no_error)
}

/// `mic_to_latin4` (latin_and_mic.c): convert a MULE_INTERNAL string to LATIN4
/// (ISO-8859-4).
pub fn mic_to_latin4(
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
        PG_LATIN4,
    )?;
    mic2latin(src, LC_ISO8859_4, PG_LATIN4, no_error)
}

/// This crate owns no inward seams (conversions are dispatched via the
/// `pg_conversion` catalog, mirroring the C module).
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4344, "latin1_to_mic", latin1_to_mic),
        make_conversion_builtin(4345, "mic_to_latin1", mic_to_latin1),
        make_conversion_builtin(4346, "latin3_to_mic", latin3_to_mic),
        make_conversion_builtin(4347, "mic_to_latin3", mic_to_latin3),
        make_conversion_builtin(4348, "latin4_to_mic", latin4_to_mic),
        make_conversion_builtin(4349, "mic_to_latin4", mic_to_latin4),
    ]);
}

#[cfg(test)]
mod tests;
