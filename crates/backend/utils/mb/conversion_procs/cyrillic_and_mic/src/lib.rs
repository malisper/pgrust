//! Port of
//! `src/backend/utils/mb/conversion_procs/cyrillic_and_mic/cyrillic_and_mic.c` —
//! the Cyrillic encoding-conversion procedures.
//!
//! Supported Cyrillic encodings: KOI8-R (also the MULE_INTERNAL Cyrillic
//! charset), ISO-8859-5, Microsoft CP1251 (windows-1251), and Alternativny
//! Variant (MS-DOS CP866 / WIN866).
//!
//! The twenty `PG_FUNCTION_ARGS` entry points become plain Rust functions over
//! the source/destination encoding ids, the source bytes, and the `no_error`
//! flag. Each validates its encodings with `check_encoding_conversion_args`
//! (`CHECK_ENCODING_CONVERSION_ARGS`) and delegates to the shared engine in
//! `conv_string_helpers` (`conv.c`):
//!
//! - KOI8-R <-> MULE: `latin2mic` / `mic2latin` (KOI8-R is the MULE Cyrillic
//!   charset, so this is a pure `LC_KOI8_R` prefix/strip).
//! - ISO-8859-5 / WIN1251 / WIN866 <-> MULE: `latin2mic_with_table` /
//!   `mic2latin_with_table` (the source is first table-mapped to KOI8-R bytes,
//!   then prefixed with `LC_KOI8_R`).
//! - All single-byte <-> single-byte Cyrillic pairs: `local2local`.
//!
//! The twelve conversion tables in [`tables`] are ported byte-for-byte from the
//! C `static const unsigned char` arrays.
//!
//! Like the C module, this crate owns no inward seams: the conversion
//! procedures are reached through the `pg_conversion` catalog, not a seam.
//! `init_seams()` is a no-op.

#![allow(clippy::result_large_err)]

mod tables;

use ::utils_error::PgResult;
use conv_string_helpers::{
    latin2mic, latin2mic_with_table, local2local, make_conversion_builtin, mic2latin,
    mic2latin_with_table, ConversionResult,
};
use ::mbutils_seams::check_encoding_conversion_args;
use tables::{
    ISO2KOI, ISO2WIN1251, ISO2WIN866, KOI2ISO, KOI2WIN1251, KOI2WIN866, WIN12512ISO, WIN12512KOI,
    WIN12512WIN866, WIN8662ISO, WIN8662KOI, WIN8662WIN1251,
};
use ::types_wchar::encoding::{
    pg_enc, PG_ISO_8859_5, PG_KOI8R, PG_MULE_INTERNAL, PG_WIN1251, PG_WIN866,
};
use ::types_wchar::wchar::LC_KOI8_R;

/// `koi8r_to_mic` (cyrillic_and_mic.c): KOI8-R -> MULE_INTERNAL.
pub fn koi8r_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_KOI8R,
        PG_MULE_INTERNAL,
    )?;
    latin2mic(src, LC_KOI8_R, PG_KOI8R, no_error)
}

/// `mic_to_koi8r` (cyrillic_and_mic.c): MULE_INTERNAL -> KOI8-R.
pub fn mic_to_koi8r(
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
        PG_KOI8R,
    )?;
    mic2latin(src, LC_KOI8_R, PG_KOI8R, no_error)
}

/// `iso_to_mic` (cyrillic_and_mic.c): ISO-8859-5 -> MULE_INTERNAL.
pub fn iso_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_ISO_8859_5,
        PG_MULE_INTERNAL,
    )?;
    latin2mic_with_table(src, LC_KOI8_R, PG_ISO_8859_5, &ISO2KOI, no_error)
}

/// `mic_to_iso` (cyrillic_and_mic.c): MULE_INTERNAL -> ISO-8859-5.
pub fn mic_to_iso(
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
        PG_ISO_8859_5,
    )?;
    mic2latin_with_table(src, LC_KOI8_R, PG_ISO_8859_5, &KOI2ISO, no_error)
}

/// `win1251_to_mic` (cyrillic_and_mic.c): WIN1251 -> MULE_INTERNAL.
pub fn win1251_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN1251,
        PG_MULE_INTERNAL,
    )?;
    latin2mic_with_table(src, LC_KOI8_R, PG_WIN1251, &WIN12512KOI, no_error)
}

/// `mic_to_win1251` (cyrillic_and_mic.c): MULE_INTERNAL -> WIN1251.
pub fn mic_to_win1251(
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
        PG_WIN1251,
    )?;
    mic2latin_with_table(src, LC_KOI8_R, PG_WIN1251, &KOI2WIN1251, no_error)
}

/// `win866_to_mic` (cyrillic_and_mic.c): WIN866 -> MULE_INTERNAL.
pub fn win866_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN866,
        PG_MULE_INTERNAL,
    )?;
    latin2mic_with_table(src, LC_KOI8_R, PG_WIN866, &WIN8662KOI, no_error)
}

/// `mic_to_win866` (cyrillic_and_mic.c): MULE_INTERNAL -> WIN866.
pub fn mic_to_win866(
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
        PG_WIN866,
    )?;
    mic2latin_with_table(src, LC_KOI8_R, PG_WIN866, &KOI2WIN866, no_error)
}

/// `koi8r_to_win1251` (cyrillic_and_mic.c): KOI8-R -> WIN1251.
pub fn koi8r_to_win1251(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_KOI8R,
        PG_WIN1251,
    )?;
    local2local(src, PG_KOI8R, PG_WIN1251, &KOI2WIN1251, no_error)
}

/// `win1251_to_koi8r` (cyrillic_and_mic.c): WIN1251 -> KOI8-R.
pub fn win1251_to_koi8r(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN1251,
        PG_KOI8R,
    )?;
    local2local(src, PG_WIN1251, PG_KOI8R, &WIN12512KOI, no_error)
}

/// `koi8r_to_win866` (cyrillic_and_mic.c): KOI8-R -> WIN866.
pub fn koi8r_to_win866(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_KOI8R,
        PG_WIN866,
    )?;
    local2local(src, PG_KOI8R, PG_WIN866, &KOI2WIN866, no_error)
}

/// `win866_to_koi8r` (cyrillic_and_mic.c): WIN866 -> KOI8-R.
pub fn win866_to_koi8r(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN866,
        PG_KOI8R,
    )?;
    local2local(src, PG_WIN866, PG_KOI8R, &WIN8662KOI, no_error)
}

/// `win866_to_win1251` (cyrillic_and_mic.c): WIN866 -> WIN1251.
pub fn win866_to_win1251(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN866,
        PG_WIN1251,
    )?;
    local2local(src, PG_WIN866, PG_WIN1251, &WIN8662WIN1251, no_error)
}

/// `win1251_to_win866` (cyrillic_and_mic.c): WIN1251 -> WIN866.
pub fn win1251_to_win866(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN1251,
        PG_WIN866,
    )?;
    local2local(src, PG_WIN1251, PG_WIN866, &WIN12512WIN866, no_error)
}

/// `iso_to_koi8r` (cyrillic_and_mic.c): ISO-8859-5 -> KOI8-R.
pub fn iso_to_koi8r(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_ISO_8859_5,
        PG_KOI8R,
    )?;
    local2local(src, PG_ISO_8859_5, PG_KOI8R, &ISO2KOI, no_error)
}

/// `koi8r_to_iso` (cyrillic_and_mic.c): KOI8-R -> ISO-8859-5.
pub fn koi8r_to_iso(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_KOI8R,
        PG_ISO_8859_5,
    )?;
    local2local(src, PG_KOI8R, PG_ISO_8859_5, &KOI2ISO, no_error)
}

/// `iso_to_win1251` (cyrillic_and_mic.c): ISO-8859-5 -> WIN1251.
pub fn iso_to_win1251(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_ISO_8859_5,
        PG_WIN1251,
    )?;
    local2local(src, PG_ISO_8859_5, PG_WIN1251, &ISO2WIN1251, no_error)
}

/// `win1251_to_iso` (cyrillic_and_mic.c): WIN1251 -> ISO-8859-5.
pub fn win1251_to_iso(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN1251,
        PG_ISO_8859_5,
    )?;
    local2local(src, PG_WIN1251, PG_ISO_8859_5, &WIN12512ISO, no_error)
}

/// `iso_to_win866` (cyrillic_and_mic.c): ISO-8859-5 -> WIN866.
pub fn iso_to_win866(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_ISO_8859_5,
        PG_WIN866,
    )?;
    local2local(src, PG_ISO_8859_5, PG_WIN866, &ISO2WIN866, no_error)
}

/// `win866_to_iso` (cyrillic_and_mic.c): WIN866 -> ISO-8859-5.
pub fn win866_to_iso(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args::call(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_WIN866,
        PG_ISO_8859_5,
    )?;
    local2local(src, PG_WIN866, PG_ISO_8859_5, &WIN8662ISO, no_error)
}

/// Register the twenty ported conversion procedures as fmgr builtins
/// (`cyrillic_and_mic`, `pg_proc.dat` OIDs 4302-4321) so that `fmgr_info`
/// resolves their proc OIDs to the in-process Rust bodies instead of
/// `dlopen`ing `$libdir/cyrillic_and_mic` — the C bodies have no C ABI here.
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4302, "koi8r_to_mic", koi8r_to_mic),
        make_conversion_builtin(4303, "mic_to_koi8r", mic_to_koi8r),
        make_conversion_builtin(4304, "iso_to_mic", iso_to_mic),
        make_conversion_builtin(4305, "mic_to_iso", mic_to_iso),
        make_conversion_builtin(4306, "win1251_to_mic", win1251_to_mic),
        make_conversion_builtin(4307, "mic_to_win1251", mic_to_win1251),
        make_conversion_builtin(4308, "win866_to_mic", win866_to_mic),
        make_conversion_builtin(4309, "mic_to_win866", mic_to_win866),
        make_conversion_builtin(4310, "koi8r_to_win1251", koi8r_to_win1251),
        make_conversion_builtin(4311, "win1251_to_koi8r", win1251_to_koi8r),
        make_conversion_builtin(4312, "koi8r_to_win866", koi8r_to_win866),
        make_conversion_builtin(4313, "win866_to_koi8r", win866_to_koi8r),
        make_conversion_builtin(4314, "win866_to_win1251", win866_to_win1251),
        make_conversion_builtin(4315, "win1251_to_win866", win1251_to_win866),
        make_conversion_builtin(4316, "iso_to_koi8r", iso_to_koi8r),
        make_conversion_builtin(4317, "koi8r_to_iso", koi8r_to_iso),
        make_conversion_builtin(4318, "iso_to_win1251", iso_to_win1251),
        make_conversion_builtin(4319, "win1251_to_iso", win1251_to_iso),
        make_conversion_builtin(4320, "iso_to_win866", iso_to_win866),
        make_conversion_builtin(4321, "win866_to_iso", win866_to_iso),
    ]);
}

#[cfg(test)]
mod tests;
