//! Port of `src/backend/utils/mb/conversion_procs/euc_kr_and_mic/euc_kr_and_mic.c`
//! — the `EUC_KR <--> MULE_INTERNAL` encoding-conversion procedures.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`euc_kr_to_mic`, `mic_to_euc_kr`)
//! become plain Rust functions over `&[u8]`. Each validates its source and
//! destination encodings with `check_encoding_conversion_args` (the C
//! `CHECK_ENCODING_CONVERSION_ARGS` macro) and then delegates to the file-local
//! `euc_kr2mic` / `mic2euc_kr` engine, a 1:1 port of the C static functions.
//!
//! `euc_kr_and_mic.c` has no conversion tables: an EUC_KR two-byte character
//! maps directly into a three-byte MULE_INTERNAL character whose leading byte is
//! the charset code [`LC_KS5601`]. The engine is therefore a pure prefix/strip
//! plus a length verification (`pg_encoding_verifymbchar`).
//!
//! The C code writes into a caller-supplied `dest` buffer and returns the number
//! of source bytes successfully consumed; here we build a plain [`Vec<u8>`] and
//! return it as a [`ConversionResult`] (`bytes` + `converted`).

#![allow(clippy::result_large_err)]

use backend_utils_error::PgResult;
use backend_utils_mb::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use backend_utils_mb_conv_string_helpers::ConversionResult;
use backend_utils_mb_conv_string_helpers::make_conversion_builtin;
use common_wchar::pg_encoding_verifymbchar;
use types_wchar::encoding::{pg_enc, PG_EUC_KR, PG_MULE_INTERNAL};

/// `LC_KS5601` (mb/pg_wchar.h) — the MULE_INTERNAL charset code for Korean.
const LC_KS5601: u8 = 0x93;

/// `HIGHBIT` / `IS_HIGHBIT_SET` (mb/pg_wchar.h).
const HIGHBIT: u8 = 0x80;

#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & HIGHBIT != 0
}

/// Bridge a fgram-typed conversion `PgResult` into the real
/// `types_error::PgResult` the fmgr-builtin dispatcher expects. The
/// `ConversionResult` payload is the shared real type; only the error
/// universe differs, so map it by message + sqlstate.
fn into_real(
    r: PgResult<ConversionResult>,
) -> types_error_real::PgResult<ConversionResult> {
    r.map_err(|e| types_error_real::PgError::error(e.message().to_string()))
}

fn adapt_euc_kr_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(euc_kr_to_mic(src_encoding, dest_encoding, src, no_error))
}

fn adapt_mic_to_euc_kr(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(mic_to_euc_kr(src_encoding, dest_encoding, src, no_error))
}

/// Register the ported conversion procedures as fmgr builtins so
/// `fmgr_info` resolves their proc OIDs to the in-process Rust bodies
/// instead of `dlopen`ing `$libdir/euc_kr_and_mic`.
pub fn init_seams() {
    backend_utils_fmgr_core::register_builtins_native([
        make_conversion_builtin(4330, "euc_kr_to_mic", adapt_euc_kr_to_mic),
        make_conversion_builtin(4331, "mic_to_euc_kr", adapt_mic_to_euc_kr),
    ]);
}

/// `euc_kr_to_mic` — convert an EUC_KR string to MULE_INTERNAL.
pub fn euc_kr_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_EUC_KR,
        PG_MULE_INTERNAL,
    )?;
    euc_kr2mic(src, no_error)
}

/// `mic_to_euc_kr` — convert a MULE_INTERNAL string to EUC_KR.
pub fn mic_to_euc_kr(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_MULE_INTERNAL,
        PG_EUC_KR,
    )?;
    mic2euc_kr(src, no_error)
}

/// `euc_kr2mic` (euc_kr_and_mic.c) — EUC_KR ---> MIC.
///
/// Each high-bit-set EUC_KR character is the two-byte sequence `b1 b2`, verified
/// by `pg_encoding_verifymbchar`, emitted as the three-byte MULE character
/// `LC_KS5601 b1 b2`. Low-bit bytes pass through unchanged; an embedded NUL is
/// reported as an invalid EUC_KR byte sequence.
fn euc_kr2mic(euc: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = euc.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        let c1 = euc[pos];
        if is_highbit_set(c1) {
            let l = pg_encoding_verifymbchar(PG_EUC_KR, &euc[pos..]);
            if l != 2 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_KR, &euc[pos..])?;
            }
            p.push(LC_KS5601);
            p.push(c1);
            p.push(euc[pos + 1]);
            pos += 2;
        } else {
            // should be ASCII
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_KR, &euc[pos..])?;
            }
            p.push(c1);
            pos += 1;
        }
    }

    Ok(ConversionResult {
        bytes: p,
        converted: pos as i32,
    })
}

/// `mic2euc_kr` (euc_kr_and_mic.c) — MIC ---> EUC_KR.
///
/// A high-bit-set byte must begin a valid MULE character (verified by
/// `pg_encoding_verifymbchar`); only the three-byte `LC_KS5601 b1 b2` form has
/// an EUC_KR equivalent (its `b1 b2` are emitted). Any other MULE charset is
/// untranslatable. Low-bit bytes pass through; an embedded NUL is reported as an
/// invalid MULE_INTERNAL byte sequence.
fn mic2euc_kr(mic: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = mic.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        let c1 = mic[pos];
        if !is_highbit_set(c1) {
            // ASCII
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL, &mic[pos..])?;
            }
            p.push(c1);
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_MULE_INTERNAL, &mic[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_MULE_INTERNAL, &mic[pos..])?;
        }
        if c1 == LC_KS5601 {
            p.push(mic[pos + 1]);
            p.push(mic[pos + 2]);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(PG_MULE_INTERNAL, PG_EUC_KR, &mic[pos..])?;
        }
        pos += l as usize;
    }

    Ok(ConversionResult {
        bytes: p,
        converted: pos as i32,
    })
}

#[cfg(test)]
mod tests;
