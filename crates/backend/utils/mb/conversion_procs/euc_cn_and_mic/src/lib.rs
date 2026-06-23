//! `euc_cn_and_mic.c` — EUC_CN <-> MULE_INTERNAL conversion procs.
//!
//! 1:1 port of
//! `src/backend/utils/mb/conversion_procs/euc_cn_and_mic/euc_cn_and_mic.c`.
//!
//! The C entrypoints `euc_cn_to_mic` / `mic_to_euc_cn` are SQL-callable
//! conversion procs that validate the (source, dest) encoding pair via
//! `CHECK_ENCODING_CONVERSION_ARGS` and then run the file-local byte loops
//! `euc_cn2mic` / `mic2euc_cn`. An EUC_CN two-byte character `b1 b2` maps to the
//! three-byte MULE character `LC_GB2312_80 b1 b2`, and back.
//!
//! Unlike `euc_kr_and_mic.c`, the EUC_CN conversions do not call
//! `pg_encoding_verifymbchar`; a multibyte character is recognised by a
//! high-bit-set lead byte and validated by inspecting the high bit of the
//! following byte(s) directly (the C `IS_HIGHBIT_SET(*euc)` checks).

use error_fgram::PgResult;
use mb_fgram::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use conv_string_helpers::ConversionResult;
use conv_string_helpers::make_conversion_builtin;
use types_wchar::encoding::{pg_enc, PG_EUC_CN, PG_MULE_INTERNAL};

/// Convention no-op: this crate installs no inward seams.
/// Bridge a fgram-typed conversion `PgResult` into the real
/// `types_error::PgResult` the fmgr-builtin dispatcher expects. The
/// `ConversionResult` payload is the shared real type; only the error
/// universe differs, so map it by message + sqlstate.
fn into_real(
    r: PgResult<ConversionResult>,
) -> types_error_real::PgResult<ConversionResult> {
    r.map_err(|e| types_error_real::PgError::error(e.message().to_string()))
}

fn adapt_euc_cn_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(euc_cn_to_mic(src_encoding, dest_encoding, src, no_error))
}

fn adapt_mic_to_euc_cn(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(mic_to_euc_cn(src_encoding, dest_encoding, src, no_error))
}

/// Register the ported conversion procedures as fmgr builtins so
/// `fmgr_info` resolves their proc OIDs to the in-process Rust bodies
/// instead of `dlopen`ing `$libdir/euc_cn_and_mic`.
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4322, "euc_cn_to_mic", adapt_euc_cn_to_mic),
        make_conversion_builtin(4323, "mic_to_euc_cn", adapt_mic_to_euc_cn),
    ]);
}

/// `LC_GB2312_80` (mb/pg_wchar.h) — MULE_INTERNAL charset leading byte for
/// GB2312. Value verified against pgrust-pg-ffi-fgram.
const LC_GB2312_80: u8 = 0x91;

/// `HIGHBIT` / `IS_HIGHBIT_SET` (mb/pg_wchar.h).
const HIGHBIT: u8 = 0x80;

#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & HIGHBIT != 0
}

/// `euc_cn_to_mic` (euc_cn_and_mic.c) — SQL conversion proc EUC_CN -> MIC.
pub fn euc_cn_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_EUC_CN,
        PG_MULE_INTERNAL,
    )?;

    euc_cn2mic(src, no_error)
}

/// `mic_to_euc_cn` (euc_cn_and_mic.c) — SQL conversion proc MIC -> EUC_CN.
pub fn mic_to_euc_cn(
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
        PG_EUC_CN,
    )?;

    mic2euc_cn(src, no_error)
}

/// `euc_cn2mic` (euc_cn_and_mic.c) — convert an EUC_CN string to
/// `MULE_INTERNAL`. A high-bit-set lead byte begins the two-byte EUC_CN
/// sequence `b1 b2`; `b2` must also have its high bit set (else the sequence is
/// invalid). The character is emitted as the three-byte MULE character
/// `LC_GB2312_80 b1 b2`. Low-bit bytes pass through unchanged; an embedded NUL
/// is reported as an invalid EUC_CN byte sequence.
fn euc_cn2mic(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < src.len() {
        let c1 = src[pos];
        if is_highbit_set(c1) {
            if pos + 1 >= src.len() || !is_highbit_set(src[pos + 1]) {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_CN, &src[pos..])?;
            }
            dest.push(LC_GB2312_80);
            dest.push(c1);
            dest.push(src[pos + 1]);
            pos += 2;
        } else {
            // should be ASCII
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_CN, &src[pos..])?;
            }
            dest.push(c1);
            pos += 1;
        }
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

/// `mic2euc_cn` (euc_cn_and_mic.c) — convert a `MULE_INTERNAL` string to
/// EUC_CN. A high-bit-set lead byte must be `LC_GB2312_80` (any other charset
/// is untranslatable) and must be followed by two more high-bit-set bytes
/// `b1 b2` (else the MULE sequence is invalid); the `b1 b2` pair is emitted.
/// Low-bit bytes pass through; an embedded NUL is reported as an invalid
/// MULE_INTERNAL byte sequence.
fn mic2euc_cn(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < src.len() {
        let c1 = src[pos];
        if is_highbit_set(c1) {
            if c1 != LC_GB2312_80 {
                if no_error {
                    break;
                }
                report_untranslatable_char(PG_MULE_INTERNAL, PG_EUC_CN, &src[pos..])?;
            }
            if pos + 2 >= src.len()
                || !is_highbit_set(src[pos + 1])
                || !is_highbit_set(src[pos + 2])
            {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..])?;
            }
            // Skip the LC_GB2312_80 lead byte and emit the two following bytes.
            dest.push(src[pos + 1]);
            dest.push(src[pos + 2]);
            pos += 3;
        } else {
            // should be ASCII
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..])?;
            }
            dest.push(c1);
            pos += 1;
        }
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

#[cfg(test)]
mod tests;
