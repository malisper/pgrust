//! Port of `utf8_and_iso8859_1.c` — the `ISO8859_1 <--> UTF8`
//! encoding-conversion procedures.
//!
//! Faithful 1:1 translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_iso8859_1/utf8_and_iso8859_1.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`iso8859_1_to_utf8`,
//! `utf8_to_iso8859_1`) become plain Rust functions over `&[u8]`, returning the
//! produced bytes plus the count of source bytes successfully consumed (the C
//! `src - start` return value), exactly like the conv.c-driven siblings.
//!
//! ISO-8859-1 needs no conversion tables: the C code computes the UTF-8 octets
//! inline (LATIN1 is identical to the first 256 Unicode code points). The
//! argument check (`CHECK_ENCODING_CONVERSION_ARGS`) and the two reporters
//! (`report_invalid_encoding`/`report_untranslatable_char`) are reused from the
//! merged `utils/mb` framework crates.

#![allow(clippy::result_large_err)]

use backend_utils_mb_conv_string_helpers::{check_encoding_conversion_args, ConversionResult};
use backend_utils_mb_mbutils_seams::{report_invalid_encoding, report_untranslatable_char};
use common_wchar::{pg_utf8_islegal, pg_utf_mblen_private};
use types_error::PgResult;
use types_wchar::encoding::{pg_enc, PG_LATIN1, PG_UTF8};

/// `HIGHBIT` / `IS_HIGHBIT_SET` (mb/pg_wchar.h).
const HIGHBIT: u8 = 0x80;

#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & HIGHBIT != 0
}

/// `iso8859_1_to_utf8` — convert an ISO-8859-1 (`LATIN1`) string to UTF-8.
pub fn iso8859_1_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_LATIN1,
        PG_UTF8,
    )?;

    let mut dest = Vec::with_capacity(src.len().saturating_mul(2));
    let mut pos = 0;

    while pos < src.len() {
        let c = src[pos];
        if c == 0 {
            if no_error {
                break;
            }
            report_invalid_encoding::call(PG_LATIN1, &src[pos..])?;
        }
        if !is_highbit_set(c) {
            dest.push(c);
        } else {
            dest.push((c >> 6) | 0xc0);
            dest.push((c & 0x3f) | HIGHBIT);
        }
        pos += 1;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

/// `utf8_to_iso8859_1` — convert a UTF-8 string to ISO-8859-1 (`LATIN1`).
pub fn utf8_to_iso8859_1(
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
        PG_LATIN1,
    )?;

    let mut dest = Vec::with_capacity(src.len());
    let mut pos = 0;

    while pos < src.len() {
        let c = src[pos];
        if c == 0 {
            if no_error {
                break;
            }
            report_invalid_encoding::call(PG_UTF8, &src[pos..])?;
        }

        // Fast path for ASCII-subset characters.
        if !is_highbit_set(c) {
            dest.push(c);
            pos += 1;
            continue;
        }

        let len = src.len() - pos;
        let l = pg_utf_mblen_private(&src[pos..]).unwrap_or(0) as usize;
        if l > len || l == 0 || !pg_utf8_islegal(&src[pos..pos + l.min(len)]) {
            if no_error {
                break;
            }
            report_invalid_encoding::call(PG_UTF8, &src[pos..])?;
        }
        if l != 2 {
            if no_error {
                break;
            }
            report_untranslatable_char::call(PG_UTF8, PG_LATIN1, &src[pos..])?;
        }

        let c1 = (src[pos + 1] & 0x3f) as u16;
        let cc = (((c & 0x1f) as u16) << 6) | c1;
        if (0x80..=0xff).contains(&cc) {
            dest.push(cc as u8);
            pos += 2;
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char::call(PG_UTF8, PG_LATIN1, &src[pos..])?;
        }
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

/// Wires this crate's seams. It declares none of its own, so this is a no-op
/// kept for the uniform `seams-init` startup convention.
pub fn init_seams() {}
