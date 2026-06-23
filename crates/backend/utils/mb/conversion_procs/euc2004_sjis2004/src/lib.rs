//! `euc2004_sjis2004.c` — EUC_JIS_2004 <-> SHIFT_JIS_2004 conversion procs.
//!
//! 1:1 port of
//! `src/backend/utils/mb/conversion_procs/euc2004_sjis2004/euc2004_sjis2004.c`.
//!
//! EUC_JIS_2004 and SHIFT_JIS_2004 share the same JIS X 0213 repertoire, so the
//! conversion is purely *algorithmic* ku/ten arithmetic — there is no radix tree
//! or combined map (the C file `#include`s no `.map` header). Both encodings
//! carry the 2-codepoint combining sequences as ordinary multibyte characters;
//! the byte-loops below mirror the C branch-for-branch, including the plane-2
//! `SS3` and JIS X 0201 kana `SS2` handling.
//!
//! The two C `PG_FUNCTION_ARGS` entry points (`euc_jis_2004_to_shift_jis_2004`,
//! `shift_jis_2004_to_euc_jis_2004`) become plain Rust functions over `&[u8]`,
//! returning a [`ConversionResult`] (bytes + count of source bytes consumed,
//! the C `src - start` return value). Each validates its (source, dest)
//! encoding pair with [`check_encoding_conversion_args`] (the C
//! `CHECK_ENCODING_CONVERSION_ARGS` macro).

use ::error_fgram::PgResult;
use mb_fgram::{check_encoding_conversion_args, report_invalid_encoding};
use ::conv_string_helpers::ConversionResult;
use ::conv_string_helpers::make_conversion_builtin;
use ::common_wchar::pg_encoding_verifymbchar;
use ::types_wchar::encoding::{pg_enc, PG_EUC_JIS_2004, PG_SHIFT_JIS_2004};

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

fn adapt_euc_jis_2004_to_shift_jis_2004(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(euc_jis_2004_to_shift_jis_2004(src_encoding, dest_encoding, src, no_error))
}

fn adapt_shift_jis_2004_to_euc_jis_2004(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(shift_jis_2004_to_euc_jis_2004(src_encoding, dest_encoding, src, no_error))
}

/// Register the ported conversion procedures as fmgr builtins so
/// `fmgr_info` resolves their proc OIDs to the in-process Rust bodies
/// instead of `dlopen`ing `$libdir/euc2004_sjis2004`.
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4386, "euc_jis_2004_to_shift_jis_2004", adapt_euc_jis_2004_to_shift_jis_2004),
        make_conversion_builtin(4387, "shift_jis_2004_to_euc_jis_2004", adapt_shift_jis_2004_to_euc_jis_2004),
    ]);
}

/// `HIGHBIT` (mb/pg_wchar.h).
const HIGHBIT: u8 = 0x80;
/// `SS2` (mb/pg_wchar.h) — single-shift two (JIS X 0201 kana lead in EUC).
const SS2: u8 = 0x8e;
/// `SS3` (mb/pg_wchar.h) — single-shift three (JIS X 0213 plane 2 lead in EUC).
const SS3: u8 = 0x8f;

/// `IS_HIGHBIT_SET` (mb/pg_wchar.h).
#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & HIGHBIT != 0
}

/// `euc_jis_2004_to_shift_jis_2004` (euc2004_sjis2004.c) — SQL conversion proc
/// EUC_JIS_2004 -> SHIFT_JIS_2004.
pub fn euc_jis_2004_to_shift_jis_2004(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_EUC_JIS_2004,
        PG_SHIFT_JIS_2004,
    )?;

    euc_jis_20042shift_jis_2004(src, no_error)
}

/// `shift_jis_2004_to_euc_jis_2004` (euc2004_sjis2004.c) — SQL conversion proc
/// SHIFT_JIS_2004 -> EUC_JIS_2004.
pub fn shift_jis_2004_to_euc_jis_2004(
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
        PG_EUC_JIS_2004,
    )?;

    shift_jis_20042euc_jis_2004(src, no_error)
}

/// `euc_jis_20042shift_jis_2004` (euc2004_sjis2004.c) — EUC_JIS_2004 ->
/// SHIFT_JIS_2004.
fn euc_jis_20042shift_jis_2004(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < src.len() {
        let c1 = src[pos];
        if !is_highbit_set(c1) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..])?;
            }
            dest.push(c1);
            pos += 1;
            continue;
        }

        let l = pg_encoding_verifymbchar(PG_EUC_JIS_2004, &src[pos..]);

        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..])?;
        }
        let l = l as usize;

        if c1 == SS2 && l == 2 {
            /* JIS X 0201 kana? */
            dest.push(src[pos + 1]);
        } else if c1 == SS3 && l == 3 {
            /* JIS X 0213 plane 2? */
            let ku = src[pos + 1] as i32 - 0xa0;
            let ten = src[pos + 2] as i32 - 0xa0;

            match ku {
                1 | 3 | 4 | 5 | 8 | 12 | 13 | 14 | 15 => {
                    dest.push((((ku + 0x1df) >> 1) - (ku >> 3) * 3) as u8);
                }
                _ => {
                    if (78..=94).contains(&ku) {
                        dest.push(((ku + 0x19b) >> 1) as u8);
                    } else {
                        if no_error {
                            break;
                        }
                        report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..])?;
                    }
                }
            }

            if ku % 2 != 0 {
                if (1..=63).contains(&ten) {
                    dest.push((ten + 0x3f) as u8);
                } else if (64..=94).contains(&ten) {
                    dest.push((ten + 0x40) as u8);
                } else {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..])?;
                }
            } else {
                dest.push((ten + 0x9e) as u8);
            }
        } else if l == 2 {
            /* JIS X 0213 plane 1? */
            let ku = c1 as i32 - 0xa0;
            let ten = src[pos + 1] as i32 - 0xa0;

            if (1..=62).contains(&ku) {
                dest.push(((ku + 0x101) >> 1) as u8);
            } else if (63..=94).contains(&ku) {
                dest.push(((ku + 0x181) >> 1) as u8);
            } else {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..])?;
            }

            if ku % 2 != 0 {
                if (1..=63).contains(&ten) {
                    dest.push((ten + 0x3f) as u8);
                } else if (64..=94).contains(&ten) {
                    dest.push((ten + 0x40) as u8);
                } else {
                    if no_error {
                        break;
                    }
                    report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..])?;
                }
            } else {
                dest.push((ten + 0x9e) as u8);
            }
        } else {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_EUC_JIS_2004, &src[pos..])?;
        }

        pos += l;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

/// `get_ten` (euc2004_sjis2004.c) — returns the SHIFT_JIS_2004 "ten" code
/// indicated by the second byte `b`, plus `*ku`: `1` if the implied "ku" is odd,
/// `0` if even. Returns `ten = -1` (here `None`) on an out-of-range byte.
fn get_ten(b: i32) -> Option<(i32, i32)> {
    if (0x40..=0x7e).contains(&b) {
        Some((b - 0x3f, 1))
    } else if (0x80..=0x9e).contains(&b) {
        Some((b - 0x40, 1))
    } else if (0x9f..=0xfc).contains(&b) {
        Some((b - 0x9e, 0))
    } else {
        None
    }
}

/// `shift_jis_20042euc_jis_2004` (euc2004_sjis2004.c) — SHIFT_JIS_2004 ->
/// EUC_JIS_2004.
fn shift_jis_20042euc_jis_2004(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < src.len() {
        let c1 = src[pos];

        if !is_highbit_set(c1) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..])?;
            }
            dest.push(c1);
            pos += 1;
            continue;
        }

        let l = pg_encoding_verifymbchar(PG_SHIFT_JIS_2004, &src[pos..]);

        if l < 0 || (l as usize) > src.len() - pos {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..])?;
        }
        let l = l as usize;

        if (0xa1..=0xdf).contains(&c1) && l == 1 {
            /* JIS X0201 (1 byte kana) */
            dest.push(SS2);
            dest.push(c1);
        } else if l == 2 {
            let c1 = c1 as i32;
            let c2 = src[pos + 1] as i32;

            let mut plane = 1;
            // C initializes `ku = 1; ten = 1;` defensively, but every reachable
            // branch below assigns both before they are read (the error paths
            // diverge), so we leave them unbound and rely on definite-assignment.
            let mut ku;
            let ten;

            /*
             * JIS X 0213
             */
            if (0x81..=0x9f).contains(&c1) {
                /* plane 1 1ku-62ku */
                ku = (c1 << 1) - 0x100;
                match get_ten(c2) {
                    Some((t, kubun)) => {
                        ten = t;
                        ku -= kubun;
                    }
                    None => {
                        if no_error {
                            break;
                        }
                        report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..])?;
                        unreachable!();
                    }
                }
            } else if (0xe0..=0xef).contains(&c1) {
                /* plane 1 62ku-94ku */
                ku = (c1 << 1) - 0x180;
                match get_ten(c2) {
                    Some((t, kubun)) => {
                        ten = t;
                        ku -= kubun;
                    }
                    None => {
                        if no_error {
                            break;
                        }
                        report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..])?;
                        unreachable!();
                    }
                }
            } else if (0xf0..=0xf3).contains(&c1) {
                /* plane 2 1,3,4,5,8,12,13,14,15 ku */
                plane = 2;
                match get_ten(c2) {
                    Some((t, kubun)) => {
                        ten = t;
                        ku = match c1 {
                            0xf0 => {
                                if kubun == 0 {
                                    8
                                } else {
                                    1
                                }
                            }
                            0xf1 => {
                                if kubun == 0 {
                                    4
                                } else {
                                    3
                                }
                            }
                            0xf2 => {
                                if kubun == 0 {
                                    12
                                } else {
                                    5
                                }
                            }
                            _ => {
                                if kubun == 0 {
                                    14
                                } else {
                                    13
                                }
                            }
                        };
                    }
                    None => {
                        if no_error {
                            break;
                        }
                        report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..])?;
                        unreachable!();
                    }
                }
            } else if (0xf4..=0xfc).contains(&c1) {
                /* plane 2 78-94ku */
                plane = 2;
                match get_ten(c2) {
                    Some((t, kubun)) => {
                        ten = t;
                        ku = if c1 == 0xf4 && kubun == 1 {
                            15
                        } else {
                            (c1 << 1) - 0x19a - kubun
                        };
                    }
                    None => {
                        if no_error {
                            break;
                        }
                        report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..])?;
                        unreachable!();
                    }
                }
            } else {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_SHIFT_JIS_2004, &src[pos..])?;
                unreachable!();
            }

            if plane == 2 {
                dest.push(SS3);
            }

            dest.push((ku + 0xa0) as u8);
            dest.push((ten + 0xa0) as u8);
        }
        /*
         * Note: like the C, a high-bit-set lead byte that is neither the
         * 1-byte kana range nor a 2-byte character (l != 1 && l != 2) falls
         * through without emitting anything; the verifymbchar check above has
         * already accepted it, so this simply advances past it.
         */

        pos += l;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

#[cfg(test)]
mod tests;
