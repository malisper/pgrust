//! `euc_tw_and_big5.c` + `big5.c` — conversions among `EUC_TW`, `BIG5` and
//! `MULE_INTERNAL`.
//!
//! 1:1 port of
//! `src/backend/utils/mb/conversion_procs/euc_tw_and_big5/euc_tw_and_big5.c`
//! together with the `big5.c` helper (BIG5 <-> CNS 11643 plane mapping tables
//! and the `BIG5toCNS` / `CNStoBIG5` / `BinarySearchRange` lookups).
//!
//! The six C `PG_FUNCTION_ARGS` entry points (`euc_tw_to_big5`,
//! `big5_to_euc_tw`, `euc_tw_to_mic`, `mic_to_euc_tw`, `big5_to_mic`,
//! `mic_to_big5`) become plain Rust functions over `&[u8]`. Each validates its
//! source/dest encodings with the faithful
//! [`check_encoding_conversion_args`] (the C `CHECK_ENCODING_CONVERSION_ARGS`
//! macro) and then runs the corresponding C conversion loop, returning a
//! [`ConversionResult`] (the produced bytes plus the number of source bytes
//! successfully consumed — the C `src - start` return value).
//!
//! Unlike the UTF-8 radix-tree conversions, these encodings convert via the CNS
//! 11643 plane numbers and the `MULE_INTERNAL` intermediate representation; the
//! per-plane mapping tables (`big5.c`) are reproduced verbatim in [`tables`].

#![allow(non_snake_case)]

mod tables;

use backend_utils_error::PgResult;
use backend_utils_mb::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use backend_utils_mb_conv_string_helpers::ConversionResult;
use common_wchar::pg_encoding_verifymbchar;
use tables::CodePair;
use types_wchar::encoding::{pg_enc, PG_BIG5, PG_EUC_TW, PG_MULE_INTERNAL};

/// Convention no-op: this crate installs no inward seams.
pub fn init_seams() {}

// ============================================================================
// Constants from mb/pg_wchar.h (values confirmed against
// pgrust-pg-ffi-fgram/src/wchar.rs).
// ============================================================================

/// `SS2` (mb/pg_wchar.h) — EUC single-shift 2, introduces CNS planes >= 2.
const SS2: u8 = 0x8e;

const LC_CNS11643_1: u8 = 0x95;
const LC_CNS11643_2: u8 = 0x96;
/// `LCPRV2_B` — MULE private-charset (2-byte) lead-charset escape.
const LCPRV2_B: u8 = 0x9d;
const LC_CNS11643_3: u8 = 0xf6;
const LC_CNS11643_4: u8 = 0xf7;
#[allow(dead_code)]
const LC_CNS11643_5: u8 = 0xf8;
#[allow(dead_code)]
const LC_CNS11643_6: u8 = 0xf9;
const LC_CNS11643_7: u8 = 0xfa;

/// `HIGHBIT` / `IS_HIGHBIT_SET` (mb/pg_wchar.h).
const HIGHBIT: u8 = 0x80;

#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & HIGHBIT != 0
}

// ============================================================================
// The six SQL conversion procs (euc_tw_and_big5.c entry points).
// ============================================================================

/// `euc_tw_to_big5` (euc_tw_and_big5.c).
pub fn euc_tw_to_big5(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_EUC_TW,
        PG_BIG5,
    )?;
    euc_tw2big5(src, no_error)
}

/// `big5_to_euc_tw` (euc_tw_and_big5.c).
pub fn big5_to_euc_tw(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_BIG5,
        PG_EUC_TW,
    )?;
    big52euc_tw(src, no_error)
}

/// `euc_tw_to_mic` (euc_tw_and_big5.c).
pub fn euc_tw_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_EUC_TW,
        PG_MULE_INTERNAL,
    )?;
    euc_tw2mic(src, no_error)
}

/// `mic_to_euc_tw` (euc_tw_and_big5.c).
pub fn mic_to_euc_tw(
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
        PG_EUC_TW,
    )?;
    mic2euc_tw(src, no_error)
}

/// `big5_to_mic` (euc_tw_and_big5.c).
pub fn big5_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_BIG5,
        PG_MULE_INTERNAL,
    )?;
    big52mic(src, no_error)
}

/// `mic_to_big5` (euc_tw_and_big5.c).
pub fn mic_to_big5(
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
        PG_BIG5,
    )?;
    mic2big5(src, no_error)
}

// ============================================================================
// The six conversion loops (1:1 with euc_tw_and_big5.c). The C functions write
// into a caller-supplied dest buffer with a trailing '\0'; here we accumulate
// into a Vec<u8> (the string-helpers protocol omits the NUL sentinel) and
// return `src - start` via ConversionResult.converted.
// ============================================================================

/// `euc_tw2big5` (euc_tw_and_big5.c) — EUC_TW -> Big5.
fn euc_tw2big5(euc: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = euc.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        let c1 = euc[pos];
        if is_highbit_set(c1) {
            // Verify and decode the next EUC_TW input character
            let l = pg_encoding_verifymbchar(PG_EUC_TW, &euc[pos..]);
            if l < 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_TW, &euc[pos..])?;
            }
            let l = l as usize;

            let lc;
            let cns_buf: u16;
            if c1 == SS2 {
                let plane = euc[pos + 1]; /* plane No. */
                if plane == 0xa1 {
                    lc = LC_CNS11643_1;
                } else if plane == 0xa2 {
                    lc = LC_CNS11643_2;
                } else {
                    lc = plane - 0xa3 + LC_CNS11643_3;
                }
                cns_buf = ((euc[pos + 2] as u16) << 8) | euc[pos + 3] as u16;
            } else {
                /* CNS11643-1 */
                lc = LC_CNS11643_1;
                cns_buf = ((c1 as u16) << 8) | euc[pos + 1] as u16;
            }

            /* Write it out in Big5 */
            let big5_buf = CNStoBIG5(cns_buf, lc);
            if big5_buf == 0 {
                if no_error {
                    break;
                }
                report_untranslatable_char(PG_EUC_TW, PG_BIG5, &euc[pos..])?;
            }
            p.push(((big5_buf >> 8) & 0x00ff) as u8);
            p.push((big5_buf & 0x00ff) as u8);

            pos += l;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_TW, &euc[pos..])?;
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

/// `big52euc_tw` (euc_tw_and_big5.c) — Big5 -> EUC_TW.
fn big52euc_tw(big5: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = big5.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        /* Verify and decode the next Big5 input character */
        let c1 = big5[pos];
        if is_highbit_set(c1) {
            let l = pg_encoding_verifymbchar(PG_BIG5, &big5[pos..]);
            if l < 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_BIG5, &big5[pos..])?;
            }
            let l = l as usize;

            let big5_buf = ((c1 as u16) << 8) | big5[pos + 1] as u16;
            let mut lc: u8 = 0;
            let cns_buf = BIG5toCNS(big5_buf, &mut lc);

            if lc == LC_CNS11643_1 {
                p.push(((cns_buf >> 8) & 0x00ff) as u8);
                p.push((cns_buf & 0x00ff) as u8);
            } else if lc == LC_CNS11643_2 {
                p.push(SS2);
                p.push(0xa2);
                p.push(((cns_buf >> 8) & 0x00ff) as u8);
                p.push((cns_buf & 0x00ff) as u8);
            } else if lc >= LC_CNS11643_3 && lc <= LC_CNS11643_7 {
                p.push(SS2);
                p.push(lc - LC_CNS11643_3 + 0xa3);
                p.push(((cns_buf >> 8) & 0x00ff) as u8);
                p.push((cns_buf & 0x00ff) as u8);
            } else {
                if no_error {
                    break;
                }
                report_untranslatable_char(PG_BIG5, PG_EUC_TW, &big5[pos..])?;
            }

            pos += l;
        } else {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_BIG5, &big5[pos..])?;
            }
            p.push(c1);
            pos += 1;
            continue;
        }
    }

    Ok(ConversionResult {
        bytes: p,
        converted: pos as i32,
    })
}

/// `euc_tw2mic` (euc_tw_and_big5.c) — EUC_TW -> MIC.
fn euc_tw2mic(euc: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = euc.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        let c1 = euc[pos];
        if is_highbit_set(c1) {
            let l = pg_encoding_verifymbchar(PG_EUC_TW, &euc[pos..]);
            if l < 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_TW, &euc[pos..])?;
            }
            let l = l as usize;

            if c1 == SS2 {
                let plane = euc[pos + 1]; /* plane No. */
                if plane == 0xa1 {
                    p.push(LC_CNS11643_1);
                } else if plane == 0xa2 {
                    p.push(LC_CNS11643_2);
                } else {
                    /* other planes are MULE private charsets */
                    p.push(LCPRV2_B);
                    p.push(plane - 0xa3 + LC_CNS11643_3);
                }
                p.push(euc[pos + 2]);
                p.push(euc[pos + 3]);
            } else {
                /* CNS11643-1 */
                p.push(LC_CNS11643_1);
                p.push(c1);
                p.push(euc[pos + 1]);
            }
            pos += l;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_TW, &euc[pos..])?;
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

/// `mic2euc_tw` (euc_tw_and_big5.c) — MIC -> EUC_TW.
fn mic2euc_tw(mic: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = mic.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        let c1 = mic[pos];
        if !is_highbit_set(c1) {
            /* ASCII */
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
        let l = l as usize;

        if c1 == LC_CNS11643_1 {
            p.push(mic[pos + 1]);
            p.push(mic[pos + 2]);
        } else if c1 == LC_CNS11643_2 {
            p.push(SS2);
            p.push(0xa2);
            p.push(mic[pos + 1]);
            p.push(mic[pos + 2]);
        } else if c1 == LCPRV2_B && mic[pos + 1] >= LC_CNS11643_3 && mic[pos + 1] <= LC_CNS11643_7 {
            p.push(SS2);
            p.push(mic[pos + 1] - LC_CNS11643_3 + 0xa3);
            p.push(mic[pos + 2]);
            p.push(mic[pos + 3]);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(PG_MULE_INTERNAL, PG_EUC_TW, &mic[pos..])?;
        }
        pos += l;
    }

    Ok(ConversionResult {
        bytes: p,
        converted: pos as i32,
    })
}

/// `big52mic` (euc_tw_and_big5.c) — Big5 -> MIC.
fn big52mic(big5: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = big5.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        let c1 = big5[pos];
        if !is_highbit_set(c1) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_BIG5, &big5[pos..])?;
            }
            p.push(c1);
            pos += 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_BIG5, &big5[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_BIG5, &big5[pos..])?;
        }
        let l = l as usize;

        let big5_buf = ((c1 as u16) << 8) | big5[pos + 1] as u16;
        let mut lc: u8 = 0;
        let cns_buf = BIG5toCNS(big5_buf, &mut lc);
        if lc != 0 {
            /* Planes 3 and 4 are MULE private charsets */
            if lc == LC_CNS11643_3 || lc == LC_CNS11643_4 {
                p.push(LCPRV2_B);
            }
            p.push(lc); /* Plane No. */
            p.push(((cns_buf >> 8) & 0x00ff) as u8);
            p.push((cns_buf & 0x00ff) as u8);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(PG_BIG5, PG_MULE_INTERNAL, &big5[pos..])?;
        }
        pos += l;
    }

    Ok(ConversionResult {
        bytes: p,
        converted: pos as i32,
    })
}

/// `mic2big5` (euc_tw_and_big5.c) — MIC -> Big5.
fn mic2big5(mic: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let len = mic.len();
    let mut p: Vec<u8> = Vec::new();
    let mut pos = 0;

    while pos < len {
        let c1 = mic[pos];
        if !is_highbit_set(c1) {
            /* ASCII */
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
        let l = l as usize;

        if c1 == LC_CNS11643_1 || c1 == LC_CNS11643_2 || c1 == LCPRV2_B {
            let plane: u8;
            let cns_buf: u16;
            if c1 == LCPRV2_B {
                plane = mic[pos + 1]; /* get plane no. */
                cns_buf = ((mic[pos + 2] as u16) << 8) | mic[pos + 3] as u16;
            } else {
                plane = c1;
                cns_buf = ((mic[pos + 1] as u16) << 8) | mic[pos + 2] as u16;
            }
            let big5_buf = CNStoBIG5(cns_buf, plane);
            if big5_buf == 0 {
                if no_error {
                    break;
                }
                report_untranslatable_char(PG_MULE_INTERNAL, PG_BIG5, &mic[pos..])?;
            }
            p.push(((big5_buf >> 8) & 0x00ff) as u8);
            p.push((big5_buf & 0x00ff) as u8);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(PG_MULE_INTERNAL, PG_BIG5, &mic[pos..])?;
        }
        pos += l;
    }

    Ok(ConversionResult {
        bytes: p,
        converted: pos as i32,
    })
}

// ============================================================================
// big5.c — BIG5 <-> CNS 11643 mapping helpers (1:1 port).
// ============================================================================

/// `BinarySearchRange` (big5.c) — locate `code` within the range table and
/// interpolate the peer code point. Returns 0 for "no mapping".
fn BinarySearchRange(array: &[CodePair], high: usize, code: u16) -> u16 {
    let mut low: usize = 0;
    let mut high = high;
    let mut mid = high >> 1;

    loop {
        if low > high {
            break;
        }

        if array[mid].code <= code && array[mid + 1].code > code {
            if array[mid].peer == 0 {
                return 0;
            }
            if code >= 0xa140 {
                /* big5 to cns */
                let tmp_row = (((code & 0xff00) as i32 - (array[mid].code & 0xff00) as i32) >> 8) as i32;
                let high_b = (code & 0x00ff) as i32;
                let low_b = (array[mid].code & 0x00ff) as i32;

                let distance = tmp_row * 0x9d + high_b - low_b
                    + if high_b >= 0xa1 {
                        if low_b >= 0xa1 {
                            0
                        } else {
                            -0x22
                        }
                    } else if low_b >= 0xa1 {
                        0x22
                    } else {
                        0
                    };

                let tmp = (array[mid].peer & 0x00ff) as i32 + distance - 0x21;
                let tmp = (array[mid].peer & 0xff00) as i32 + ((tmp / 0x5e) << 8) + 0x21 + tmp % 0x5e;
                return tmp as u16;
            } else {
                /* cns to big5 */
                let tmp_row = (((code & 0xff00) as i32 - (array[mid].code & 0xff00) as i32) >> 8) as i32;

                let distance =
                    tmp_row * 0x5e + ((code & 0x00ff) as i32 - (array[mid].code & 0x00ff) as i32);

                let low_b = (array[mid].peer & 0x00ff) as i32;
                let tmp = low_b + distance - if low_b >= 0xa1 { 0x62 } else { 0x40 };
                let low_b = tmp % 0x9d;
                let tmp = (array[mid].peer & 0xff00) as i32
                    + ((tmp / 0x9d) << 8)
                    + (if low_b > 0x3e { 0x62 } else { 0x40 })
                    + low_b;
                return tmp as u16;
            }
        } else if array[mid].code > code {
            // C: high = mid - 1; with `int` high, mid==0 makes high = -1 and the
            // `low <= high` test (0 <= -1) ends the loop. Mirror with a guard.
            if mid == 0 {
                break;
            }
            high = mid - 1;
        } else {
            low = mid + 1;
        }

        // C reinit step: mid = (low + high) >> 1
        mid = (low + high) >> 1;
    }

    0
}

/// `BIG5toCNS` (big5.c) — map a Big5 code to its CNS 11643 code, setting `*lc`
/// to the CNS plane. Returns `'?'` (and `*lc = 0`) when there is no mapping.
fn BIG5toCNS(big5: u16, lc: &mut u8) -> u16 {
    // C `unsigned short cns = 0;` — the 0xc94a branch overwrites without reading
    // the init, but the level-1/level-2 fallthrough paths read it after the
    // BinarySearchRange call, so the initializer is required.
    #[allow(unused_assignments)]
    let mut cns: u16 = 0;

    if big5 < 0xc940 {
        /* level 1 */
        for entry in tables::B1C4.iter() {
            if entry.0 == big5 {
                *lc = LC_CNS11643_4;
                return entry.1 | 0x8080;
            }
        }

        cns = BinarySearchRange(&tables::BIG5LEVEL1TOCNSPLANE1, 23, big5);
        if cns > 0 {
            *lc = LC_CNS11643_1;
        }
    } else if big5 == 0xc94a {
        /* level 2 */
        *lc = LC_CNS11643_1;
        cns = 0x4442;
    } else {
        /* level 2 */
        for entry in tables::B2C3.iter() {
            if entry.0 == big5 {
                *lc = LC_CNS11643_3;
                return entry.1 | 0x8080;
            }
        }

        cns = BinarySearchRange(&tables::BIG5LEVEL2TOCNSPLANE2, 46, big5);
        if cns > 0 {
            *lc = LC_CNS11643_2;
        }
    }

    if cns == 0 {
        /* no mapping Big5 to CNS 11643-1992 */
        *lc = 0;
        return b'?' as u16;
    }

    cns | 0x8080
}

/// `CNStoBIG5` (big5.c) — map a CNS 11643 code (with plane `lc`) to its Big5
/// code. Returns 0 when there is no mapping.
fn CNStoBIG5(cns: u16, lc: u8) -> u16 {
    let cns = cns & 0x7f7f;
    let mut big5: u16 = 0;

    match lc {
        LC_CNS11643_1 => {
            big5 = BinarySearchRange(&tables::CNSPLANE1TOBIG5LEVEL1, 24, cns);
        }
        LC_CNS11643_2 => {
            big5 = BinarySearchRange(&tables::CNSPLANE2TOBIG5LEVEL2, 47, cns);
        }
        LC_CNS11643_3 => {
            for entry in tables::B2C3.iter() {
                if entry.1 == cns {
                    return entry.0;
                }
            }
        }
        LC_CNS11643_4 => {
            for entry in tables::B1C4.iter() {
                if entry.1 == cns {
                    return entry.0;
                }
            }
            // C falls through to default here (no break before default).
        }
        _ => {}
    }
    big5
}

#[cfg(test)]
mod tests;
