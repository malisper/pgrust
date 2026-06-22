//! `euc_jp_and_sjis.c` — EUC_JP <-> SJIS <-> MULE_INTERNAL conversion procs.
//!
//! 1:1 port of
//! `src/backend/utils/mb/conversion_procs/euc_jp_and_sjis/euc_jp_and_sjis.c`.
//!
//! The six C `PG_FUNCTION_ARGS` entrypoints (`euc_jp_to_sjis`, `sjis_to_euc_jp`,
//! `euc_jp_to_mic`, `mic_to_euc_jp`, `sjis_to_mic`, `mic_to_sjis`) become plain
//! Rust functions over `&[u8]`. Each validates its (source, dest) encoding pair
//! via `CHECK_ENCODING_CONVERSION_ARGS` and then runs the file-local byte loops
//! (`euc_jp2sjis`, `sjis2euc_jp`, `euc_jp2mic`, `mic2euc_jp`, `sjis2mic`,
//! `mic2sjis`) plus the IBM-Kanji fixup table in [`tables`].
//!
//! These are pure byte-level algorithms; SJIS<->EUC pairs convert directly, and
//! the MULE_INTERNAL conversions tag each char with its `LC_*` charset leading
//! byte. EUC_JP uses the SS2/SS3 single-shift bytes (`SS2` for JIS X0201 kana,
//! `SS3` for JIS X0212 kanji); the MULE form uses `LC_JISX0201K` / `LC_JISX0208`
//! / `LC_JISX0212` leading bytes instead.
//!
//! The C writes into a caller-supplied NUL-terminated `dest`; this port returns
//! the produced bytes in a [`ConversionResult`] `Vec<u8>` (the trailing C
//! `*p = '\0'` terminator is not part of the byte payload), along with the
//! number of source bytes consumed (`sjis - start` / `mic - start` /
//! `euc - start`).

use backend_utils_error::PgResult;
use backend_utils_mb::{
    check_encoding_conversion_args, report_invalid_encoding, report_untranslatable_char,
};
use backend_utils_mb_conv_string_helpers::ConversionResult;
use backend_utils_mb_conv_string_helpers::make_conversion_builtin;
use common_wchar::pg_encoding_verifymbchar;
use types_wchar::encoding::{pg_enc, PG_EUC_JP, PG_MULE_INTERNAL, PG_SJIS};

mod tables;
use tables::IBMKANJI;

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

fn adapt_euc_jp_to_sjis(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(euc_jp_to_sjis(src_encoding, dest_encoding, src, no_error))
}

fn adapt_sjis_to_euc_jp(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(sjis_to_euc_jp(src_encoding, dest_encoding, src, no_error))
}

fn adapt_euc_jp_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(euc_jp_to_mic(src_encoding, dest_encoding, src, no_error))
}

fn adapt_sjis_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(sjis_to_mic(src_encoding, dest_encoding, src, no_error))
}

fn adapt_mic_to_euc_jp(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(mic_to_euc_jp(src_encoding, dest_encoding, src, no_error))
}

fn adapt_mic_to_sjis(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> types_error_real::PgResult<ConversionResult> {
    into_real(mic_to_sjis(src_encoding, dest_encoding, src, no_error))
}

/// Register the ported conversion procedures as fmgr builtins so
/// `fmgr_info` resolves their proc OIDs to the in-process Rust bodies
/// instead of `dlopen`ing `$libdir/euc_jp_and_sjis`.
pub fn init_seams() {
    backend_utils_fmgr_core::register_builtins_native([
        make_conversion_builtin(4324, "euc_jp_to_sjis", adapt_euc_jp_to_sjis),
        make_conversion_builtin(4325, "sjis_to_euc_jp", adapt_sjis_to_euc_jp),
        make_conversion_builtin(4326, "euc_jp_to_mic", adapt_euc_jp_to_mic),
        make_conversion_builtin(4327, "sjis_to_mic", adapt_sjis_to_mic),
        make_conversion_builtin(4328, "mic_to_euc_jp", adapt_mic_to_euc_jp),
        make_conversion_builtin(4329, "mic_to_sjis", adapt_mic_to_sjis),
    ]);
}

// ---------------------------------------------------------------------------
// Constants from euc_jp_and_sjis.c and mb/pg_wchar.h.
// ---------------------------------------------------------------------------

/// SJIS alternative code, used when a mapping EUC -> SJIS is not defined.
const PGSJISALTCODE: i32 = 0x81ac;
/// EUC alternative code, used when a mapping SJIS -> EUC is not defined.
const PGEUCALTCODE: i32 = 0xa2ae;

/// `HIGHBIT` / `IS_HIGHBIT_SET` (mb/pg_wchar.h).
const HIGHBIT: u8 = 0x80;

/// `SS2` / `SS3` single-shift bytes (mb/pg_wchar.h). Values verified against
/// pgrust-pg-ffi-fgram.
const SS2: u8 = 0x8e;
const SS3: u8 = 0x8f;

/// MULE_INTERNAL charset leading bytes (mb/pg_wchar.h). Values verified against
/// pgrust-pg-ffi-fgram.
const LC_JISX0201K: u8 = 0x89;
const LC_JISX0208: u8 = 0x92;
const LC_JISX0212: u8 = 0x94;

#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & HIGHBIT != 0
}

/// `ISSJISHEAD` (euc_jp_and_sjis.c is implicit via pg_sjis_verifier; here the
/// inline C checks in `sjis2mic`).
#[inline]
fn is_sjis_head(c: i32) -> bool {
    (0x81..=0x9f).contains(&c) || (0xe0..=0xfc).contains(&c)
}

/// `ISSJISTAIL`.
#[inline]
fn is_sjis_tail(c: i32) -> bool {
    (0x40..=0x7e).contains(&c) || (0x80..=0xfc).contains(&c)
}

// ---------------------------------------------------------------------------
// The six SQL-callable conversion procs.
// ---------------------------------------------------------------------------

/// `euc_jp_to_sjis` (euc_jp_and_sjis.c) — SQL conversion proc EUC_JP -> SJIS.
pub fn euc_jp_to_sjis(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_EUC_JP,
        PG_SJIS,
    )?;

    euc_jp2sjis(src, no_error)
}

/// `sjis_to_euc_jp` (euc_jp_and_sjis.c) — SQL conversion proc SJIS -> EUC_JP.
pub fn sjis_to_euc_jp(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_SJIS,
        PG_EUC_JP,
    )?;

    sjis2euc_jp(src, no_error)
}

/// `euc_jp_to_mic` (euc_jp_and_sjis.c) — SQL conversion proc EUC_JP -> MIC.
pub fn euc_jp_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_EUC_JP,
        PG_MULE_INTERNAL,
    )?;

    euc_jp2mic(src, no_error)
}

/// `mic_to_euc_jp` (euc_jp_and_sjis.c) — SQL conversion proc MIC -> EUC_JP.
pub fn mic_to_euc_jp(
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
        PG_EUC_JP,
    )?;

    mic2euc_jp(src, no_error)
}

/// `sjis_to_mic` (euc_jp_and_sjis.c) — SQL conversion proc SJIS -> MIC.
pub fn sjis_to_mic(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_SJIS,
        PG_MULE_INTERNAL,
    )?;

    sjis2mic(src, no_error)
}

/// `mic_to_sjis` (euc_jp_and_sjis.c) — SQL conversion proc MIC -> SJIS.
pub fn mic_to_sjis(
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
        PG_SJIS,
    )?;

    mic2sjis(src, no_error)
}

// ---------------------------------------------------------------------------
// SJIS ---> MIC
// ---------------------------------------------------------------------------
fn sjis2mic(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;
    let mut len = src.len() as i32;

    while len > 0 {
        let mut c1 = src[pos] as i32;
        if (0xa1..=0xdf).contains(&c1) {
            /* JIS X0201 (1 byte kana) */
            dest.push(LC_JISX0201K);
            dest.push(c1 as u8);
            pos += 1;
            len -= 1;
        } else if is_highbit_set(c1 as u8) {
            /*
             * JIS X0208, X0212, user defined extended characters
             */
            if len < 2 || !is_sjis_head(c1) || !is_sjis_tail(src[pos + 1] as i32) {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_SJIS, &src[pos..])?;
            }
            let mut c2 = src[pos + 1] as i32;
            let mut k = (c1 << 8) + c2;
            if (0xed40..0xf040).contains(&k) {
                /* NEC selection IBM kanji */
                let mut i = 0;
                loop {
                    let k2 = IBMKANJI[i].nec as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].sjis as i32;
                        c1 = (k >> 8) & 0xff;
                        c2 = k & 0xff;
                    }
                    i += 1;
                }
            }

            if k < 0xeb3f {
                /* JIS X0208 */
                dest.push(LC_JISX0208);
                dest.push((((c1 & 0x3f) << 1) + 0x9f + i32::from(c2 > 0x9e)) as u8);
                dest.push((c2 + if c2 > 0x9e { 2 } else { 0x60 } + i32::from(c2 < 0x80)) as u8);
            } else if (0xeb40..0xf040).contains(&k) || (0xfc4c..=0xfcfc).contains(&k) {
                /* NEC selection IBM kanji - Other undecided justice */
                dest.push(LC_JISX0208);
                dest.push((PGEUCALTCODE >> 8) as u8);
                dest.push((PGEUCALTCODE & 0xff) as u8);
            } else if (0xf040..0xf540).contains(&k) {
                /*
                 * UDC1 mapping to X0208 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0xf5a1 - 0xfefe
                 */
                dest.push(LC_JISX0208);
                c1 -= 0x6f;
                dest.push((((c1 & 0x3f) << 1) + 0xf3 + i32::from(c2 > 0x9e)) as u8);
                dest.push((c2 + if c2 > 0x9e { 2 } else { 0x60 } + i32::from(c2 < 0x80)) as u8);
            } else if (0xf540..0xfa40).contains(&k) {
                /*
                 * UDC2 mapping to X0212 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0x8ff5a1 - 0x8ffefe
                 */
                dest.push(LC_JISX0212);
                c1 -= 0x74;
                dest.push((((c1 & 0x3f) << 1) + 0xf3 + i32::from(c2 > 0x9e)) as u8);
                dest.push((c2 + if c2 > 0x9e { 2 } else { 0x60 } + i32::from(c2 < 0x80)) as u8);
            } else if k >= 0xfa40 {
                /*
                 * mapping IBM kanji to X0208 and X0212
                 */
                let mut i = 0;
                loop {
                    let k2 = IBMKANJI[i].sjis as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        let euc = IBMKANJI[i].euc;
                        if euc >= 0x8f0000 {
                            dest.push(LC_JISX0212);
                            dest.push((0x80 | ((euc & 0xff00) >> 8)) as u8);
                            dest.push((0x80 | (euc & 0xff)) as u8);
                        } else {
                            dest.push(LC_JISX0208);
                            dest.push((0x80 | (euc >> 8)) as u8);
                            dest.push((0x80 | (euc & 0xff)) as u8);
                        }
                    }
                    i += 1;
                }
            }
            pos += 2;
            len -= 2;
        } else {
            /* should be ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_SJIS, &src[pos..])?;
            }
            dest.push(c1 as u8);
            pos += 1;
            len -= 1;
        }
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

// ---------------------------------------------------------------------------
// MIC ---> SJIS
// ---------------------------------------------------------------------------
fn mic2sjis(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;
    let mut len = src.len() as i32;

    while len > 0 {
        let c1 = src[pos] as i32;
        if !is_highbit_set(c1 as u8) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..])?;
            }
            dest.push(c1 as u8);
            pos += 1;
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_MULE_INTERNAL, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..])?;
        }
        if c1 == LC_JISX0201K as i32 {
            dest.push(src[pos + 1]);
        } else if c1 == LC_JISX0208 as i32 {
            let mut c1 = src[pos + 1] as i32;
            let c2 = src[pos + 2] as i32;
            let k = (c1 << 8) | (c2 & 0xff);
            if k >= 0xf5a1 {
                /* UDC1 */
                c1 -= 0x54;
                dest.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x6f) as u8);
            } else {
                dest.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 }) as u8);
            }
            dest.push(
                (c2 - if c1 & 1 != 0 {
                    if c2 < 0xe0 {
                        0x61
                    } else {
                        0x60
                    }
                } else {
                    2
                }) as u8,
            );
        } else if c1 == LC_JISX0212 as i32 {
            let mut c1 = src[pos + 1] as i32;
            let c2 = src[pos + 2] as i32;
            let k = (c1 << 8) | c2;
            if k >= 0xf5a1 {
                /* UDC2 */
                c1 -= 0x54;
                dest.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x74) as u8);
                dest.push(
                    (c2 - if c1 & 1 != 0 {
                        if c2 < 0xe0 {
                            0x61
                        } else {
                            0x60
                        }
                    } else {
                        2
                    }) as u8,
                );
            } else {
                /* IBM kanji */
                let mut i = 0;
                loop {
                    let k2 = (IBMKANJI[i].euc & 0xffff) as i32;
                    if k2 == 0xffff {
                        dest.push((PGSJISALTCODE >> 8) as u8);
                        dest.push((PGSJISALTCODE & 0xff) as u8);
                        break;
                    }
                    if k2 == k {
                        let k = IBMKANJI[i].sjis as i32;
                        dest.push((k >> 8) as u8);
                        dest.push((k & 0xff) as u8);
                        break;
                    }
                    i += 1;
                }
            }
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(PG_MULE_INTERNAL, PG_SJIS, &src[pos..])?;
        }
        pos += l as usize;
        len -= l;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

// ---------------------------------------------------------------------------
// EUC_JP ---> MIC
// ---------------------------------------------------------------------------
fn euc_jp2mic(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;
    let mut len = src.len() as i32;

    while len > 0 {
        let c1 = src[pos] as i32;
        if !is_highbit_set(c1 as u8) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_JP, &src[pos..])?;
            }
            dest.push(c1 as u8);
            pos += 1;
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_EUC_JP, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_EUC_JP, &src[pos..])?;
        }
        if c1 == SS2 as i32 {
            /* 1 byte kana? */
            dest.push(LC_JISX0201K);
            dest.push(src[pos + 1]);
        } else if c1 == SS3 as i32 {
            /* JIS X0212 kanji? */
            dest.push(LC_JISX0212);
            dest.push(src[pos + 1]);
            dest.push(src[pos + 2]);
        } else {
            /* kanji? */
            dest.push(LC_JISX0208);
            dest.push(c1 as u8);
            dest.push(src[pos + 1]);
        }
        pos += l as usize;
        len -= l;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

// ---------------------------------------------------------------------------
// MIC ---> EUC_JP
// ---------------------------------------------------------------------------
fn mic2euc_jp(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;
    let mut len = src.len() as i32;

    while len > 0 {
        let c1 = src[pos] as i32;
        if !is_highbit_set(c1 as u8) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..])?;
            }
            dest.push(c1 as u8);
            pos += 1;
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_MULE_INTERNAL, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_MULE_INTERNAL, &src[pos..])?;
        }
        if c1 == LC_JISX0201K as i32 {
            dest.push(SS2);
            dest.push(src[pos + 1]);
        } else if c1 == LC_JISX0212 as i32 {
            dest.push(SS3);
            dest.push(src[pos + 1]);
            dest.push(src[pos + 2]);
        } else if c1 == LC_JISX0208 as i32 {
            dest.push(src[pos + 1]);
            dest.push(src[pos + 2]);
        } else {
            if no_error {
                break;
            }
            report_untranslatable_char(PG_MULE_INTERNAL, PG_EUC_JP, &src[pos..])?;
        }
        pos += l as usize;
        len -= l;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

// ---------------------------------------------------------------------------
// EUC_JP -> SJIS
// ---------------------------------------------------------------------------
fn euc_jp2sjis(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;
    let mut len = src.len() as i32;

    while len > 0 {
        let c1 = src[pos] as i32;
        if !is_highbit_set(c1 as u8) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_EUC_JP, &src[pos..])?;
            }
            dest.push(c1 as u8);
            pos += 1;
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_EUC_JP, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_EUC_JP, &src[pos..])?;
        }
        if c1 == SS2 as i32 {
            /* hankaku kana? */
            dest.push(src[pos + 1]);
        } else if c1 == SS3 as i32 {
            /* JIS X0212 kanji? */
            let mut c1 = src[pos + 1] as i32;
            let c2 = src[pos + 2] as i32;
            let k = c1 << 8 | c2;
            if k >= 0xf5a1 {
                /* UDC2 */
                c1 -= 0x54;
                dest.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x74) as u8);
                dest.push(
                    (c2 - if c1 & 1 != 0 {
                        if c2 < 0xe0 {
                            0x61
                        } else {
                            0x60
                        }
                    } else {
                        2
                    }) as u8,
                );
            } else {
                /* IBM kanji */
                let mut i = 0;
                loop {
                    let k2 = (IBMKANJI[i].euc & 0xffff) as i32;
                    if k2 == 0xffff {
                        dest.push((PGSJISALTCODE >> 8) as u8);
                        dest.push((PGSJISALTCODE & 0xff) as u8);
                        break;
                    }
                    if k2 == k {
                        let k = IBMKANJI[i].sjis as i32;
                        dest.push((k >> 8) as u8);
                        dest.push((k & 0xff) as u8);
                        break;
                    }
                    i += 1;
                }
            }
        } else {
            /* JIS X0208 kanji? */
            let mut c1 = c1;
            let c2 = src[pos + 1] as i32;
            let k = (c1 << 8) | (c2 & 0xff);
            if k >= 0xf5a1 {
                /* UDC1 */
                c1 -= 0x54;
                dest.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 } + 0x6f) as u8);
            } else {
                dest.push((((c1 - 0xa1) >> 1) + if c1 < 0xdf { 0x81 } else { 0xc1 }) as u8);
            }
            dest.push(
                (c2 - if c1 & 1 != 0 {
                    if c2 < 0xe0 {
                        0x61
                    } else {
                        0x60
                    }
                } else {
                    2
                }) as u8,
            );
        }
        pos += l as usize;
        len -= l;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

// ---------------------------------------------------------------------------
// SJIS ---> EUC_JP
// ---------------------------------------------------------------------------
fn sjis2euc_jp(src: &[u8], no_error: bool) -> PgResult<ConversionResult> {
    let mut dest: Vec<u8> = Vec::new();
    let mut pos = 0;
    let mut len = src.len() as i32;

    while len > 0 {
        let mut c1 = src[pos] as i32;
        if !is_highbit_set(c1 as u8) {
            /* ASCII */
            if c1 == 0 {
                if no_error {
                    break;
                }
                report_invalid_encoding(PG_SJIS, &src[pos..])?;
            }
            dest.push(c1 as u8);
            pos += 1;
            len -= 1;
            continue;
        }
        let l = pg_encoding_verifymbchar(PG_SJIS, &src[pos..]);
        if l < 0 {
            if no_error {
                break;
            }
            report_invalid_encoding(PG_SJIS, &src[pos..])?;
        }
        if (0xa1..=0xdf).contains(&c1) {
            /* JIS X0201 (1 byte kana) */
            dest.push(SS2);
            dest.push(c1 as u8);
        } else {
            /*
             * JIS X0208, X0212, user defined extended characters
             */
            let mut c2 = src[pos + 1] as i32;
            let mut k = (c1 << 8) + c2;
            if (0xed40..0xf040).contains(&k) {
                /* NEC selection IBM kanji */
                let mut i = 0;
                loop {
                    let k2 = IBMKANJI[i].nec as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        k = IBMKANJI[i].sjis as i32;
                        c1 = (k >> 8) & 0xff;
                        c2 = k & 0xff;
                    }
                    i += 1;
                }
            }

            if k < 0xeb3f {
                /* JIS X0208 */
                dest.push((((c1 & 0x3f) << 1) + 0x9f + i32::from(c2 > 0x9e)) as u8);
                dest.push((c2 + if c2 > 0x9e { 2 } else { 0x60 } + i32::from(c2 < 0x80)) as u8);
            } else if (0xeb40..0xf040).contains(&k) || (0xfc4c..=0xfcfc).contains(&k) {
                /* NEC selection IBM kanji - Other undecided justice */
                dest.push((PGEUCALTCODE >> 8) as u8);
                dest.push((PGEUCALTCODE & 0xff) as u8);
            } else if (0xf040..0xf540).contains(&k) {
                /*
                 * UDC1 mapping to X0208 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0xf5a1 - 0xfefe
                 */
                c1 -= 0x6f;
                dest.push((((c1 & 0x3f) << 1) + 0xf3 + i32::from(c2 > 0x9e)) as u8);
                dest.push((c2 + if c2 > 0x9e { 2 } else { 0x60 } + i32::from(c2 < 0x80)) as u8);
            } else if (0xf540..0xfa40).contains(&k) {
                /*
                 * UDC2 mapping to X0212 85 ku - 94 ku JIS code 0x7521 -
                 * 0x7e7e EUC 0x8ff5a1 - 0x8ffefe
                 */
                dest.push(SS3);
                c1 -= 0x74;
                dest.push((((c1 & 0x3f) << 1) + 0xf3 + i32::from(c2 > 0x9e)) as u8);
                dest.push((c2 + if c2 > 0x9e { 2 } else { 0x60 } + i32::from(c2 < 0x80)) as u8);
            } else if k >= 0xfa40 {
                /*
                 * mapping IBM kanji to X0208 and X0212
                 */
                let mut i = 0;
                loop {
                    let k2 = IBMKANJI[i].sjis as i32;
                    if k2 == 0xffff {
                        break;
                    }
                    if k2 == k {
                        let euc = IBMKANJI[i].euc;
                        if euc >= 0x8f0000 {
                            dest.push(SS3);
                            dest.push((0x80 | ((euc & 0xff00) >> 8)) as u8);
                            dest.push((0x80 | (euc & 0xff)) as u8);
                        } else {
                            dest.push((0x80 | (euc >> 8)) as u8);
                            dest.push((0x80 | (euc & 0xff)) as u8);
                        }
                    }
                    i += 1;
                }
            }
        }
        pos += l as usize;
        len -= l;
    }

    Ok(ConversionResult {
        bytes: dest,
        converted: pos as i32,
    })
}

#[cfg(test)]
mod tests;
