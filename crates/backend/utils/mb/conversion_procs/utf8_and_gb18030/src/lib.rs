//! Port of `utf8_and_gb18030.c` — the `GB18030 <--> UTF8` encoding-conversion
//! procedures.
//!
//! Faithful translation of
//! `src/backend/utils/mb/conversion_procs/utf8_and_gb18030/utf8_and_gb18030.c`.
//!
//! The two `PG_FUNCTION_ARGS` entry points (`gb18030_to_utf8`,
//! `utf8_to_gb18030`) become plain Rust functions over `&[u8]`. Each validates
//! its source/destination encodings with the faithful
//! [`check_encoding_conversion_args`] (the C `CHECK_ENCODING_CONVERSION_ARGS`
//! macro) and then delegates to the merged radix-tree engine ([`LocalToUtf`] /
//! [`UtfToLocal`]), a 1:1 port of `conv.c`.
//!
//! Unlike the single-byte legacy encodings, GB18030 covers all of Unicode via a
//! pair of *algorithmic* ranges (the four-byte GB18030 region) that the static
//! radix tables do not enumerate. The C code passes a `utf_local_conversion_func`
//! ([`conv_18030_to_utf8`] / [`conv_utf8_to_18030`]) to the engine; when the
//! radix lookup misses, the engine calls that function, which maps the unmapped
//! code via the linear GB18030 / Unicode range arithmetic ([`gb_linear`] /
//! [`gb_unlinear`]).
//!
//! The two GB18030 <-> Unicode radix tables (generated from the `.map` files)
//! are ported as `const` arrays in [`tables`].

#![allow(clippy::result_large_err)]
#![allow(non_snake_case)]

mod tables;

use ::conv_string_helpers::{
    check_encoding_conversion_args, ConversionResult, LocalToUtf, UtfToLocal,
};
use ::conv_string_helpers::make_conversion_builtin;
use ::types_error::PgResult;
use ::types_wchar::encoding::{pg_enc, PG_GB18030, PG_UTF8};

/// `gb18030_to_utf8` — convert a GB18030 string to UTF-8.
pub fn gb18030_to_utf8(
    src_encoding: pg_enc,
    dest_encoding: pg_enc,
    src: &[u8],
    no_error: bool,
) -> PgResult<ConversionResult> {
    check_encoding_conversion_args(
        src_encoding,
        dest_encoding,
        src.len() as i32,
        PG_GB18030,
        PG_UTF8,
    )?;
    LocalToUtf(
        src,
        Some(&tables::gb18030_to_unicode_tree()),
        &[],
        Some(conv_18030_to_utf8 as fn(u32) -> u32),
        PG_GB18030,
        no_error,
    )
}

/// `utf8_to_gb18030` — convert a UTF-8 string to GB18030.
pub fn utf8_to_gb18030(
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
        PG_GB18030,
    )?;
    UtfToLocal(
        src,
        Some(&tables::gb18030_from_unicode_tree()),
        &[],
        Some(conv_utf8_to_18030 as fn(u32) -> u32),
        PG_GB18030,
        no_error,
    )
}

/// `gb_linear` (utf8_and_gb18030.c) — turn a packed 4-byte GB18030 code into a
/// linear offset within the four-byte GB18030 space.
fn gb_linear(gb: u32) -> u32 {
    let b0 = (gb & 0xff00_0000) >> 24;
    let b1 = (gb & 0x00ff_0000) >> 16;
    let b2 = (gb & 0x0000_ff00) >> 8;
    let b3 = gb & 0x0000_00ff;

    b0 * 12600 + b1 * 1260 + b2 * 10 + b3 - (0x81 * 12600 + 0x30 * 1260 + 0x81 * 10 + 0x30)
}

/// `gb_unlinear` (utf8_and_gb18030.c) — inverse of [`gb_linear`]: turn a linear
/// offset back into the packed 4-byte GB18030 code.
fn gb_unlinear(lin: u32) -> u32 {
    let r0 = 0x81 + lin / 12600;
    let r1 = 0x30 + (lin / 1260) % 10;
    let r2 = 0x81 + (lin / 10) % 126;
    let r3 = 0x30 + lin % 10;

    (r0 << 24) | (r1 << 16) | (r2 << 8) | r3
}

/// `unicode_to_utf8word` (utf8_and_gb18030.c) — encode a Unicode code point as a
/// packed (big-endian-in-an-integer) UTF-8 byte sequence.
fn unicode_to_utf8word(c: u32) -> u32 {
    if c <= 0x7f {
        c
    } else if c <= 0x7ff {
        ((0xc0 | ((c >> 6) & 0x1f)) << 8) | (0x80 | (c & 0x3f))
    } else if c <= 0xffff {
        ((0xe0 | ((c >> 12) & 0x0f)) << 16)
            | ((0x80 | ((c >> 6) & 0x3f)) << 8)
            | (0x80 | (c & 0x3f))
    } else {
        ((0xf0 | ((c >> 18) & 0x07)) << 24)
            | ((0x80 | ((c >> 12) & 0x3f)) << 16)
            | ((0x80 | ((c >> 6) & 0x3f)) << 8)
            | (0x80 | (c & 0x3f))
    }
}

/// `utf8word_to_unicode` (utf8_and_gb18030.c) — decode a packed UTF-8 byte
/// sequence back into a Unicode code point.
fn utf8word_to_unicode(c: u32) -> u32 {
    if c <= 0x7f {
        c
    } else if c <= 0xffff {
        (((c >> 8) & 0x1f) << 6) | (c & 0x3f)
    } else if c <= 0xff_ffff {
        (((c >> 16) & 0x0f) << 12) | (((c >> 8) & 0x3f) << 6) | (c & 0x3f)
    } else {
        (((c >> 24) & 0x07) << 18)
            | (((c >> 16) & 0x3f) << 12)
            | (((c >> 8) & 0x3f) << 6)
            | (c & 0x3f)
    }
}

/// `conv_18030_to_utf8` (utf8_and_gb18030.c) — the `utf_local_conversion_func`
/// used by `LocalToUtf` for the four-byte GB18030 region the radix table omits.
fn conv_18030_to_utf8(code: u32) -> u32 {
    for &(min_unicode, min_code, max_code) in GB18030_RANGES {
        if code >= min_code && code <= max_code {
            return unicode_to_utf8word(gb_linear(code) - gb_linear(min_code) + min_unicode);
        }
    }
    0
}

/// `conv_utf8_to_18030` (utf8_and_gb18030.c) — the `utf_local_conversion_func`
/// used by `UtfToLocal` for the four-byte GB18030 region the radix table omits.
fn conv_utf8_to_18030(code: u32) -> u32 {
    let ucs = utf8word_to_unicode(code);
    for &(min_unicode, max_unicode, min_code) in UTF8_RANGES {
        if ucs >= min_unicode && ucs <= max_unicode {
            return gb_unlinear(ucs - min_unicode + gb_linear(min_code));
        }
    }
    0
}

/// The `gb18030_ranges` table (utf8_and_gb18030.c), keyed for GB18030 -> Unicode:
/// `(min_unicode, min_gb18030_code, max_gb18030_code)`.
const GB18030_RANGES: &[(u32, u32, u32)] = &[
    (0x0452, 0x8130d330, 0x8136a531),
    (0x2643, 0x8137a839, 0x8138fd38),
    (0x361b, 0x8230a633, 0x8230f237),
    (0x3ce1, 0x8231d438, 0x8232af32),
    (0x4160, 0x8232c937, 0x8232f837),
    (0x44d7, 0x8233a339, 0x8233c931),
    (0x478e, 0x8233e838, 0x82349638),
    (0x49b8, 0x8234a131, 0x8234e733),
    (0x9fa6, 0x82358f33, 0x8336c738),
    (0xe865, 0x8336d030, 0x84308534),
    (0xfa2a, 0x84309c38, 0x84318537),
    (0xffe6, 0x8431a234, 0x8431a439),
    (0x10000, 0x90308130, 0xe3329a35),
];

/// The same `gb18030_ranges` table, keyed for Unicode -> GB18030:
/// `(min_unicode, max_unicode, min_gb18030_code)`.
const UTF8_RANGES: &[(u32, u32, u32)] = &[
    (0x0452, 0x200f, 0x8130d330),
    (0x2643, 0x2e80, 0x8137a839),
    (0x361b, 0x3917, 0x8230a633),
    (0x3ce1, 0x4055, 0x8231d438),
    (0x4160, 0x4336, 0x8232c937),
    (0x44d7, 0x464b, 0x8233a339),
    (0x478e, 0x4946, 0x8233e838),
    (0x49b8, 0x4c76, 0x8234a131),
    (0x9fa6, 0xd7ff, 0x82358f33),
    (0xe865, 0xf92b, 0x8336d030),
    (0xfa2a, 0xfe2f, 0x84309c38),
    (0xffe6, 0xffff, 0x8431a234),
    (0x10000, 0x10ffff, 0x90308130),
];

/// Registers this crate's ported conversion procedures as fmgr builtins so
/// their `pg_proc` OIDs resolve to the in-process Rust bodies (no `dlopen`).
pub fn init_seams() {
    fmgr_core::register_builtins_native([
        make_conversion_builtin(4368, "gb18030_to_utf8", gb18030_to_utf8),
        make_conversion_builtin(4369, "utf8_to_gb18030", utf8_to_gb18030),
    ]);
}
