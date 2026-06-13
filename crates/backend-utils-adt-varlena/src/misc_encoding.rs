//! FAMILY: base conversions, levenshtein/closest-match, and unicode
//! normalize/unistr.
//!
//! `convert_to_base` + `to_bin32`/`to_bin64`/`to_oct32`/`to_oct64`/
//! `to_hex32`/`to_hex64`; the Levenshtein edit-distance closest-match helpers
//! (`rest_of_char_same`, `initClosestMatch`/`updateClosestMatch`/
//! `getClosestMatch`); the unicode family
//! (`unicode_norm_form_from_string`, `unicode_version`, `icu_unicode_version`,
//! `unicode_assigned`, `unicode_normalize_func`, `unicode_is_normalized`,
//! `unistr`, the `isxdigits_n`/`hexval`/`hexval_n` hex helpers).
//!
//! The unicode normalization tables + `pg_unicode_to_server` and the ICU
//! version are genuinely-external owners (common/unicode, mbutils); the UTF-8
//! byte math is pure and ported in-family.

#![allow(unused_variables)]

use mcx::{Mcx, PgVec};
use types_error::PgResult;

/// C: `convert_to_base(uint64 value, int base)` — render `value` in `base`
/// (2/8/16). Shared by the `to_bin`/`to_oct`/`to_hex` entry points.
pub fn convert_to_base<'mcx>(mcx: Mcx<'mcx>, value: u64, base: i32) -> PgResult<PgVec<'mcx, u8>> {
    todo!("misc_encoding family: port convert_to_base")
}

/// C: `to_hex32(PG_FUNCTION_ARGS)` — int4 -> hex (zero-extends negatives to
/// 32 bits).
pub fn to_hex32<'mcx>(mcx: Mcx<'mcx>, value: i32) -> PgResult<PgVec<'mcx, u8>> {
    todo!("misc_encoding family: port to_hex32")
}

/// C: `to_hex64(PG_FUNCTION_ARGS)`.
pub fn to_hex64<'mcx>(mcx: Mcx<'mcx>, value: i64) -> PgResult<PgVec<'mcx, u8>> {
    todo!("misc_encoding family: port to_hex64")
}

/// C: `isxdigits_n(const char *instr, size_t n)` — are the first `n` bytes all
/// hex digits?
pub fn isxdigits_n(instr: &[u8], n: usize) -> bool {
    todo!("misc_encoding family: port isxdigits_n")
}

/// C: `hexval(unsigned char c)` — hex digit value (errors on non-hex).
pub fn hexval(c: u8) -> PgResult<u32> {
    todo!("misc_encoding family: port hexval")
}

/// C: `unistr(PG_FUNCTION_ARGS)` — decode `\xxxx` / `\+xxxxxx` / surrogate
/// pairs in a `text` into the server encoding.
pub fn unistr<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    todo!("misc_encoding family: port unistr")
}

/// C: `unicode_normalize_func(PG_FUNCTION_ARGS)` — `normalize(text, form)`
/// (consults the common/unicode normalization-table owner).
pub fn unicode_normalize_func<'mcx>(
    mcx: Mcx<'mcx>,
    t: &[u8],
    form: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    todo!("misc_encoding family: port unicode_normalize_func (unicode-table owner)")
}

/// C: `getClosestMatch` family — Levenshtein-based suggestion of the closest
/// candidate to a source string (used for `column "x" does not exist` hints).
pub fn levenshtein_closest_match<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    candidates: &[&[u8]],
    max_d: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    todo!("misc_encoding family: port initClosestMatch/updateClosestMatch/getClosestMatch")
}
