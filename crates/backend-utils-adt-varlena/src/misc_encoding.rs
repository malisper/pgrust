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
//! ## Owners
//!
//! - `unicode_normalize` / `unicode_is_normalized_quickcheck` — the
//!   `common/unicode_norm` normalization tables, REALLY ported here as
//!   [`common_unicode_norm_bitfields`] (called directly, no seam).
//! - `unicode_category` — the `common/unicode_category` general-category
//!   tables. NOT YET PORTED (no `common-unicode-category` crate); reached
//!   through the [`common_unicode_category_seams`] owner seam.
//! - `pg_unicode_to_server` — `utils/mb/mbutils.c` server-encoding conversion.
//!   Reached through the [`mb`] (mbutils) owner seam.
//! - `GetDatabaseEncoding` / `pg_mbstrlen_with_len` — mbutils, via the [`mb`]
//!   seam.
//!
//! The UTF-8 byte math (`utf8_to_unicode`, `unicode_to_utf8`, `pg_utf_mblen`,
//! the surrogate helpers, `is_valid_unicode_codepoint`) is pure code-point
//! arithmetic with no external state, so it is ported here 1:1 (per the
//! DESIGN HINT) rather than routed through a seam.

use mcx::{Mcx, PgVec};
use types_core::PgWChar;
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_SYNTAX_ERROR,
};
use types_wchar::encoding::PG_UTF8;

use backend_utils_mb_mbutils_seams as mb;
use common_unicode_category_seams as ucat;
use common_unicode_norm_bitfields as unorm;

use crate::keystone::cstring_to_text_with_len;

// ===========================================================================
// Constants
// ===========================================================================

/// C: `PG_UNICODE_VERSION` (common/unicode_version.h) — the bundled Unicode
/// version in "major.minor" form.
const PG_UNICODE_VERSION: &[u8] = b"16.0";

/// C: `PG_U_UNASSIGNED` (common/unicode_category.h) — the `Cn` category, value
/// 0.
const PG_U_UNASSIGNED: i32 = 0;

/// C: `levenshtein.c` `MAX_LEVENSHTEIN_STRLEN`.
const MAX_LEVENSHTEIN_STRLEN: i32 = 255;

// ===========================================================================
// base conversions (varlena.c:5186-5267)
// ===========================================================================

/// C: `convert_to_base(uint64 value, int base)` (varlena.c:5190-5210) — render
/// `value` in `base` (2/8/16). `base` must be `> 1` and `<= 16`. Shared by the
/// `to_bin`/`to_oct`/`to_hex` entry points.
pub fn convert_to_base<'mcx>(mcx: Mcx<'mcx>, mut value: u64, base: i32) -> PgResult<PgVec<'mcx, u8>> {
    // C:5193 const char *digits = "0123456789abcdef";
    const DIGITS: &[u8; 16] = b"0123456789abcdef";

    // C:5195-5198 char buf[sizeof(uint64) * BITS_PER_BYTE]; ptr = end = buf + sizeof(buf);
    // BITS_PER_BYTE is 8, so the buffer is exactly 64 bytes (to_bin's longest).
    let mut buf = [0u8; 64];
    let end = buf.len();
    let mut ptr = end;

    debug_assert!(base > 1);
    debug_assert!(base <= 16);

    let base_u = base as u64;
    // C:5203-5207 do { *--ptr = digits[value % base]; value /= base; } while (ptr > buf && value);
    loop {
        ptr -= 1;
        buf[ptr] = DIGITS[(value % base_u) as usize];
        value /= base_u;
        if !(ptr > 0 && value != 0) {
            break;
        }
    }

    // C:5209 return cstring_to_text_with_len(ptr, end - ptr);
    let slice = &buf[ptr..end];
    cstring_to_text_with_len(mcx, slice, slice.len() as i32)
}

/// C: `to_bin32(PG_FUNCTION_ARGS)` (varlena.c:5216-5222) — int4 -> binary
/// (zero-extends negatives to 32 bits).
pub fn to_bin32<'mcx>(mcx: Mcx<'mcx>, value: i32) -> PgResult<PgVec<'mcx, u8>> {
    // C:5219 uint64 value = (uint32) PG_GETARG_INT32(0);
    convert_to_base(mcx, (value as u32) as u64, 2)
}

/// C: `to_bin64(PG_FUNCTION_ARGS)` (varlena.c:5223-5229).
pub fn to_bin64<'mcx>(mcx: Mcx<'mcx>, value: i64) -> PgResult<PgVec<'mcx, u8>> {
    convert_to_base(mcx, value as u64, 2)
}

/// C: `to_oct32(PG_FUNCTION_ARGS)` (varlena.c:5235-5241) — int4 -> octal
/// (zero-extends negatives to 32 bits).
pub fn to_oct32<'mcx>(mcx: Mcx<'mcx>, value: i32) -> PgResult<PgVec<'mcx, u8>> {
    convert_to_base(mcx, (value as u32) as u64, 8)
}

/// C: `to_oct64(PG_FUNCTION_ARGS)` (varlena.c:5242-5248).
pub fn to_oct64<'mcx>(mcx: Mcx<'mcx>, value: i64) -> PgResult<PgVec<'mcx, u8>> {
    convert_to_base(mcx, value as u64, 8)
}

/// C: `to_hex32(PG_FUNCTION_ARGS)` (varlena.c:5254-5260) — int4 -> hex
/// (zero-extends negatives to 32 bits).
pub fn to_hex32<'mcx>(mcx: Mcx<'mcx>, value: i32) -> PgResult<PgVec<'mcx, u8>> {
    convert_to_base(mcx, (value as u32) as u64, 16)
}

/// C: `to_hex64(PG_FUNCTION_ARGS)` (varlena.c:5261-5267).
pub fn to_hex64<'mcx>(mcx: Mcx<'mcx>, value: i64) -> PgResult<PgVec<'mcx, u8>> {
    convert_to_base(mcx, value as u64, 16)
}

// ===========================================================================
// Levenshtein distance + closest-match (levenshtein.c + varlena.c:6408-6509)
// ===========================================================================

/// C: `rest_of_char_same(const char *s1, const char *s2, int len)`
/// (varlena.c:6412-6422) — are the first `len` bytes of `s1` and `s2` equal?
/// (Compares back-to-front like the C original; faster than `memcmp` for this
/// use case because the distinguishing byte is usually near the end.)
pub fn rest_of_char_same(s1: &[u8], s2: &[u8], len: i32) -> bool {
    // C:6415-6420 while (len > 0) { len--; if (s1[len] != s2[len]) return false; }
    let mut len = len;
    while len > 0 {
        len -= 1;
        if s1[len as usize] != s2[len as usize] {
            return false;
        }
    }
    true
}

/// C: `varstr_levenshtein_less_equal(source, slen, target, tlen, ins_c, del_c,
/// sub_c, max_d, trusted)` (levenshtein.c, the `LEVENSHTEIN_LESS_EQUAL`
/// expansion) — Levenshtein edit distance between `source` and `target`
/// (byte slices in the database encoding), capped at `max_d`: when the true
/// distance would exceed `max_d`, returns `max_d + 1`. `max_d < 0` disables
/// the bound. `trusted` callers skip the `MAX_LEVENSHTEIN_STRLEN` length cap.
///
/// This is the single source for both C expansions of `levenshtein.c`; the
/// non-bounded `varstr_levenshtein` is the `max_d = -1` case (the plain
/// expansion just compiles out the bound code, computing the same value).
pub fn varstr_levenshtein_less_equal(
    source: &[u8],
    slen: i32,
    target: &[u8],
    tlen: i32,
    ins_c: i32,
    del_c: i32,
    mut sub_c: i32,
    mut max_d: i32,
    trusted: bool,
) -> PgResult<i32> {
    // C:110-111 m = pg_mbstrlen_with_len(source, slen); n = pg_mbstrlen_with_len(target, tlen);
    let m0 = mb::pg_mbstrlen_with_len::call(source, slen);
    let n0 = mb::pg_mbstrlen_with_len::call(target, tlen);

    // C:117-120 empty-string fast paths.
    if m0 == 0 {
        return Ok(n0 * ins_c);
    }
    if n0 == 0 {
        return Ok(m0 * del_c);
    }

    // C:129-135 untrusted length guard.
    if !trusted && (m0 > MAX_LEVENSHTEIN_STRLEN || n0 > MAX_LEVENSHTEIN_STRLEN) {
        return Err(PgError::error(format!(
            "levenshtein argument exceeds maximum length of {MAX_LEVENSHTEIN_STRLEN} characters"
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    }

    // C:139-140 start_column = 0; stop_column = m + 1; (m here is char count m0)
    let mut start_column: i32 = 0;
    let mut stop_column: i32 = m0 + 1;

    // C:148-184 max_d bound tightening.
    if max_d >= 0 {
        let net_inserts = n0 - m0;
        let min_theo_d = if net_inserts < 0 {
            -net_inserts * del_c
        } else {
            net_inserts * ins_c
        };
        if min_theo_d > max_d {
            return Ok(max_d + 1);
        }
        if ins_c + del_c < sub_c {
            sub_c = ins_c + del_c;
        }
        let max_theo_d = min_theo_d + sub_c * m0.min(n0);
        if max_d >= max_theo_d {
            max_d = -1;
        } else if ins_c + del_c > 0 {
            let slack_d = max_d - min_theo_d;
            let best_column = if net_inserts < 0 { -net_inserts } else { 0 };

            stop_column = best_column + (slack_d / (ins_c + del_c)) + 1;
            if stop_column > m0 {
                stop_column = m0 + 1;
            }
        }
    }

    // C:195-207 cache per-source-char byte lengths when multibyte chars exist.
    let mut s_char_len: Option<Vec<i32>> = None;
    if m0 != slen || n0 != tlen {
        // s_char_len has (m0 + 1) entries; last is 0.
        let mut v: Vec<i32> = Vec::with_capacity((m0 + 1) as usize);
        let mut off = 0usize;
        for _ in 0..m0 {
            let cl = mb::pg_mblen_range::call(&source[off..]);
            v.push(cl);
            off += cl as usize;
        }
        v.push(0);
        s_char_len = Some(v);
    }

    // C:210-211 ++m; ++n; (now m/n are the cell counts: char count + 1)
    let m = m0 + 1;
    let n = n0 + 1;

    // C:214-215 two notional rows.
    let mut prev: Vec<i32> = vec![0; m as usize];
    let mut curr: Vec<i32> = vec![0; m as usize];

    // C:221-222 for (i = START_COLUMN; i < STOP_COLUMN; i++) prev[i] = i * del_c;
    let mut i = start_column;
    while i < stop_column {
        prev[i as usize] = i * del_c;
        i += 1;
    }

    // `source` advances as start_column slides right (C mutates `source`).
    let mut source_off = 0usize;

    // C:225 for (y = target, j = 1; j < n; j++)
    let mut y_off = 0usize;
    let mut j = 1;
    while j < n {
        // C:229 y_char_len = (n != tlen + 1) ? pg_mblen_range(y, tend) : 1;
        let y_char_len = if n != tlen + 1 {
            mb::pg_mblen_range::call(&target[y_off..])
        } else {
            1
        };

        // C:240-244 grow stop_column.
        if stop_column < m {
            prev[stop_column as usize] = max_d + 1;
            stop_column += 1;
        }

        // C:252-258 curr[0] special case / start_column.
        let mut i;
        if start_column == 0 {
            curr[0] = j * ins_c;
            i = 1;
        } else {
            i = start_column;
        }

        // x walks source bytes from source_off.
        let mut x_off = source_off;

        if let Some(ref scl) = s_char_len {
            // C:271-305 multibyte-aware path.
            while i < stop_column {
                let x_char_len = scl[(i - 1) as usize];

                let ins = prev[i as usize] + ins_c;
                let del = curr[(i - 1) as usize] + del_c;
                let sub = if source[x_off + (x_char_len - 1) as usize]
                    == target[y_off + (y_char_len - 1) as usize]
                    && x_char_len == y_char_len
                    && (x_char_len == 1
                        || rest_of_char_same(&source[x_off..], &target[y_off..], x_char_len))
                {
                    prev[(i - 1) as usize]
                } else {
                    prev[(i - 1) as usize] + sub_c
                };

                let mut c = ins.min(del);
                c = c.min(sub);
                curr[i as usize] = c;

                x_off += x_char_len as usize;
                i += 1;
            }
        } else {
            // C:308-325 single-byte fast path.
            while i < stop_column {
                let ins = prev[i as usize] + ins_c;
                let del = curr[(i - 1) as usize] + del_c;
                let sub = prev[(i - 1) as usize]
                    + if source[x_off] == target[y_off] { 0 } else { sub_c };

                let mut c = ins.min(del);
                c = c.min(sub);
                curr[i as usize] = c;

                x_off += 1;
                i += 1;
            }
        }

        // C:329-331 swap current and previous rows.
        std::mem::swap(&mut curr, &mut prev);

        // C:334 y += y_char_len;
        y_off += y_char_len as usize;

        // C:345-394 slide the start/stop columns under the max_d bound.
        if max_d >= 0 {
            let zp = j - (n - m);

            // C:358-367 stop column can slide left.
            while stop_column > 0 {
                let ii = stop_column - 1;
                let net_inserts = ii - zp;
                let resid = if net_inserts > 0 {
                    net_inserts * ins_c
                } else {
                    -net_inserts * del_c
                };
                if prev[ii as usize] + resid <= max_d {
                    break;
                }
                stop_column -= 1;
            }

            // C:370-389 start column can slide right.
            while start_column < stop_column {
                let net_inserts = start_column - zp;
                let resid = if net_inserts > 0 {
                    net_inserts * ins_c
                } else {
                    -net_inserts * del_c
                };
                if prev[start_column as usize] + resid <= max_d {
                    break;
                }

                prev[start_column as usize] = max_d + 1;
                curr[start_column as usize] = max_d + 1;
                if start_column != 0 {
                    let step = match s_char_len {
                        Some(ref scl) => scl[(start_column - 1) as usize],
                        None => 1,
                    };
                    source_off += step as usize;
                }
                start_column += 1;
            }

            // C:392-393 if they cross, we exceed the bound.
            if start_column >= stop_column {
                return Ok(max_d + 1);
            }
        }

        j += 1;
    }

    // C:402 return prev[m - 1];
    Ok(prev[(m - 1) as usize])
}

/// `getClosestMatch` family — Levenshtein-based suggestion of the closest
/// candidate to a source string (used for `column "x" does not exist` hints).
/// Models the C `initClosestMatch`/`updateClosestMatch`/`getClosestMatch`
/// usage pattern (varlena.c:6448-6509) inlined into one call: scan every
/// `candidate`, keep the closest under `max_d`, return it (or `None`).
///
/// `max_d` must be `>= 0` (C `initClosestMatch` asserts it). A copy of the
/// winning candidate is charged to `mcx` (the C `state->match` aliases the
/// caller's candidate; the carrier here is an owned copy).
pub fn levenshtein_closest_match<'mcx>(
    mcx: Mcx<'mcx>,
    source: &[u8],
    candidates: &[&[u8]],
    max_d: i32,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    debug_assert!(max_d >= 0);

    // C:6455 state->min_d = -1; state->match = NULL;
    let mut min_d: i32 = -1;
    let mut best: Option<&[u8]> = None;

    for &candidate in candidates {
        // C:6475-6477 skip NULL/empty source or candidate.
        if source.is_empty() || candidate.is_empty() {
            continue;
        }

        // C:6483-6485 length cap (checked instead of trusted=false to avoid ERROR).
        if source.len() as i32 > MAX_LEVENSHTEIN_STRLEN
            || candidate.len() as i32 > MAX_LEVENSHTEIN_STRLEN
        {
            continue;
        }

        // C:6487-6489 dist = varstr_levenshtein_less_equal(source, ..., candidate, ..., 1,1,1, max_d, true);
        let dist = varstr_levenshtein_less_equal(
            source,
            source.len() as i32,
            candidate,
            candidate.len() as i32,
            1,
            1,
            1,
            max_d,
            true,
        )?;

        // C:6490-6496 keep the closer match within max_d and within half the source length.
        if dist <= max_d
            && dist <= source.len() as i32 / 2
            && (min_d == -1 || dist < min_d)
        {
            min_d = dist;
            best = Some(candidate);
        }
    }

    // C:6504-6508 getClosestMatch -> state->match.
    match best {
        Some(b) => Ok(Some(mcx::slice_in(mcx, b)?)),
        None => Ok(None),
    }
}

// ===========================================================================
// Unicode normalization-form parsing & version reporting (varlena.c:6516-6570)
// ===========================================================================

/// C: `unicode_norm_form_from_string(const char *formstr)`
/// (varlena.c:6516-6543) — parse the form name (case-insensitive); errors
/// unless the server encoding is UTF8. `formstr` is the raw C-string bytes of
/// the form name.
pub fn unicode_norm_form_from_string(formstr: &[u8]) -> PgResult<unorm::UnicodeNormalizationForm> {
    // C:6524-6527 if (GetDatabaseEncoding() != PG_UTF8) ereport.
    if mb::get_database_encoding::call() != PG_UTF8 {
        return Err(PgError::error(
            "Unicode normalization can only be performed if server encoding is UTF8",
        )
        .with_sqlstate(ERRCODE_SYNTAX_ERROR));
    }

    // C:6529-6540 pg_strcasecmp(formstr, "NFC"/"NFD"/"NFKC"/"NFKD").
    let form = if formstr.eq_ignore_ascii_case(b"NFC") {
        unorm::UNICODE_NFC
    } else if formstr.eq_ignore_ascii_case(b"NFD") {
        unorm::UNICODE_NFD
    } else if formstr.eq_ignore_ascii_case(b"NFKC") {
        unorm::UNICODE_NFKC
    } else if formstr.eq_ignore_ascii_case(b"NFKD") {
        unorm::UNICODE_NFKD
    } else {
        // C:6538-6540
        return Err(PgError::error(format!(
            "invalid normalization form: {}",
            String::from_utf8_lossy(formstr)
        ))
        .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE));
    };

    // C:6542 return form;
    Ok(form)
}

/// C: `unicode_version(PG_FUNCTION_ARGS)` (varlena.c:6553-6557) — the bundled
/// Unicode version ("major.minor") as text.
pub fn unicode_version<'mcx>(mcx: Mcx<'mcx>) -> PgResult<PgVec<'mcx, u8>> {
    // C:6556 PG_RETURN_TEXT_P(cstring_to_text(PG_UNICODE_VERSION));
    cstring_to_text_with_len(mcx, PG_UNICODE_VERSION, PG_UNICODE_VERSION.len() as i32)
}

/// C: `icu_unicode_version(PG_FUNCTION_ARGS)` (varlena.c:6562-6570) — ICU's
/// Unicode version, or `None` when not built with ICU. This build is not
/// compiled with ICU (C `#else PG_RETURN_NULL()`).
pub fn icu_unicode_version<'mcx>(_mcx: Mcx<'mcx>) -> PgResult<Option<PgVec<'mcx, u8>>> {
    Ok(None)
}

// ===========================================================================
// unicode_assigned / normalize / is_normalized (varlena.c:6576-6717)
// ===========================================================================

/// C: `unicode_assigned(PG_FUNCTION_ARGS)` (varlena.c:6576-6602) — true iff
/// every code point in the input is an assigned Unicode code point (UTF8
/// only). `input` is the `text` payload bytes.
pub fn unicode_assigned(input: &[u8]) -> PgResult<bool> {
    // C:6583-6585 if (GetDatabaseEncoding() != PG_UTF8) ereport.
    if mb::get_database_encoding::call() != PG_UTF8 {
        return Err(PgError::error(
            "Unicode categorization can only be performed if server encoding is UTF8",
        ));
    }

    // C:6588 size = pg_mbstrlen_with_len(VARDATA_ANY(input), VARSIZE_ANY_EXHDR(input));
    let size = mb::pg_mbstrlen_with_len::call(input, input.len() as i32);

    // C:6589-6599 walk each character.
    let mut p = input;
    for _ in 0..size {
        // C:6592 pg_wchar uchar = utf8_to_unicode(p);
        let uchar = utf8_to_unicode(p);
        // C:6593 int category = unicode_category(uchar);
        let category = ucat::unicode_category::call(uchar);

        // C:6595-6596 if (category == PG_U_UNASSIGNED) PG_RETURN_BOOL(false);
        if category == PG_U_UNASSIGNED {
            return Ok(false);
        }

        // C:6598 p += pg_utf_mblen(p);
        p = &p[pg_utf_mblen(p) as usize..];
    }

    // C:6601 PG_RETURN_BOOL(true);
    Ok(true)
}

/// Convert a UTF-8 `text` payload to its sequence of code points charged to
/// `mcx` (C: `palloc((size+1)*sizeof(pg_wchar))` then the `for` loop filling
/// `input_chars`). The C array carries a trailing `'\0'`; the repo
/// `unicode_normalize`/`unicode_is_normalized_quickcheck` take the code points
/// directly (no terminator), so this returns just the `size` code points.
fn input_to_wchars<'mcx>(
    mcx: Mcx<'mcx>,
    input: &[u8],
    size: usize,
) -> PgResult<PgVec<'mcx, PgWChar>> {
    // C:6621-6628 convert to pg_wchar.
    let mut input_chars = mcx::vec_with_capacity_in(mcx, size)?;
    let mut p = input;
    for _ in 0..size {
        input_chars.push(utf8_to_unicode(p));
        p = &p[pg_utf_mblen(p) as usize..];
    }
    // C:6629 Assert((char *) p == VARDATA_ANY(input) + VARSIZE_ANY_EXHDR(input));
    debug_assert!(p.is_empty());
    Ok(input_chars)
}

/// C: `unicode_normalize_func(PG_FUNCTION_ARGS)` (varlena.c:6604-6656) —
/// `normalize(text, form)`: normalize the input to the named form and return
/// the re-encoded UTF-8 text. `t` is the `text` payload; `form` is the raw
/// form-name bytes.
pub fn unicode_normalize_func<'mcx>(
    mcx: Mcx<'mcx>,
    t: &[u8],
    form: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    // C:6617 form = unicode_norm_form_from_string(formstr);
    let nform = unicode_norm_form_from_string(form)?;

    // C:6620 size = pg_mbstrlen_with_len(...);
    let size = mb::pg_mbstrlen_with_len::call(t, t.len() as i32) as usize;

    // C:6621-6629 convert to pg_wchar.
    let input_chars = input_to_wchars(mcx, t, size)?;

    // C:6632 output_chars = unicode_normalize(form, input_chars);
    let output_chars = unorm::unicode_normalize(mcx, nform, &input_chars)?;

    // C:6634-6642 compute output byte length.
    let mut out_size = 0usize;
    for &wp in output_chars.iter() {
        let mut buf = [0u8; 4];
        unicode_to_utf8(wp, &mut buf);
        out_size += pg_utf_mblen(&buf) as usize;
    }

    // C:6644-6653 allocate result and write each char as UTF-8.
    let mut result = mcx::vec_with_capacity_in(mcx, out_size)?;
    for &wp in output_chars.iter() {
        let mut buf = [0u8; 4];
        unicode_to_utf8(wp, &mut buf);
        let len = pg_utf_mblen(&buf) as usize;
        result.extend_from_slice(&buf[..len]);
    }
    // C:6653 Assert((char *) p == (char *) result + size + VARHDRSZ);
    debug_assert_eq!(result.len(), out_size);

    // C:6655 PG_RETURN_TEXT_P(result);
    Ok(result)
}

/// C: `unicode_is_normalized(PG_FUNCTION_ARGS)` (varlena.c:6670-6717) — quick-
/// check then full compare to decide whether the input is already in the named
/// normal form. `t` is the `text` payload; `form` is the raw form-name bytes.
pub fn unicode_is_normalized(mcx: Mcx<'_>, t: &[u8], form: &[u8]) -> PgResult<bool> {
    // C:6685 form = unicode_norm_form_from_string(formstr);
    let nform = unicode_norm_form_from_string(form)?;

    // C:6688 size = pg_mbstrlen_with_len(...);
    let size = mb::pg_mbstrlen_with_len::call(t, t.len() as i32) as usize;

    // C:6689-6697 convert to pg_wchar.
    let input_chars = input_to_wchars(mcx, t, size)?;

    // C:6700 quickcheck = unicode_is_normalized_quickcheck(form, input_chars);
    let quickcheck = unorm::unicode_is_normalized_quickcheck(nform, &input_chars);
    // C:6701-6702 if (quickcheck == UNICODE_NORM_QC_YES) PG_RETURN_BOOL(true);
    if quickcheck == unorm::UNICODE_NORM_QC_YES {
        return Ok(true);
    }
    // C:6703-6704 else if (quickcheck == UNICODE_NORM_QC_NO) PG_RETURN_BOOL(false);
    else if quickcheck == unorm::UNICODE_NORM_QC_NO {
        return Ok(false);
    }

    // C:6707 output_chars = unicode_normalize(form, input_chars);
    let output_chars = unorm::unicode_normalize(mcx, nform, &input_chars)?;

    // C:6709-6711 output_size = count of output_chars.
    let output_size = output_chars.len();

    // C:6713-6714 result = (size == output_size) &&
    //   (memcmp(input_chars, output_chars, size * sizeof(pg_wchar)) == 0);
    let result = (size == output_size) && (input_chars[..size] == output_chars[..size]);

    // C:6716 PG_RETURN_BOOL(result);
    Ok(result)
}

// ===========================================================================
// unistr escape decoder (varlena.c:6722-6928)
// ===========================================================================

/// C: `isxdigits_n(const char *instr, size_t n)` (varlena.c:6722-6730) — are
/// the first `n` bytes all hex digits?
pub fn isxdigits_n(instr: &[u8], n: usize) -> bool {
    // C:6725-6727 for (i = 0; i < n; i++) if (!isxdigit(instr[i])) return false;
    instr.len() >= n && instr[..n].iter().all(u8::is_ascii_hexdigit)
}

/// C: `hexval(unsigned char c)` (varlena.c:6732-6743) — hex digit value
/// (errors on a non-hex byte).
pub fn hexval(c: u8) -> PgResult<u32> {
    // C:6735-6736 if (c >= '0' && c <= '9') return c - '0';
    if c.is_ascii_digit() {
        return Ok(u32::from(c - b'0'));
    }
    // C:6737-6738 if (c >= 'a' && c <= 'f') return c - 'a' + 0xA;
    if (b'a'..=b'f').contains(&c) {
        return Ok(u32::from(c - b'a') + 0xA);
    }
    // C:6739-6740 if (c >= 'A' && c <= 'F') return c - 'A' + 0xA;
    if (b'A'..=b'F').contains(&c) {
        return Ok(u32::from(c - b'A') + 0xA);
    }
    // C:6741 elog(ERROR, "invalid hexadecimal digit");
    Err(PgError::error("invalid hexadecimal digit"))
}

/// C: `hexval_n(const char *instr, size_t n)` (varlena.c:6748-6757) — value of
/// an `n`-digit hex run.
pub fn hexval_n(instr: &[u8], n: usize) -> PgResult<u32> {
    // C:6751 unsigned int result = 0;
    let mut result: u32 = 0;

    // C:6753-6754 for (i = 0; i < n; i++) result += hexval(instr[i]) << (4 * (n - i - 1));
    for (i, &c) in instr.iter().enumerate().take(n) {
        result = result.wrapping_add(hexval(c)? << (4 * (n - i - 1)));
    }

    // C:6756 return result;
    Ok(result)
}

/// C: `unistr(PG_FUNCTION_ARGS)` (varlena.c:6762-6928) — decode `\xxxx` /
/// `\+xxxxxx` / `\uXXXX` / `\UXXXXXXXX` escapes and surrogate pairs in a
/// `text` into the server encoding. `t` is the `text` payload bytes.
pub fn unistr<'mcx>(mcx: Mcx<'mcx>, t: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // C:6770 pg_wchar pair_first = 0;
    let mut pair_first: PgWChar = 0;

    // C:6773-6774 instr = VARDATA_ANY(input_text); len = VARSIZE_ANY_EXHDR(input_text);
    let total = t.len();
    let mut pos = 0usize;

    // C:6776 initStringInfo(&str); (the working buffer is charged to mcx; the
    // result is the same buffer handed back, so no copy-out is needed — C
    // copies because the StringInfo is freed, but our buffer IS the payload.)
    let mut str: PgVec<u8> = mcx::vec_with_capacity_in(mcx, total)?;

    // C:6778 while (len > 0)
    while pos < total {
        let instr = &t[pos..];
        let len = total - pos;

        // C:6780 if (instr[0] == '\\')
        if instr[0] == b'\\' {
            // C:6782-6789 if (len >= 2 && instr[1] == '\\')
            if len >= 2 && instr[1] == b'\\' {
                // C:6785-6786 if (pair_first) goto invalid_pair;
                if pair_first != 0 {
                    return Err(invalid_pair());
                }
                // C:6787 appendStringInfoChar(&str, '\\');
                str.push(b'\\');
                // C:6788-6789 instr += 2; len -= 2;
                pos += 2;
            }
            // C:6791-6792 4-digit escape: \XXXX or \uXXXX.
            else if (len >= 5 && isxdigits_n(&instr[1..], 4))
                || (len >= 6 && instr[1] == b'u' && isxdigits_n(&instr[2..], 4))
            {
                // C:6795 int offset = instr[1] == 'u' ? 2 : 1;
                let offset = if instr[1] == b'u' { 2 } else { 1 };
                // C:6797 unicode = hexval_n(instr + offset, 4);
                let unicode = hexval_n(&instr[offset..], 4)?;
                // C:6799-6823 validate + surrogate/append.
                process_codepoint(&mut str, &mut pair_first, unicode)?;
                // C:6825-6826 instr += 4 + offset; len -= 4 + offset;
                pos += 4 + offset;
            }
            // C:6828 6-digit escape: \+XXXXXX.
            else if len >= 8 && instr[1] == b'+' && isxdigits_n(&instr[2..], 6) {
                // C:6832 unicode = hexval_n(instr + 2, 6);
                let unicode = hexval_n(&instr[2..], 6)?;
                // C:6834-6858 validate + surrogate/append.
                process_codepoint(&mut str, &mut pair_first, unicode)?;
                // C:6860-6861 instr += 8; len -= 8;
                pos += 8;
            }
            // C:6863 8-digit escape: \UXXXXXXXX.
            else if len >= 10 && instr[1] == b'U' && isxdigits_n(&instr[2..], 8) {
                // C:6867 unicode = hexval_n(instr + 2, 8);
                let unicode = hexval_n(&instr[2..], 8)?;
                // C:6869-6893 validate + surrogate/append.
                process_codepoint(&mut str, &mut pair_first, unicode)?;
                // C:6895-6896 instr += 10; len -= 10;
                pos += 10;
            }
            // C:6898-6902 else ereport invalid Unicode escape.
            else {
                return Err(PgError::error("invalid Unicode escape")
                    .with_sqlstate(ERRCODE_SYNTAX_ERROR)
                    .with_hint(
                        "Unicode escapes must be \\XXXX, \\+XXXXXX, \\uXXXX, or \\UXXXXXXXX.",
                    ));
            }
        }
        // C:6904-6911 else { if (pair_first) goto invalid_pair; appendStringInfoChar; }
        else {
            // C:6906-6907 if (pair_first) goto invalid_pair;
            if pair_first != 0 {
                return Err(invalid_pair());
            }
            // C:6909 appendStringInfoChar(&str, *instr++);
            str.push(instr[0]);
            // C:6910 len--;
            pos += 1;
        }
    }

    // C:6914-6916 if (pair_first) goto invalid_pair;
    if pair_first != 0 {
        return Err(invalid_pair());
    }

    // C:6918 result = cstring_to_text_with_len(str.data, str.len);
    // C:6921 PG_RETURN_TEXT_P(result). The charged `str` buffer is itself the
    // payload carrier, so it is returned directly.
    Ok(str)
}

/// Shared body of the three escape branches of `unistr` (varlena.c:6799-6823,
/// 6834-6858, 6869-6893) — they are byte-for-byte identical. Validates the
/// code point, handles surrogate-pair joining, and either records a pending
/// high surrogate or appends the encoded server-string for a complete code
/// point.
fn process_codepoint(
    str: &mut PgVec<'_, u8>,
    pair_first: &mut PgWChar,
    mut unicode: PgWChar,
) -> PgResult<()> {
    // C:6799-6802 if (!is_valid_unicode_codepoint(unicode)) ereport.
    if !is_valid_unicode_codepoint(unicode) {
        return Err(
            PgError::error(format!("invalid Unicode code point: {unicode:04X}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        );
    }

    // C:6804-6815
    if *pair_first != 0 {
        // C:6806-6810 if (is_utf16_surrogate_second(unicode)) { join; pair_first = 0; }
        if is_utf16_surrogate_second(unicode) {
            unicode = surrogate_pair_to_codepoint(*pair_first, unicode);
            *pair_first = 0;
        } else {
            // C:6811-6812 else goto invalid_pair;
            return Err(invalid_pair());
        }
    }
    // C:6814-6815 else if (is_utf16_surrogate_second(unicode)) goto invalid_pair;
    else if is_utf16_surrogate_second(unicode) {
        return Err(invalid_pair());
    }

    // C:6817-6823
    if is_utf16_surrogate_first(unicode) {
        // C:6818 pair_first = unicode;
        *pair_first = unicode;
    } else {
        // C:6821 pg_unicode_to_server(unicode, (unsigned char *) cbuf);
        // C:6822 appendStringInfoString(&str, cbuf);
        // Build the encoded bytes in the SAME context the str buffer lives in.
        let encoded = mb::pg_unicode_to_server::call(*str.allocator(), unicode)?;
        str.extend_from_slice(&encoded);
    }

    Ok(())
}

/// C: `invalid_pair:` label (varlena.c:6923-6926) — the "invalid Unicode
/// surrogate pair" error.
fn invalid_pair() -> PgError {
    PgError::error("invalid Unicode surrogate pair").with_sqlstate(ERRCODE_SYNTAX_ERROR)
}

// ===========================================================================
// UTF-8 byte math shared with the mb subsystem (pure code-point arithmetic).
// Ported in-family per the DESIGN HINT (no external state).
// ===========================================================================

/// C: `utf8_to_unicode(const unsigned char *c)` (mb/pg_wchar.h) — decode one
/// UTF-8 char to a code point (no error checks; `c` must point to a
/// long-enough string).
fn utf8_to_unicode(c: &[u8]) -> PgWChar {
    if (c[0] & 0x80) == 0 {
        PgWChar::from(c[0])
    } else if (c[0] & 0xe0) == 0xc0 {
        (PgWChar::from(c[0] & 0x1f) << 6) | PgWChar::from(c[1] & 0x3f)
    } else if (c[0] & 0xf0) == 0xe0 {
        (PgWChar::from(c[0] & 0x0f) << 12)
            | (PgWChar::from(c[1] & 0x3f) << 6)
            | PgWChar::from(c[2] & 0x3f)
    } else if (c[0] & 0xf8) == 0xf0 {
        (PgWChar::from(c[0] & 0x07) << 18)
            | (PgWChar::from(c[1] & 0x3f) << 12)
            | (PgWChar::from(c[2] & 0x3f) << 6)
            | PgWChar::from(c[3] & 0x3f)
    } else {
        // an invalid code on purpose
        0xffffffff
    }
}

/// C: `unicode_to_utf8(pg_wchar c, unsigned char *utf8string)` (common/wchar.c)
/// — encode one code point as UTF-8 into `utf8string` (which must have
/// `unicode_utf8len(c)` bytes available).
fn unicode_to_utf8(c: PgWChar, utf8string: &mut [u8]) {
    if c <= 0x7F {
        utf8string[0] = c as u8;
    } else if c <= 0x7FF {
        utf8string[0] = 0xC0 | ((c >> 6) & 0x1F) as u8;
        utf8string[1] = 0x80 | (c & 0x3F) as u8;
    } else if c <= 0xFFFF {
        utf8string[0] = 0xE0 | ((c >> 12) & 0x0F) as u8;
        utf8string[1] = 0x80 | ((c >> 6) & 0x3F) as u8;
        utf8string[2] = 0x80 | (c & 0x3F) as u8;
    } else {
        utf8string[0] = 0xF0 | ((c >> 18) & 0x07) as u8;
        utf8string[1] = 0x80 | ((c >> 12) & 0x3F) as u8;
        utf8string[2] = 0x80 | ((c >> 6) & 0x3F) as u8;
        utf8string[3] = 0x80 | (c & 0x3F) as u8;
    }
}

/// C: `pg_utf_mblen(const unsigned char *s)` (common/wchar.c) — byte length of
/// the UTF-8 char at the start of `s`.
fn pg_utf_mblen(s: &[u8]) -> i32 {
    if (s[0] & 0x80) == 0 {
        1
    } else if (s[0] & 0xe0) == 0xc0 {
        2
    } else if (s[0] & 0xf0) == 0xe0 {
        3
    } else if (s[0] & 0xf8) == 0xf0 {
        4
    } else {
        1
    }
}

/// C: `is_valid_unicode_codepoint(pg_wchar c)` (mb/pg_wchar.h).
fn is_valid_unicode_codepoint(c: PgWChar) -> bool {
    c > 0 && c <= 0x10FFFF
}

/// C: `is_utf16_surrogate_first(pg_wchar c)` (mb/pg_wchar.h).
fn is_utf16_surrogate_first(c: PgWChar) -> bool {
    (0xD800..=0xDBFF).contains(&c)
}

/// C: `is_utf16_surrogate_second(pg_wchar c)` (mb/pg_wchar.h).
fn is_utf16_surrogate_second(c: PgWChar) -> bool {
    (0xDC00..=0xDFFF).contains(&c)
}

/// C: `surrogate_pair_to_codepoint(pg_wchar first, pg_wchar second)`
/// (mb/pg_wchar.h).
fn surrogate_pair_to_codepoint(first: PgWChar, second: PgWChar) -> PgWChar {
    ((first & 0x3FF) << 10) + 0x10000 + (second & 0x3FF)
}
