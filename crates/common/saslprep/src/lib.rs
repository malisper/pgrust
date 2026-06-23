//! `src/common/saslprep.c` — SASLprep normalization, for SCRAM authentication.
//!
//! The SASLprep algorithm (RFC 4013, a profile of stringprep RFC 3454)
//! processes a user-supplied password into canonical form:
//!
//!   1. Map  — non-ASCII spaces become U+0020; "commonly mapped to nothing"
//!             characters are dropped.
//!   2. Normalize — apply Unicode NFKC normalization.
//!   3. Prohibit — reject prohibited-output and unassigned code points.
//!   4. Check bidi — enforce the RFC 3454 §6 bidirectional rules.
//!
//! Faithful to the C source:
//!   * The prohibit (step 3) and bidi (step 4) checks operate on the *mapped*
//!     code points (the C `input_chars` array after step 1), NOT on the
//!     normalized output — exactly as `saslprep.c` does.
//!   * The result is re-encoded with the same `unicode_to_utf8` byte layout as
//!     `wchar.c`, so the output bytes are identical to the C implementation.
//!   * A pure-ASCII input short-circuits and is returned unchanged.
//!
//! The normalization working buffers are allocated in the supplied [`Mcx`],
//! mirroring the C `ALLOC`/`palloc` calls; OOM surfaces as a [`PgResult`] error
//! (the backend `ereport(ERROR)`). The C `pg_saslprep_rc` return code plus the
//! `char **output` out-param become a `PgResult<Option<Vec<u8>>>`:
//! `Ok(Some(bytes))` is `SASLPREP_SUCCESS`, `Ok(None)` is
//! `SASLPREP_INVALID_UTF8` / `SASLPREP_PROHIBITED`, and `Err` is the backend
//! OOM error (`SASLPREP_OOM` would be the frontend code).

#![allow(non_camel_case_types, non_upper_case_globals, non_snake_case)]

use unicode_norm_bitfields::{unicode_normalize, UNICODE_NFKC};
use common_wchar::{pg_utf8_islegal, pg_utf_mblen_private};
use mcx::Mcx;
use types_error::PgResult;
use types_wchar::pg_wchar;

mod tables;

use tables::{
    COMMONLY_MAPPED_TO_NOTHING_RANGES, L_CAT_CODEPOINT_RANGES, NON_ASCII_SPACE_RANGES,
    PROHIBITED_OUTPUT_RANGES, RAND_A_L_CAT_CODEPOINT_RANGES, UNASSIGNED_CODEPOINT_RANGES,
};

/// Normalize a password with SASLprep (`pg_saslprep`).
///
/// SASLprep requires UTF-8 input; the input is validated and `Ok(None)` is
/// returned (the C `SASLPREP_INVALID_UTF8`) if it is not valid UTF-8. If the
/// (mapped) string would contain prohibited characters after normalization, or
/// would be empty, `Ok(None)` is returned (the C `SASLPREP_PROHIBITED`). On
/// success the normalized UTF-8 bytes are returned. An allocation failure
/// returns `Err` (the backend `ereport(ERROR)`; the C frontend `SASLPREP_OOM`).
pub fn pg_saslprep<'mcx>(mcx: Mcx<'mcx>, input: &[u8]) -> PgResult<Option<alloc::vec::Vec<u8>>> {
    // Quick check: a pure-ASCII string needs no further processing. The C
    // `pg_is_ascii` tests the high bit of each byte (no UTF-8 validation), so
    // do the same directly on the bytes; the C code STRDUPs the input verbatim.
    if !input.iter().any(|&b| b & 0x80 != 0) {
        return Ok(Some(input.to_vec()));
    }

    // Convert the input from UTF-8 to an array of Unicode code points. This
    // also checks that the input is a legal UTF-8 string.
    let input_chars = match pg_utf8_string_to_codepoints(input) {
        Some(v) => v,
        None => return Ok(None), // SASLPREP_INVALID_UTF8
    };

    // Step 1) Map. For each character, replace non-ASCII spaces with U+0020 and
    // drop "commonly mapped to nothing" characters; everything else is kept.
    // C maps in place into the front of the same array (the mapped length never
    // exceeds the input length).
    let mut mapped: alloc::vec::Vec<pg_wchar> = alloc::vec::Vec::new();
    mapped
        .try_reserve(input_chars.len())
        .map_err(|_| mcx.oom(input_chars.len()))?;
    for &code in &input_chars {
        if is_code_in_table(code, NON_ASCII_SPACE_RANGES) {
            mapped.push(0x0020);
        } else if is_code_in_table(code, COMMONLY_MAPPED_TO_NOTHING_RANGES) {
            // map to nothing
        } else {
            mapped.push(code);
        }
    }

    // The C code rejects an empty post-mapping password.
    if mapped.is_empty() {
        return Ok(None); // SASLPREP_PROHIBITED
    }

    // Step 2) Normalize using Unicode NFKC normalization. (Allocated in `mcx`.)
    let output_chars = unicode_normalize(mcx, UNICODE_NFKC, &mapped)?;

    // Step 3) Prohibit. Reject any prohibited-output or unassigned code points.
    // As in C, these checks run over the MAPPED code points, not the
    // normalized output.
    for &code in &mapped {
        if is_code_in_table(code, PROHIBITED_OUTPUT_RANGES)
            || is_code_in_table(code, UNASSIGNED_CODEPOINT_RANGES)
        {
            return Ok(None); // SASLPREP_PROHIBITED
        }
    }

    // Step 4) Check bidi (RFC 3454 §6). If the string contains any RandALCat
    // character it must contain no LCat character, and both the first and last
    // characters must be RandALCat. Again, over the mapped code points.
    let contains_RandALCat = mapped
        .iter()
        .any(|&code| is_code_in_table(code, RAND_A_L_CAT_CODEPOINT_RANGES));

    if contains_RandALCat {
        let first = mapped[0];
        let last = mapped[mapped.len() - 1];

        if mapped
            .iter()
            .any(|&code| is_code_in_table(code, L_CAT_CODEPOINT_RANGES))
        {
            return Ok(None); // SASLPREP_PROHIBITED
        }

        if !is_code_in_table(first, RAND_A_L_CAT_CODEPOINT_RANGES)
            || !is_code_in_table(last, RAND_A_L_CAT_CODEPOINT_RANGES)
        {
            return Ok(None); // SASLPREP_PROHIBITED
        }
    }

    // Finally, convert the normalized result back to UTF-8, matching the
    // `unicode_to_utf8` byte layout from wchar.c. The C code sizes the buffer in
    // a first pass and fills it in a second; do the same so the OOM check
    // happens before any bytes are written.
    let mut result_size = 0usize;
    for &code in &output_chars {
        let mut buf = [0u8; 4];
        unicode_to_utf8(code, &mut buf);
        result_size += utf8_leading_len(buf[0]);
    }
    let mut result_bytes: alloc::vec::Vec<u8> = alloc::vec::Vec::new();
    result_bytes
        .try_reserve(result_size)
        .map_err(|_| mcx.oom(result_size))?;
    for &code in &output_chars {
        let mut buf = [0u8; 4];
        unicode_to_utf8(code, &mut buf);
        let len = utf8_leading_len(buf[0]);
        result_bytes.extend_from_slice(&buf[..len]);
    }

    Ok(Some(result_bytes))
}

/// Decode a UTF-8 byte string into Unicode code points, validating it.
///
/// Combines the C `pg_utf8_string_len` length/validity check with the
/// subsequent decode loop in `pg_saslprep`. Returns `None` if the input is not
/// valid UTF-8 (the C `SASLPREP_INVALID_UTF8`).
fn pg_utf8_string_to_codepoints(input: &[u8]) -> Option<alloc::vec::Vec<pg_wchar>> {
    // First validate and count, exactly like pg_utf8_string_len.
    let mut num_chars = 0usize;
    {
        let mut remaining = input;
        while !remaining.is_empty() {
            let l = pg_utf_mblen_private(remaining)? as usize;
            if remaining.len() < l || !pg_utf8_islegal(&remaining[..l]) {
                return None;
            }
            remaining = &remaining[l..];
            num_chars += 1;
        }
    }

    let mut result: alloc::vec::Vec<pg_wchar> = alloc::vec::Vec::new();
    result.try_reserve(num_chars).ok()?;

    let mut remaining = input;
    while !remaining.is_empty() {
        // Length is known-valid from the pass above.
        let l = pg_utf_mblen_private(remaining)? as usize;
        result.push(utf8_to_unicode(&remaining[..l]));
        remaining = &remaining[l..];
    }

    Some(result)
}

/// Is the given Unicode code point within one of the (inclusive) ranges?
///
/// `ranges` is the sorted, non-overlapping `(first, last)` pair table — the
/// analog of the C `is_code_in_table` / `IS_CODE_IN_TABLE` bsearch over the
/// flat `pg_wchar` range arrays.
fn is_code_in_table(code: pg_wchar, ranges: &[(u32, u32)]) -> bool {
    if ranges.is_empty() || code < ranges[0].0 || code > ranges[ranges.len() - 1].1 {
        return false;
    }

    ranges
        .binary_search_by(|&(first, last)| {
            if code < first {
                core::cmp::Ordering::Greater
            } else if code > last {
                core::cmp::Ordering::Less
            } else {
                core::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

/// Decode the leading UTF-8 sequence into a Unicode code point.
///
/// Port of `utf8_to_unicode` from `wchar.c`. The slice must hold at least the
/// number of bytes the leading byte implies; the caller guarantees this from a
/// prior `pg_utf_mblen_private`/`pg_utf8_islegal` check.
fn utf8_to_unicode(c: &[u8]) -> pg_wchar {
    let b0 = c[0] as u32;
    if b0 & 0x80 == 0 {
        c[0] as pg_wchar
    } else if b0 & 0xe0 == 0xc0 {
        (((c[0] as u32 & 0x1f) << 6) | (c[1] as u32 & 0x3f)) as pg_wchar
    } else if b0 & 0xf0 == 0xe0 {
        (((c[0] as u32 & 0x0f) << 12) | ((c[1] as u32 & 0x3f) << 6) | (c[2] as u32 & 0x3f))
            as pg_wchar
    } else if b0 & 0xf8 == 0xf0 {
        (((c[0] as u32 & 0x07) << 18)
            | ((c[1] as u32 & 0x3f) << 12)
            | ((c[2] as u32 & 0x3f) << 6)
            | (c[3] as u32 & 0x3f)) as pg_wchar
    } else {
        0xffff_ffff
    }
}

/// Encode `c` as UTF-8 into the start of `utf8string`.
///
/// Port of `unicode_to_utf8` from `wchar.c`. `utf8string` must have room for
/// 1-4 bytes.
fn unicode_to_utf8(c: pg_wchar, utf8string: &mut [u8]) {
    if c <= 0x7f {
        utf8string[0] = c as u8;
    } else if c <= 0x7ff {
        utf8string[0] = (0xc0 | ((c >> 6) & 0x1f)) as u8;
        utf8string[1] = (0x80 | (c & 0x3f)) as u8;
    } else if c <= 0xffff {
        utf8string[0] = (0xe0 | ((c >> 12) & 0x0f)) as u8;
        utf8string[1] = (0x80 | ((c >> 6) & 0x3f)) as u8;
        utf8string[2] = (0x80 | (c & 0x3f)) as u8;
    } else {
        utf8string[0] = (0xf0 | ((c >> 18) & 0x07)) as u8;
        utf8string[1] = (0x80 | ((c >> 12) & 0x3f)) as u8;
        utf8string[2] = (0x80 | ((c >> 6) & 0x3f)) as u8;
        utf8string[3] = (0x80 | (c & 0x3f)) as u8;
    }
}

/// Byte length of a UTF-8 character from its leading byte.
///
/// The C back-conversion loop calls `pg_utf_mblen(buf)` on the byte just
/// produced by `unicode_to_utf8`; this is that `pg_utf_mblen` (from `wchar.c`)
/// for a single leading byte.
fn utf8_leading_len(first: u8) -> usize {
    let b = first as u32;
    if b & 0x80 == 0 {
        1
    } else if b & 0xe0 == 0xc0 {
        2
    } else if b & 0xf0 == 0xe0 {
        3
    } else if b & 0xf8 == 0xf0 {
        4
    } else {
        1
    }
}

/// Install the `pg_saslprep` seam (`common/saslprep.c`).
///
/// The seam carries the C return-code surface as `Option<Vec<u8>>`: `Some` on
/// `SASLPREP_SUCCESS`, `None` for any other return (invalid UTF-8 / prohibited
/// chars / OOM). A private scratch context holds the normalization working
/// buffers for the duration of the call; the result bytes are copied out into a
/// caller-owned `Vec` before it is dropped.
pub fn init_seams() {
    scram_seams::pg_saslprep::set(|input| {
        let cx = mcx::MemoryContext::new("SASLprep");
        match pg_saslprep(cx.mcx(), &input) {
            Ok(opt) => opt,
            // Backend OOM is `ereport(ERROR)`; the seam cannot carry an error,
            // so degrade to `None` (the caller falls back to the raw password).
            Err(_) => None,
        }
    });
}

extern crate alloc;

#[cfg(test)]
mod tests;
