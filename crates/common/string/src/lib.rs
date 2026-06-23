//! Port of PostgreSQL's `src/common/string.c` — string handling helpers.
//!
//! `pg_clean_ascii` is this crate's seam (it allocates in the current memory
//! context in the backend, via `palloc_extended`); it is installed in
//! [`init_seams`]. The remaining helpers (`pg_str_endswith`, `strtoint`,
//! `pg_is_ascii`, `pg_strip_crlf`) do no allocation of their own — they return
//! scalars, borrowed slices, or operate in place — so they are plain functions.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;
#[cfg(test)]
extern crate std;

use mcx::{Mcx, PgString};
use ::types_error::PgResult;

/// `pg_str_endswith` — whether `str` has the suffix `end`.
pub fn pg_str_endswith(s: &str, end: &str) -> bool {
    // C compares the raw bytes after `str += slen - elen`; for `&str` this is
    // the same as `ends_with`.
    s.as_bytes().ends_with(end.as_bytes())
}

/// `pg_clean_ascii` — return a copy of `str` with every non-printable /
/// non-ASCII byte replaced by a `"\xXX"` escape, allocated in `mcx`.
///
/// Mirrors C `pg_clean_ascii(const char *str, int alloc_flags)`: the backend
/// build allocates the result via `palloc_extended(dstlen, alloc_flags)`. Here
/// the allocation is fallible through `mcx`, so an allocator refusal (the
/// `palloc` failure / `MCXT_ALLOC_NO_OOM` NULL return) surfaces as `Err`
/// rather than aborting. `alloc_flags` is accepted to match the C surface; the
/// fallible `mcx` path already covers the OOM behavior it selects.
pub fn pg_clean_ascii<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    _alloc_flags: i32,
) -> PgResult<PgString<'mcx>> {
    let bytes = s.as_bytes();

    // Each `try_push*` grows the buffer fallibly (worst case each byte expands
    // to four, "\xXX"); an allocator refusal surfaces as `Err`.
    let mut dst = PgString::new_in(mcx);

    for &c in bytes {
        // Only allow clean ASCII chars in the string.
        if c < 32 || c > 126 {
            dst.try_push('\\')?;
            dst.try_push('x')?;
            dst.try_push(hex_digit(c >> 4))?;
            dst.try_push(hex_digit(c & 0x0f))?;
        } else {
            dst.try_push(c as char)?;
        }
    }

    Ok(dst)
}

/// `pg_is_ascii` — whether `str` is made only of ASCII characters.
pub fn pg_is_ascii(s: &str) -> bool {
    // C: returns false on the first byte with the high bit set (IS_HIGHBIT_SET).
    !s.as_bytes().iter().any(|&c| c & 0x80 != 0)
}

/// `pg_strip_crlf` — remove any trailing `\n`/`\r` characters, returning the
/// trimmed prefix. The C routine zero-terminates in place and returns the new
/// length; for a `&str` the trimmed slice carries the same information and its
/// `.len()` is the returned length.
pub fn pg_strip_crlf(s: &str) -> &str {
    s.trim_end_matches(['\n', '\r'])
}

/// `strtoint` — like `strtol`, but returns `int`.
///
/// C: `val = strtol(str, endptr, base); if (val != (int) val) errno = ERANGE;
/// return (int) val;`. This is a *total* function — it never "fails": on a
/// string with no parseable digits `strtol` returns 0 (and leaves `endptr` at
/// the start), and the only out-of-band signal is `errno = ERANGE` when the
/// `long` value does not fit in `int`. The returned [`StrToInt`] mirrors that:
/// `value` is the truncated `(int) val`, `end` is the `endptr` offset, and
/// `erange` is the ERANGE flag.
///
/// `base` follows `strtol`: 0 auto-detects (leading `0x`/`0X` → hex, leading
/// `0` → octal, else decimal), or a fixed 2..=36. Like the C build's callers,
/// only valid bases are passed; a base outside `{0} ∪ 2..=36` yields the
/// `strtol(EINVAL)` outcome (value 0, `end` 0, `erange` false).
pub fn strtoint(s: &str, base: u32) -> StrToInt {
    let parsed = strtol(s, base);
    let value = parsed.value as i32;
    StrToInt {
        value,
        end: parsed.end,
        // C: `errno = ERANGE` when (int) val != val, i.e. the long didn't fit
        // in int. `parsed.erange` already covers the long-range clamp strtol
        // performs (LONG_MIN/LONG_MAX), which also implies ERANGE here.
        erange: parsed.erange || value as i64 != parsed.value,
    }
}

/// Result of [`strtoint`]: the truncated `int` value, the `endptr` offset (the
/// byte index of the first unconsumed character; equals the start offset when
/// nothing was parsed), and the ERANGE flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrToInt {
    pub value: i32,
    pub end: usize,
    pub erange: bool,
}

/// Result of the internal `strtol` emulation: the `long` value (clamped to
/// `i64::MIN`/`i64::MAX` on overflow, as `strtol` clamps to `LONG_MIN`/
/// `LONG_MAX`), the `endptr` offset, and the ERANGE flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedLong {
    value: i64,
    end: usize,
    erange: bool,
}

/// Emulate libc `strtol(str, endptr, base)` on a `&str` (the C build is
/// 64-bit, so `long` is `i64`). Returns the parsed value, the `endptr` offset,
/// and the ERANGE flag. No-conversion returns `value 0`, `end` at the original
/// start (offset 0). An invalid base returns the EINVAL outcome (`0`, end `0`).
fn strtol(text: &str, base: u32) -> ParsedLong {
    if base != 0 && !(2..=36).contains(&base) {
        // strtol with an invalid base: returns 0, endptr = str (offset 0).
        return ParsedLong { value: 0, end: 0, erange: false };
    }

    let bytes = text.as_bytes();
    // strtol skips leading C-isspace, then sets endptr = str (offset 0) if no
    // digits are found — note the no-conversion endptr is the *original* start,
    // not the post-whitespace position.
    let after_ws = bytes
        .iter()
        .position(|byte| !is_c_space(*byte))
        .unwrap_or(bytes.len());
    let mut index = after_ws;
    let negative = match bytes.get(index) {
        Some(b'-') => {
            index += 1;
            true
        }
        Some(b'+') => {
            index += 1;
            false
        }
        _ => false,
    };

    let mut actual_base = base;
    if actual_base == 0 {
        if bytes.get(index) == Some(&b'0') {
            if matches!(bytes.get(index + 1), Some(b'x' | b'X'))
                && bytes
                    .get(index + 2)
                    .and_then(|byte| digit_value(*byte))
                    .is_some_and(|digit| digit < 16)
            {
                actual_base = 16;
                index += 2;
            } else {
                actual_base = 8;
            }
        } else {
            actual_base = 10;
        }
    } else if actual_base == 16
        && bytes.get(index) == Some(&b'0')
        && matches!(bytes.get(index + 1), Some(b'x' | b'X'))
        && bytes
            .get(index + 2)
            .and_then(|byte| digit_value(*byte))
            .is_some_and(|digit| digit < actual_base)
    {
        index += 2;
    }

    let digits_start = index;
    let mut value: i64 = 0;
    let mut erange = false;

    while let Some(&byte) = bytes.get(index) {
        let Some(digit) = digit_value(byte) else {
            break;
        };
        if digit >= actual_base {
            break;
        }

        // strtol keeps consuming digits even past overflow, clamping the value
        // to LONG_MIN/LONG_MAX and setting ERANGE.
        if !erange {
            value = match value
                .checked_mul(actual_base as i64)
                .and_then(|value| value.checked_add(digit as i64))
            {
                Some(value) => value,
                None => {
                    erange = true;
                    if negative { i64::MIN } else { i64::MAX }
                }
            };
        }
        index += 1;
    }

    if index == digits_start {
        // No digits converted: strtol returns 0 with endptr = the original str.
        return ParsedLong { value: 0, end: 0, erange: false };
    }

    let value = if negative && !erange {
        // -value; the negation of an in-range positive magnitude cannot
        // overflow i64 here (max magnitude consumed without erange is i64::MAX).
        -value
    } else {
        value
    };

    ParsedLong { value, end: index, erange }
}

/// The C `isspace` set in the "C" locale, as `strtol` skips leading
/// whitespace: space, `\t`, `\n`, vertical tab, form feed, `\r`. Rust's
/// `is_ascii_whitespace` omits vertical tab, so this is spelled out.
fn is_c_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

fn digit_value(byte: u8) -> Option<u32> {
    match byte {
        b'0'..=b'9' => Some((byte - b'0') as u32),
        b'a'..=b'z' => Some((byte - b'a' + 10) as u32),
        b'A'..=b'Z' => Some((byte - b'A' + 10) as u32),
        _ => None,
    }
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + nibble - 10) as char,
    }
}

/// Install this crate's seams. Only `set()` calls.
pub fn init_seams() {
    string_seams::pg_clean_ascii::set(pg_clean_ascii);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::mcx::MemoryContext;

    #[test]
    fn detects_suffixes() {
        assert!(pg_str_endswith("archive.tar", ".tar"));
        assert!(pg_str_endswith("archive", ""));
        assert!(!pg_str_endswith("archive", "longer suffix"));
        assert!(!pg_str_endswith("archive.tar", ".zip"));
    }

    #[test]
    fn parses_integer_prefixes() {
        assert_eq!(
            strtoint("  -123xyz", 10),
            StrToInt { value: -123, end: 6, erange: false }
        );
        assert_eq!(strtoint("7fffffff", 16).value, i32::MAX);
        assert_eq!(strtoint("010", 0), StrToInt { value: 8, end: 3, erange: false });
        assert_eq!(strtoint("0x10", 0), StrToInt { value: 16, end: 4, erange: false });
        assert_eq!(strtoint("0x10", 16).value, 16);
        // `0xABCdef` under base 10: strtol parses the leading `0` then stops.
        assert_eq!(strtoint("  0xABCdef", 10), StrToInt { value: 0, end: 3, erange: false });
        // Trailing non-digit after digits leaves endptr at the digit boundary.
        assert_eq!(strtoint("123 456", 16), StrToInt { value: 0x123, end: 3, erange: false });
    }

    #[test]
    fn parses_bare_zero_when_hex_prefix_has_no_following_digit() {
        assert_eq!(strtoint("0x", 16), StrToInt { value: 0, end: 1, erange: false });
        assert_eq!(strtoint("0X", 16), StrToInt { value: 0, end: 1, erange: false });
        assert_eq!(strtoint("0xg", 16), StrToInt { value: 0, end: 1, erange: false });
        assert_eq!(strtoint("0x", 0), StrToInt { value: 0, end: 1, erange: false });
        assert_eq!(strtoint("0xz", 0), StrToInt { value: 0, end: 1, erange: false });
    }

    #[test]
    fn skips_full_c_isspace_set_including_vertical_tab() {
        assert_eq!(strtoint("\x0b42", 10), StrToInt { value: 42, end: 3, erange: false });
        assert_eq!(strtoint("\x0c42", 10), StrToInt { value: 42, end: 3, erange: false });
        assert_eq!(
            strtoint(" \t\n\x0b\x0c\r7", 10),
            StrToInt { value: 7, end: 7, erange: false }
        );
    }

    #[test]
    fn no_conversion_returns_zero_like_strtol() {
        // C `strtoint` never errors on no-digit input: strtol returns 0 with
        // endptr at the original start (offset 0) and no ERANGE.
        assert_eq!(strtoint("xyz", 10), StrToInt { value: 0, end: 0, erange: false });
        assert_eq!(strtoint("", 10), StrToInt { value: 0, end: 0, erange: false });
        assert_eq!(strtoint("-", 10), StrToInt { value: 0, end: 0, erange: false });
        assert_eq!(strtoint("  ", 10), StrToInt { value: 0, end: 0, erange: false });
        assert_eq!(strtoint("  -", 10), StrToInt { value: 0, end: 0, erange: false });
        assert_eq!(strtoint("z", 10), StrToInt { value: 0, end: 0, erange: false });
        // Invalid base: strtol(EINVAL) returns 0, endptr at start.
        assert_eq!(strtoint("10", 1), StrToInt { value: 0, end: 0, erange: false });
    }

    #[test]
    fn flags_erange_with_truncated_value() {
        // (int) 0x80000000 == i32::MIN; ERANGE because the value doesn't fit int.
        assert_eq!(
            strtoint("80000000", 16),
            StrToInt { value: i32::MIN, end: 8, erange: true }
        );
        // long overflow: strtol clamps to LONG_MAX, (int) LONG_MAX == -1.
        assert_eq!(
            strtoint("99999999999999999999", 0),
            StrToInt { value: -1, end: 20, erange: true }
        );
        // negative long overflow: clamps to LONG_MIN, (int) LONG_MIN == 0.
        assert_eq!(
            strtoint("-99999999999999999999", 0),
            StrToInt { value: 0, end: 21, erange: true }
        );
        // 0x2147483648 fits in long but not int: (int) low-32 == 0x47483648.
        assert_eq!(
            strtoint("2147483648", 16),
            StrToInt { value: 0x4748_3648, end: 10, erange: true }
        );
    }

    #[test]
    fn cleans_non_printable_ascii_and_non_ascii_bytes() {
        let ctx = MemoryContext::new("clean-ascii-test");
        let mcx = ctx.mcx();
        assert_eq!(pg_clean_ascii(mcx, "abc", 0).unwrap().as_str(), "abc");
        assert_eq!(
            pg_clean_ascii(mcx, "a\nb\rc\t", 0).unwrap().as_str(),
            "a\\x0ab\\x0dc\\x09"
        );
        // Non-ASCII bytes arrive as UTF-8 encodings of higher code points.
        assert_eq!(
            pg_clean_ascii(mcx, "~\u{7f}", 0).unwrap().as_str(),
            "~\\x7f"
        );
        assert_eq!(
            pg_clean_ascii(mcx, "\u{80}", 0).unwrap().as_str(),
            "\\xc2\\x80"
        );
    }

    #[test]
    fn checks_high_bit_ascii_rule() {
        assert!(pg_is_ascii("abc\x7f"));
        assert!(pg_is_ascii("abc"));
        assert!(!pg_is_ascii("snowman \u{2603}"));
    }

    #[test]
    fn strips_trailing_crlf_only() {
        assert_eq!(pg_strip_crlf("abc\r\n\r\n"), "abc");
        assert_eq!(pg_strip_crlf("a\rb\n"), "a\rb");
        assert_eq!(pg_strip_crlf("abc"), "abc");
    }

    #[test]
    fn empty_input_yields_empty_string() {
        let ctx = MemoryContext::new("clean-ascii-empty");
        assert_eq!(pg_clean_ascii(ctx.mcx(), "", 0).unwrap().as_str(), "");
    }
}
