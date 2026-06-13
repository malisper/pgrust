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
use types_error::PgResult;

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

/// `strtoint` — like `strtol`, but returns `i32`. Parses a leading integer in
/// `base` (with the libc semantics: leading C-`isspace`, optional sign,
/// optional `0x`/octal prefix when `base` is 0 or 16), returning the value and
/// the offset of the first unconsumed byte. Out-of-`i32`-range sets the
/// `OutOfRange` error (the C analog sets `errno = ERANGE`).
pub fn strtoint(s: &str, base: u32) -> Result<i32, StrToIntError> {
    strtoint_prefix(s, base).map(|parsed| parsed.value)
}

/// As [`strtoint`], but also returns the byte offset where parsing stopped
/// (the C `endptr`).
pub fn strtoint_prefix(s: &str, base: u32) -> Result<StrToInt, StrToIntError> {
    let parsed = parse_c_long_prefix(s, base)?;

    if parsed.value < i32::MIN as i64 || parsed.value > i32::MAX as i64 {
        return Err(StrToIntError {
            value: parsed.value,
            end: parsed.end,
            kind: StrToIntErrorKind::OutOfRange,
        });
    }

    Ok(StrToInt {
        value: parsed.value as i32,
        end: parsed.end,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrToInt {
    pub value: i32,
    pub end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ParsedLong {
    value: i64,
    end: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StrToIntError {
    pub value: i64,
    pub end: usize,
    pub kind: StrToIntErrorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrToIntErrorKind {
    InvalidBase,
    NoDigits,
    OutOfRange,
}

fn parse_c_long_prefix(text: &str, base: u32) -> Result<ParsedLong, StrToIntError> {
    if base != 0 && !(2..=36).contains(&base) {
        return Err(StrToIntError {
            value: 0,
            end: 0,
            kind: StrToIntErrorKind::InvalidBase,
        });
    }

    let bytes = text.as_bytes();
    let mut index = bytes
        .iter()
        .position(|byte| !is_c_space(*byte))
        .unwrap_or(bytes.len());
    let start = index;
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

    while let Some(&byte) = bytes.get(index) {
        let Some(digit) = digit_value(byte) else {
            break;
        };
        if digit >= actual_base {
            break;
        }

        value = match value
            .checked_mul(actual_base as i64)
            .and_then(|value| value.checked_add(digit as i64))
        {
            Some(value) => value,
            None => {
                return Err(StrToIntError {
                    value: if negative { i64::MIN } else { i64::MAX },
                    end: index,
                    kind: StrToIntErrorKind::OutOfRange,
                });
            }
        };
        index += 1;
    }

    if index == digits_start {
        return Err(StrToIntError {
            value: 0,
            end: start,
            kind: StrToIntErrorKind::NoDigits,
        });
    }

    let value = if negative {
        value.checked_neg().ok_or(StrToIntError {
            value: i64::MIN,
            end: index,
            kind: StrToIntErrorKind::OutOfRange,
        })?
    } else {
        value
    };

    Ok(ParsedLong { value, end: index })
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
    common_string_seams::pg_clean_ascii::set(pg_clean_ascii);
}

#[cfg(test)]
mod tests {
    use super::*;
    use mcx::MemoryContext;

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
            strtoint_prefix("  -123xyz", 10),
            Ok(StrToInt { value: -123, end: 6 })
        );
        assert_eq!(strtoint("7fffffff", 16), Ok(i32::MAX));
        assert_eq!(strtoint("010", 0), Ok(8));
        assert_eq!(strtoint("0x10", 0), Ok(16));
        assert_eq!(strtoint("0x10", 16), Ok(16));
    }

    #[test]
    fn parses_bare_zero_when_hex_prefix_has_no_following_digit() {
        assert_eq!(strtoint_prefix("0x", 16), Ok(StrToInt { value: 0, end: 1 }));
        assert_eq!(strtoint_prefix("0X", 16), Ok(StrToInt { value: 0, end: 1 }));
        assert_eq!(strtoint_prefix("0xg", 16), Ok(StrToInt { value: 0, end: 1 }));
        assert_eq!(strtoint("0x", 0), Ok(0));
        assert_eq!(strtoint("0xz", 0), Ok(0));
    }

    #[test]
    fn skips_full_c_isspace_set_including_vertical_tab() {
        assert_eq!(strtoint_prefix("\x0b42", 10), Ok(StrToInt { value: 42, end: 3 }));
        assert_eq!(strtoint_prefix("\x0c42", 10), Ok(StrToInt { value: 42, end: 3 }));
        assert_eq!(
            strtoint_prefix(" \t\n\x0b\x0c\r7", 10),
            Ok(StrToInt { value: 7, end: 7 })
        );
    }

    #[test]
    fn reports_integer_errors() {
        assert_eq!(
            strtoint("80000000", 16),
            Err(StrToIntError {
                value: 2_147_483_648,
                end: 8,
                kind: StrToIntErrorKind::OutOfRange,
            })
        );
        assert_eq!(
            strtoint("xyz", 10),
            Err(StrToIntError {
                value: 0,
                end: 0,
                kind: StrToIntErrorKind::NoDigits,
            })
        );
        assert_eq!(
            strtoint("10", 1),
            Err(StrToIntError {
                value: 0,
                end: 0,
                kind: StrToIntErrorKind::InvalidBase,
            })
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
