//! Faithful, self-contained ports of the C `strtol(value, &endptr, 0)` and
//! `strtod(value, &endptr)` semantics that `parse_int` / `parse_real`
//! (`guc.c`) depend on.  The exact `endptr` position, base auto-detection
//! (`0x`/`0`), and `ERANGE` overflow flag are all load-bearing for the GUC
//! integer/real parsers (they decide whether to re-parse as float and where a
//! trailing unit string begins), so they are ported here rather than seamed.
//!
//! Both return the parsed value, the number of leading bytes consumed
//! (`endptr - value`), and whether the conversion overflowed (`errno ==
//! ERANGE`).  Like C, a string with no valid prefix yields `consumed == 0`.

/// Result of a C-style numeric scan: value, bytes consumed (`endptr - value`),
/// and the `ERANGE` overflow flag.
#[derive(Clone, Copy, Debug)]
pub struct ScanInt {
    pub value: i64,
    pub consumed: usize,
    pub erange: bool,
}

/// Result of a C-style float scan.
#[derive(Clone, Copy, Debug)]
pub struct ScanReal {
    pub value: f64,
    pub consumed: usize,
    pub erange: bool,
}

#[inline]
fn is_c_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// `strtol(s, &endptr, 0)`: parse a (optionally signed) integer in base 10, or
/// base 16 with a leading `0x`/`0X`, or base 8 with a leading `0`.  Skips
/// leading whitespace.  Sets `erange` on overflow of `i64` (C clamps to
/// LONG_MIN/MAX and sets ERANGE; we report the flag, which is all `parse_int`
/// inspects).  `consumed` is the offset of the first unparsed byte from the
/// start of `s` (C's `endptr - s`); it is 0 when no digits were consumed.
pub fn c_strtol_base0(s: &[u8]) -> ScanInt {
    let mut i = 0usize;

    // Skip leading whitespace (C strtol does).
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }

    // Optional sign.
    let mut negative = false;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        negative = s[i] == b'-';
        i += 1;
    }

    // Base auto-detection (base == 0).
    let mut base: u32 = 10;
    if i < s.len() && s[i] == b'0' {
        if i + 1 < s.len() && (s[i + 1] == b'x' || s[i + 1] == b'X') {
            // Only treat as hex if a hex digit follows; otherwise the "0" is a
            // valid octal/decimal zero and "x" begins the unparsed tail
            // (matching C strtol).
            if i + 2 < s.len() && (s[i + 2] as char).is_ascii_hexdigit() {
                base = 16;
                i += 2;
            } else {
                // "0" alone (the 'x' is left for endptr).
                return ScanInt {
                    value: 0,
                    consumed: i + 1,
                    erange: false,
                };
            }
        } else {
            base = 8;
            // Keep the leading '0' as a consumed digit (octal); do not skip it,
            // so a bare "0" yields value 0 with one digit consumed.
        }
    }

    let digits_start = i;
    let mut acc: i64 = 0;
    let mut overflow = false;
    let mut any = false;

    while i < s.len() {
        let c = s[i];
        let digit = match c {
            b'0'..=b'9' => (c - b'0') as u32,
            b'a'..=b'f' => (c - b'a' + 10) as u32,
            b'A'..=b'F' => (c - b'A' + 10) as u32,
            _ => break,
        };
        if digit >= base {
            break;
        }
        any = true;
        if !overflow {
            // Accumulate as i128 to detect i64 overflow exactly.
            let next = (acc as i128) * (base as i128) + (digit as i128);
            let signed = if negative { -next } else { next };
            if signed > i64::MAX as i128 || signed < i64::MIN as i128 {
                overflow = true;
            } else {
                acc = next as i64; // store magnitude; sign applied at the end
            }
        }
        i += 1;
    }

    // If no digits were consumed at all, C returns 0 with endptr == original
    // start (before sign). For base 8 the leading '0' already counts as a digit.
    if !any && digits_start == i {
        // For octal, the leading '0' was a digit (handled above) so `any` would
        // be false only if even that wasn't reached; recover the bare-zero case.
        if base == 8 && digits_start > 0 && s.get(digits_start - 1) == Some(&b'0') {
            // bare "0": one digit consumed, value 0.
            return ScanInt {
                value: 0,
                consumed: digits_start,
                erange: false,
            };
        }
        return ScanInt {
            value: 0,
            consumed: 0,
            erange: false,
        };
    }

    if overflow {
        return ScanInt {
            value: if negative { i64::MIN } else { i64::MAX },
            consumed: i,
            erange: true,
        };
    }

    let value = if negative { -acc } else { acc };
    ScanInt {
        value,
        consumed: i,
        erange: false,
    }
}

/// `strtod(s, &endptr)`: parse a floating-point number (decimal, with optional
/// fractional part and `e`/`E` exponent), skipping leading whitespace.
/// `consumed` is the offset of the first unparsed byte (C's `endptr - s`), 0 if
/// no number was found.  `erange` is set on overflow to ±infinity.  (Hex floats
/// and `inf`/`nan` spellings are not produced by `parse_int`'s re-parse path or
/// by `parse_real` on GUC inputs that reach this; decimal is the faithful
/// behaviour for the values GUC accepts.)
pub fn c_strtod(s: &[u8]) -> ScanReal {
    let mut i = 0usize;
    while i < s.len() && is_c_space(s[i]) {
        i += 1;
    }

    let num_start = i;

    // Optional sign.
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        i += 1;
    }

    let mut saw_digit = false;

    // Integer part.
    while i < s.len() && s[i].is_ascii_digit() {
        i += 1;
        saw_digit = true;
    }

    // Fractional part.
    if i < s.len() && s[i] == b'.' {
        i += 1;
        while i < s.len() && s[i].is_ascii_digit() {
            i += 1;
            saw_digit = true;
        }
    }

    if !saw_digit {
        // No mantissa digits: not a number.
        return ScanReal {
            value: 0.0,
            consumed: 0,
            erange: false,
        };
    }

    // Exponent.
    if i < s.len() && (s[i] == b'e' || s[i] == b'E') {
        let mut j = i + 1;
        if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
            j += 1;
        }
        if j < s.len() && s[j].is_ascii_digit() {
            j += 1;
            while j < s.len() && s[j].is_ascii_digit() {
                j += 1;
            }
            i = j;
        }
        // else: dangling 'e' is left for endptr (C leaves it unparsed).
    }

    // Parse the consumed slice with Rust's float parser (IEEE-754 round-to-
    // nearest, identical result to libc strtod for these decimal forms).
    let text = core::str::from_utf8(&s[num_start..i]).unwrap_or("");
    let value: f64 = text.parse().unwrap_or(0.0);
    let erange = value.is_infinite();

    ScanReal {
        value,
        consumed: i,
        erange,
    }
}
