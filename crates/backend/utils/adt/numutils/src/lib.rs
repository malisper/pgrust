//! Port of PostgreSQL `src/backend/utils/adt/numutils.c`.
//!
//! Utility functions for I/O of the built-in numeric types: parsing signed
//! 16/32/64-bit integers (base 10, hex, octal, binary, with `_` digit
//! separators), parsing unsigned 32/64-bit integers with C `strtoul`/`strtou64`
//! base-0 semantics, and formatting integers to their decimal text.
//!
//! The formatters keep the C shape: they write into a caller-provided byte
//! buffer and return the number of bytes written (the C end pointer, as an
//! offset), so callers like datetime.c's `EncodeDateTime`/`EncodeTimezone`
//! can build one output string piecewise in a single buffer. They perform no
//! heap allocation and take no `Mcx`, exactly like the C; the only Rust
//! deviation is that no NUL terminator is written. The parsers return the
//! unconsumed tail as a `&str` where C uses the `uint*in_subr` "endloc"
//! out-parameter. Hard errors surface as [`PgError`] via `Result`; the
//! `*_safe` / `escontext` variants route to a soft [`SoftErrorContext`] when
//! one is supplied, via `::types_error::ereturn`.
//!
//! This crate has no cyclic callers, so it declares no seams.

use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

/// A table of all two-digit numbers, used to speed up decimal digit generation
/// by copying pairs of digits into the output. Mirrors C `DIGIT_TABLE`.
const DIGIT_TABLE: &[u8; 200] = b"\
0001020304050607080910111213141516171819\
2021222324252627282930313233343536373839\
4041424344454647484950515253545556575859\
6061626364656667686970717273747576777879\
8081828384858687888990919293949596979899";

/// Hex digit value for an ASCII byte, or `None` for non-hex bytes. Mirrors C
/// `hexlookup[128]`.
fn hexlookup(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Number of decimal digits in a nonzero 32-bit value. Mirrors C
/// `decimalLength32`, which derives `floor(log10(v))` from `floor(log2(v))`.
fn decimal_length32(v: u32) -> usize {
    const POWERS_OF_TEN: [u32; 10] = [
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000,
    ];
    // pg_leftmost_one_pos32(v): index of the most-significant set bit; callers
    // guard v == 0.
    let leftmost = 31 - v.leading_zeros() as i32;
    let t = ((leftmost + 1) * 1233 / 4096) as usize;
    t + usize::from(v >= POWERS_OF_TEN[t])
}

/// Number of decimal digits in a nonzero 64-bit value. Mirrors C
/// `decimalLength64`.
fn decimal_length64(v: u64) -> usize {
    const POWERS_OF_TEN: [u64; 20] = [
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000,
        10000000000,
        100000000000,
        1000000000000,
        10000000000000,
        100000000000000,
        1000000000000000,
        10000000000000000,
        100000000000000000,
        1000000000000000000,
        10000000000000000000,
    ];
    let leftmost = 63 - v.leading_zeros() as i32;
    let t = ((leftmost + 1) * 1233 / 4096) as usize;
    t + usize::from(v >= POWERS_OF_TEN[t])
}

// ---------------------------------------------------------------------------
// Signed parsers: pg_strtoint16 / 32 / 64
// ---------------------------------------------------------------------------

/// Convert input string to a signed 16-bit integer. Errors on bad format or
/// overflow. Mirrors C `pg_strtoint16`.
pub fn pg_strtoint16(s: &str) -> PgResult<i16> {
    pg_strtoint16_safe(s, None)
}

/// Soft-error variant of [`pg_strtoint16`]. When `escontext` is supplied,
/// errors are saved into it (returning `Ok(0)`) instead of propagated. Mirrors
/// C `pg_strtoint16_safe`.
pub fn pg_strtoint16_safe(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i16> {
    parse_signed::<i16>(s, "smallint", escontext)
}

/// Convert input string to a signed 32-bit integer. Mirrors C `pg_strtoint32`.
pub fn pg_strtoint32(s: &str) -> PgResult<i32> {
    pg_strtoint32_safe(s, None)
}

/// Soft-error variant of [`pg_strtoint32`]. Mirrors C `pg_strtoint32_safe`.
pub fn pg_strtoint32_safe(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i32> {
    parse_signed::<i32>(s, "integer", escontext)
}

/// Convert input string to a signed 64-bit integer. Mirrors C `pg_strtoint64`.
pub fn pg_strtoint64(s: &str) -> PgResult<i64> {
    pg_strtoint64_safe(s, None)
}

/// Soft-error variant of [`pg_strtoint64`]. Mirrors C `pg_strtoint64_safe`.
pub fn pg_strtoint64_safe(s: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i64> {
    parse_signed::<i64>(s, "bigint", escontext)
}

/// A small signed-integer abstraction so the three width-specialized C
/// functions collapse to one generic implementation without losing any width's
/// exact min/max behavior. The accumulator is `u128`, wide enough to hold any
/// of i16/i32/i64 plus its negation.
trait SignedInt: Copy {
    /// Two's-complement absolute value of the type minimum (e.g. 32768 for i16).
    fn min_unsigned_abs() -> u128;
    /// The type maximum as `u128` (e.g. 32767 for i16).
    fn max_as_u128() -> u128;
    /// Reconstruct the value from the unsigned magnitude and the sign.
    fn from_magnitude(magnitude: u128, neg: bool) -> Self;
    /// Zero, the soft-error path's return (mirrors C `ereturn(escontext, 0, …)`).
    fn zero() -> Self;
}

macro_rules! impl_signed_int {
    ($ty:ty) => {
        impl SignedInt for $ty {
            fn min_unsigned_abs() -> u128 {
                <$ty>::MIN.unsigned_abs() as u128
            }
            fn max_as_u128() -> u128 {
                <$ty>::MAX as u128
            }
            fn from_magnitude(magnitude: u128, neg: bool) -> Self {
                if neg {
                    if magnitude == <$ty>::MIN.unsigned_abs() as u128 {
                        <$ty>::MIN
                    } else {
                        -(magnitude as $ty)
                    }
                } else {
                    magnitude as $ty
                }
            }
            fn zero() -> Self {
                0
            }
        }
    };
}

impl_signed_int!(i16);
impl_signed_int!(i32);
impl_signed_int!(i64);

fn parse_signed<T: SignedInt>(
    s: &str,
    typname: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<T> {
    match parse_signed_inner::<T>(s, typname) {
        Ok(value) => Ok(value),
        Err(error) => ereturn(escontext, T::zero(), error),
    }
}

fn parse_signed_inner<T: SignedInt>(s: &str, typname: &str) -> PgResult<T> {
    let bytes = s.as_bytes();
    let mut index = 0;

    // C numutils.c splits into a base-10 "fast path" and a "slow path"; both
    // produce identical results, so the single loop below implements the slow
    // path, which is the complete spec: leading/trailing spaces, sign,
    // 0x/0o/0b prefixes, and `_` separators.

    // Skip leading whitespace.
    while bytes.get(index).is_some_and(|&b| is_space(b)) {
        index += 1;
    }

    // Handle sign.
    let mut neg = false;
    if bytes.get(index) == Some(&b'-') {
        neg = true;
        index += 1;
    } else if bytes.get(index) == Some(&b'+') {
        index += 1;
    }

    // Detect a base prefix. `firstdigit` is the index of the first digit byte;
    // a string with no digits after the prefix is invalid syntax. `is_decimal`
    // tracks the base-10 branch, which is the ONLY branch in numutils.c that
    // forbids a leading underscore (`if (ptr == firstdigit) goto
    // invalid_syntax`). The hex/octal/binary branches have no such check, so
    // e.g. `0x_1` is a valid 1 in C; only `1_` / `_1` (decimal) and a
    // trailing/dangling `_` are rejected.
    let (base, firstdigit, is_decimal) = match (bytes.get(index), bytes.get(index + 1)) {
        (Some(b'0'), Some(b'x' | b'X')) => {
            index += 2;
            (16u128, index, false)
        }
        (Some(b'0'), Some(b'o' | b'O')) => {
            index += 2;
            (8u128, index, false)
        }
        (Some(b'0'), Some(b'b' | b'B')) => {
            index += 2;
            (2u128, index, false)
        }
        _ => (10u128, index, true),
    };

    // Accumulate the magnitude unsigned to handle the two's-complement
    // most-negative value. C guards each digit with `tmp > -(PG_INT*_MIN /
    // base)` (so tmp can briefly reach |MIN| + base - 1) and applies the real
    // range check only AFTER the trailing-junk syntax checks. The split
    // matters for which error fires: e.g. int16 "32768x" is invalid_syntax in
    // C (the per-digit guard admits 32768, then the trailing 'x' is seen),
    // not out_of_range.
    let digit_guard = T::min_unsigned_abs() / base;

    let mut tmp = 0u128;
    loop {
        let Some(byte) = bytes.get(index).copied() else {
            break;
        };

        if byte == b'_' {
            // Underscore may not be first in the decimal branch only; in every
            // branch it must be followed by a digit of this base.
            if is_decimal && index == firstdigit {
                return Err(invalid_syntax(s, typname));
            }
            index += 1;
            match bytes.get(index).copied() {
                Some(next) if digit_value(next, base).is_some() => {}
                _ => return Err(invalid_syntax(s, typname)),
            }
            continue;
        }

        let Some(digit) = digit_value(byte, base) else {
            break;
        };

        // Per-digit overflow guard, mirroring C `tmp > -(PG_INT*_MIN / base)`.
        if tmp > digit_guard {
            return Err(out_of_range(s, typname));
        }
        tmp = tmp * base + digit;
        index += 1;
    }

    // Require at least one digit.
    if index == firstdigit {
        return Err(invalid_syntax(s, typname));
    }

    // Allow trailing whitespace, but not other trailing chars.
    while bytes.get(index).is_some_and(|&b| is_space(b)) {
        index += 1;
    }
    if index != bytes.len() {
        return Err(invalid_syntax(s, typname));
    }

    // Final range check, after syntax validation, like C: pg_neg_u*_overflow
    // when negative (tmp > |MIN|), `tmp > PG_INT*_MAX` when positive.
    if neg {
        if tmp > T::min_unsigned_abs() {
            return Err(out_of_range(s, typname));
        }
    } else if tmp > T::max_as_u128() {
        return Err(out_of_range(s, typname));
    }

    Ok(T::from_magnitude(tmp, neg))
}

// ---------------------------------------------------------------------------
// Unsigned parsers: uint32in_subr / uint64in_subr
// ---------------------------------------------------------------------------

/// Convert input string to an unsigned 32-bit integer, mirroring C
/// `uint32in_subr`.
///
/// `s` is parsed with C `strtoul(s, &endptr, 0)` base-0 semantics: a `0x`/`0X`
/// prefix is hexadecimal, a bare leading `0` is octal, otherwise decimal. A
/// leading `-` is accepted for backwards compatibility (the value wraps around
/// `2^32`). When `endloc` is `true` the unconsumed tail is returned as the
/// `&str`; when `false` only trailing whitespace may follow. `typname` appears
/// in error messages; `escontext` routes soft errors.
pub fn uint32in_subr<'a>(
    s: &'a str,
    endloc: bool,
    typname: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(u32, &'a str)> {
    match parse_unsigned(s, true, typname, endloc) {
        Ok(parsed) => Ok((parsed.value as u32, parsed.rest)),
        Err(error) => ereturn(escontext, (0, ""), error),
    }
}

/// Convert input string to an unsigned 64-bit integer, mirroring C
/// `uint64in_subr`. See [`uint32in_subr`] for the parsing semantics.
pub fn uint64in_subr<'a>(
    s: &'a str,
    endloc: bool,
    typname: &str,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<(u64, &'a str)> {
    match parse_unsigned(s, false, typname, endloc) {
        Ok(parsed) => Ok((parsed.value as u64, parsed.rest)),
        Err(error) => ereturn(escontext, (0, ""), error),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UnsignedParse<'a> {
    value: u128,
    rest: &'a str,
}

/// Model of C `strtoul`/`strtou64` with base 0, plus the surrounding
/// `uint*in_subr` checks.
///
/// `is_u32` selects the `uint32in_subr` backwards-compat sign handling (which
/// on a 64-bit platform also accepts a value matching after signed extension of
/// the 32-bit result to `long`); `uint64in_subr` has no such allowance.
fn parse_unsigned<'a>(
    s: &'a str,
    is_u32: bool,
    typname: &str,
    endloc: bool,
) -> PgResult<UnsignedParse<'a>> {
    let bytes = s.as_bytes();
    let mut index = 0;

    // C strtoul skips leading whitespace.
    while bytes.get(index).is_some_and(|&b| is_space(b)) {
        index += 1;
    }

    // Optional sign.
    let mut neg = false;
    if bytes.get(index) == Some(&b'-') {
        neg = true;
        index += 1;
    } else if bytes.get(index) == Some(&b'+') {
        index += 1;
    }

    // Base detection for base-0 strtoul: 0x/0X => hex; a bare leading 0 =>
    // octal (the 0 is itself the first octal digit, so a lone "0" is value 0);
    // otherwise decimal. There is no 0o/0b prefix and no `_` separator here.
    //
    // For the hex prefix, C strtoul/strtou64 only consume `0x` if a hex digit
    // follows. On a bare `0x` they BACKTRACK: the `0` is parsed (value 0) and
    // `endptr` is left pointing at the `x`, which then becomes trailing junk
    // (or the returned tail). `digit_start` is where the first digit of the
    // chosen base lives; `index` advances over consumed prefix bytes.
    let (base, digit_start) = match (bytes.get(index), bytes.get(index + 1)) {
        (Some(b'0'), Some(b'x' | b'X'))
            if bytes
                .get(index + 2)
                .copied()
                .is_some_and(|b| hexlookup(b).is_some()) =>
        {
            index += 2;
            (16u128, index)
        }
        (Some(b'0'), _) => (8u128, index),
        _ => (10u128, index),
    };

    // strtoul accumulates in `unsigned long` and reports ERANGE on overflow;
    // accumulate in u128 and treat overflow past the 64-bit backing width as
    // out_of_range, matching strtou64. The uint32in_subr narrowing below
    // tightens the 32-bit case.
    let mut cvt = 0u128;
    while let Some(byte) = bytes.get(index).copied() {
        let Some(digit) = digit_value(byte, base) else {
            break;
        };
        if cvt > (u64::MAX as u128 - digit) / base {
            return Err(out_of_range(s, typname));
        }
        cvt = cvt * base + digit;
        index += 1;
    }

    // endptr == s (no digits consumed) is C's "invalid input" signal. The
    // octal branch always has its leading `0` as a consumed digit (including a
    // bare `0x`, which backtracks to digit_start at the `0`), so this only
    // fires for an empty/sign-only/all-junk decimal input.
    if index == digit_start {
        return Err(invalid_syntax(s, typname));
    }

    let endptr = index;

    if !endloc {
        // Allow only trailing whitespace after the number.
        while bytes.get(index).is_some_and(|&b| is_space(b)) {
            index += 1;
        }
        if index != bytes.len() {
            return Err(invalid_syntax(s, typname));
        }
    }

    // Apply the sign. C strtoul negates within unsigned long modulo 2^64.
    const TWO_POW_64: u128 = 1u128 << 64;
    let value = if neg {
        TWO_POW_64.wrapping_sub(cvt) & (TWO_POW_64 - 1)
    } else {
        cvt
    };

    let value = if is_u32 {
        // uint32in_subr: result = (uint32) cvt. On a 64-bit platform where
        // unsigned long is wider than uint32, PG accepts cvt only if it
        // matches the uint32 result after either zero- or sign-extension back
        // to long; otherwise ERANGE.
        let result = (value & 0xffff_ffff) as u32;
        let zero_ext = result as u128;
        let sign_ext = (result as i32 as i64 as u64) as u128;
        if value != zero_ext && value != sign_ext {
            return Err(out_of_range(s, typname));
        }
        result as u128
    } else {
        // uint64in_subr: strtou64 already covers the whole 64-bit range.
        value
    };

    Ok(UnsignedParse {
        value,
        rest: &s[endptr..],
    })
}

// ---------------------------------------------------------------------------
// Formatters: pg_itoa / pg_ultoa_n / pg_ltoa / pg_ulltoa_n / pg_lltoa /
//             pg_ultostr_zeropad / pg_ultostr
// ---------------------------------------------------------------------------

/// Maximum bytes for an unsigned 32-bit decimal string ("4294967295").
pub const MAX_UINT32_DIGITS: usize = 10;
/// Maximum bytes for an unsigned 64-bit decimal string ("18446744073709551615").
pub const MAX_UINT64_DIGITS: usize = 20;
/// Maximum bytes for a signed 32-bit decimal string ("-2147483648").
pub const MAX_INT32_DIGITS: usize = MAX_UINT32_DIGITS + 1;
/// Maximum bytes for a signed 64-bit decimal string ("-9223372036854775808").
pub const MAX_INT64_DIGITS: usize = MAX_UINT64_DIGITS + 1;

/// Convert a signed 16-bit integer to decimal text at the start of `buf` and
/// return the number of bytes written. Mirrors C `pg_itoa`, which simply
/// delegates to `pg_ltoa` (so `buf` must be at least [`MAX_INT32_DIGITS`]
/// long; no NUL terminator is written).
pub fn pg_itoa(i: i16, buf: &mut [u8]) -> usize {
    pg_ltoa(i32::from(i), buf)
}

/// Render an unsigned 32-bit value as decimal digits into `buf` (no sign, no
/// NUL terminator) and return the number of bytes written. Mirrors C
/// `pg_ultoa_n`, including the two-digit `DIGIT_TABLE` blitting. `buf` must be
/// at least [`MAX_UINT32_DIGITS`] long.
pub fn pg_ultoa_n(mut value: u32, buf: &mut [u8]) -> usize {
    // Degenerate case.
    if value == 0 {
        buf[0] = b'0';
        return 1;
    }

    let olength = decimal_length32(value);
    let mut i = 0usize;

    while value >= 10000 {
        let c = value - 10000 * (value / 10000);
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;

        let pos = olength - i; // one past where this pair lands
        value /= 10000;

        buf[pos - 2..pos].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        buf[pos - 4..pos - 2].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        i += 4;
    }
    if value >= 100 {
        let c = ((value % 100) << 1) as usize;
        let pos = olength - i;
        value /= 100;
        buf[pos - 2..pos].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
        i += 2;
    }
    if value >= 10 {
        let c = (value << 1) as usize;
        let pos = olength - i;
        buf[pos - 2..pos].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
    } else {
        buf[0] = b'0' + value as u8;
    }

    olength
}

/// Render an unsigned 64-bit value as decimal digits into `buf` (no sign, no
/// NUL terminator) and return the number of bytes written. Mirrors C
/// `pg_ulltoa_n`. `buf` must be at least [`MAX_UINT64_DIGITS`] long.
pub fn pg_ulltoa_n(mut value: u64, buf: &mut [u8]) -> usize {
    if value == 0 {
        buf[0] = b'0';
        return 1;
    }

    let olength = decimal_length64(value);
    let mut i = 0usize;

    while value >= 100000000 {
        let q = value / 100000000;
        let value3 = (value - 100000000 * q) as u32;

        let c = value3 % 10000;
        let d = value3 / 10000;
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        let d0 = ((d % 100) << 1) as usize;
        let d1 = ((d / 100) << 1) as usize;

        let pos = olength - i;
        value = q;

        buf[pos - 2..pos].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        buf[pos - 4..pos - 2].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        buf[pos - 6..pos - 4].copy_from_slice(&DIGIT_TABLE[d0..d0 + 2]);
        buf[pos - 8..pos - 6].copy_from_slice(&DIGIT_TABLE[d1..d1 + 2]);
        i += 8;
    }

    // Switch to 32-bit for speed (matches the C tail).
    let mut value2 = value as u32;

    if value2 >= 10000 {
        let c = value2 - 10000 * (value2 / 10000);
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        let pos = olength - i;
        value2 /= 10000;
        buf[pos - 2..pos].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        buf[pos - 4..pos - 2].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        i += 4;
    }
    if value2 >= 100 {
        let c = ((value2 % 100) << 1) as usize;
        let pos = olength - i;
        value2 /= 100;
        buf[pos - 2..pos].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
        i += 2;
    }
    if value2 >= 10 {
        let c = (value2 << 1) as usize;
        let pos = olength - i;
        buf[pos - 2..pos].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
    } else {
        buf[0] = b'0' + value2 as u8;
    }

    olength
}

/// Convert a signed 32-bit integer to decimal text at the start of `buf` and
/// return the number of bytes written (no NUL terminator). Mirrors C
/// `pg_ltoa`. `buf` must be at least [`MAX_INT32_DIGITS`] long.
pub fn pg_ltoa(value: i32, buf: &mut [u8]) -> usize {
    let mut len = 0;
    let uvalue = if value < 0 {
        buf[len] = b'-';
        len += 1;
        // (uint32) 0 - uvalue: the C two's-complement negation.
        0u32.wrapping_sub(value as u32)
    } else {
        value as u32
    };
    len + pg_ultoa_n(uvalue, &mut buf[len..])
}

/// Convert a signed 64-bit integer to decimal text at the start of `buf` and
/// return the number of bytes written (no NUL terminator). Mirrors C
/// `pg_lltoa`. `buf` must be at least [`MAX_INT64_DIGITS`] long.
pub fn pg_lltoa(value: i64, buf: &mut [u8]) -> usize {
    let mut len = 0;
    let uvalue = if value < 0 {
        buf[len] = b'-';
        len += 1;
        0u64.wrapping_sub(value as u64)
    } else {
        value as u64
    };
    len + pg_ulltoa_n(uvalue, &mut buf[len..])
}

/// Write the decimal text of `value`, zero-padded on the left to at least
/// `minwidth` characters, at the start of `buf`; return the number of bytes
/// written (the C end pointer, as an offset, so callers can keep appending
/// into the same buffer — `pg_ultostr_zeropad(str, hours, 2); *str++ = ':';
/// …`). No NUL terminator is written. Mirrors C `pg_ultostr_zeropad`,
/// including the `value < 100 && minwidth == 2` shortcut. Panics if `minwidth
/// <= 0`, mirroring the C `Assert(minwidth > 0)`. `buf` must be at least
/// `max(minwidth, MAX_UINT32_DIGITS)` long.
pub fn pg_ultostr_zeropad(buf: &mut [u8], value: u32, minwidth: i32) -> usize {
    assert!(minwidth > 0, "minwidth must be positive");
    let minwidth = minwidth as usize;

    // Short cut for the common case.
    if value < 100 && minwidth == 2 {
        let idx = (value as usize) * 2;
        buf[..2].copy_from_slice(&DIGIT_TABLE[idx..idx + 2]);
        return 2;
    }

    let len = pg_ultoa_n(value, buf);
    if len >= minwidth {
        return len;
    }

    // Left-pad with zeros to minwidth (the C memmove + memset('0')).
    buf.copy_within(..len, minwidth - len);
    buf[..minwidth - len].fill(b'0');
    minwidth
}

/// Write the decimal text of an unsigned 32-bit integer at the start of `buf`
/// and return the number of bytes written (the C end pointer, as an offset,
/// for piecewise appending — see [`pg_ultostr_zeropad`]). No NUL terminator
/// is written. Mirrors C `pg_ultostr`. `buf` must be at least
/// [`MAX_UINT32_DIGITS`] long.
pub fn pg_ultostr(buf: &mut [u8], value: u32) -> usize {
    pg_ultoa_n(value, buf)
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn invalid_syntax(input: &str, typname: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {typname}: \"{input}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

fn out_of_range(input: &str, typname: &str) -> PgError {
    PgError::error(format!(
        "value \"{input}\" is out of range for type {typname}"
    ))
    .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// Numeric value of an ASCII digit byte in the given base, or `None`. Mirrors
/// both the per-base `isxdigit`/`'0'..'7'`/`'0'..'1'`/`isdigit` checks and the
/// `hexlookup` table.
fn digit_value(byte: u8, base: u128) -> Option<u128> {
    let value = if base == 16 {
        hexlookup(byte)?
    } else {
        match byte {
            b'0'..=b'9' => byte - b'0',
            _ => return None,
        }
    };
    let value = u128::from(value);
    (value < base).then_some(value)
}

/// C `isspace` for the "C" locale: space, tab, newline, vertical tab, form
/// feed, carriage return.
fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c)
}

/// This crate declares no seams; nothing to install.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

    #[test]
    fn signed_parsers_accept_postgres_bases_and_underscores() {
        assert_eq!(pg_strtoint16("32767").unwrap(), 32767);
        assert_eq!(pg_strtoint16("-32768").unwrap(), -32768);
        assert_eq!(pg_strtoint16("  +0x7fff  ").unwrap(), 32767);
        assert_eq!(pg_strtoint32("0o177777").unwrap(), 65_535);
        assert_eq!(pg_strtoint32("0b1010_0101").unwrap(), 165);
        assert_eq!(
            pg_strtoint64("9_223_372_036_854_775_807").unwrap(),
            i64::MAX
        );
        assert_eq!(
            pg_strtoint64("-9_223_372_036_854_775_808").unwrap(),
            i64::MIN
        );
    }

    #[test]
    fn signed_parsers_accept_plus_and_octal_binary_and_spaces() {
        assert_eq!(pg_strtoint32("+42").unwrap(), 42);
        assert_eq!(pg_strtoint32("\t  10  \n").unwrap(), 10);
        assert_eq!(pg_strtoint16("0").unwrap(), 0);
        assert_eq!(pg_strtoint16("-0x8000").unwrap(), i16::MIN);
        assert_eq!(pg_strtoint64("0b0").unwrap(), 0);
        assert_eq!(pg_strtoint32("0X1F").unwrap(), 31);
        assert_eq!(pg_strtoint32("0O17").unwrap(), 15);
        assert_eq!(pg_strtoint32("0B101").unwrap(), 5);
    }

    #[test]
    fn signed_parsers_reject_invalid_underscore_and_trailing_junk() {
        let error = pg_strtoint32("_1").unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(
            error.message(),
            "invalid input syntax for type integer: \"_1\""
        );

        let error = pg_strtoint32("1_").unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

        let error = pg_strtoint32("1x").unwrap_err();
        assert_eq!(
            error.message(),
            "invalid input syntax for type integer: \"1x\""
        );

        // `0x` with no following hex digit is invalid syntax (no digits after
        // the prefix => ptr == firstdigit).
        assert!(pg_strtoint32("0x").is_err());
        assert!(pg_strtoint32("0o").is_err());
        assert!(pg_strtoint32("0b").is_err());
        // empty / sign-only / spaces-only.
        assert!(pg_strtoint32("").is_err());
        assert!(pg_strtoint32("-").is_err());
        assert!(pg_strtoint32("  ").is_err());
    }

    /// numutils.c only forbids a *leading* underscore in the base-10 branch
    /// (`if (ptr == firstdigit) goto invalid_syntax`). The hex/octal/binary
    /// branches have no such guard, so an underscore right after the prefix is
    /// accepted and simply ignored. A trailing/dangling underscore is rejected
    /// in every branch (it must be followed by a digit of that base).
    #[test]
    fn underscore_after_prefix_matches_c_per_branch() {
        assert!(pg_strtoint32("_1").is_err());

        assert_eq!(pg_strtoint32("0x_1").unwrap(), 1);
        assert_eq!(pg_strtoint32("0X_ff").unwrap(), 255);
        assert_eq!(pg_strtoint32("0o_17").unwrap(), 15);
        assert_eq!(pg_strtoint32("0b_101").unwrap(), 5);
        assert_eq!(pg_strtoint16("-0x_8000").unwrap(), i16::MIN);
        assert_eq!(pg_strtoint32("0xff_ff").unwrap(), 0xffff);

        assert!(pg_strtoint32("0x_").is_err());
        assert!(pg_strtoint32("0o_").is_err());
        assert!(pg_strtoint32("0b_").is_err());
        assert!(pg_strtoint32("0xff_").is_err());
        assert!(pg_strtoint32("0x1_g").is_err());
        assert!(pg_strtoint32("1_").is_err());
    }

    #[test]
    fn signed_parsers_report_overflow_with_pg_messages() {
        let error = pg_strtoint16("32768").unwrap_err();
        assert_eq!(error.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(
            error.message(),
            "value \"32768\" is out of range for type smallint"
        );

        let error = pg_strtoint32("-2147483649").unwrap_err();
        assert_eq!(
            error.message(),
            "value \"-2147483649\" is out of range for type integer"
        );

        assert_eq!(pg_strtoint32("2147483647").unwrap(), i32::MAX);
        assert_eq!(pg_strtoint32("-2147483648").unwrap(), i32::MIN);
        assert!(pg_strtoint32("2147483648").is_err());
        assert_eq!(pg_strtoint16("-32768").unwrap(), i16::MIN);
        assert!(pg_strtoint16("-32769").is_err());
    }

    #[test]
    fn safe_signed_parser_saves_soft_error_and_returns_zero() {
        let mut context = SoftErrorContext::new(true);

        let value = pg_strtoint32_safe("bad", Some(&mut context)).unwrap();

        assert_eq!(value, 0);
        assert!(context.error_occurred());
        assert_eq!(
            context.error().unwrap().message(),
            "invalid input syntax for type integer: \"bad\""
        );
    }

    #[test]
    fn safe_signed_parser_without_details_only_marks() {
        let mut context = SoftErrorContext::new(false);
        let value = pg_strtoint32_safe("bad", Some(&mut context)).unwrap();
        assert_eq!(value, 0);
        assert!(context.error_occurred());
        assert!(context.error().is_none());
    }

    #[test]
    fn unsigned_parsers_handle_rest_and_trailing_whitespace() {
        assert_eq!(
            uint32in_subr("42 rest", true, "oid", None).unwrap(),
            (42, " rest")
        );
        assert_eq!(
            uint64in_subr("18446744073709551615  ", false, "xid", None).unwrap(),
            (u64::MAX, "  ")
        );
        assert_eq!(
            uint32in_subr("-1", false, "oid", None).unwrap(),
            (u32::MAX, "")
        );
        assert_eq!(
            uint64in_subr("-1", false, "xid", None).unwrap(),
            (u64::MAX, "")
        );
    }

    /// `uint{32,64}in_subr` mirror C `strtoul`/`strtou64` with base 0: a bare
    /// leading `0` is octal and a `0x` prefix is hexadecimal (unlike the
    /// signed `pg_strtoint*` parsers, which require `0o`/`0x`/`0b`).
    #[test]
    fn unsigned_parsers_use_strtoul_base0_octal_and_hex() {
        assert_eq!(uint32in_subr("010", false, "xid", None).unwrap(), (8, ""));
        assert_eq!(uint64in_subr("010", false, "xid8", None).unwrap(), (8, ""));
        assert_eq!(uint32in_subr("0", false, "xid", None).unwrap(), (0, ""));
        assert_eq!(
            uint32in_subr("0xffffffff", false, "xid", None).unwrap(),
            (u32::MAX, "")
        );
        assert_eq!(
            uint64in_subr("0xffffffffffffffff", false, "xid8", None).unwrap(),
            (u64::MAX, "")
        );
        // No 0o / 0b prefixes: the leading 0 is octal, so the trailing
        // 'o'/'b' is junk -> invalid syntax.
        assert!(uint32in_subr("0o17", false, "xid", None).is_err());
        assert!(uint32in_subr("0b101", false, "xid", None).is_err());
        // 08 is not a valid octal number ('8' is junk after octal '0').
        assert!(uint32in_subr("08", false, "xid", None).is_err());
    }

    /// C `strtoul`/`strtou64` with base 0 only consume the `0x` prefix when a
    /// hex digit follows. On a bare `0x` they BACKTRACK: the `0` is parsed
    /// (value 0) and `endptr` is left at the `x`. With `endloc == false` the
    /// `x` is trailing junk (invalid syntax); with `endloc == true` the value
    /// is 0 and the returned tail begins at the `x`.
    #[test]
    fn unsigned_bare_0x_backtracks_to_zero() {
        assert_eq!(uint32in_subr("0x", true, "xid", None).unwrap(), (0, "x"));
        assert_eq!(uint64in_subr("0X", true, "xid8", None).unwrap(), (0, "X"));
        assert_eq!(uint32in_subr("0xg", true, "xid", None).unwrap(), (0, "xg"));
        // A valid hex value still parses fully (no spurious backtrack).
        assert_eq!(uint32in_subr("0x10", true, "xid", None).unwrap(), (16, ""));
        // Without endloc, the dangling 'x' is trailing junk -> invalid syntax.
        assert!(uint32in_subr("0x", false, "xid", None).is_err());
        assert!(uint64in_subr("0x", false, "xid8", None).is_err());
        // The error message reports the whole original string, like C.
        let error = uint32in_subr("0x", false, "xid", None).unwrap_err();
        assert_eq!(error.message(), "invalid input syntax for type xid: \"0x\"");
    }

    #[test]
    fn unsigned_parsers_reject_invalid_or_out_of_range_input() {
        let error = uint32in_subr("12x", false, "oid", None).unwrap_err();
        assert_eq!(error.message(), "invalid input syntax for type oid: \"12x\"");

        let error = uint32in_subr("4294967296", false, "oid", None).unwrap_err();
        assert_eq!(
            error.message(),
            "value \"4294967296\" is out of range for type oid"
        );

        let error = uint32in_subr("-2147483649", false, "oid", None).unwrap_err();
        assert_eq!(
            error.message(),
            "value \"-2147483649\" is out of range for type oid"
        );

        // u64 overflow past 2^64-1.
        assert!(uint64in_subr("18446744073709551616", false, "xid8", None).is_err());
    }

    #[test]
    fn unsigned_safe_saves_soft_error() {
        let mut context = SoftErrorContext::new(true);
        let (value, rest) = uint32in_subr("nope", false, "oid", Some(&mut context)).unwrap();
        assert_eq!((value, rest), (0, ""));
        assert!(context.error_occurred());
    }

    /// Test-only owned-string views over the buffer-writing formatters.
    fn itoa(v: i16) -> String {
        let mut buf = [0u8; MAX_INT32_DIGITS];
        let len = pg_itoa(v, &mut buf);
        String::from_utf8(buf[..len].to_vec()).unwrap()
    }

    fn ultoa_n(v: u32) -> String {
        let mut buf = [0u8; MAX_UINT32_DIGITS];
        let len = pg_ultoa_n(v, &mut buf);
        String::from_utf8(buf[..len].to_vec()).unwrap()
    }

    fn ltoa(v: i32) -> String {
        let mut buf = [0u8; MAX_INT32_DIGITS];
        let len = pg_ltoa(v, &mut buf);
        String::from_utf8(buf[..len].to_vec()).unwrap()
    }

    fn ulltoa_n(v: u64) -> String {
        let mut buf = [0u8; MAX_UINT64_DIGITS];
        let len = pg_ulltoa_n(v, &mut buf);
        String::from_utf8(buf[..len].to_vec()).unwrap()
    }

    fn lltoa(v: i64) -> String {
        let mut buf = [0u8; MAX_INT64_DIGITS];
        let len = pg_lltoa(v, &mut buf);
        String::from_utf8(buf[..len].to_vec()).unwrap()
    }

    fn ultostr(v: u32) -> String {
        let mut buf = [0u8; MAX_UINT32_DIGITS];
        let len = pg_ultostr(&mut buf, v);
        String::from_utf8(buf[..len].to_vec()).unwrap()
    }

    fn ultostr_zeropad(v: u32, minwidth: i32) -> String {
        let mut buf = [0u8; 32];
        let len = pg_ultostr_zeropad(&mut buf, v, minwidth);
        String::from_utf8(buf[..len].to_vec()).unwrap()
    }

    #[test]
    fn decimal_formatters_match_postgres_text() {
        assert_eq!(itoa(-32768), "-32768");
        assert_eq!(ultoa_n(4_294_967_295), "4294967295");
        assert_eq!(ltoa(i32::MIN), "-2147483648");
        assert_eq!(ulltoa_n(u64::MAX), "18446744073709551615");
        assert_eq!(lltoa(i64::MIN), "-9223372036854775808");
        assert_eq!(ultostr_zeropad(42, 5), "00042");
        assert_eq!(ultostr(42), "42");
    }

    #[test]
    fn formatters_cover_digit_table_boundaries() {
        assert_eq!(ultoa_n(0), "0");
        assert_eq!(ultoa_n(7), "7");
        assert_eq!(ultoa_n(99), "99");
        assert_eq!(ultoa_n(100), "100");
        assert_eq!(ultoa_n(9999), "9999");
        assert_eq!(ultoa_n(10000), "10000");
        assert_eq!(ultoa_n(123456789), "123456789");

        assert_eq!(ulltoa_n(0), "0");
        assert_eq!(ulltoa_n(99999999), "99999999");
        assert_eq!(ulltoa_n(100000000), "100000000");
        assert_eq!(ulltoa_n(1234567890123456789), "1234567890123456789");

        assert_eq!(ltoa(0), "0");
        assert_eq!(ltoa(-1), "-1");
        assert_eq!(ltoa(i32::MAX), "2147483647");
        assert_eq!(itoa(0), "0");
        assert_eq!(itoa(i16::MAX), "32767");
        assert_eq!(lltoa(i64::MAX), "9223372036854775807");
    }

    /// Cross-check every formatter against the standard library over a range
    /// of values, including the digit-table loop boundaries.
    #[test]
    fn formatters_match_std_to_string_over_range() {
        for v in [
            0u32, 1, 9, 10, 99, 100, 999, 1000, 9999, 10000, 99999, 100000, 999_999_999,
            1_000_000_000,
            u32::MAX,
        ] {
            assert_eq!(ultoa_n(v), v.to_string());
            assert_eq!(ultostr(v), v.to_string());
        }
        for v in [0i32, 1, -1, 12345, -12345, i32::MAX, i32::MIN, i32::MIN + 1] {
            assert_eq!(ltoa(v), v.to_string());
        }
        for v in [
            0u64,
            1,
            99_999_999,
            100_000_000,
            9_999_999_999_999_999_999,
            u64::MAX,
        ] {
            assert_eq!(ulltoa_n(v), v.to_string());
        }
        for v in [0i64, -1, 1, i64::MAX, i64::MIN, i64::MIN + 1] {
            assert_eq!(lltoa(v), v.to_string());
        }
    }

    #[test]
    fn zeropad_shortcut_and_overflow_width() {
        // value < 100 && minwidth == 2 shortcut.
        assert_eq!(ultostr_zeropad(7, 2), "07");
        assert_eq!(ultostr_zeropad(42, 2), "42");
        assert_eq!(ultostr_zeropad(0, 2), "00");
        // value already wider than minwidth: no padding.
        assert_eq!(ultostr_zeropad(12345, 3), "12345");
        // wide padding.
        assert_eq!(ultostr_zeropad(5, 6), "000005");
        // value >= 100 with minwidth 2 takes the general path, not the
        // shortcut.
        assert_eq!(ultostr_zeropad(100, 2), "100");
    }

    /// The C intended use-case: build one string piecewise in a single buffer
    /// (datetime.c's `str = pg_ultostr_zeropad(str, hours, 2); *str++ = ':';
    /// …`), tracking the end pointer as an offset.
    #[test]
    fn formatters_append_piecewise_into_one_buffer() {
        let mut buf = [0u8; 32];
        let mut pos = 0;
        pos += pg_ultostr_zeropad(&mut buf[pos..], 9, 2);
        buf[pos] = b':';
        pos += 1;
        pos += pg_ultostr_zeropad(&mut buf[pos..], 5, 2);
        buf[pos] = b':';
        pos += 1;
        pos += pg_ultostr_zeropad(&mut buf[pos..], 42, 2);
        buf[pos] = b' ';
        pos += 1;
        pos += pg_ultostr(&mut buf[pos..], 2026);
        assert_eq!(&buf[..pos], b"09:05:42 2026");
    }

    #[test]
    #[should_panic]
    fn zeropad_requires_positive_minwidth() {
        let mut buf = [0u8; 32];
        let _ = pg_ultostr_zeropad(&mut buf, 1, 0);
    }
}
