//! Common routines for Ryū floating-point output.
//!
//! Faithful port of `src/common/ryu_common.h`, `src/common/digit_table.h`, and
//! the `src/common/d2s_intrinsics.h` helpers, restricted to the 64-bit code
//! path (the equivalent of `HAVE_INT128` && !`RYU_32_BIT_PLATFORM`, which is
//! what every platform this workspace targets uses). Rust has native `u128` and
//! 64-bit division, so the 32-bit fallbacks and MSVC intrinsics in the C are not
//! needed; the visible numeric results are identical.

/// Upstream Ryū's output is always the shortest possible. PostgreSQL adjusts
/// that slightly to improve portability: it avoids outputting the exact midpoint
/// value between two representable floats, since that relies on the reader
/// getting the round-to-even rule correct. Defining this `true` would restore
/// the upstream behavior. (Mirrors `STRICTLY_SHORTEST` in `ryu_common.h`.)
pub const STRICTLY_SHORTEST: bool = false;

/// A table of all two-digit numbers, used to speed up decimal digit generation
/// by copying pairs of digits into the final output. (Mirrors `DIGIT_TABLE` in
/// `digit_table.h`: index `2*n .. 2*n+2` is the two ASCII digits of `n`.)
pub static DIGIT_TABLE: [u8; 200] = build_digit_table();

const fn build_digit_table() -> [u8; 200] {
    let mut t = [0u8; 200];
    let mut n = 0usize;
    while n < 100 {
        t[2 * n] = b'0' + (n / 10) as u8;
        t[2 * n + 1] = b'0' + (n % 10) as u8;
        n += 1;
    }
    t
}

/// Returns `e == 0 ? 1 : ceil(log_2(5^e))`. (Mirrors `pow5bits`.)
#[inline]
pub fn pow5bits(e: i32) -> u32 {
    // This approximation works up to the point that the multiplication
    // overflows at e = 3529.
    debug_assert!(e >= 0);
    debug_assert!(e <= 3528);
    (((e as u32).wrapping_mul(1217359)) >> 19) + 1
}

/// Returns `floor(log_10(2^e))`. (Mirrors `log10Pow2`.)
#[inline]
pub fn log10_pow2(e: i32) -> i32 {
    // The first value this approximation fails for is 2^1651.
    debug_assert!(e >= 0);
    debug_assert!(e <= 1650);
    (((e as u32).wrapping_mul(78913)) >> 18) as i32
}

/// Returns `floor(log_10(5^e))`. (Mirrors `log10Pow5`.)
#[inline]
pub fn log10_pow5(e: i32) -> i32 {
    // The first value this approximation fails for is 5^2621.
    debug_assert!(e >= 0);
    debug_assert!(e <= 2620);
    (((e as u32).wrapping_mul(732923)) >> 20) as i32
}

/// Writes the special-value string (NaN / ±Infinity / ±0) into `result` and
/// returns the number of bytes written. (Mirrors `copy_special_str`.)
#[inline]
pub fn copy_special_str(result: &mut [u8], sign: bool, exponent: bool, mantissa: bool) -> usize {
    if mantissa {
        result[..3].copy_from_slice(b"NaN");
        return 3;
    }
    let s = sign as usize;
    if sign {
        result[0] = b'-';
    }
    if exponent {
        result[s..s + 8].copy_from_slice(b"Infinity");
        return s + 8;
    }
    result[s] = b'0';
    s + 1
}

/// Reinterpret a `f32` as its raw IEEE-754 bits. (Mirrors `float_to_bits`.)
#[inline]
pub fn float_to_bits(f: f32) -> u32 {
    f.to_bits()
}

/// Reinterpret a `f64` as its raw IEEE-754 bits. (Mirrors `double_to_bits`.)
#[inline]
pub fn double_to_bits(d: f64) -> u64 {
    d.to_bits()
}
