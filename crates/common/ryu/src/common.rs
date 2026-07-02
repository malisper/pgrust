// PostgreSQL diverges from upstream Ryū: it avoids emitting exact midpoints
// (STRICTLY_SHORTEST false in ryu_common.h) for reader portability.
pub const STRICTLY_SHORTEST: bool = false;

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

#[inline]
pub fn pow5bits(e: i32) -> u32 {
    debug_assert!(e >= 0 && e <= 3528);
    (((e as u32).wrapping_mul(1217359)) >> 19) + 1
}

#[inline]
pub fn log10_pow2(e: i32) -> i32 {
    debug_assert!(e >= 0 && e <= 1650);
    (((e as u32).wrapping_mul(78913)) >> 18) as i32
}

#[inline]
pub fn log10_pow5(e: i32) -> i32 {
    debug_assert!(e >= 0 && e <= 2620);
    (((e as u32).wrapping_mul(732923)) >> 20) as i32
}

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
