//! Ryū floating-point output for single precision. Faithful port of
//! `src/common/f2s.c` (64-bit / non-`RYU_32_BIT_PLATFORM` code path).

use crate::common::{
    copy_special_str, float_to_bits, log10_pow2, log10_pow5, pow5bits, DIGIT_TABLE,
    STRICTLY_SHORTEST,
};

const FLOAT_MANTISSA_BITS: u32 = 23;
const FLOAT_EXPONENT_BITS: u32 = 8;
const FLOAT_BIAS: i32 = 127;

/// Required buffer length for `float_to_shortest_decimal_buf` (mirrors
/// `FLOAT_SHORTEST_DECIMAL_LEN` in `shortest_dec.h`): 16 includes the NUL.
pub const FLOAT_SHORTEST_DECIMAL_LEN: usize = 16;

const FLOAT_POW5_INV_BITCOUNT: i32 = 59;
static FLOAT_POW5_INV_SPLIT: [u64; 31] = [
    576460752303423489,
    461168601842738791,
    368934881474191033,
    295147905179352826,
    472236648286964522,
    377789318629571618,
    302231454903657294,
    483570327845851670,
    386856262276681336,
    309485009821345069,
    495176015714152110,
    396140812571321688,
    316912650057057351,
    507060240091291761,
    405648192073033409,
    324518553658426727,
    519229685853482763,
    415383748682786211,
    332306998946228969,
    531691198313966350,
    425352958651173080,
    340282366920938464,
    544451787073501542,
    435561429658801234,
    348449143727040987,
    557518629963265579,
    446014903970612463,
    356811923176489971,
    570899077082383953,
    456719261665907162,
    365375409332725730,
];
const FLOAT_POW5_BITCOUNT: i32 = 61;
static FLOAT_POW5_SPLIT: [u64; 47] = [
    1152921504606846976,
    1441151880758558720,
    1801439850948198400,
    2251799813685248000,
    1407374883553280000,
    1759218604441600000,
    2199023255552000000,
    1374389534720000000,
    1717986918400000000,
    2147483648000000000,
    1342177280000000000,
    1677721600000000000,
    2097152000000000000,
    1310720000000000000,
    1638400000000000000,
    2048000000000000000,
    1280000000000000000,
    1600000000000000000,
    2000000000000000000,
    1250000000000000000,
    1562500000000000000,
    1953125000000000000,
    1220703125000000000,
    1525878906250000000,
    1907348632812500000,
    1192092895507812500,
    1490116119384765625,
    1862645149230957031,
    1164153218269348144,
    1455191522836685180,
    1818989403545856475,
    2273736754432320594,
    1421085471520200371,
    1776356839400250464,
    2220446049250313080,
    1387778780781445675,
    1734723475976807094,
    2168404344971008868,
    1355252715606880542,
    1694065894508600678,
    2117582368135750847,
    1323488980084844279,
    1654361225106055349,
    2067951531382569187,
    1292469707114105741,
    1615587133892632177,
    2019483917365790221,
];

#[inline]
fn pow5_factor(mut value: u32) -> u32 {
    let mut count = 0u32;
    loop {
        debug_assert!(value != 0);
        let q = value / 5;
        let r = value % 5;
        if r != 0 {
            break;
        }
        value = q;
        count += 1;
    }
    count
}

#[inline]
fn multiple_of_power_of5(value: u32, p: u32) -> bool {
    pow5_factor(value) >= p
}

#[inline]
fn multiple_of_power_of2(value: u32, p: u32) -> bool {
    debug_assert!(p < 32);
    (value & ((1u32 << p) - 1)) == 0
}

#[inline]
fn mul_shift(m: u32, factor: u64, shift: i32) -> u32 {
    let factor_lo = factor as u32;
    let factor_hi = (factor >> 32) as u32;
    let bits0 = (m as u64) * (factor_lo as u64);
    let bits1 = (m as u64) * (factor_hi as u64);

    debug_assert!(shift > 32);

    let sum = (bits0 >> 32) + bits1;
    let shifted_sum = sum >> (shift - 32);
    debug_assert!(shifted_sum <= u32::MAX as u64);
    shifted_sum as u32
}

#[inline]
fn mul_pow5_inv_div_pow2(m: u32, q: u32, j: i32) -> u32 {
    mul_shift(m, FLOAT_POW5_INV_SPLIT[q as usize], j)
}

#[inline]
fn mul_pow5_div_pow2(m: u32, i: u32, j: i32) -> u32 {
    mul_shift(m, FLOAT_POW5_SPLIT[i as usize], j)
}

#[inline]
fn decimal_length(v: u32) -> u32 {
    debug_assert!(v < 1000000000);
    if v >= 100000000 {
        return 9;
    }
    if v >= 10000000 {
        return 8;
    }
    if v >= 1000000 {
        return 7;
    }
    if v >= 100000 {
        return 6;
    }
    if v >= 10000 {
        return 5;
    }
    if v >= 1000 {
        return 4;
    }
    if v >= 100 {
        return 3;
    }
    if v >= 10 {
        return 2;
    }
    1
}

/// A floating decimal representing m * 10^e.
#[derive(Clone, Copy)]
struct FloatingDecimal32 {
    mantissa: u32,
    exponent: i32,
}

// `vp /= 10` in the trailing-zeros sub-loop is a dead store that mirrors the C
// source (f2s.c) verbatim; kept for a faithful 1:1 transcription.
#[allow(unused_assignments)]
fn f2d(ieee_mantissa: u32, ieee_exponent: u32) -> FloatingDecimal32 {
    let e2: i32;
    let m2: u32;

    if ieee_exponent == 0 {
        e2 = 1 - FLOAT_BIAS - FLOAT_MANTISSA_BITS as i32 - 2;
        m2 = ieee_mantissa;
    } else {
        e2 = ieee_exponent as i32 - FLOAT_BIAS - FLOAT_MANTISSA_BITS as i32 - 2;
        m2 = (1u32 << FLOAT_MANTISSA_BITS) | ieee_mantissa;
    }

    let accept_bounds = if STRICTLY_SHORTEST {
        (m2 & 1) == 0
    } else {
        false
    };

    // Step 2: Determine the interval of legal decimal representations.
    let mv = 4 * m2;
    let mp = 4 * m2 + 2;
    let mm_shift: u32 = (ieee_mantissa != 0 || ieee_exponent <= 1) as u32;
    let mm = 4 * m2 - 1 - mm_shift;

    // Step 3: Convert to a decimal power base using 64-bit arithmetic.
    let mut vr: u32;
    let mut vp: u32;
    let mut vm: u32;
    let e10: i32;
    let mut vm_is_trailing_zeros = false;
    let mut vr_is_trailing_zeros = false;
    let mut last_removed_digit: u8 = 0;

    if e2 >= 0 {
        let q = log10_pow2(e2) as u32;
        e10 = q as i32;
        let k = FLOAT_POW5_INV_BITCOUNT + pow5bits(q as i32) as i32 - 1;
        let i = -e2 + q as i32 + k;

        vr = mul_pow5_inv_div_pow2(mv, q, i);
        vp = mul_pow5_inv_div_pow2(mp, q, i);
        vm = mul_pow5_inv_div_pow2(mm, q, i);

        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            let l = FLOAT_POW5_INV_BITCOUNT + pow5bits(q as i32 - 1) as i32 - 1;
            last_removed_digit =
                (mul_pow5_inv_div_pow2(mv, q - 1, -e2 + q as i32 - 1 + l) % 10) as u8;
        }
        if q <= 9 {
            // Only one of mp, mv, and mm can be a multiple of 5, if any.
            if mv % 5 == 0 {
                vr_is_trailing_zeros = multiple_of_power_of5(mv, q);
            } else if accept_bounds {
                vm_is_trailing_zeros = multiple_of_power_of5(mm, q);
            } else {
                vp -= multiple_of_power_of5(mp, q) as u32;
            }
        }
    } else {
        let q = log10_pow5(-e2) as u32;
        e10 = q as i32 + e2;
        let i = -e2 - q as i32;
        let k = pow5bits(i) as i32 - FLOAT_POW5_BITCOUNT;
        let mut j = q as i32 - k;

        vr = mul_pow5_div_pow2(mv, i as u32, j);
        vp = mul_pow5_div_pow2(mp, i as u32, j);
        vm = mul_pow5_div_pow2(mm, i as u32, j);

        if q != 0 && (vp - 1) / 10 <= vm / 10 {
            j = q as i32 - 1 - (pow5bits(i + 1) as i32 - FLOAT_POW5_BITCOUNT);
            last_removed_digit = (mul_pow5_div_pow2(mv, (i + 1) as u32, j) % 10) as u8;
        }
        if q <= 1 {
            // {vr,vp,vm} is trailing zeros if {mv,mp,mm} has at least q trailing
            // 0 bits. mv = 4 * m2, so it always has >= two trailing 0 bits.
            vr_is_trailing_zeros = true;
            if accept_bounds {
                vm_is_trailing_zeros = mm_shift == 1;
            } else {
                vp -= 1;
            }
        } else if q < 31 {
            vr_is_trailing_zeros = multiple_of_power_of2(mv, q - 1);
        }
    }

    // Step 4: Find the shortest decimal representation.
    let mut removed: u32 = 0;
    let output: u32;

    if vm_is_trailing_zeros || vr_is_trailing_zeros {
        // General case, which happens rarely (~4.0%).
        while vp / 10 > vm / 10 {
            vm_is_trailing_zeros &= vm - (vm / 10) * 10 == 0;
            vr_is_trailing_zeros &= last_removed_digit == 0;
            last_removed_digit = (vr % 10) as u8;
            vr /= 10;
            vp /= 10;
            vm /= 10;
            removed += 1;
        }
        if vm_is_trailing_zeros {
            while vm % 10 == 0 {
                vr_is_trailing_zeros &= last_removed_digit == 0;
                last_removed_digit = (vr % 10) as u8;
                vr /= 10;
                vp /= 10;
                vm /= 10;
                removed += 1;
            }
        }
        if vr_is_trailing_zeros && last_removed_digit == 5 && vr % 2 == 0 {
            // Round even if the exact number is .....50..0.
            last_removed_digit = 4;
        }
        output = vr
            + ((vr == vm && (!accept_bounds || !vm_is_trailing_zeros)) || last_removed_digit >= 5)
                as u32;
    } else {
        // Specialized for the common case (~96.0%).
        while vp / 10 > vm / 10 {
            last_removed_digit = (vr % 10) as u8;
            vr /= 10;
            vp /= 10;
            vm /= 10;
            removed += 1;
        }
        output = vr + (vr == vm || last_removed_digit >= 5) as u32;
    }

    let exp = e10 + removed as i32;

    FloatingDecimal32 {
        exponent: exp,
        mantissa: output,
    }
}

fn to_chars_f(v: FloatingDecimal32, olength: u32, result: &mut [u8]) -> i32 {
    // C initializes `int index = 0;`; the memset branch leaves it 0.
    let mut index: i32 = 0;
    let mut output = v.mantissa;
    let exp = v.exponent;

    let mut i: u32 = 0;
    let nexp: i32 = exp + olength as i32;

    if nexp <= 0 {
        debug_assert!(nexp >= -3);
        index = 2 - nexp;
        result[..8].copy_from_slice(b"0.000000");
    } else if exp < 0 {
        index = 1;
    } else {
        debug_assert!(exp < 6 && exp + olength as i32 <= 6);
        for b in result[..8].iter_mut() {
            *b = b'0';
        }
    }

    while output >= 10000 {
        let c = output - 10000 * (output / 10000);
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        output /= 10000;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 2..off].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        result[off - 4..off - 2].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        i += 4;
    }
    if output >= 100 {
        let c = ((output % 100) << 1) as usize;
        output /= 100;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 2..off].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
        i += 2;
    }
    if output >= 10 {
        let c = (output << 1) as usize;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 2..off].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
    } else {
        result[index as usize] = b'0' + output as u8;
    }

    if index == 1 {
        // nexp is 1..6 here.
        debug_assert!(nexp < 7);
        if nexp & 4 != 0 {
            result.copy_within(index as usize..index as usize + 4, index as usize - 1);
            index += 4;
        }
        if nexp & 2 != 0 {
            result.copy_within(index as usize..index as usize + 2, index as usize - 1);
            index += 2;
        }
        if nexp & 1 != 0 {
            result[index as usize - 1] = result[index as usize];
        }
        result[nexp as usize] = b'.';
        index = olength as i32 + 1;
    } else if exp >= 0 {
        index = olength as i32 + exp;
    } else {
        index = olength as i32 + (2 - nexp);
    }

    index
}

fn to_chars(v: FloatingDecimal32, sign: bool, result: &mut [u8]) -> i32 {
    let mut index: i32 = 0;
    let mut output = v.mantissa;
    let mut olength = decimal_length(output);
    let mut exp = v.exponent + olength as i32 - 1;

    if sign {
        result[index as usize] = b'-';
        index += 1;
    }

    // Thresholds for fixed-point output chosen to match printf defaults.
    if exp >= -4 && exp < 6 {
        return to_chars_f(v, olength, &mut result[index as usize..]) + sign as i32;
    }

    // Small-integer fast path: move trailing decimal zeros into the exponent.
    if v.exponent == 0 {
        while (output & 1) == 0 {
            let q = output / 10;
            let r = output - 10 * q;
            if r != 0 {
                break;
            }
            output = q;
            olength -= 1;
        }
    }

    let mut i: u32 = 0;

    while output >= 10000 {
        let c = output - 10000 * (output / 10000);
        output /= 10000;
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 1..off + 1].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        result[off - 3..off - 1].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        i += 4;
    }
    if output >= 100 {
        let c = ((output % 100) << 1) as usize;
        output /= 100;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 1..off + 1].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
        i += 2;
    }
    if output >= 10 {
        let c = (output << 1) as usize;
        // Decimal dot goes between these two digits, so can't memcpy.
        let off = (index + olength as i32 - i as i32) as usize;
        result[off] = DIGIT_TABLE[c + 1];
        result[index as usize] = DIGIT_TABLE[c];
    } else {
        result[index as usize] = b'0' + output as u8;
    }

    // Print decimal point if needed.
    if olength > 1 {
        result[index as usize + 1] = b'.';
        index += olength as i32 + 1;
    } else {
        index += 1;
    }

    // Print the exponent.
    result[index as usize] = b'e';
    index += 1;
    if exp < 0 {
        result[index as usize] = b'-';
        index += 1;
        exp = -exp;
    } else {
        result[index as usize] = b'+';
        index += 1;
    }

    let t = (2 * exp) as usize;
    result[index as usize..index as usize + 2].copy_from_slice(&DIGIT_TABLE[t..t + 2]);
    index += 2;

    index
}

fn f2d_small_int(ieee_mantissa: u32, ieee_exponent: u32) -> Option<FloatingDecimal32> {
    let e2 = ieee_exponent as i32 - FLOAT_BIAS - FLOAT_MANTISSA_BITS as i32;

    if e2 >= -(FLOAT_MANTISSA_BITS as i32) && e2 <= 0 {
        let mask = (1u32 << -e2) - 1;
        let fraction = ieee_mantissa & mask;
        if fraction == 0 {
            let m2 = (1u32 << FLOAT_MANTISSA_BITS) | ieee_mantissa;
            return Some(FloatingDecimal32 {
                mantissa: m2 >> -e2,
                exponent: 0,
            });
        }
    }
    None
}

/// Store the shortest decimal representation of the given float as an
/// UNTERMINATED string in the caller's supplied buffer (which must be at least
/// `FLOAT_SHORTEST_DECIMAL_LEN - 1` bytes long). Returns the number of bytes
/// stored. (Mirrors `float_to_shortest_decimal_bufn`.)
pub fn float_to_shortest_decimal_bufn(f: f32, result: &mut [u8]) -> usize {
    let bits = float_to_bits(f);

    let ieee_sign = ((bits >> (FLOAT_MANTISSA_BITS + FLOAT_EXPONENT_BITS)) & 1) != 0;
    let ieee_mantissa = bits & ((1u32 << FLOAT_MANTISSA_BITS) - 1);
    let ieee_exponent = (bits >> FLOAT_MANTISSA_BITS) & ((1u32 << FLOAT_EXPONENT_BITS) - 1);

    if ieee_exponent == ((1u32 << FLOAT_EXPONENT_BITS) - 1)
        || (ieee_exponent == 0 && ieee_mantissa == 0)
    {
        return copy_special_str(result, ieee_sign, ieee_exponent != 0, ieee_mantissa != 0);
    }

    let v = match f2d_small_int(ieee_mantissa, ieee_exponent) {
        Some(v) => v,
        None => f2d(ieee_mantissa, ieee_exponent),
    };

    to_chars(v, ieee_sign, result) as usize
}

/// Store the shortest decimal representation of the given float as a
/// null-terminated string in the caller's supplied buffer (which must be at
/// least `FLOAT_SHORTEST_DECIMAL_LEN` bytes long). Returns the string length.
/// (Mirrors `float_to_shortest_decimal_buf`.)
pub fn float_to_shortest_decimal_buf(f: f32, result: &mut [u8]) -> usize {
    let index = float_to_shortest_decimal_bufn(f, result);
    debug_assert!(index < FLOAT_SHORTEST_DECIMAL_LEN);
    result[index] = b'\0';
    index
}

/// Return the shortest decimal representation as an owned `String`. (Mirrors
/// `float_to_shortest_decimal`.)
pub fn float_to_shortest_decimal(f: f32) -> alloc::string::String {
    let mut buf = [0u8; FLOAT_SHORTEST_DECIMAL_LEN];
    let len = float_to_shortest_decimal_bufn(f, &mut buf);
    alloc::string::String::from_utf8(buf[..len].to_vec()).expect("Ryū output is ASCII")
}
