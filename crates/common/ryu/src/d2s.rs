use crate::common::{copy_special_str, log10_pow2, log10_pow5, pow5bits, DIGIT_TABLE, STRICTLY_SHORTEST};
use crate::d2s_table::{
    DOUBLE_POW5_BITCOUNT, DOUBLE_POW5_INV_BITCOUNT, DOUBLE_POW5_INV_SPLIT, DOUBLE_POW5_SPLIT,
};

const DOUBLE_MANTISSA_BITS: u32 = 52;
const DOUBLE_EXPONENT_BITS: u32 = 11;
const DOUBLE_BIAS: i32 = 1023;

pub const DOUBLE_SHORTEST_DECIMAL_LEN: usize = 25;

#[inline]
fn div5(x: u64) -> u64 {
    x / 5
}
#[inline]
fn div10(x: u64) -> u64 {
    x / 10
}
#[inline]
fn div100(x: u64) -> u64 {
    x / 100
}
#[inline]
fn div1e8(x: u64) -> u64 {
    x / 100000000
}

#[inline]
fn pow5_factor(mut value: u64) -> u32 {
    let mut count = 0u32;
    loop {
        debug_assert!(value != 0);
        let q = div5(value);
        let r = (value - 5 * q) as u32;
        if r != 0 {
            break;
        }
        value = q;
        count += 1;
    }
    count
}

#[inline]
fn multiple_of_power_of5(value: u64, p: u32) -> bool {
    pow5_factor(value) >= p
}

#[inline]
fn multiple_of_power_of2(value: u64, p: u32) -> bool {
    debug_assert!(p < 64);
    (value & ((1u64 << p) - 1)) == 0
}

#[inline]
fn mul_shift(m: u64, mul: &[u64; 2], j: i32) -> u64 {
    let b0 = (m as u128) * (mul[0] as u128);
    let b2 = (m as u128) * (mul[1] as u128);
    (((b0 >> 64) + b2) >> (j - 64)) as u64
}

#[inline]
fn mul_shift_all(m: u64, mul: &[u64; 2], j: i32, mm_shift: u32) -> (u64, u64, u64) {
    let vp = mul_shift(4 * m + 2, mul, j);
    let vm = mul_shift(4 * m - 1 - mm_shift as u64, mul, j);
    let vr = mul_shift(4 * m, mul, j);
    (vr, vp, vm)
}

#[inline]
fn decimal_length(v: u64) -> u32 {
    // Precondition: v is not an 18-, 19-, or 20-digit number.
    debug_assert!(v < 100000000000000000);
    if v >= 10000000000000000 {
        return 17;
    }
    if v >= 1000000000000000 {
        return 16;
    }
    if v >= 100000000000000 {
        return 15;
    }
    if v >= 10000000000000 {
        return 14;
    }
    if v >= 1000000000000 {
        return 13;
    }
    if v >= 100000000000 {
        return 12;
    }
    if v >= 10000000000 {
        return 11;
    }
    if v >= 1000000000 {
        return 10;
    }
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

#[derive(Clone, Copy)]
struct FloatingDecimal64 {
    mantissa: u64,
    exponent: i32,
}

fn d2d(ieee_mantissa: u64, ieee_exponent: u32) -> FloatingDecimal64 {
    let e2: i32;
    let m2: u64;

    if ieee_exponent == 0 {
        e2 = 1 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS as i32 - 2;
        m2 = ieee_mantissa;
    } else {
        e2 = ieee_exponent as i32 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS as i32 - 2;
        m2 = (1u64 << DOUBLE_MANTISSA_BITS) | ieee_mantissa;
    }

    let accept_bounds = if STRICTLY_SHORTEST { (m2 & 1) == 0 } else { false };

    let mv = 4 * m2;
    let mm_shift: u32 = (ieee_mantissa != 0 || ieee_exponent <= 1) as u32;

    let vr: u64;
    let mut vp: u64;
    let vm: u64;
    let e10: i32;
    let mut vm_is_trailing_zeros = false;
    let mut vr_is_trailing_zeros = false;

    if e2 >= 0 {
        let q = (log10_pow2(e2) - (e2 > 3) as i32) as u32;
        let k = DOUBLE_POW5_INV_BITCOUNT + pow5bits(q as i32) as i32 - 1;
        let i = -e2 + q as i32 + k;

        e10 = q as i32;

        let (r, p, m) = mul_shift_all(m2, &DOUBLE_POW5_INV_SPLIT[q as usize], i, mm_shift);
        vr = r;
        vp = p;
        vm = m;

        if q <= 21 {
            // At most one of mp, mv, mm can be a multiple of 5.
            let mv_mod5 = (mv - 5 * div5(mv)) as u32;
            if mv_mod5 == 0 {
                vr_is_trailing_zeros = multiple_of_power_of5(mv, q);
            } else if accept_bounds {
                vm_is_trailing_zeros = multiple_of_power_of5(mv - 1 - mm_shift as u64, q);
            } else {
                vp -= multiple_of_power_of5(mv + 2, q) as u64;
            }
        }
    } else {
        let q = (log10_pow5(-e2) - (-e2 > 1) as i32) as u32;
        let i = -e2 - q as i32;
        let k = pow5bits(i) as i32 - DOUBLE_POW5_BITCOUNT;
        let j = q as i32 - k;

        e10 = q as i32 + e2;

        let (r, p, m) = mul_shift_all(m2, &DOUBLE_POW5_SPLIT[i as usize], j, mm_shift);
        vr = r;
        vp = p;
        vm = m;

        if q <= 1 {
            // mv = 4 * m2 always has >= two trailing 0 bits.
            vr_is_trailing_zeros = true;
            if accept_bounds {
                vm_is_trailing_zeros = mm_shift == 1;
            } else {
                vp -= 1;
            }
        } else if q < 63 {
            vr_is_trailing_zeros = multiple_of_power_of2(mv, q - 1);
        }
    }

    let mut removed: u32 = 0;
    let mut last_removed_digit: u8 = 0;
    let output: u64;
    let mut vr = vr;
    let mut vp = vp;
    let mut vm = vm;

    if vm_is_trailing_zeros || vr_is_trailing_zeros {
        // General case (~0.7%).
        loop {
            let vp_div10 = div10(vp);
            let vm_div10 = div10(vm);
            if vp_div10 <= vm_div10 {
                break;
            }
            let vm_mod10 = (vm - 10 * vm_div10) as u32;
            let vr_div10 = div10(vr);
            let vr_mod10 = (vr - 10 * vr_div10) as u32;
            vm_is_trailing_zeros &= vm_mod10 == 0;
            vr_is_trailing_zeros &= last_removed_digit == 0;
            last_removed_digit = vr_mod10 as u8;
            vr = vr_div10;
            vp = vp_div10;
            vm = vm_div10;
            removed += 1;
        }

        if vm_is_trailing_zeros {
            loop {
                let vm_div10 = div10(vm);
                let vm_mod10 = (vm - 10 * vm_div10) as u32;
                if vm_mod10 != 0 {
                    break;
                }
                let vp_div10 = div10(vp);
                let vr_div10 = div10(vr);
                let vr_mod10 = (vr - 10 * vr_div10) as u32;
                vr_is_trailing_zeros &= last_removed_digit == 0;
                last_removed_digit = vr_mod10 as u8;
                vr = vr_div10;
                vp = vp_div10;
                vm = vm_div10;
                removed += 1;
            }
        }

        if vr_is_trailing_zeros && last_removed_digit == 5 && vr % 2 == 0 {
            // Round even if the exact number is .....50..0.
            last_removed_digit = 4;
        }

        output = vr
            + ((vr == vm && (!accept_bounds || !vm_is_trailing_zeros)) || last_removed_digit >= 5)
                as u64;
    } else {
        // Common case (~99.3%); remove two digits at a time when possible.
        let mut round_up = false;
        let vp_div100 = div100(vp);
        let vm_div100 = div100(vm);
        if vp_div100 > vm_div100 {
            let vr_div100 = div100(vr);
            let vr_mod100 = (vr - 100 * vr_div100) as u32;
            round_up = vr_mod100 >= 50;
            vr = vr_div100;
            vp = vp_div100;
            vm = vm_div100;
            removed += 2;
        }

        loop {
            let vp_div10 = div10(vp);
            let vm_div10 = div10(vm);
            if vp_div10 <= vm_div10 {
                break;
            }
            let vr_div10 = div10(vr);
            let vr_mod10 = (vr - 10 * vr_div10) as u32;
            round_up = vr_mod10 >= 5;
            vr = vr_div10;
            vp = vp_div10;
            vm = vm_div10;
            removed += 1;
        }

        output = vr + (vr == vm || round_up) as u64;
    }

    FloatingDecimal64 {
        exponent: e10 + removed as i32,
        mantissa: output,
    }
}

fn to_chars_df(v: FloatingDecimal64, olength: u32, result: &mut [u8]) -> i32 {
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
        debug_assert!(exp < 16 && exp + olength as i32 <= 16);
        for b in result[..16].iter_mut() {
            *b = b'0';
        }
    }

    if (output >> 32) != 0 {
        let q = div1e8(output);
        let mut output2 = (output - 100000000 * q) as u32;
        let c = output2 % 10000;
        output = q;
        output2 /= 10000;
        let d = output2 % 10000;
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        let d0 = ((d % 100) << 1) as usize;
        let d1 = ((d / 100) << 1) as usize;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 2..off].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        result[off - 4..off - 2].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        result[off - 6..off - 4].copy_from_slice(&DIGIT_TABLE[d0..d0 + 2]);
        result[off - 8..off - 6].copy_from_slice(&DIGIT_TABLE[d1..d1 + 2]);
        i += 8;
    }

    let mut output2 = output as u32;

    while output2 >= 10000 {
        let c = output2 - 10000 * (output2 / 10000);
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        output2 /= 10000;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 2..off].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        result[off - 4..off - 2].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        i += 4;
    }
    if output2 >= 100 {
        let c = ((output2 % 100) << 1) as usize;
        output2 /= 100;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 2..off].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
        i += 2;
    }
    if output2 >= 10 {
        let c = (output2 << 1) as usize;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 2..off].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
    } else {
        result[index as usize] = b'0' + output2 as u8;
    }

    if index == 1 {
        debug_assert!(nexp < 16);
        if nexp & 8 != 0 {
            result.copy_within(index as usize..index as usize + 8, index as usize - 1);
            index += 8;
        }
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

fn to_chars(v: FloatingDecimal64, sign: bool, result: &mut [u8]) -> i32 {
    let mut index: i32 = 0;
    let mut output = v.mantissa;
    let mut olength = decimal_length(output);
    let mut exp = v.exponent + olength as i32 - 1;

    if sign {
        result[index as usize] = b'-';
        index += 1;
    }

    // Fixed-point thresholds chosen to match printf defaults (%.17g shape).
    if exp >= -4 && exp < 15 {
        return to_chars_df(v, olength, &mut result[index as usize..]) + sign as i32;
    }

    // exponent==0 means the small-integer fast path may carry trailing
    // decimal zeros; fold them into the exponent for scientific form.
    if v.exponent == 0 {
        while (output & 1) == 0 {
            let q = div10(output);
            let r = (output - 10 * q) as u32;
            if r != 0 {
                break;
            }
            output = q;
            olength -= 1;
        }
    }

    let mut i: u32 = 0;

    if (output >> 32) != 0 {
        let q = div1e8(output);
        let mut output2 = (output - 100000000 * q) as u32;
        output = q;
        let c = output2 % 10000;
        output2 /= 10000;
        let d = output2 % 10000;
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        let d0 = ((d % 100) << 1) as usize;
        let d1 = ((d / 100) << 1) as usize;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 1..off + 1].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        result[off - 3..off - 1].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        result[off - 5..off - 3].copy_from_slice(&DIGIT_TABLE[d0..d0 + 2]);
        result[off - 7..off - 5].copy_from_slice(&DIGIT_TABLE[d1..d1 + 2]);
        i += 8;
    }

    let mut output2 = output as u32;

    while output2 >= 10000 {
        let c = output2 - 10000 * (output2 / 10000);
        output2 /= 10000;
        let c0 = ((c % 100) << 1) as usize;
        let c1 = ((c / 100) << 1) as usize;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 1..off + 1].copy_from_slice(&DIGIT_TABLE[c0..c0 + 2]);
        result[off - 3..off - 1].copy_from_slice(&DIGIT_TABLE[c1..c1 + 2]);
        i += 4;
    }
    if output2 >= 100 {
        let c = ((output2 % 100) << 1) as usize;
        output2 /= 100;
        let off = (index + olength as i32 - i as i32) as usize;
        result[off - 1..off + 1].copy_from_slice(&DIGIT_TABLE[c..c + 2]);
        i += 2;
    }
    if output2 >= 10 {
        let c = (output2 << 1) as usize;
        // The decimal dot goes between these two digits.
        let off = (index + olength as i32 - i as i32) as usize;
        result[off] = DIGIT_TABLE[c + 1];
        result[index as usize] = DIGIT_TABLE[c];
    } else {
        result[index as usize] = b'0' + output2 as u8;
    }

    if olength > 1 {
        result[index as usize + 1] = b'.';
        index += olength as i32 + 1;
    } else {
        index += 1;
    }

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

    if exp >= 100 {
        let c = exp % 10;
        let t = (2 * (exp / 10)) as usize;
        result[index as usize..index as usize + 2].copy_from_slice(&DIGIT_TABLE[t..t + 2]);
        result[index as usize + 2] = b'0' + c as u8;
        index += 3;
    } else {
        let t = (2 * exp) as usize;
        result[index as usize..index as usize + 2].copy_from_slice(&DIGIT_TABLE[t..t + 2]);
        index += 2;
    }

    index
}

fn d2d_small_int(ieee_mantissa: u64, ieee_exponent: u32) -> Option<FloatingDecimal64> {
    let e2 = ieee_exponent as i32 - DOUBLE_BIAS - DOUBLE_MANTISSA_BITS as i32;

    if e2 >= -(DOUBLE_MANTISSA_BITS as i32) && e2 <= 0 {
        let mask = (1u64 << -e2) - 1;
        let fraction = ieee_mantissa & mask;
        if fraction == 0 {
            let m2 = (1u64 << DOUBLE_MANTISSA_BITS) | ieee_mantissa;
            return Some(FloatingDecimal64 {
                mantissa: m2 >> -e2,
                exponent: 0,
            });
        }
    }
    None
}

pub fn double_to_shortest_decimal_bufn(f: f64, result: &mut [u8]) -> usize {
    let bits = f.to_bits();

    let ieee_sign = ((bits >> (DOUBLE_MANTISSA_BITS + DOUBLE_EXPONENT_BITS)) & 1) != 0;
    let ieee_mantissa = bits & ((1u64 << DOUBLE_MANTISSA_BITS) - 1);
    let ieee_exponent =
        ((bits >> DOUBLE_MANTISSA_BITS) & ((1u64 << DOUBLE_EXPONENT_BITS) - 1)) as u32;

    if ieee_exponent == ((1u32 << DOUBLE_EXPONENT_BITS) - 1)
        || (ieee_exponent == 0 && ieee_mantissa == 0)
    {
        return copy_special_str(result, ieee_sign, ieee_exponent != 0, ieee_mantissa != 0);
    }

    let v = match d2d_small_int(ieee_mantissa, ieee_exponent) {
        Some(v) => v,
        None => d2d(ieee_mantissa, ieee_exponent),
    };

    to_chars(v, ieee_sign, result) as usize
}

pub fn double_to_shortest_decimal_buf(f: f64, result: &mut [u8]) -> usize {
    let index = double_to_shortest_decimal_bufn(f, result);
    debug_assert!(index < DOUBLE_SHORTEST_DECIMAL_LEN);
    result[index] = b'\0';
    index
}
