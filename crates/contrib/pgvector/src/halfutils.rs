//! pgvector 0.8.5 halfutils.h software conversion paths (the F16C / _Float16
//! arms produce the same IEEE round-to-nearest-even results). DIVERGENCES
//! (recorded): none; SIMD dispatch (halfutils.c HalfvecL2SquaredDistance*
//! target clones) is not ported — kernels live in half.rs as scalar loops.

use types_error::{PgError, PgResult, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

#[inline]
pub fn half_is_nan(h: u16) -> bool { (h & 0x7C00) == 0x7C00 && (h & 0x03FF) != 0 }
#[inline]
pub fn half_is_inf(h: u16) -> bool { (h & 0x7FFF) == 0x7C00 }
#[inline]
pub fn half_is_zero(h: u16) -> bool { (h & 0x7FFF) == 0 }

/// C: HalfToFloat4 (software arm).
pub fn half_to_float4(h: u16) -> f32 {
    let bin = h as u32;
    let mut exponent = (bin & 0x7C00) >> 10;
    let mut mantissa = bin & 0x03FF;
    let mut result = (bin & 0x8000) << 16;
    if exponent == 31 {
        result |= if mantissa == 0 { 0x7F80_0000 } else { 0x7FC0_0000 };
    } else if exponent == 0 {
        if mantissa != 0 {
            let mut e: i32 = -14;
            for _ in 0..10 {
                mantissa <<= 1;
                e -= 1;
                if (mantissa >> 10) % 2 == 1 {
                    mantissa &= 0x03FF;
                    break;
                }
            }
            result |= ((e + 127) as u32) << 23;
        }
    } else {
        result |= (exponent - 15 + 127) << 23;
    }
    let _ = &mut exponent;
    result |= mantissa << 13;
    f32::from_bits(result)
}

/// C: Float4ToHalfUnchecked (software arm): round to nearest even, overflow -> inf.
pub fn float4_to_half_unchecked(num: f32) -> u16 {
    let bin = num.to_bits();
    let mut exponent = ((bin & 0x7F80_0000) >> 23) as i32;
    let mut mantissa = (bin & 0x007F_FFFF) as i32;
    let mut result: u16 = ((bin & 0x8000_0000) >> 16) as u16;
    if num.is_infinite() {
        result |= 0x7C00;
    } else if num.is_nan() {
        result |= 0x7E00;
        result |= (mantissa >> 13) as u16;
    } else if exponent > 98 {
        exponent -= 127;
        let mut s = mantissa & 0x0000_0FFF;
        if exponent < -14 {
            let diff = -exponent - 14;
            mantissa >>= diff;
            mantissa += 1 << (23 - diff);
            s |= mantissa & 0x0000_0FFF;
        }
        let mut m = mantissa >> 13;
        let gr = (mantissa >> 12) % 4;
        if gr == 3 || (gr == 1 && s != 0) {
            m += 1;
        }
        if m == 1024 {
            m = 0;
            exponent += 1;
        }
        if exponent > 15 {
            result |= 0x7C00;
        } else {
            if exponent >= -14 {
                result |= ((exponent + 15) as u16) << 10;
            }
            result |= m as u16;
        }
    }
    result
}

/// C: Float4ToHalf — overflow of a finite input is an error.
pub fn float4_to_half(num: f32) -> PgResult<u16> {
    let r = float4_to_half_unchecked(num);
    if half_is_inf(r) && !num.is_infinite() {
        let mut buf = [0u8; ryu::FLOAT_SHORTEST_DECIMAL_LEN];
        let n = ryu::float_to_shortest_decimal_bufn(num, &mut buf);
        return Err(PgError::error(format!(
            "\"{}\" is out of range for type halfvec",
            String::from_utf8_lossy(&buf[..n])
        ))
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .into());
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;

    // (f32 bits, expected half bits) — hand-derived from IEEE 754 binary16.
    const F32_TO_HALF: &[(u32, u16)] = &[
        (0x0000_0000, 0x0000), // +0
        (0x8000_0000, 0x8000), // -0
        (0x3F80_0000, 0x3C00), // 1.0
        (0xC000_0000, 0xC000), // -2.0
        (0x477F_E000, 0x7BFF), // 65504 = max half
        (0x477F_F000, 0x7C00), // 65520 rounds to +inf
        (0x3DCC_CCCD, 0x2E66), // 0.1
        (0x3F80_2000, 0x3C01), // 1 + 2^-10 exact
        (0x3F80_1000, 0x3C00), // 1 + 2^-11: tie -> even (down)
        (0x3F80_3000, 0x3C02), // 1 + 3*2^-11: tie -> even (up)
        (0x3380_0000, 0x0001), // 2^-24 smallest subnormal
        (0x3300_0000, 0x0000), // 2^-25: tie with 0 -> even -> 0
        (0x3340_0000, 0x0001), // 1.5*2^-25 -> rounds up to smallest subnormal
        (0x387F_C000, 0x03FF), // largest subnormal 6.097e-5
        (0x3880_0000, 0x0400), // 2^-14 smallest normal
        (0x7F80_0000, 0x7C00), // +inf
        (0xFF80_0000, 0xFC00), // -inf
    ];

    #[test]
    fn float4_to_half_matches_ieee_vectors() {
        for &(fb, hb) in F32_TO_HALF {
            assert_eq!(float4_to_half_unchecked(f32::from_bits(fb)), hb, "f32 bits {fb:#x}");
        }
    }

    #[test]
    fn half_to_float4_round_trips_exact_halves() {
        // Direct assertions for values that should round-trip exactly.
        // Excluded: 0x2E66 (0.1 rounded), 0x3C00 from tie (0x3F80_1000),
        // 0x3C02 from tie (0x3F80_3000), 0x0000 from tie (0x3300_0000)
        assert_eq!(half_to_float4(0x0000).to_bits(), 0x0000_0000);
        assert_eq!(half_to_float4(0x8000).to_bits(), 0x8000_0000);
        assert_eq!(half_to_float4(0xC000).to_bits(), 0xC000_0000);
        assert_eq!(half_to_float4(0x7BFF).to_bits(), 0x477F_E000);
        assert_eq!(half_to_float4(0x7C00).to_bits(), 0x7F80_0000);
        assert_eq!(half_to_float4(0x3C01).to_bits(), 0x3F80_2000);
        assert_eq!(half_to_float4(0x0001).to_bits(), 0x3380_0000);
        assert_eq!(half_to_float4(0x03FF).to_bits(), 0x387F_C000);
        assert_eq!(half_to_float4(0x0400).to_bits(), 0x3880_0000);
        assert_eq!(half_to_float4(0xFC00).to_bits(), 0xFF80_0000);

        // Additional explicit assertions
        assert_eq!(half_to_float4(0x3C00), 1.0);
        assert_eq!(half_to_float4(0x0001), 2f32.powi(-24));
        assert_eq!(half_to_float4(0x03FF).to_bits(), 0x387F_C000);
        assert!(half_to_float4(0x7C01).is_nan());
        assert!(half_is_nan(0x7E00) && half_is_inf(0xFC00) && half_is_zero(0x8000));
        assert!(!half_is_nan(0x7C00) && !half_is_inf(0x7E00) && !half_is_zero(0x0001));
    }

    #[test]
    fn nan_keeps_sign_and_payload_like_c() {
        // C: result |= 0x7E00; result |= mantissa >> 13
        let h = float4_to_half_unchecked(f32::from_bits(0xFFC0_0000));
        assert_eq!(h, 0xFE00);
    }

    #[test]
    fn float4_to_half_reports_overflow_with_shortest_decimal() {
        let err = float4_to_half(70000.0).unwrap_err();
        assert_eq!(err.message(), "\"70000\" is out of range for type halfvec");
        assert!(float4_to_half(f32::INFINITY).is_ok(), "inf input is not an overflow");
        assert_eq!(float4_to_half(65504.0).unwrap(), 0x7BFF);
    }
}
