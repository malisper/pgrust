//! Constants mirroring the ALP reference implementation for f64
//! (SIGMOD 2024, github.com/cwida/ALP: include/alp/{constants,config}.hpp).
//! Values are copied, not re-derived: the sampling estimates and scheme
//! election reproduce the reference's decisions only if these match.

pub const VECTOR_SIZE: usize = 1024;
pub const N_VECTORS_PER_ROWGROUP: usize = 100;
pub const ROWGROUP_SIZE: usize = N_VECTORS_PER_ROWGROUP * VECTOR_SIZE;

/// Vector-index stride for first-stage sampling. The reference computes
/// (ROWGROUP_SIZE / ROWGROUP_VECTOR_SAMPLES) / VECTOR_SIZE in integer
/// arithmetic, so a full rowgroup samples 9 vectors (0, 12, .., 96), not 8.
pub const ROWGROUP_VECTOR_SAMPLES: usize = 8;
pub const ROWGROUP_SAMPLES_JUMP: usize =
    (ROWGROUP_SIZE / ROWGROUP_VECTOR_SAMPLES) / VECTOR_SIZE;

pub const SAMPLES_PER_VECTOR: usize = 32;
pub const MAX_K_COMBINATIONS: usize = 5;
pub const SAMPLING_EARLY_EXIT_THRESHOLD: usize = 2;

pub const MAX_EXPONENT: u8 = 18;

/// 2^51 + 2^52: adding then subtracting rounds an f64 to an integer using
/// the FPU's current rounding (ties-to-even). Only valid for magnitudes
/// below 2^51; out-of-domain inputs fail the round-trip check and become
/// exceptions, so no domain guard is needed on the value itself.
pub const MAGIC_NUMBER: f64 = 6_755_399_441_055_744.0;

/// Largest f64 below 2^63 (and its negation): scaled values beyond this
/// cannot live in the i64 encoded domain.
pub const ENCODING_UPPER_LIMIT: f64 = 9_223_372_036_854_774_784.0;
pub const ENCODING_LOWER_LIMIT: f64 = -9_223_372_036_854_774_784.0;

pub const EXCEPTION_SIZE_BITS: u64 = 64;
pub const EXCEPTION_POSITION_SIZE_BITS: u64 = 16;

/// If no sampled vector's best (e,f) beats 48 bits/value on its samples,
/// the rowgroup switches to ALP-RD (reference: 48 * SAMPLES_PER_VECTOR,
/// compared against the per-vector sample-total size).
pub const RD_SIZE_THRESHOLD_LIMIT: u64 = 48 * SAMPLES_PER_VECTOR as u64;

/// ALP-RD tries left widths 1..=16 (cut position p = 64 - left >= 48).
pub const CUTTING_LIMIT: usize = 16;
pub const MAX_RD_DICTIONARY_SIZE: usize = 8;
pub const RD_EXCEPTION_SIZE_BITS: u64 = 16;
pub const RD_EXCEPTION_POSITION_SIZE_BITS: u64 = 16;

/// 10^e as f64 (exact through 10^18: 5^18 < 2^53).
pub const EXP_ARR: [f64; 19] = [
    1.0,
    1e1,
    1e2,
    1e3,
    1e4,
    1e5,
    1e6,
    1e7,
    1e8,
    1e9,
    1e10,
    1e11,
    1e12,
    1e13,
    1e14,
    1e15,
    1e16,
    1e17,
    1e18,
];

/// 10^-f as f64. Decimal literals parse to the same nearest-doubles as the
/// reference's C++ table; do not replace with powi (not correctly rounded).
pub const FRAC_ARR: [f64; 19] = [
    1.0,
    1e-1,
    1e-2,
    1e-3,
    1e-4,
    1e-5,
    1e-6,
    1e-7,
    1e-8,
    1e-9,
    1e-10,
    1e-11,
    1e-12,
    1e-13,
    1e-14,
    1e-15,
    1e-16,
    1e-17,
    1e-18,
];

/// 10^f as exact integers (decode multiplies these back in as f64).
pub const FACT_ARR: [i64; 19] = [
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
    10_000_000_000,
    100_000_000_000,
    1_000_000_000_000,
    10_000_000_000_000,
    100_000_000_000_000,
    1_000_000_000_000_000,
    10_000_000_000_000_000,
    100_000_000_000_000_000,
    1_000_000_000_000_000_000,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_are_consistent() {
        for i in 0..19 {
            assert_eq!(FACT_ARR[i] as f64, EXP_ARR[i]);
            if i > 0 {
                assert_eq!(FACT_ARR[i], FACT_ARR[i - 1] * 10);
            }
            // FRAC entries are the nearest doubles to 10^-i; the product
            // with the exact 10^i must land within one ulp of 1.0.
            let p = FRAC_ARR[i] * FACT_ARR[i] as f64;
            assert!((p - 1.0).abs() < 1e-15, "FRAC_ARR[{i}] off: {p}");
        }
        assert_eq!(ROWGROUP_SAMPLES_JUMP, 12);
        assert_eq!(MAGIC_NUMBER, (1u64 << 51) as f64 + (1u64 << 52) as f64);
    }
}
