//! Faithful port of PostgreSQL's Ryū shortest round-trip float serializer
//! (`src/common/d2s.c` and `src/common/f2s.c`, with the supporting
//! `ryu_common.h` / `digit_table.h` / `d2s_full_table.h` headers).
//!
//! This is the algorithm PostgreSQL uses for float text output: it produces the
//! SHORTEST decimal string that round-trips back to the exact same IEEE-754
//! value. The output is byte-identical to PostgreSQL's
//! `double_to_shortest_decimal_buf` / `float_to_shortest_decimal_buf`, which is
//! what `outDouble` (outfuncs.c, `WRITE_FLOAT_FIELD`) emits into the node text
//! stream.
//!
//! It is a pure deterministic algorithm with no PostgreSQL runtime
//! dependencies (no memory contexts, no error reporting), so this crate has no
//! dependencies. The `*_buf` / `*_bufn` entry points write into a caller-owned
//! fixed buffer exactly as the C does; the `*_to_shortest_decimal` entry points
//! return an owned `String` (the analogue of the C functions that return a
//! palloc'd / malloc'd string the caller frees).
//!
//! Only the 64-bit code path of the C is ported (`HAVE_INT128` &&
//! !`RYU_32_BIT_PLATFORM`): Rust has native `u128` and 64-bit integer division,
//! so the MSVC intrinsic and 32-bit-platform fallbacks are unnecessary and the
//! numeric results are identical.

#![no_std]

extern crate alloc;

mod common;
mod d2s;
mod d2s_table;
mod f2s;

pub use d2s::{
    double_to_shortest_decimal, double_to_shortest_decimal_buf, double_to_shortest_decimal_bufn,
    DOUBLE_SHORTEST_DECIMAL_LEN,
};
pub use f2s::{
    float_to_shortest_decimal, float_to_shortest_decimal_buf, float_to_shortest_decimal_bufn,
    FLOAT_SHORTEST_DECIMAL_LEN,
};

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use std::string::String;

    // --- double: known-value byte-exactness vs PostgreSQL's outDouble. -------
    //
    // Expected strings are exactly what PostgreSQL's double_to_shortest_decimal
    // produces (the Ryū shortest round-trip + printf-style fixed/scientific
    // threshold: fixed when display exp in [-4, 15), scientific otherwise).

    #[test]
    fn double_known_values_byte_exact() {
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-1.0, "-1"),
            (1.5, "1.5"),
            (-1.25, "-1.25"),
            (0.1, "0.1"),
            (0.2, "0.2"),
            (0.3, "0.3"),
            (100.0, "100"),
            (0.0001, "0.0001"),
            (1e-5, "1e-05"),
            (1e15, "1e+15"),
            (1e16, "1e+16"),
            (1234567.0, "1234567"),
            (1.2345678901234567, "1.2345678901234567"),
            (1e308, "1e+308"),
            (1e-308, "1e-308"),
            (4.9406564584124654e-324, "5e-324"), // smallest subnormal
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            // Typical planner costs (Cost/Selectivity f64 fields).
            (0.0025, "0.0025"),
            (0.005, "0.005"),
            (10000.0, "10000"),
            (0.5, "0.5"),
        ];
        for &(v, expected) in cases {
            assert_eq!(
                double_to_shortest_decimal(v),
                expected,
                "double {v:?} should render {expected}"
            );
        }
        assert_eq!(double_to_shortest_decimal(f64::NAN), "NaN");
    }

    #[test]
    fn float_known_values_byte_exact() {
        let cases: &[(f32, &str)] = &[
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-1.25, "-1.25"),
            (0.1, "0.1"),
            (0.0001, "0.0001"),
            (1e-5, "1e-05"),
            (1e6, "1e+06"),
            (123456.0, "123456"),
            (1.234567, "1.234567"),
            (f32::INFINITY, "Infinity"),
            (f32::NEG_INFINITY, "-Infinity"),
        ];
        for &(v, expected) in cases {
            assert_eq!(
                float_to_shortest_decimal(v),
                expected,
                "float {v:?} should render {expected}"
            );
        }
        assert_eq!(float_to_shortest_decimal(f32::NAN), "NaN");
    }

    // --- buffer API: length + NUL terminator. --------------------------------

    #[test]
    fn buffer_apis_terminate_and_length() {
        let mut buf = [b'x'; DOUBLE_SHORTEST_DECIMAL_LEN];
        let len = double_to_shortest_decimal_buf(12.5, &mut buf);
        assert_eq!(len, 4);
        assert_eq!(&buf[..5], b"12.5\0");

        let mut fbuf = [b'x'; FLOAT_SHORTEST_DECIMAL_LEN];
        let flen = float_to_shortest_decimal_buf(12.5_f32, &mut fbuf);
        assert_eq!(flen, 4);
        assert_eq!(&fbuf[..5], b"12.5\0");
    }

    // --- round-trip: parsing the output back yields the EXACT same bits. -----

    fn roundtrip_double(v: f64) {
        let s = double_to_shortest_decimal(v);
        let parsed: f64 = s.parse().expect("output must parse as f64");
        assert_eq!(
            parsed.to_bits(),
            v.to_bits(),
            "double {v:?} -> {s} did not round-trip"
        );
    }

    fn roundtrip_float(v: f32) {
        let s = float_to_shortest_decimal(v);
        let parsed: f32 = s.parse().expect("output must parse as f32");
        assert_eq!(
            parsed.to_bits(),
            v.to_bits(),
            "float {v:?} -> {s} did not round-trip"
        );
    }

    #[test]
    fn double_roundtrips_exhaustive_sample() {
        // A spread of exponents and mantissas, plus all the known cases.
        let mut values = std::vec![
            0.0, -0.0, 1.0, -1.0, 0.1, 0.2, 0.3, 3.141592653589793,
            2.718281828459045, 1e-300, 1e300, 1.7976931348623157e308,
            5e-324, 123456789.123456789, 0.0000123, 9999999999999999.0,
            42.0, -273.15, 6.022e23, 1.602176634e-19,
        ];
        // LCG-driven random bit patterns (skip non-finite).
        let mut state: u64 = 0x1234_5678_9abc_def0;
        for _ in 0..20000 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let v = f64::from_bits(state);
            if v.is_finite() {
                values.push(v);
            }
        }
        for v in values {
            roundtrip_double(v);
        }
    }

    #[test]
    fn float_roundtrips_random_sample() {
        let mut state: u32 = 0x9e37_79b9;
        for _ in 0..200000 {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            let v = f32::from_bits(state);
            if v.is_finite() {
                roundtrip_float(v);
            }
        }
    }

    #[test]
    fn out_string_is_ascii() {
        // The owned-String path must not panic on UTF-8 validation for any input.
        let _: String = double_to_shortest_decimal(-9.87654321e-50);
        let _: String = float_to_shortest_decimal(-9.876e-20);
    }
}
