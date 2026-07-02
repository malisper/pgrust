//! PostgreSQL's Ryū shortest round-trip float serializer (src/common/d2s.c,
//! f2s.c; 64-bit code path only — u128 replaces the MSVC/32-bit fallbacks).
//! Output is byte-identical to double/float_to_shortest_decimal_buf.

#![no_std]

mod common;
mod d2s;
mod d2s_table;
mod f2s;

pub use d2s::{
    double_to_shortest_decimal_buf, double_to_shortest_decimal_bufn, DOUBLE_SHORTEST_DECIMAL_LEN,
};
pub use f2s::{
    float_to_shortest_decimal_buf, float_to_shortest_decimal_bufn, FLOAT_SHORTEST_DECIMAL_LEN,
};

#[cfg(test)]
mod tests {
    extern crate std;
    use super::*;
    use std::string::String;

    fn d2s(v: f64) -> String {
        let mut buf = [0u8; DOUBLE_SHORTEST_DECIMAL_LEN];
        let n = double_to_shortest_decimal_bufn(v, &mut buf);
        core::str::from_utf8(&buf[..n]).unwrap().into()
    }

    fn f2s(v: f32) -> String {
        let mut buf = [0u8; FLOAT_SHORTEST_DECIMAL_LEN];
        let n = float_to_shortest_decimal_bufn(v, &mut buf);
        core::str::from_utf8(&buf[..n]).unwrap().into()
    }

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
            (4.9406564584124654e-324, "5e-324"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
            (0.0025, "0.0025"),
            (10000.0, "10000"),
            (0.5, "0.5"),
        ];
        for &(v, expected) in cases {
            assert_eq!(d2s(v), expected, "double {v:?}");
        }
        assert_eq!(d2s(f64::NAN), "NaN");
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
            assert_eq!(f2s(v), expected, "float {v:?}");
        }
        assert_eq!(f2s(f32::NAN), "NaN");
    }

    #[test]
    fn buffer_apis_terminate_and_length() {
        let mut buf = [b'x'; DOUBLE_SHORTEST_DECIMAL_LEN];
        assert_eq!(double_to_shortest_decimal_buf(12.5, &mut buf), 4);
        assert_eq!(&buf[..5], b"12.5\0");

        let mut fbuf = [b'x'; FLOAT_SHORTEST_DECIMAL_LEN];
        assert_eq!(float_to_shortest_decimal_buf(12.5_f32, &mut fbuf), 4);
        assert_eq!(&fbuf[..5], b"12.5\0");
    }

    #[test]
    fn double_roundtrips_sample() {
        let mut values = std::vec![
            0.0,
            -0.0,
            1.0,
            -1.0,
            0.1,
            0.2,
            0.3,
            3.141592653589793,
            2.718281828459045,
            1e-300,
            1e300,
            1.7976931348623157e308,
            5e-324,
            123456789.123456789,
            0.0000123,
            9999999999999999.0,
            42.0,
            -273.15,
            6.022e23,
            1.602176634e-19,
        ];
        let mut state: u64 = 0x1234_5678_9abc_def0;
        for _ in 0..50000 {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = f64::from_bits(state);
            if v.is_finite() {
                values.push(v);
            }
        }
        for v in values {
            let s = d2s(v);
            let parsed: f64 = s.parse().unwrap();
            assert_eq!(parsed.to_bits(), v.to_bits(), "double {v:?} -> {s}");
        }
    }

    #[test]
    fn float_roundtrips_random_sample() {
        let mut state: u32 = 0x9e37_79b9;
        for _ in 0..200000 {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            let v = f32::from_bits(state);
            if v.is_finite() {
                let s = f2s(v);
                let parsed: f32 = s.parse().unwrap();
                assert_eq!(parsed.to_bits(), v.to_bits(), "float {v:?} -> {s}");
            }
        }
    }
}
