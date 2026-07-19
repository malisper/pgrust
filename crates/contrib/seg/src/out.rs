//! seg display formatting: C's `restore()` (print a float with exactly n
//! significant digits, seg.c), `significant_digits()`, a C-`printf`
//! compatible `%e` / `%g` formatter pair, and the `seg_out` string builder.

use crate::repr::Seg;

pub const FLT_DIG: i32 = 6;

/// C `sprintf("%.*e", prec, val)`: mantissa with `prec` fraction digits,
/// exponent as `e` + sign + at least two digits. Rust's float formatting is
/// correctly rounded, same as glibc's.
pub fn c_sprintf_e(val: f64, prec: usize) -> String {
    if val.is_nan() {
        return "nan".to_string();
    }
    if val.is_infinite() {
        return if val < 0.0 { "-inf" } else { "inf" }.to_string();
    }
    let s = format!("{:.*e}", prec, val);
    let (mant, exp) = s.split_once('e').unwrap();
    let exp: i32 = exp.parse().unwrap();
    format!(
        "{}e{}{:02}",
        mant,
        if exp < 0 { '-' } else { '+' },
        exp.abs()
    )
}

/// C `snprintf("%g", val)` (default precision 6): shortest of %e/%f with
/// trailing zeros stripped; %e chosen when exp < -4 or >= precision.
pub fn c_format_g(val: f64) -> String {
    const PREC: i32 = 6;
    if val.is_nan() {
        return "nan".to_string();
    }
    if val.is_infinite() {
        return if val < 0.0 { "-inf" } else { "inf" }.to_string();
    }
    if val == 0.0 {
        return if val.is_sign_negative() { "-0" } else { "0" }.to_string();
    }
    // decimal exponent after rounding to PREC significant digits
    let e_form = c_sprintf_e(val, (PREC - 1) as usize);
    let (mant, exp_s) = e_form.split_once('e').unwrap();
    let exp: i32 = exp_s.parse().unwrap();
    if exp < -4 || exp >= PREC {
        // %e style with trailing zeros stripped from the fraction
        let mut m = mant.to_string();
        if m.contains('.') {
            while m.ends_with('0') {
                m.pop();
            }
            if m.ends_with('.') {
                m.pop();
            }
        }
        format!(
            "{}e{}{:02}",
            m,
            if exp < 0 { '-' } else { '+' },
            exp.abs()
        )
    } else {
        // %f style with PREC-1-exp fraction digits, trailing zeros stripped
        let frac = (PREC - 1 - exp).max(0) as usize;
        let mut s = format!("{:.*}", frac, val);
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        s
    }
}

/// C seg.c `restore(result, val, n)`: print `val` with exactly `n`
/// significant digits (8.00 where %.ng would print 8). Returns the bytes C
/// would leave in `result`.
pub fn restore(val: f32, n_in: i32) -> Vec<u8> {
    // cap the digit count (n <= 0 protects against corrupted data)
    let n = if n_in <= 0 { FLT_DIG } else { n_in.min(FLT_DIG) };
    let sign = val < 0.0;

    // print in %e style to start with (float promoted to double)
    let e_form = c_sprintf_e(val as f64, (n - 1) as usize);
    let bytes = e_form.as_bytes();

    // punt if we have 'inf' or similar
    let Some(epos) = bytes.iter().position(|&c| c == b'e') else {
        return bytes.to_vec();
    };
    let exp: i32 = e_form[epos + 1..].parse().unwrap();

    if exp == 0 {
        // just truncate off the 'e+00'
        return bytes[..epos].to_vec();
    }
    if exp.abs() > 4 {
        // %e must be OK
        return bytes.to_vec();
    }

    // C copies the mantissa digits (skipping '.') into a '0'-prefilled
    // buffer at offset 10 with dp marking the decimal point slot; the %e
    // mantissa always has exactly one digit before the point, so
    // dp - 10 == 1 (n == 1 has no '.', and C's fallback sets dp past the
    // digit, also 11).
    let mstart = if sign { 1 } else { 0 };
    let digits: Vec<u8> = bytes[mstart..epos]
        .iter()
        .copied()
        .filter(|&c| c != b'.')
        .collect();
    debug_assert_eq!(digits.len(), n as usize);
    let dpk: i32 = 1; // dp - 10

    let mut out: Vec<u8> = Vec::new();
    if exp > 0 {
        if dpk + exp >= n {
            // the decimal point is behind the last significant digit;
            // re-notate with the point after the first digit and a bare
            // "e%d" exponent (no '+', no zero padding — C sprintf "%d")
            let exp2 = dpk + exp - n; // C: exp = dp - 10 + exp - n
            if sign {
                out.push(b'-');
            }
            out.push(digits[0]);
            if n > 1 {
                out.push(b'.');
                out.extend_from_slice(&digits[1..]);
            }
            out.extend_from_slice(format!("e{}", exp2 + n - 1).as_bytes());
        } else {
            // insert the decimal point inside the digit run
            let k = (dpk + exp) as usize;
            if sign {
                out.push(b'-');
            }
            out.extend_from_slice(&digits[..k]);
            out.push(b'.');
            out.extend_from_slice(&digits[k..]);
        }
    } else {
        // exp < 0: leading "0." plus |exp|-1 zeros from the prefilled buffer
        if sign {
            out.push(b'-');
        }
        out.push(b'0');
        out.push(b'.');
        for _ in 0..(-exp - 1) {
            out.push(b'0');
        }
        out.extend_from_slice(&digits);
    }
    out
}

/// seg.c `significant_digits()`: digits carried by a float literal.
pub fn significant_digits(s: &[u8]) -> i32 {
    let mut p = 0usize;
    let mut zeroes = 1i32;

    // skip leading zeroes and sign
    while p < s.len() && matches!(s[p], b'0' | b'+' | b'-') {
        p += 1;
    }

    // skip decimal point and following zeroes
    while p < s.len() && matches!(s[p], b'0' | b'.') {
        if s[p] != b'.' {
            zeroes += 1;
        }
        p += 1;
    }

    // count significant digits (n)
    let mut n = 0i32;
    while p < s.len() {
        let c = s[p];
        if !(c.is_ascii_digit() || c == b'.') {
            break;
        }
        if c != b'.' {
            n += 1;
        }
        p += 1;
    }

    if n == 0 {
        return zeroes;
    }
    n
}

/// `sig_digits` from segparse.y: clamp to what the sigd byte can carry.
pub fn sig_digits(s: &[u8]) -> i32 {
    significant_digits(s).min(FLT_DIG)
}

/// seg.c `seg_out`, including its bug-compatible quirk: with `l_ext == '~'`
/// the upper boundary's extension byte is emitted even when it is `'\0'`,
/// which (as in C's `sprintf("%c", 0)`) truncates the cstring right there.
pub fn seg_out(seg: &Seg) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::new();

    if seg.l_ext == b'>' || seg.l_ext == b'<' || seg.l_ext == b'~' {
        out.push(seg.l_ext);
    }

    if seg.lower == seg.upper && seg.l_ext == seg.u_ext {
        // built by seg_in off a single point
        out.extend_from_slice(&restore(seg.lower, seg.l_sigd as i32));
    } else {
        if seg.l_ext != b'-' {
            // print the lower boundary if exists
            out.extend_from_slice(&restore(seg.lower, seg.l_sigd as i32));
            out.push(b' ');
        }
        out.extend_from_slice(b"..");
        if seg.u_ext != b'-' {
            // print the upper boundary if exists
            out.push(b' ');
            if seg.u_ext == b'>' || seg.u_ext == b'<' || seg.l_ext == b'~' {
                out.push(seg.u_ext);
            }
            out.extend_from_slice(&restore(seg.upper, seg.u_sigd as i32));
        }
    }

    // cstring semantics: C's embedded-NUL quirk cuts the string
    if let Some(z) = out.iter().position(|&c| c == 0) {
        out.truncate(z);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(val: f32, n: i32) -> String {
        String::from_utf8(restore(val, n)).unwrap()
    }

    #[test]
    fn restore_matches_c_expected_corpus() {
        // The full '2eN' ladder from seg's regress corpus.
        assert_eq!(r(2e-2, 1), "0.02");
        assert_eq!(r(2e-1, 1), "0.2");
        assert_eq!(r(2.0, 1), "2");
        assert_eq!(r(2e1, 1), "2e1");
        assert_eq!(r(2e2, 1), "2e2");
        assert_eq!(r(2e3, 1), "2e3");
        assert_eq!(r(2e4, 1), "2e4");
        assert_eq!(r(2e5, 1), "2e+05");
        assert_eq!(r(2e6, 1), "2e+06");
        // Significant-digits preservation ('1.000000' clamps at FLT_DIG).
        assert_eq!(r(1.0, 1), "1");
        assert_eq!(r(1.0, 2), "1.0");
        assert_eq!(r(1.0, 5), "1.0000");
        assert_eq!(r(1.0, 6), "1.00000");
        assert_eq!(r(1.0, 7), "1.00000");
        // Rounding/truncation to 6 digits.
        assert_eq!(r(12.34567890123456, 6), "12.3457");
        // %e passthrough for small magnitudes.
        assert_eq!(r(1.2e-7, 3), "1.20e-07");
        assert_eq!(r(3.4e5, 4), "3.400e+05");
        // Negative values.
        assert_eq!(r(-1.0, 2), "-1.0");
        assert_eq!(r(-1e7, 1), "-1e+07");
        assert_eq!(r(-0.02, 1), "-0.02");
        // n <= 0 falls back to FLT_DIG.
        assert_eq!(r(1.5, 0), "1.50000");
    }

    #[test]
    fn significant_digits_matches_c() {
        assert_eq!(significant_digits(b"1"), 1);
        assert_eq!(significant_digits(b"1.0"), 2);
        assert_eq!(significant_digits(b"1.000000"), 7);
        assert_eq!(significant_digits(b"0.000000120"), 3);
        assert_eq!(significant_digits(b"-1.5e3"), 2);
        // all-zero input: the zeroes counter (leading skip + 1)
        assert_eq!(significant_digits(b"0"), 1);
        assert_eq!(significant_digits(b"0.00"), 3);
        assert_eq!(significant_digits(b"12.34567890123456"), 16);
        assert_eq!(sig_digits(b"12.34567890123456"), 6);
    }

    #[test]
    fn format_g_matches_c_printf() {
        assert_eq!(c_format_g(5.0), "5");
        assert_eq!(c_format_g(-3.25), "-3.25");
        assert_eq!(c_format_g(100000.0), "100000");
        assert_eq!(c_format_g(1000000.0), "1e+06");
        assert_eq!(c_format_g(0.0001), "0.0001");
        assert_eq!(c_format_g(0.00001), "1e-05");
        assert_eq!(c_format_g(123456789.0), "1.23457e+08");
        assert_eq!(c_format_g(0.0), "0");
    }

    #[test]
    fn seg_out_forms() {
        let seg = |lower: f32, upper: f32, l_sigd, u_sigd, l_ext, u_ext| Seg {
            lower,
            upper,
            l_sigd,
            u_sigd,
            l_ext,
            u_ext,
        };
        // point
        assert_eq!(seg_out(&seg(1.0, 1.0, 1, 1, 0, 0)), b"1");
        // plain range
        assert_eq!(seg_out(&seg(1.0, 2.0, 1, 1, 0, 0)), b"1 .. 2");
        // open-ended
        assert_eq!(seg_out(&seg(1.0, f32::INFINITY, 1, 0, 0, b'-')), b"1 ..");
        assert_eq!(
            seg_out(&seg(f32::NEG_INFINITY, 2.0, 0, 1, b'-', 0)),
            b".. 2"
        );
        assert_eq!(
            seg_out(&seg(f32::NEG_INFINITY, f32::INFINITY, 0, 0, b'-', b'-')),
            b".."
        );
        // extensions
        assert_eq!(seg_out(&seg(1.0, 2.0, 1, 1, b'<', b'>')), b"<1 .. >2");
        // the '~' lower-ext quirk: u_ext 0 emitted -> string cut after ".. "
        assert_eq!(seg_out(&seg(1.0, 2.0, 1, 1, b'~', 0)), b"~1 .. ");
    }
}
