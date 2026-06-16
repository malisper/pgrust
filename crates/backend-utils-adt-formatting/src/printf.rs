//! Tiny faithful re-implementations of the `sprintf` format specifiers used by
//! `DCH_to_char` / `NUM_processor` / the `*_to_char` entry points:
//!   * `%0*d` / `%0*lld`  -- zero-padded, sign-aware integer with dynamic width
//!   * `%*s`              -- space-padded string (negative width => left-justify)
//!   * `%+.*e`            -- exponential with explicit sign
//!   * `%.*f` / `%.0f`    -- fixed-point float
//!
//! Behaviors match C's `printf` family (which `pg_sprintf` mirrors).

/// C: `sprintf(s, "%0*d", width, val)` -- zero-pad to at least `width` columns,
/// counting the sign.  Width 0 means "no padding" (just the number).
pub fn fmt_0d(width: usize, val: i64) -> String {
    let neg = val < 0;
    let mag = (val as i128).unsigned_abs();
    let digits = mag.to_string();
    // total field width includes the sign character for negatives
    let sign_len = if neg { 1 } else { 0 };
    let cur = digits.len() + sign_len;
    let pad = width.saturating_sub(cur);
    let mut out = String::with_capacity(width.max(cur));
    if neg {
        out.push('-');
    }
    for _ in 0..pad {
        out.push('0');
    }
    out.push_str(&digits);
    out
}

/// C: `sprintf(s, "%d", val)` / `"%1d"` -- plain decimal (min-width handled by
/// caller via `min_width`).
pub fn fmt_d(val: i64) -> String {
    val.to_string()
}

/// C: `sprintf(s, "%*s", width, str)` with C's `printf` width semantics: a
/// positive `width` right-justifies (pad on left), a negative width
/// left-justifies (pad on right) to `|width|` columns.
pub fn fmt_pad_str(width: i32, s: &str) -> String {
    let target = width.unsigned_abs() as usize;
    let len = s.chars().count();
    if len >= target {
        return s.to_string();
    }
    let pad = target - len;
    let spaces: String = std::iter::repeat_n(' ', pad).collect();
    if width < 0 {
        format!("{s}{spaces}")
    } else {
        format!("{spaces}{s}")
    }
}

/// C: `psprintf("%+.*e", prec, val)` for f64.  Produces an explicit leading
/// sign, `prec` fraction digits, and a sign + at-least-two-digit exponent.
pub fn fmt_plus_e(prec: usize, val: f64) -> String {
    if val.is_nan() {
        // matches glibc "%+.*e" of NaN -> "+nan" — but callers handle NaN
        // separately, so this is only a safety net.
        return "+nan".to_string();
    }
    let neg = val.is_sign_negative();
    let s = format!("{:.*e}", prec, val.abs());
    // Rust prints like "1.23e4"; C prints "1.23e+04". Convert.
    let s = normalize_exponent(&s);
    if neg {
        format!("-{s}")
    } else {
        format!("+{s}")
    }
}

/// C: `psprintf("%.*f", prec, val)` for f64.
pub fn fmt_f(prec: usize, val: f64) -> String {
    format!("{val:.prec$}")
}

/// C: `psprintf("%.0f", val)` for f64.
pub fn fmt_f0(val: f64) -> String {
    format!("{val:.0}")
}

/// Rewrite Rust's `1.23e4` / `1.23e-4` exponent form into C's `1.23e+04`
/// (sign always present, exponent at least two digits).
fn normalize_exponent(s: &str) -> String {
    if let Some(epos) = s.find(['e', 'E']) {
        let (mantissa, exp) = s.split_at(epos);
        let exp = &exp[1..]; // skip 'e'
        let (sign, digits) = if let Some(rest) = exp.strip_prefix('-') {
            ('-', rest)
        } else if let Some(rest) = exp.strip_prefix('+') {
            ('+', rest)
        } else {
            ('+', exp)
        };
        let digits = if digits.len() < 2 {
            format!("{digits:0>2}")
        } else {
            digits.to_string()
        };
        format!("{mantissa}e{sign}{digits}")
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_pad_integer() {
        assert_eq!(fmt_0d(2, 5), "05");
        assert_eq!(fmt_0d(2, 12), "12");
        assert_eq!(fmt_0d(3, -7), "-07");
        assert_eq!(fmt_0d(0, 5), "5");
        assert_eq!(fmt_0d(4, 2020), "2020");
        assert_eq!(fmt_0d(5, -2020), "-2020");
    }

    #[test]
    fn pad_string() {
        assert_eq!(fmt_pad_str(-9, "January"), "January  ");
        assert_eq!(fmt_pad_str(9, "Jan"), "      Jan");
        assert_eq!(fmt_pad_str(0, "Jan"), "Jan");
    }

    #[test]
    fn plus_exponent() {
        assert_eq!(fmt_plus_e(2, 12345.0), "+1.23e+04");
        assert_eq!(fmt_plus_e(2, -12345.0), "-1.23e+04");
        assert_eq!(fmt_plus_e(1, 1.0), "+1.0e+00");
    }

    #[test]
    fn fixed_point() {
        assert_eq!(fmt_f(2, 5.678), "5.68");
        assert_eq!(fmt_f0(3.7), "4");
    }
}
