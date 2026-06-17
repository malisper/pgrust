//! USER I/O ROUTINES (float.c:131-575): `float{4,8}in` / `float{4,8}out`
//! and their `*_internal` guts, plus the binary wire codec
//! `float{4,8}recv` / `float{4,8}send`.
//!
//! `float8in_internal` / `float4in_internal` reproduce the C functions, which
//! are essentially `strtod`/`strtof` wrapped with whitespace skipping, explicit
//! NaN/[+-]Inf recognition, and ERANGE handling. A syntax error raises
//! `ERRCODE_INVALID_TEXT_REPRESENTATION` (22P02); a genuine out-of-range raises
//! `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE` (22003).
//!
//! `float8out_internal` / `float{4,8}out` use the shortest round-trip decimal
//! from `common-ryu` when `extra_float_digits > 0` (the default); otherwise they
//! reproduce the legacy `pg_strfromd` (`snprintf("%.*g", ndig)`) path.

use common_ryu::{double_to_shortest_decimal, float_to_shortest_decimal};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    ERRCODE_PROTOCOL_VIOLATION,
};

use crate::{get_float4_infinity, get_float4_nan, get_float8_infinity, get_float8_nan};
use crate::{DBL_DIG, FLT_DIG};

/// `pg_strncasecmp(s, lit, n) == 0` over ASCII.
fn strncasecmp_eq(s: &[u8], lit: &[u8]) -> bool {
    if s.len() < lit.len() {
        return false;
    }
    s[..lit.len()]
        .iter()
        .zip(lit)
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// `ldexp(mant, exp)` — `mant * 2^exp`, exact, handling subnormal + overflow as
/// C's `ldexp`/`scalbn`. Pure-Rust so the crate carries no `libm` dependency.
fn ldexp(mant: f64, exp: i32) -> f64 {
    if mant == 0.0 || !mant.is_finite() {
        return mant;
    }
    let mut result = mant;
    let mut e = exp;
    while e > 0 {
        let step = e.min(1023);
        result *= f64::from_bits(((step as u64) + 1023) << 52); // 2^step
        if !result.is_finite() {
            return result;
        }
        e -= step;
    }
    while e < 0 {
        let step = (-e).min(1022);
        result *= f64::from_bits(((1023 - step) as u64) << 52); // 2^-step
        if result == 0.0 {
            return result;
        }
        e += step;
    }
    result
}

/// Lexical shape of a scanned number token.
enum NumKind {
    Decimal,
    Hex,
}

/// Outcome of the `strtod`/`strtof` token scan.
struct NumToken {
    len: usize,
    nonzero: bool,
    kind: NumKind,
}

/// Scan the leading `strtod`/`strtof`-recognizable number token. Recognizes
/// both the decimal grammar and the C99 hex-float grammar (`0x`/`0X` prefix),
/// because the platform `strtod`/`strtof` PostgreSQL delegates to accepts both.
/// Returns `None` if no number is present. Does NOT skip leading whitespace.
fn scan_number(s: &[u8]) -> Option<NumToken> {
    let mut i = 0usize;
    if i < s.len() && (s[i] == b'+' || s[i] == b'-') {
        i += 1;
    }

    if i + 1 < s.len() && s[i] == b'0' && (s[i + 1] == b'x' || s[i + 1] == b'X') {
        let mut j = i + 2;
        let mut saw_hex = false;
        let mut nonzero = false;
        while j < s.len() && s[j].is_ascii_hexdigit() {
            if s[j] != b'0' {
                nonzero = true;
            }
            j += 1;
            saw_hex = true;
        }
        if j < s.len() && s[j] == b'.' {
            j += 1;
            while j < s.len() && s[j].is_ascii_hexdigit() {
                if s[j] != b'0' {
                    nonzero = true;
                }
                j += 1;
                saw_hex = true;
            }
        }
        if saw_hex {
            if j < s.len() && (s[j] == b'p' || s[j] == b'P') {
                let mut k = j + 1;
                if k < s.len() && (s[k] == b'+' || s[k] == b'-') {
                    k += 1;
                }
                let exp_start = k;
                while k < s.len() && s[k].is_ascii_digit() {
                    k += 1;
                }
                if k > exp_start {
                    j = k;
                }
            }
            return Some(NumToken {
                len: j,
                nonzero,
                kind: NumKind::Hex,
            });
        }
    }

    let mut saw_digit = false;
    let mut nonzero = false;
    while i < s.len() && s[i].is_ascii_digit() {
        if s[i] != b'0' {
            nonzero = true;
        }
        i += 1;
        saw_digit = true;
    }
    if i < s.len() && s[i] == b'.' {
        i += 1;
        while i < s.len() && s[i].is_ascii_digit() {
            if s[i] != b'0' {
                nonzero = true;
            }
            i += 1;
            saw_digit = true;
        }
    }
    if !saw_digit {
        return None;
    }
    if i < s.len() && (s[i] == b'e' || s[i] == b'E') {
        let mut j = i + 1;
        if j < s.len() && (s[j] == b'+' || s[j] == b'-') {
            j += 1;
        }
        let exp_start = j;
        while j < s.len() && s[j].is_ascii_digit() {
            j += 1;
        }
        if j > exp_start {
            i = j;
        }
    }
    Some(NumToken {
        len: i,
        nonzero,
        kind: NumKind::Decimal,
    })
}

/// Parse a C99 hex-float token to a correctly-rounded `f64`.
fn parse_hex_float(token: &[u8]) -> f64 {
    round_to_float(token, 52, 11)
}

/// `parse_hex_float` for `f32` (reproducing `strtof`).
fn parse_hex_float32(token: &[u8]) -> f32 {
    round_to_float(token, 23, 8) as f32
}

/// Core hex-float rounding shared by the f64/f32 parsers. Round-to-nearest,
/// ties-to-even, with overflow -> infinity and gradual underflow -> subnormal/
/// zero, as the platform `strtod`/`strtof` does.
fn round_to_float(token: &[u8], mantissa_bits: u32, exp_bits: u32) -> f64 {
    let mut i = 0usize;
    let neg = token[i] == b'-';
    if token[i] == b'+' || token[i] == b'-' {
        i += 1;
    }
    debug_assert!(token[i] == b'0' && (token[i + 1] == b'x' || token[i + 1] == b'X'));
    i += 2;

    let mut mant: u128 = 0;
    let mut sticky = false;
    let mut frac_digits: i64 = 0;
    let mut low_drop: i64 = 0;
    let mut seen_dot = false;
    const MAX_MANT_BITS: u32 = 120;

    while i < token.len() {
        let c = token[i];
        if c == b'.' {
            seen_dot = true;
            i += 1;
            continue;
        }
        if c == b'p' || c == b'P' {
            break;
        }
        let nib = (c as char).to_digit(16).expect("scan_number validated hex") as u8;
        let cur_bits = 128 - mant.leading_zeros();
        if cur_bits + 4 <= MAX_MANT_BITS {
            mant = (mant << 4) | nib as u128;
        } else {
            if nib != 0 {
                sticky = true;
            }
            low_drop += 4;
        }
        if seen_dot {
            frac_digits += 1;
        }
        i += 1;
    }

    let mut pexp: i64 = 0;
    if i < token.len() && (token[i] == b'p' || token[i] == b'P') {
        i += 1;
        let esign = if i < token.len() && (token[i] == b'+' || token[i] == b'-') {
            let s = token[i] == b'-';
            i += 1;
            s
        } else {
            false
        };
        let mut e: i64 = 0;
        while i < token.len() && token[i].is_ascii_digit() {
            e = (e.saturating_mul(10)).saturating_add((token[i] - b'0') as i64);
            if e > 1 << 40 {
                e = 1 << 40;
            }
            i += 1;
        }
        pexp = if esign { -e } else { e };
    }

    if mant == 0 {
        return if neg { -0.0 } else { 0.0 };
    }

    let exp2: i64 = pexp - 4 * frac_digits + low_drop;
    let bits: i64 = 128 - mant.leading_zeros() as i64;
    let target_bits = mantissa_bits + 1;
    let unbiased_msb = (bits - 1) + exp2;

    let bias = (1i64 << (exp_bits - 1)) - 1;
    let max_exp = bias;
    let min_normal_exp = 1 - bias;

    if unbiased_msb > max_exp {
        return if neg {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }

    let keep_bits: i64 = if unbiased_msb >= min_normal_exp {
        target_bits as i64
    } else {
        unbiased_msb - (min_normal_exp - mantissa_bits as i64) + 1
    };

    let drop: i64 = bits - keep_bits;

    let (mut kept, round_up) = if drop <= 0 {
        let shift = (-drop) as u32;
        (mant << shift.min(127), false)
    } else {
        let drop = drop as u32;
        let kept = if drop >= 128 { 0 } else { mant >> drop };
        let guard = if drop == 0 {
            false
        } else if drop <= 128 {
            (mant >> (drop - 1)) & 1 == 1
        } else {
            false
        };
        let rest_mask: u128 = if drop >= 1 {
            if drop > 128 {
                u128::MAX
            } else {
                (1u128 << (drop - 1)) - 1
            }
        } else {
            0
        };
        let rest_nonzero = (mant & rest_mask) != 0 || sticky;
        let round_up = guard && (rest_nonzero || (kept & 1) == 1);
        (kept, round_up)
    };

    if round_up {
        kept += 1;
    }

    if kept == 0 {
        return if neg { -0.0 } else { 0.0 };
    }

    let kept_bits = 128 - kept.leading_zeros() as i64;
    let lsb_weight = unbiased_msb - keep_bits + 1;
    let result_msb = lsb_weight + (kept_bits - 1);

    if result_msb > max_exp {
        return if neg {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        };
    }

    let mant_f = kept as f64;
    let scaled = ldexp(
        mant_f,
        lsb_weight.clamp(i32::MIN as i64, i32::MAX as i64) as i32,
    );
    if neg {
        -scaled
    } else {
        scaled
    }
}

/// Build the `float{4,8}in_internal` syntax error (float.c:206/293/...).
fn invalid_input(type_name: &str, orig_string: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {type_name}: \"{orig_string}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// Build the ERANGE out-of-range error (float.c:287/489). `errnumber` is the
/// number token only (C truncates at `endptr`); the type name is fixed.
fn out_of_range(errnumber: &str, fixed_type: &str) -> PgError {
    PgError::error(format!(
        "\"{errnumber}\" is out of range for type {fixed_type}"
    ))
    .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `float8in_internal()` (float.c:394) — guts of `float8in`.
///
/// `endptr_consumed` mirrors the C `endptr_p` out-parameter: when `Some`, on
/// success it is set to the byte offset (into the original `num`) just past the
/// trailing whitespace, and trailing junk is the caller's problem; when `None`,
/// trailing junk is an error here.
pub fn float8in_internal(
    num: &str,
    endptr_consumed: Option<&mut usize>,
    type_name: &str,
    orig_string: &str,
) -> PgResult<f64> {
    let bytes = num.as_bytes();

    let mut start = 0usize;
    while start < bytes.len() && bytes[start].is_ascii_whitespace() {
        start += 1;
    }

    if start >= bytes.len() {
        return Err(invalid_input(type_name, orig_string));
    }

    let rest = &bytes[start..];

    let (val, tok_end): (f64, usize) = match scan_number(rest) {
        Some(tok) => {
            let token = &num[start..start + tok.len];
            let parsed: f64 = match tok.kind {
                NumKind::Decimal => token.parse().map_err(|e| {
                    PgError::error(format!(
                        "float8in_internal: scan_number yields a strtod-shaped token: {e}"
                    ))
                })?,
                NumKind::Hex => parse_hex_float(token.as_bytes()),
            };
            if parsed.is_infinite() {
                return Err(out_of_range(token, "double precision"));
            }
            if parsed == 0.0 && tok.nonzero {
                return Err(out_of_range(token, "double precision"));
            }
            (parsed, tok.len)
        }
        None => match special_float8(rest) {
            Some((v, n)) => (v, n),
            None => return Err(invalid_input(type_name, orig_string)),
        },
    };

    let mut end = start + tok_end;
    while end < bytes.len() && bytes[end].is_ascii_whitespace() {
        end += 1;
    }

    if let Some(slot) = endptr_consumed {
        *slot = end;
    } else if end != bytes.len() {
        return Err(invalid_input(type_name, orig_string));
    }

    Ok(val)
}

/// `float4in_internal()` (float.c:182) — guts of `float4in`. Uses `strtof`
/// (parse as `f32`); the fixed type name in the ERANGE message is "real".
pub fn float4in_internal(
    num: &str,
    endptr_consumed: Option<&mut usize>,
    type_name: &str,
    orig_string: &str,
) -> PgResult<f32> {
    let bytes = num.as_bytes();

    let mut start = 0usize;
    while start < bytes.len() && bytes[start].is_ascii_whitespace() {
        start += 1;
    }

    if start >= bytes.len() {
        return Err(invalid_input(type_name, orig_string));
    }

    let rest = &bytes[start..];

    let (val, tok_end): (f32, usize) = match scan_number(rest) {
        Some(tok) => {
            let token = &num[start..start + tok.len];
            let parsed: f32 = match tok.kind {
                NumKind::Decimal => token.parse().map_err(|e| {
                    PgError::error(format!(
                        "float4in_internal: scan_number yields a strtof-shaped token: {e}"
                    ))
                })?,
                NumKind::Hex => parse_hex_float32(token.as_bytes()),
            };
            if parsed.is_infinite() {
                return Err(out_of_range(token, "real"));
            }
            if parsed == 0.0 && tok.nonzero {
                return Err(out_of_range(token, "real"));
            }
            (parsed, tok.len)
        }
        None => match special_float4(rest) {
            Some((v, n)) => (v, n),
            None => return Err(invalid_input(type_name, orig_string)),
        },
    };

    let mut end = start + tok_end;
    while end < bytes.len() && bytes[end].is_ascii_whitespace() {
        end += 1;
    }

    if let Some(slot) = endptr_consumed {
        *slot = end;
    } else if end != bytes.len() {
        return Err(invalid_input(type_name, orig_string));
    }

    Ok(val)
}

/// Recognize the explicit special spellings the C code checks after `strtod`
/// fails (float.c:434-468). Order matters: "Infinity" before "inf".
fn special_float8(s: &[u8]) -> Option<(f64, usize)> {
    if strncasecmp_eq(s, b"NaN") {
        Some((get_float8_nan(), 3))
    } else if strncasecmp_eq(s, b"Infinity") {
        Some((get_float8_infinity(), 8))
    } else if strncasecmp_eq(s, b"+Infinity") {
        Some((get_float8_infinity(), 9))
    } else if strncasecmp_eq(s, b"-Infinity") {
        Some((-get_float8_infinity(), 9))
    } else if strncasecmp_eq(s, b"inf") {
        Some((get_float8_infinity(), 3))
    } else if strncasecmp_eq(s, b"+inf") {
        Some((get_float8_infinity(), 4))
    } else if strncasecmp_eq(s, b"-inf") {
        Some((-get_float8_infinity(), 4))
    } else {
        None
    }
}

/// `special_float8` for `f32` (float.c:228-262).
fn special_float4(s: &[u8]) -> Option<(f32, usize)> {
    if strncasecmp_eq(s, b"NaN") {
        Some((get_float4_nan(), 3))
    } else if strncasecmp_eq(s, b"Infinity") {
        Some((get_float4_infinity(), 8))
    } else if strncasecmp_eq(s, b"+Infinity") {
        Some((get_float4_infinity(), 9))
    } else if strncasecmp_eq(s, b"-Infinity") {
        Some((-get_float4_infinity(), 9))
    } else if strncasecmp_eq(s, b"inf") {
        Some((get_float4_infinity(), 3))
    } else if strncasecmp_eq(s, b"+inf") {
        Some((get_float4_infinity(), 4))
    } else if strncasecmp_eq(s, b"-inf") {
        Some((-get_float4_infinity(), 4))
    } else {
        None
    }
}

/// `float8in()` core (float.c:363): parse a full cstring as `double precision`.
pub fn float8in(num: &str) -> PgResult<f64> {
    float8in_internal(num, None, "double precision", num)
}

/// `float4in()` core (float.c:163): parse a full cstring as `real`.
pub fn float4in(num: &str) -> PgResult<f32> {
    float4in_internal(num, None, "real", num)
}

/// `float8out_internal()` (float.c:536). Reads the live `extra_float_digits`
/// GUC global (C's `extra_float_digits`), exactly as the C function does.
pub fn float8out_internal(num: f64) -> String {
    float8out_internal_with(num, crate::get_extra_float_digits())
}

/// `float8out_internal()` parameterized by `extra_float_digits`.
pub fn float8out_internal_with(num: f64, extra_float_digits: i32) -> String {
    if extra_float_digits > 0 {
        return double_to_shortest_decimal(num);
    }
    let ndig = DBL_DIG + extra_float_digits;
    pg_strfromd_f64(num, ndig)
}

/// `float8out()` core (float.c:521).
pub fn float8out(num: f64) -> String {
    float8out_internal(num)
}

/// `float4out()` core (float.c:318). Reads the live `extra_float_digits` GUC
/// global (C's `extra_float_digits`), exactly as the C function does.
pub fn float4out(num: f32) -> String {
    float4out_with(num, crate::get_extra_float_digits())
}

/// `float4out()` parameterized by `extra_float_digits`.
pub fn float4out_with(num: f32, extra_float_digits: i32) -> String {
    if extra_float_digits > 0 {
        return float_to_shortest_decimal(num);
    }
    let ndig = FLT_DIG + extra_float_digits;
    pg_strfromd_f32(num, ndig)
}

/// `pg_strfromd` for `f64`: `snprintf("%.*g", ndig, num)` with PostgreSQL's
/// special-value spellings.
fn pg_strfromd_f64(num: f64, ndig: i32) -> String {
    if num.is_nan() {
        return "NaN".to_string();
    }
    if num.is_infinite() {
        return if num < 0.0 {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    format_g(num, ndig)
}

/// `pg_strfromd` for `f32`: the value is widened to `f64` for the `%g`
/// conversion, exactly as the C library does with a promoted float arg.
fn pg_strfromd_f32(num: f32, ndig: i32) -> String {
    if num.is_nan() {
        return "NaN".to_string();
    }
    if num.is_infinite() {
        return if num < 0.0 {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    format_g(num as f64, ndig)
}

fn trim_trailing_zeros(s: &str) -> &str {
    s.trim_end_matches('0')
}

/// Render a finite `f64` exactly as C's `snprintf("%.*g", prec, val)` would:
/// `prec` significant digits (`prec == 0` treated as 1), round-half-to-even,
/// trailing zeros stripped, `%e` iff `exp < -4 || exp >= prec`, exponent with
/// at least two digits.
fn format_g(val: f64, mut prec: i32) -> String {
    debug_assert!(val.is_finite());

    let mut out = String::new();

    if prec <= 0 {
        prec = 1;
    }

    if val == 0.0 {
        out.push_str(if val.is_sign_negative() { "-0" } else { "0" });
        return out;
    }

    let neg = val < 0.0;
    let a = val.abs();

    let sci = format!("{:.*e}", (prec - 1) as usize, a);
    let (mant, exp_str) = sci.split_once('e').expect("scientific format has 'e'");
    let exp: i32 = exp_str.parse().expect("exponent is an integer");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();

    if neg {
        out.push('-');
    }

    if exp < -4 || exp >= prec {
        let frac = trim_trailing_zeros(&digits[1..]);
        out.push_str(&digits[..1]);
        if !frac.is_empty() {
            out.push('.');
            out.push_str(frac);
        }
        out.push('e');
        out.push_str(if exp < 0 { "-" } else { "+" });
        let mag = exp.unsigned_abs();
        out.push_str(&format!("{mag:02}"));
    } else if exp >= 0 {
        let intlen = (exp + 1) as usize;
        if intlen >= digits.len() {
            out.push_str(&digits);
            for _ in 0..(intlen - digits.len()) {
                out.push('0');
            }
        } else {
            let (i, f) = digits.split_at(intlen);
            let frac = trim_trailing_zeros(f);
            out.push_str(i);
            if !frac.is_empty() {
                out.push('.');
                out.push_str(frac);
            }
        }
    } else {
        out.push_str("0.");
        for _ in 0..(-exp - 1) {
            out.push('0');
        }
        out.push_str(trim_trailing_zeros(&digits));
    }

    out
}

// ---------------------------------------------------------------------------
// BINARY WIRE CODEC (float.c:339-575): `float{4,8}recv` / `float{4,8}send`.
//
// These reproduce `libpq/pqformat.c`'s `pq_getmsgfloat{4,8}` /
// `pq_sendfloat{4,8}`: the IEEE-754 bit pattern transmitted big-endian (network
// byte order). The recv body reads its 4/8 wire bytes off the borrowed message
// slice; the send body returns the 4/8 wire bytes (the `bytea` payload).
// ---------------------------------------------------------------------------

/// `pq_copymsgbytes`' short-buffer error: SQLSTATE `08P01`
/// (`ERRCODE_PROTOCOL_VIOLATION`), "insufficient data left in message".
fn insufficient_data() -> PgError {
    PgError::error("insufficient data left in message").with_sqlstate(ERRCODE_PROTOCOL_VIOLATION)
}

/// `float4recv()` core (float.c:339): `pq_getmsgfloat4(buf)`.
pub fn float4recv(buf: &[u8]) -> PgResult<f32> {
    if buf.len() < 4 {
        return Err(insufficient_data());
    }
    let bits = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    Ok(f32::from_bits(bits))
}

/// `float8recv()` core (float.c:556): `pq_getmsgfloat8(buf)`.
pub fn float8recv(buf: &[u8]) -> PgResult<f64> {
    if buf.len() < 8 {
        return Err(insufficient_data());
    }
    let bits = u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    Ok(f64::from_bits(bits))
}

/// `float4send()` core (float.c:349): the 4-byte big-endian IEEE-754 wire image.
pub fn float4send(num: f32) -> Vec<u8> {
    num.to_bits().to_be_bytes().to_vec()
}

/// `float8send()` core (float.c:566): the 8-byte big-endian IEEE-754 wire image.
pub fn float8send(num: f64) -> Vec<u8> {
    num.to_bits().to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_doubles() {
        assert_eq!(float8in("1.5").unwrap(), 1.5);
        assert_eq!(float8in("  -2.25  ").unwrap(), -2.25);
        assert_eq!(float8in("1e10").unwrap(), 1e10);
        assert_eq!(float8in("0").unwrap(), 0.0);
        assert_eq!(float8in(".5").unwrap(), 0.5);
        assert_eq!(float8in("5.").unwrap(), 5.0);
    }

    #[test]
    fn parse_specials() {
        assert!(float8in("NaN").unwrap().is_nan());
        assert!(float8in("nan").unwrap().is_nan());
        assert_eq!(float8in("Infinity").unwrap(), f64::INFINITY);
        assert_eq!(float8in("infinity").unwrap(), f64::INFINITY);
        assert_eq!(float8in("-Infinity").unwrap(), f64::NEG_INFINITY);
        assert_eq!(float8in("inf").unwrap(), f64::INFINITY);
        assert_eq!(float8in("+inf").unwrap(), f64::INFINITY);
        assert_eq!(float8in("-inf").unwrap(), f64::NEG_INFINITY);
        assert!(float4in("NaN").unwrap().is_nan());
        assert_eq!(float4in("-Infinity").unwrap(), f32::NEG_INFINITY);
    }

    #[test]
    fn parse_hex_floats_match_pg() {
        let cases: &[(&str, u64)] = &[
            ("0x1p4", 0x4030000000000000),
            ("0X1.8p1", 0x4008000000000000),
            ("0x10", 0x4030000000000000),
            ("0xA", 0x4024000000000000),
            ("0x.8p1", 0x3ff0000000000000),
            ("-0x1p4", 0xc030000000000000),
            ("0x1.999999999999ap-4", 0x3fb999999999999a),
            ("0x0", 0x0000000000000000),
            ("0x1p-1074", 0x0000000000000001),
            ("0x0.0000000000001p-1022", 0x0000000000000001),
            ("0x1.fffffffffffffp1023", 0x7fefffffffffffff),
            ("0x1.0000000000001p0", 0x3ff0000000000001),
            ("0x1.00000000000008p0", 0x3ff0000000000000),
            ("0x1.00000000000018p0", 0x3ff0000000000002),
            ("0x1.fffffffffffff8p0", 0x4000000000000000),
            ("0x3p-1", 0x3ff8000000000000),
            ("0x1.8p0", 0x3ff8000000000000),
            ("0xabcdefp0", 0x416579bde0000000),
            ("0x1p-1022", 0x0010000000000000),
            ("0x1p-1023", 0x0008000000000000),
            ("0x.1p4", 0x3ff0000000000000),
        ];
        for &(lit, bits) in cases {
            let v = float8in(lit).unwrap_or_else(|e| panic!("{lit}: {}", e.message()));
            assert_eq!(v.to_bits(), bits, "float8 {lit}: got {:#018x} want {bits:#018x}", v.to_bits());
        }

        let cases4: &[(&str, u32)] = &[
            ("0x1p4", 0x41800000),
            ("0x10", 0x41800000),
            ("0x1.8p1", 0x40400000),
            ("-0x1.8p1", 0xc0400000),
            ("0x1p-149", 0x00000001),
            ("0x1.fffffep127", 0x7f7fffff),
            ("0x1.000001p0", 0x3f800000),
            ("0x1.000002p0", 0x3f800001),
            ("0x1.000003p0", 0x3f800002),
            ("0x1p-126", 0x00800000),
            ("0x1p-127", 0x00400000),
        ];
        for &(lit, bits) in cases4 {
            let v = float4in(lit).unwrap_or_else(|e| panic!("{lit}: {}", e.message()));
            assert_eq!(v.to_bits(), bits, "float4 {lit}: got {:#010x} want {bits:#010x}", v.to_bits());
        }
    }

    #[test]
    fn hex_float_overflow_underflow_and_junk_match_pg() {
        let err = float8in("0x1p1024").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(err.message(), "\"0x1p1024\" is out of range for type double precision");
        assert_eq!(float8in("0x1p1000000").unwrap_err().sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(float8in("0x1p-1075").unwrap_err().sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(float8in("0x1p-1000000").unwrap_err().sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(float4in("0x1p128").unwrap_err().sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(float4in("0x1p-150").unwrap_err().sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);

        let err = float8in("0x").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(err.message(), "invalid input syntax for type double precision: \"0x\"");
        assert_eq!(float8in("0x1p").unwrap_err().sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(float8in("0xg").unwrap_err().sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(float8in("0x10abcxyz").unwrap_err().sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

        assert_eq!(float8in("  0x1p4  ").unwrap(), 16.0);
        assert_eq!(float8in("+0x1p4").unwrap(), 16.0);
        assert_eq!(float8in("0x0").unwrap(), 0.0);
        assert_eq!(float8in("0x0p0").unwrap(), 0.0);
    }

    #[test]
    fn hex_float_endptr_for_geometric_types() {
        let mut endptr = 0usize;
        let v = float8in_internal("0x10,0x1p4", Some(&mut endptr), "point", "0x10,0x1p4").unwrap();
        assert_eq!(v, 16.0);
        assert_eq!(endptr, 4);

        let mut endptr = 0usize;
        let v = float8in_internal("0x1p,5", Some(&mut endptr), "point", "0x1p,5").unwrap();
        assert_eq!(v, 1.0);
        assert_eq!(endptr, 3);
    }

    #[test]
    fn empty_and_junk_are_syntax_errors() {
        let err = float8in("").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(err.message(), "invalid input syntax for type double precision: \"\"");

        let err = float8in("  ").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

        let err = float8in("1.5x").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
        assert_eq!(err.message(), "invalid input syntax for type double precision: \"1.5x\"");

        let err = float8in("xyz").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

        let err = float4in("abc").unwrap_err();
        assert_eq!(err.message(), "invalid input syntax for type real: \"abc\"");
    }

    #[test]
    fn overflow_and_underflow_raise_22003() {
        let err = float8in("1e400").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(err.message(), "\"1e400\" is out of range for type double precision");
        let err = float8in("1e-400").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(err.message(), "\"1e-400\" is out of range for type double precision");
        let err = float4in("1e40").unwrap_err();
        assert_eq!(err.message(), "\"1e40\" is out of range for type real");
        let err = float4in("1e-50").unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    }

    #[test]
    fn endptr_path_allows_trailing_junk() {
        let mut endptr = 0usize;
        let v = float8in_internal("2.71, 2.0", Some(&mut endptr), "point", "2.71, 2.0").unwrap();
        assert_eq!(v, 2.71);
        assert_eq!(endptr, 4);

        let mut endptr2 = 0usize;
        let v = float8in_internal("  2.71  rest", Some(&mut endptr2), "box", "  2.71  rest").unwrap();
        assert_eq!(v, 2.71);
        assert_eq!(endptr2, 8);
    }

    #[test]
    fn output_shortest_roundtrip_default_guc() {
        assert_eq!(float8out(0.0), "0");
        assert_eq!(float8out(-0.0), "-0");
        assert_eq!(float8out(1.5), "1.5");
        assert_eq!(float8out(0.1), "0.1");
        assert_eq!(float8out(1.0e-5), "1e-05");
        assert_eq!(float8out(f64::INFINITY), "Infinity");
        assert_eq!(float8out(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(float8out(f64::NAN), "NaN");

        assert_eq!(float4out(1.5_f32), "1.5");
        assert_eq!(float4out(0.0_f32), "0");
        assert_eq!(float4out(f32::INFINITY), "Infinity");
    }

    #[test]
    fn roundtrip_in_out() {
        for &s in &["1.5", "3.14159265358979", "1e10", "-2.5e-3", "0", "123456.789"] {
            let v = float8in(s).unwrap();
            let out = float8out(v);
            assert_eq!(float8in(&out).unwrap(), v, "roundtrip {s} -> {out}");
        }
    }

    #[test]
    fn output_legacy_g_path() {
        assert_eq!(float8out_internal_with(0.1, 0), "0.1");
        assert_eq!(float8out_internal_with(1.0, 0), "1");
        assert_eq!(float8out_internal_with(1e-5, 0), "1e-05");
        assert_eq!(float8out_internal_with(1e20, 0), "1e+20");
        assert_eq!(float8out_internal_with(f64::INFINITY, 0), "Infinity");
        assert_eq!(float4out_with(1.5_f32, 0), "1.5");
    }

    #[test]
    fn legacy_g_path_values() {
        assert_eq!(format_g(1.0e20, 15), "1e+20");
        assert_eq!(format_g(123456.789, 15), "123456.789");
        assert_eq!(format_g(0.000123, 15), "0.000123");
        assert_eq!(format_g(0.0, 1), "0");
        assert_eq!(format_g(-0.0, 1), "-0");
        assert_eq!(format_g(-2.5e-3, 6), "-0.0025");
        assert_eq!(format_g(1.0, 0), "1");
        assert_eq!(format_g(100.0, 15), "100");
    }

    #[test]
    fn wire_send_is_big_endian_ieee_bits() {
        assert_eq!(float4send(1.5_f32), 1.5_f32.to_bits().to_be_bytes().to_vec());
        assert_eq!(float8send(1.5_f64), 1.5_f64.to_bits().to_be_bytes().to_vec());
        assert_eq!(float8send(1.0), vec![0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(float4send(1.0_f32), vec![0x3F, 0x80, 0x00, 0x00]);
    }

    #[test]
    fn wire_recv_send_roundtrip() {
        for &v in &[0.0_f64, 1.5, -2.25, 1e10, -3.14159265358979, f64::INFINITY] {
            assert_eq!(float8recv(&float8send(v)).unwrap(), v);
        }
        assert!(float8recv(&float8send(f64::NAN)).unwrap().is_nan());
        for &v in &[0.0_f32, 1.5, -2.25, 1e10, f32::NEG_INFINITY] {
            assert_eq!(float4recv(&float4send(v)).unwrap(), v);
        }
        assert!(float4recv(&float4send(f32::NAN)).unwrap().is_nan());
    }

    #[test]
    fn wire_recv_short_buffer_errors() {
        let e4 = float4recv(&[0u8; 3]).unwrap_err();
        assert_eq!(e4.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
        assert_eq!(e4.message(), "insufficient data left in message");
        let e8 = float8recv(&[0u8; 7]).unwrap_err();
        assert_eq!(e8.sqlstate(), ERRCODE_PROTOCOL_VIOLATION);
        assert!(float4recv(&[]).is_err());
        assert!(float8recv(&[]).is_err());
    }

    #[test]
    fn ldexp_matches_powers_of_two() {
        assert_eq!(ldexp(1.0, 4), 16.0);
        assert_eq!(ldexp(3.0, -1), 1.5);
        assert_eq!(ldexp(1.0, 1024), f64::INFINITY);
        assert_eq!(ldexp(1.0, -1075), 0.0);
        assert_eq!(ldexp(1.0, -1074).to_bits(), 0x0000000000000001);
        assert_eq!(ldexp(1.0, -1022).to_bits(), 0x0010000000000000);
        assert!(ldexp(f64::NAN, 3).is_nan());
        assert_eq!(ldexp(0.0, 5), 0.0);
    }
}
