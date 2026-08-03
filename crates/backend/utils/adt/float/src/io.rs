use ::ryu::{double_to_shortest_decimal_bufn, float_to_shortest_decimal_bufn};
use ::types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROTOCOL_VIOLATION,
};

use crate::{get_float4_infinity, get_float4_nan, get_float8_infinity, get_float8_nan};
use crate::{DBL_DIG, FLT_DIG};

// C: palloc(32) in float4out/float8out_internal.
pub const MAXDOUBLEWIDTH: usize = 32;

// C isspace() default-locale set (\v included; is_ascii_whitespace lacks it).
#[inline]
fn c_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

fn strncasecmp_eq(s: &[u8], lit: &[u8]) -> bool {
    if s.len() < lit.len() {
        return false;
    }
    s[..lit.len()].iter().zip(lit).all(|(a, b)| a.eq_ignore_ascii_case(b))
}

// ldexp without a libm dep: exact powers of two via exponent-field bits.
fn ldexp(mant: f64, exp: i32) -> f64 {
    if mant == 0.0 || !mant.is_finite() {
        return mant;
    }
    let mut result = mant;
    let mut e = exp;
    while e > 0 {
        let step = e.min(1023);
        result *= f64::from_bits(((step as u64) + 1023) << 52);
        if !result.is_finite() {
            return result;
        }
        e -= step;
    }
    while e < 0 {
        let step = (-e).min(1022);
        result *= f64::from_bits(((1023 - step) as u64) << 52);
        if result == 0.0 {
            return result;
        }
        e += step;
    }
    result
}

pub enum NumKind {
    Decimal,
    Hex,
}

pub struct NumToken {
    pub len: usize,
    pub nonzero: bool,
    pub kind: NumKind,
}

// The strtod/strtof-recognizable leading token: decimal grammar plus the C99
// hex-float grammar the platform strtod accepts. No leading-whitespace skip.
// pub for adt_datetime's ParseISO8601Number strtod model (p1-laney: its
// "anything strtod would take" contract needs the same scanner; the fuzz
// witness is 'P0X8Y' = 8 years on real 18.3).
pub fn scan_number(s: &[u8]) -> Option<NumToken> {
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

pub fn parse_hex_float(token: &[u8]) -> f64 {
    round_to_float(token, 52, 11)
}

fn parse_hex_float32(token: &[u8]) -> f32 {
    round_to_float(token, 23, 8) as f32
}

// Round-to-nearest ties-to-even hex-float conversion, with overflow to
// infinity and gradual underflow, as strtod/strtof do.
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
        return if neg { f64::NEG_INFINITY } else { f64::INFINITY };
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
        // Cap before narrowing: a huge negative binary exponent can push
        // `drop` past 2^32, and `as u32` truncation (e.g. 2^40 -> 0) made
        // "0x...p-<huge>" round-trip to a bogus subnormal-scale value
        // instead of underflowing to 0 (fuzz-caught via guc parse_int;
        // glibc strtod returns 0 + ERANGE). Any drop > 128 behaves
        // identically (all bits dropped), so 200 is a faithful cap.
        let drop = drop.min(200) as u32;
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
        return if neg { f64::NEG_INFINITY } else { f64::INFINITY };
    }

    let mant_f = kept as f64;
    let scaled = ldexp(mant_f, lsb_weight.clamp(i32::MIN as i64, i32::MAX as i64) as i32);
    if neg {
        -scaled
    } else {
        scaled
    }
}

#[cold]
#[inline(never)]
fn invalid_input(type_name: &str, orig_string: &str) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {type_name}: \"{orig_string}\""
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

// C truncates the reported number at endptr; the type name is fixed.
#[cold]
#[inline(never)]
fn out_of_range(errnumber: &str, fixed_type: &str) -> PgError {
    PgError::error(format!(
        "\"{errnumber}\" is out of range for type {fixed_type}"
    ))
    .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

// endptr_consumed mirrors C's endptr_p: Some => report the stop offset (past
// trailing whitespace) and leave trailing junk to the caller; None => error.
/// strtod(3) model over the shared token machinery: parse the longest
/// leading number token (after optional C-locale whitespace, exactly as
/// strtod skips), returning (value, bytes_consumed, range_error).
/// `range_error` mirrors glibc errno==ERANGE — decimal/hex overflow to
/// +-inf, nonzero digits rounding to zero, or ISO C gradual underflow: a
/// subnormal result whose conversion was INEXACT (an exactly-representable
/// subnormal like 0x1p-1074 sets no errno; probed on glibc 2.36,
/// postgres:18.3 image, 2026-07-31) — while the inf/nan WORDS parse with
/// no errno. None = no token (strtod endptr == str).
/// Consumers needing C parse cascades verbatim (datetime.c
/// ParseISO8601Number) call this instead of re-modeling strtod.
pub fn strtod_c(s: &[u8]) -> Option<(f64, usize, bool)> {
    let mut start = 0usize;
    while start < s.len() && c_isspace(s[start]) {
        start += 1;
    }
    let rest = &s[start..];
    match scan_number(rest) {
        Some(tok) => {
            let token = &rest[..tok.len];
            let parsed: f64 = match tok.kind {
                NumKind::Decimal => std::str::from_utf8(token)
                    .expect("ascii token")
                    .parse()
                    .expect("scan_number yields a parseable decimal token"),
                NumKind::Hex => parse_hex_float(token),
            };
            let range = parsed.is_infinite()
                || (parsed == 0.0 && tok.nonzero)
                || subnormal_inexact(token, &tok.kind, parsed);
            Some((parsed, start + tok.len, range))
        }
        None => special_float8(rest).map(|(v, n)| (v, start + n, false)),
    }
}

/// ISO C underflow test for strtod_c: true iff `parsed` is subnormal
/// (0 < |v| < DBL_MIN) and `token` is not EXACTLY equal to it. Every
/// subnormal is k*2^-1074 (0 < k < 2^52), i.e. the integer N = k*5^1074
/// times 10^-1074, so decimal exactness is a digit-string comparison and
/// hex exactness a bit comparison — no float arithmetic, no rounding.
/// Cold path: runs only for subnormal results.
fn subnormal_inexact(token: &[u8], kind: &NumKind, parsed: f64) -> bool {
    if parsed == 0.0 || parsed.abs() >= f64::MIN_POSITIVE {
        return false;
    }
    let k = parsed.to_bits() & ((1u64 << 52) - 1); /* biased exponent is 0 */

    match kind {
        NumKind::Hex => {
            // literal = m * 2^h; exact iff m*2^h == k*2^-1074 after
            // normalizing both to odd mantissas.
            let mut i = 0usize;
            if token[i] == b'+' || token[i] == b'-' {
                i += 1;
            }
            i += 2; /* 0x */
            let mut m: u128 = 0;
            let mut h: i64 = 0;
            let mut seen_dot = false;
            while i < token.len() {
                match token[i] {
                    b'.' => seen_dot = true,
                    b'p' | b'P' => break,
                    c => {
                        let nib = (c as char).to_digit(16).expect("hex token") as u128;
                        if m >> 120 != 0 {
                            if nib != 0 {
                                return true; /* dropped nonzero bits: inexact */
                            }
                            if !seen_dot {
                                h += 4;
                            }
                        } else {
                            m = (m << 4) | nib;
                            if seen_dot {
                                h -= 4;
                            }
                        }
                    }
                }
                i += 1;
            }
            if i < token.len() {
                i += 1; /* p */
                let neg = matches!(token.get(i), Some(b'-'));
                if matches!(token.get(i), Some(b'+') | Some(b'-')) {
                    i += 1;
                }
                let mut e: i64 = 0;
                while i < token.len() && token[i].is_ascii_digit() {
                    e = e.saturating_mul(10).saturating_add((token[i] - b'0') as i64);
                    i += 1;
                }
                h = h.saturating_add(if neg { -e } else { e });
            }
            debug_assert!(m != 0, "parsed != 0 implies nonzero mantissa");
            let (m, h) = (m >> m.trailing_zeros(), h + m.trailing_zeros() as i64);
            let (k, kexp) = (k >> k.trailing_zeros(), -1074 + k.trailing_zeros() as i64);
            m != k as u128 || h != kexp
        }
        NumKind::Decimal => {
            // literal = d * 10^-q; v = N * 10^-1074 with N = k*5^1074.
            // Equal iff stripped digit strings match and the stripped
            // decimal exponents agree.
            let mut i = 0usize;
            if token[i] == b'+' || token[i] == b'-' {
                i += 1;
            }
            let mut d: Vec<u8> = Vec::new(); /* significant digits, no dot */
            let mut frac: i64 = 0;
            let mut seen_dot = false;
            while i < token.len() {
                match token[i] {
                    b'.' => seen_dot = true,
                    b'e' | b'E' => break,
                    c => {
                        if !(d.is_empty() && c == b'0') {
                            d.push(c - b'0');
                        }
                        if seen_dot {
                            frac += 1;
                        } else if d.is_empty() {
                            /* leading zeros before the dot: no digit kept,
                             * no exponent effect */
                        }
                    }
                }
                i += 1;
            }
            let mut e10: i64 = 0;
            if i < token.len() {
                i += 1; /* e */
                let neg = matches!(token.get(i), Some(b'-'));
                if matches!(token.get(i), Some(b'+') | Some(b'-')) {
                    i += 1;
                }
                let mut e: i64 = 0;
                while i < token.len() && token[i].is_ascii_digit() {
                    e = e.saturating_mul(10).saturating_add((token[i] - b'0') as i64);
                    i += 1;
                }
                e10 = if neg { -e } else { e };
            }
            let q: i64 = frac - e10;
            let tzd = d.iter().rev().take_while(|&&x| x == 0).count() as i64;
            d.truncate(d.len() - tzd as usize);

            // N = k * 5^1074, little-endian decimal digits.
            let mut n: Vec<u8> = {
                let mut v = Vec::new();
                let mut x = k;
                while x > 0 {
                    v.push((x % 10) as u8);
                    x /= 10;
                }
                v
            };
            for _ in 0..1074 {
                let mut carry = 0u16;
                for dig in n.iter_mut() {
                    let t = *dig as u16 * 5 + carry;
                    *dig = (t % 10) as u8;
                    carry = t / 10;
                }
                while carry > 0 {
                    n.push((carry % 10) as u8);
                    carry /= 10;
                }
            }
            let tzn = n.iter().take_while(|&&x| x == 0).count() as i64;
            let n_stripped = &n[tzn as usize..];

            n_stripped.len() != d.len()
                || n_stripped.iter().rev().zip(d.iter()).any(|(a, b)| a != b)
                || q + tzn != 1074 + tzd
        }
    }
}

pub fn float8in_internal(
    num: &str,
    endptr_consumed: Option<&mut usize>,
    type_name: &str,
    orig_string: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<f64> {
    let bytes = num.as_bytes();

    let mut start = 0usize;
    while start < bytes.len() && c_isspace(bytes[start]) {
        start += 1;
    }

    if start >= bytes.len() {
        return ereturn(
            escontext.as_deref_mut(),
            0.0,
            invalid_input(type_name, orig_string),
        );
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
            if parsed.is_infinite() || (parsed == 0.0 && tok.nonzero) {
                return ereturn(
                    escontext.as_deref_mut(),
                    0.0,
                    out_of_range(token, "double precision"),
                );
            }
            (parsed, tok.len)
        }
        None => match special_float8(rest) {
            Some((v, n)) => (v, n),
            None => {
                return ereturn(
                    escontext.as_deref_mut(),
                    0.0,
                    invalid_input(type_name, orig_string),
                )
            }
        },
    };

    let mut end = start + tok_end;
    while end < bytes.len() && c_isspace(bytes[end]) {
        end += 1;
    }

    if let Some(slot) = endptr_consumed {
        *slot = end;
    } else if end != bytes.len() {
        return ereturn(
            escontext.as_deref_mut(),
            0.0,
            invalid_input(type_name, orig_string),
        );
    }

    Ok(val)
}

pub fn float4in_internal(
    num: &str,
    endptr_consumed: Option<&mut usize>,
    type_name: &str,
    orig_string: &str,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<f32> {
    let bytes = num.as_bytes();

    let mut start = 0usize;
    while start < bytes.len() && c_isspace(bytes[start]) {
        start += 1;
    }

    if start >= bytes.len() {
        return ereturn(
            escontext.as_deref_mut(),
            0.0,
            invalid_input(type_name, orig_string),
        );
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
            if parsed.is_infinite() || (parsed == 0.0 && tok.nonzero) {
                return ereturn(escontext.as_deref_mut(), 0.0, out_of_range(token, "real"));
            }
            (parsed, tok.len)
        }
        None => match special_float4(rest) {
            Some((v, n)) => (v, n),
            None => {
                return ereturn(
                    escontext.as_deref_mut(),
                    0.0,
                    invalid_input(type_name, orig_string),
                )
            }
        },
    };

    let mut end = start + tok_end;
    while end < bytes.len() && c_isspace(bytes[end]) {
        end += 1;
    }

    if let Some(slot) = endptr_consumed {
        *slot = end;
    } else if end != bytes.len() {
        return ereturn(
            escontext.as_deref_mut(),
            0.0,
            invalid_input(type_name, orig_string),
        );
    }

    Ok(val)
}

// strtod also consumes an optional nan(n-char-seq) payload; PG inherits it.
fn nan_payload_len(s: &[u8]) -> usize {
    if s.len() > 3 && s[3] == b'(' {
        let mut i = 4;
        while i < s.len() && (s[i].is_ascii_alphanumeric() || s[i] == b'_') {
            i += 1;
        }
        if i < s.len() && s[i] == b')' {
            return i + 1;
        }
    }
    3
}

// Order matters: "Infinity" before "inf". strtod also accepts a signed NaN.
pub fn special_float8(s: &[u8]) -> Option<(f64, usize)> {
    if strncasecmp_eq(s, b"NaN") {
        Some((get_float8_nan(), nan_payload_len(s)))
    } else if (s.first() == Some(&b'+') || s.first() == Some(&b'-')) && strncasecmp_eq(&s[1..], b"NaN")
    {
        let v = if s[0] == b'-' { -get_float8_nan() } else { get_float8_nan() };
        Some((v, 1 + nan_payload_len(&s[1..])))
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

fn special_float4(s: &[u8]) -> Option<(f32, usize)> {
    if strncasecmp_eq(s, b"NaN") {
        Some((get_float4_nan(), nan_payload_len(s)))
    } else if (s.first() == Some(&b'+') || s.first() == Some(&b'-')) && strncasecmp_eq(&s[1..], b"NaN")
    {
        let v = if s[0] == b'-' { -get_float4_nan() } else { get_float4_nan() };
        Some((v, 1 + nan_payload_len(&s[1..])))
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

pub fn float8in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<f64> {
    float8in_internal(num, None, "double precision", num, escontext)
}

pub fn float4in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<f32> {
    float4in_internal(num, None, "real", num, escontext)
}

// Out functions write into a caller buffer (>= MAXDOUBLEWIDTH) and return the
// length, unterminated; C pallocs 32 bytes and NUL-terminates.
pub fn float8out_internal(num: f64, buf: &mut [u8]) -> usize {
    float8out_internal_with(num, crate::get_extra_float_digits(), buf)
}

pub fn float8out_internal_with(num: f64, extra_float_digits: i32, buf: &mut [u8]) -> usize {
    if extra_float_digits > 0 {
        return double_to_shortest_decimal_bufn(num, buf);
    }
    pg_strfromd_f64(num, DBL_DIG + extra_float_digits, buf)
}

pub fn float8out(num: f64, buf: &mut [u8]) -> usize {
    float8out_internal(num, buf)
}

pub fn float4out(num: f32, buf: &mut [u8]) -> usize {
    float4out_with(num, crate::get_extra_float_digits(), buf)
}

pub fn float4out_with(num: f32, extra_float_digits: i32, buf: &mut [u8]) -> usize {
    if extra_float_digits > 0 {
        return float_to_shortest_decimal_bufn(num, buf);
    }
    pg_strfromd_f64(num as f64, FLT_DIG + extra_float_digits, buf)
}

// pg_strfromd: snprintf("%.*g") with PostgreSQL's special-value spellings.
// Cold: reached only when the extra_float_digits GUC is set <= 0.
fn pg_strfromd_f64(num: f64, ndig: i32, buf: &mut [u8]) -> usize {
    if num.is_nan() {
        buf[..3].copy_from_slice(b"NaN");
        return 3;
    }
    if num.is_infinite() {
        if num < 0.0 {
            buf[..9].copy_from_slice(b"-Infinity");
            return 9;
        }
        buf[..8].copy_from_slice(b"Infinity");
        return 8;
    }
    format_g(num, ndig, buf)
}

struct SliceWriter<'a> {
    buf: &'a mut [u8],
    len: usize,
}

impl core::fmt::Write for SliceWriter<'_> {
    fn write_str(&mut self, s: &str) -> core::fmt::Result {
        let b = s.as_bytes();
        if self.len + b.len() > self.buf.len() {
            return Err(core::fmt::Error);
        }
        self.buf[self.len..self.len + b.len()].copy_from_slice(b);
        self.len += b.len();
        Ok(())
    }
}

// %.*g semantics: prec significant digits (0 -> 1), round-half-even, trailing
// zeros stripped, %e form iff exp < -4 || exp >= prec, 2+ exponent digits.
fn format_g(val: f64, mut prec: i32, out: &mut [u8]) -> usize {
    use core::fmt::Write;

    debug_assert!(val.is_finite());

    if prec <= 0 {
        prec = 1;
    }

    let mut w = SliceWriter { buf: out, len: 0 };

    if val == 0.0 {
        let s: &[u8] = if val.is_sign_negative() { b"-0" } else { b"0" };
        w.buf[..s.len()].copy_from_slice(s);
        return s.len();
    }

    let neg = val < 0.0;
    let a = val.abs();

    let mut sci_buf = [0u8; 40];
    let mut sci = SliceWriter {
        buf: &mut sci_buf,
        len: 0,
    };
    write!(sci, "{:.*e}", (prec - 1) as usize, a).expect("40 bytes fit %.16e of any f64");
    let sci_len = sci.len;
    let sci = core::str::from_utf8(&sci_buf[..sci_len]).expect("ascii");
    let (mant, exp_str) = sci.split_once('e').expect("scientific format has 'e'");
    let exp: i32 = exp_str.parse().expect("exponent is an integer");

    let mut digits = [0u8; 20];
    let mut nd = 0usize;
    for c in mant.bytes() {
        if c != b'.' {
            digits[nd] = c;
            nd += 1;
        }
    }
    let digits = &digits[..nd];

    if neg {
        let _ = w.write_str("-");
    }

    let trimmed = |d: &[u8]| -> usize {
        let mut n = d.len();
        while n > 0 && d[n - 1] == b'0' {
            n -= 1;
        }
        n
    };

    if exp < -4 || exp >= prec {
        let frac_end = trimmed(&digits[1..]);
        let start = w.len;
        w.buf[start] = digits[0];
        w.len += 1;
        if frac_end > 0 {
            let _ = w.write_str(".");
            let start = w.len;
            w.buf[start..start + frac_end].copy_from_slice(&digits[1..1 + frac_end]);
            w.len += frac_end;
        }
        let _ = write!(w, "e{}{:02}", if exp < 0 { "-" } else { "+" }, exp.unsigned_abs());
    } else if exp >= 0 {
        let intlen = (exp + 1) as usize;
        if intlen >= digits.len() {
            let start = w.len;
            w.buf[start..start + digits.len()].copy_from_slice(digits);
            w.len += digits.len();
            for _ in 0..(intlen - digits.len()) {
                let _ = w.write_str("0");
            }
        } else {
            let (ip, fp) = digits.split_at(intlen);
            let frac_end = trimmed(fp);
            let start = w.len;
            w.buf[start..start + ip.len()].copy_from_slice(ip);
            w.len += ip.len();
            if frac_end > 0 {
                let _ = w.write_str(".");
                let start = w.len;
                w.buf[start..start + frac_end].copy_from_slice(&fp[..frac_end]);
                w.len += frac_end;
            }
        }
    } else {
        let _ = w.write_str("0.");
        for _ in 0..(-exp - 1) {
            let _ = w.write_str("0");
        }
        let n = trimmed(digits);
        let start = w.len;
        w.buf[start..start + n].copy_from_slice(&digits[..n]);
        w.len += n;
    }

    w.len
}

#[cold]
#[inline(never)]
fn insufficient_data() -> PgError {
    PgError::error("insufficient data left in message").with_sqlstate(ERRCODE_PROTOCOL_VIOLATION)
}

pub fn float4recv(buf: &[u8]) -> PgResult<f32> {
    if buf.len() < 4 {
        return Err(insufficient_data().into());
    }
    Ok(f32::from_bits(u32::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3],
    ])))
}

pub fn float8recv(buf: &[u8]) -> PgResult<f64> {
    if buf.len() < 8 {
        return Err(insufficient_data().into());
    }
    Ok(f64::from_bits(u64::from_be_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ])))
}

pub fn float4send(num: f32) -> [u8; 4] {
    num.to_bits().to_be_bytes()
}

pub fn float8send(num: f64) -> [u8; 8] {
    num.to_bits().to_be_bytes()
}
