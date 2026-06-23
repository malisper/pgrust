//! Family: I/O — text input/output (numeric.c `numeric_in`/`numeric_out`/
//! `set_var_from_str`/`get_str_from_var`/`get_str_from_var_sci`) and the binary
//! wire protocol (`numeric_recv`/`numeric_send`/`numericvar_serialize`/
//! `numericvar_deserialize`).
//!
//! This family also implements the two byte-image-based seams the unit OWNS for
//! `jsonb_util` ([`seam_numeric_eq`]/[`seam_numeric_cmp`]): value
//! equality/3-way comparison over two whole on-disk `numeric` varlenas.
//!
//! Text/wire decoders allocate digit buffers, so take an explicit `Mcx<'mcx>`
//! and return [`PgResult`] where the C `ereport`s on malformed input/overflow.

use mcx::{Mcx, PgVec};
use types_error::{
    PgError, PgResult, ERRCODE_INVALID_BINARY_REPRESENTATION, ERRCODE_INVALID_TEXT_REPRESENTATION,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_PROTOCOL_VIOLATION,
};
use ::types_numeric::var::{NumericSign, NumericVar};
use types_numeric::{
    numeric_digit_at, numeric_digits, numeric_is_nan, numeric_is_ninf, numeric_is_pinf,
    numeric_is_special, numeric_ndigits, numeric_sign, numeric_weight, NumericDigit, DEC_DIGITS,
    NBASE, NUMERIC_DSCALE_MASK, NUMERIC_WEIGHT_MAX,
};

use crate::convert;
use crate::kernel_transcendental;
use crate::kernel_var;

// ---------------------------------------------------------------------------
// Error builders (mirror the exact C ereport text / SQLSTATE).
// ---------------------------------------------------------------------------

/// C `isspace()` in the C/POSIX locale: matches exactly the six ASCII bytes
/// space (0x20), tab (0x09), newline (0x0A), vertical tab (0x0B), form feed
/// (0x0C), and carriage return (0x0D). Used by `numeric_in`'s leading-space
/// skip and trailing-junk checks. Deliberately NOT `char::is_whitespace`, which
/// would also accept NEL (0x85) and NBSP (0xA0) and so diverge from C.
#[inline]
fn is_c_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | 0x0B | 0x0C | b'\r')
}

/// Build the "invalid input syntax for type numeric" error with the exact C
/// message text, embedding the original (whole) input string.
fn invalid_syntax(s: &str) -> PgError {
    PgError::error(format!("invalid input syntax for type {}: \"{}\"", "numeric", s))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// Build the "value overflows numeric format" error (used by the exponent
/// overflow / weight-overflow guards, matching `make_result`'s errcode/text).
fn out_of_range() -> PgError {
    PgError::error("value overflows numeric format").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// "insufficient data left in message" -- the error `pq_getmsgbytes` /
/// `pq_copymsgbytes` raises (SQLSTATE `08P01`, `ERRCODE_PROTOCOL_VIOLATION`)
/// when a recv function tries to read past the end of the buffer.
#[inline]
fn insufficient_data() -> PgError {
    PgError::error("insufficient data left in message").with_sqlstate(ERRCODE_PROTOCOL_VIOLATION)
}

// ---------------------------------------------------------------------------
// Text input (numeric.c set_var_from_str / numeric_in).
// ---------------------------------------------------------------------------

/// `set_var_from_str(str, cp, dest, &endptr)`: parse a decimal string into a
/// fresh `NumericVar` in `mcx`. Returns the value and the byte offset where
/// parsing stopped.
///
/// `s` is the *whole* original input (used only for error messages); `start`
/// is the byte offset within `s` at which parsing begins (after any leading
/// spaces have been skipped by the caller). Mirrors numeric.c:7131 exactly:
/// parse an optional sign, an optional leading decimal point, the
/// integer/fraction digits (tracking `dweight`/`dscale`), an optional `e`/`E`
/// exponent, then convert the pure-decimal digit run to base NBASE with the
/// correct weight/ndigits/offset, and finally `strip_var`.
pub fn set_var_from_str<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    start: usize,
) -> PgResult<(NumericVar<'mcx>, usize)> {
    let bytes = s.as_bytes();
    let at = |i: usize| -> u8 {
        if i < bytes.len() {
            bytes[i]
        } else {
            0
        }
    };
    let mut cp = start;

    let mut have_dp = false;
    let mut sign = NumericSign::Pos;
    let mut dweight: i32 = -1;
    let mut dscale: i32 = 0;

    // Parse leading sign.
    match at(cp) {
        b'+' => {
            sign = NumericSign::Pos;
            cp += 1;
        }
        b'-' => {
            sign = NumericSign::Neg;
            cp += 1;
        }
        _ => {}
    }

    if at(cp) == b'.' {
        have_dp = true;
        cp += 1;
    }

    if !at(cp).is_ascii_digit() {
        return Err(invalid_syntax(s));
    }

    // Pure-decimal digit accumulator (values 0..=9) with DEC_DIGITS of leading
    // zero padding for later alignment. `palloc(strlen(cp) + DEC_DIGITS * 2)`.
    let mut decdigits: Vec<u8> =
        Vec::with_capacity(bytes.len().saturating_sub(cp) + DEC_DIGITS as usize * 2);
    // Leading padding for digit alignment.
    decdigits.resize(DEC_DIGITS as usize, 0);

    while at(cp) != 0 {
        let c = at(cp);
        if c.is_ascii_digit() {
            decdigits.push(c - b'0');
            cp += 1;
            if !have_dp {
                dweight += 1;
            } else {
                dscale += 1;
            }
        } else if c == b'.' {
            if have_dp {
                return Err(invalid_syntax(s));
            }
            have_dp = true;
            cp += 1;
            // Decimal point must not be followed by an underscore.
            if at(cp) == b'_' {
                return Err(invalid_syntax(s));
            }
        } else if c == b'_' {
            // Underscore must be followed by more digits.
            cp += 1;
            if !at(cp).is_ascii_digit() {
                return Err(invalid_syntax(s));
            }
        } else {
            break;
        }
    }

    let ddigits: i32 = decdigits.len() as i32 - DEC_DIGITS;
    // Trailing padding for digit alignment (DEC_DIGITS - 1 zeroes).
    decdigits.resize(decdigits.len() + (DEC_DIGITS as usize - 1), 0);

    // Handle exponent, if any.
    if at(cp) == b'e' || at(cp) == b'E' {
        let mut exponent: i64 = 0;
        let mut neg = false;

        cp += 1;
        if at(cp) == b'+' {
            cp += 1;
        } else if at(cp) == b'-' {
            neg = true;
            cp += 1;
        }

        if !at(cp).is_ascii_digit() {
            return Err(invalid_syntax(s));
        }

        while at(cp) != 0 {
            let c = at(cp);
            if c.is_ascii_digit() {
                exponent = exponent * 10 + (c - b'0') as i64;
                cp += 1;
                if exponent > (i32::MAX / 2) as i64 {
                    return Err(out_of_range());
                }
            } else if c == b'_' {
                cp += 1;
                if !at(cp).is_ascii_digit() {
                    return Err(invalid_syntax(s));
                }
            } else {
                break;
            }
        }

        if neg {
            exponent = -exponent;
        }

        dweight += exponent as i32;
        dscale -= exponent as i32;
        if dscale < 0 {
            dscale = 0;
        }
    }

    // Convert pure-decimal representation to base NBASE. First determine the
    // converted weight and ndigits. `offset` is the number of decimal zeroes to
    // insert before the first given digit to align the first NBASE digit.
    let weight: i32 = if dweight >= 0 {
        (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1
    } else {
        -((-dweight - 1) / DEC_DIGITS + 1)
    };
    let offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
    let mut ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

    // alloc_var(dest, ndigits) -- leaves the leading carry-slack headroom.
    let mut dest = kernel_var::alloc_var(mcx, ndigits.max(0) as usize)?;
    dest.sign = sign;
    dest.weight = weight;
    dest.dscale = dscale;

    // i = DEC_DIGITS - offset; digits = dest->digits (logical digit 0).
    let mut i: i32 = DEC_DIGITS - offset;
    let mut di: usize = dest.headroom;
    let dd = |idx: i32| -> i32 {
        if idx >= 0 && (idx as usize) < decdigits.len() {
            decdigits[idx as usize] as i32
        } else {
            0
        }
    };
    while ndigits > 0 {
        let d = ((dd(i) * 10 + dd(i + 1)) * 10 + dd(i + 2)) * 10 + dd(i + 3);
        dest.digits[di] = d as NumericDigit;
        di += 1;
        i += DEC_DIGITS;
        ndigits -= 1;
    }

    // Strip any leading/trailing zeroes, and normalize weight if zero.
    kernel_var::strip_var(&mut dest);

    Ok((dest, cp))
}

/// `set_var_from_non_decimal_integer_str`: parse a hex/oct/bin integer literal.
///
/// `s` is the whole original input (for error messages); `start` is the byte
/// offset of the first digit *after* the base prefix (e.g. after "0x"). The
/// sign and base prefix are assumed already consumed by the caller. Digit
/// groups that fit in `i64` are accumulated and folded into `dest` with
/// `mul_var`/`add_var`, exactly as numeric.c:7361 does, with overflow detected
/// when the weight exceeds the int16 storage field.
pub fn set_var_from_non_decimal_integer_str<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    start: usize,
    base: i32,
) -> PgResult<(NumericVar<'mcx>, usize)> {
    let bytes = s.as_bytes();
    let at = |i: usize| -> u8 {
        if i < bytes.len() {
            bytes[i]
        } else {
            0
        }
    };

    let firstdigit = start;
    let mut cp = start;

    // zero_var(dest)
    let mut dest = NumericVar::zero(mcx);

    // Process input digits in groups that fit in i64. `tmp` is the value of the
    // current group, `mul` is base^n for the n digits in the group. We start a
    // new group when `mul * base` would overflow i64::MAX.
    let mut tmp: i64 = 0;
    let mut mul: i64 = 1;

    // Returns Some(digit value) if `c` is a valid digit in `base`, else None.
    // For base 16 this mirrors `xdigit_value`; for 8/2 the explicit range check.
    let digit_val = |c: u8, base: i32| -> Option<i64> {
        let v = match c {
            b'0'..=b'9' => (c - b'0') as i64,
            b'a'..=b'f' => (c - b'a') as i64 + 10,
            b'A'..=b'F' => (c - b'A') as i64 + 10,
            _ => return None,
        };
        if v < base as i64 {
            Some(v)
        } else {
            None
        }
    };

    // `base` is 2, 8, or 16; anything else is "should never happen; invalid".
    if base != 2 && base != 8 && base != 16 {
        return Err(invalid_syntax(s));
    }

    let base_i64 = base as i64;
    let threshold = i64::MAX / base_i64;

    loop {
        let c = at(cp);
        if let Some(dv) = digit_val(c, base) {
            if mul > threshold {
                // Add the contribution from this group of digits.
                let mul_var_v = kernel_transcendental::int64_to_numericvar(mcx, mul)?;
                dest = kernel_var::mul_var(mcx, &dest, &mul_var_v, 0)?;
                let tmp_var = kernel_transcendental::int64_to_numericvar(mcx, tmp)?;
                dest = kernel_var::add_var(mcx, &dest, &tmp_var)?;

                // Result will overflow if the weight overflows int16.
                if dest.weight > NUMERIC_WEIGHT_MAX {
                    return Err(out_of_range());
                }

                // Begin a new group.
                tmp = 0;
                mul = 1;
            }

            tmp = tmp * base_i64 + dv;
            mul *= base_i64;
            cp += 1;
        } else if c == b'_' {
            // Underscore must be followed by more digits.
            cp += 1;
            if digit_val(at(cp), base).is_none() {
                return Err(invalid_syntax(s));
            }
        } else {
            break;
        }
    }

    // Check that we got at least one digit.
    if cp == firstdigit {
        return Err(invalid_syntax(s));
    }

    // Add the contribution from the final group of digits.
    let mul_var_v = kernel_transcendental::int64_to_numericvar(mcx, mul)?;
    dest = kernel_var::mul_var(mcx, &dest, &mul_var_v, 0)?;
    let tmp_var = kernel_transcendental::int64_to_numericvar(mcx, tmp)?;
    dest = kernel_var::add_var(mcx, &dest, &tmp_var)?;

    if dest.weight > NUMERIC_WEIGHT_MAX {
        return Err(out_of_range());
    }

    // C sets `dest->sign = sign` here; this scaffold drops the sign parameter,
    // so the caller (`numeric_in`) applies the sign to the returned value.

    Ok((dest, cp))
}

/// `numeric_in(str, typelem, typmod)`: full SQL text-input — parse, apply
/// typmod, and produce the on-disk byte image.
///
/// Faithful port of numeric.c:637. Handles leading/trailing spaces, the sign,
/// the special values ("NaN", "Infinity"/"inf", optionally signed; NaN must be
/// unsigned), the non-decimal integer prefixes ("0x"/"0o"/"0b"), trailing-junk
/// detection, then `apply_typmod[_special]` and `make_result_opt_error`.
pub fn numeric_in<'mcx>(mcx: Mcx<'mcx>, s: &str, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    match numeric_in_safe(mcx, s, typmod, None)? {
        Some(image) => Ok(image),
        // No escontext supplied => every soft site is a hard `Err`, so `None`
        // is unreachable. (Belt-and-braces: surface the syntax error.)
        None => Err(invalid_syntax(s)),
    }
}

/// Soft-error-aware `numeric_in` (numeric.c:`numeric_in`, threading
/// `fcinfo->context`). When `escontext` is supplied, a recoverable
/// syntax/range/typmod failure is recorded into the sink (C `ereturn`) and
/// `Ok(None)` is returned (C `PG_RETURN_NULL()`); with no `escontext` every such
/// site raises a hard `Err`, exactly as the bare `numeric_in` did before.
pub fn numeric_in_safe<'mcx>(
    mcx: Mcx<'mcx>,
    s: &str,
    typmod: i32,
    mut escontext: Option<&mut ::types_error::SoftErrorContext>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    // Route a soft-eligible `Err` either into the sink (Ok(None)) or out as a
    // hard `Err`, mirroring C's `ereturn(escontext, (Datum) 0, ...)`.
    macro_rules! soft {
        ($err:expr) => {
            match ::types_error::ereturn(escontext.as_deref_mut(), (), $err) {
                Ok(()) => return Ok(None),
                Err(e) => return Err(e),
            }
        };
    }
    macro_rules! soft_try {
        ($r:expr) => {
            match $r {
                Ok(v) => v,
                Err(e) => soft!(e),
            }
        };
    }
    let bytes = s.as_bytes();
    let at = |i: usize| -> u8 {
        if i < bytes.len() {
            bytes[i]
        } else {
            0
        }
    };

    // Skip leading spaces.
    let mut cp = 0usize;
    while at(cp) != 0 && is_c_space(at(cp)) {
        cp += 1;
    }

    // Process the number's sign (duplicated from set_var_from_str so we can
    // handle infinities and non-decimal integers uniformly).
    let numstart = cp;
    let mut sign = NumericSign::Pos;
    if at(cp) == b'+' {
        cp += 1;
    } else if at(cp) == b'-' {
        sign = NumericSign::Neg;
        cp += 1;
    }

    // Check for NaN and infinities: only if the char after the sign is neither a
    // digit nor a decimal point.
    if !at(cp).is_ascii_digit() && at(cp) != b'.' {
        // Must be NaN or infinity; anything else is a syntax error. Note NaN
        // mustn't have a sign (C compares from numstart).
        let var: NumericVar<'mcx>;
        if starts_with_ci(&s[numstart..], "NaN") {
            var = NumericVar::special(mcx, NumericSign::NaN);
            cp = numstart + 3;
        } else if starts_with_ci(&s[cp..], "Infinity") {
            var = NumericVar::special(
                mcx,
                if sign == NumericSign::Pos {
                    NumericSign::PInf
                } else {
                    NumericSign::NInf
                },
            );
            cp += 8;
        } else if starts_with_ci(&s[cp..], "inf") {
            var = NumericVar::special(
                mcx,
                if sign == NumericSign::Pos {
                    NumericSign::PInf
                } else {
                    NumericSign::NInf
                },
            );
            cp += 3;
        } else {
            soft!(invalid_syntax(s));
        }

        // Check for trailing junk; nothing but spaces allowed. We do this check
        // before applying the typmod, matching C.
        while at(cp) != 0 {
            if !is_c_space(at(cp)) {
                soft!(invalid_syntax(s));
            }
            cp += 1;
        }

        let res = convert::make_result(mcx, &var)?;
        soft_try!(convert::apply_typmod_special(&res, typmod));
        return Ok(Some(res));
    }

    // Normal numeric value, which may be a non-decimal integer (PG 18) or a
    // regular decimal number. Determine the base from any "0x"/"0o"/"0b" prefix
    // (numeric.c:739).
    let base: i32 = if at(cp) == b'0' {
        match at(cp + 1) {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            b'b' | b'B' => 2,
            _ => 10,
        }
    } else {
        10
    };

    let (mut value, end) = if base == 10 {
        let (mut value, end) = soft_try!(set_var_from_str(mcx, s, cp));
        value.sign = sign;
        (value, end)
    } else {
        // Skip the two-character base prefix.
        let (mut v, end) = soft_try!(set_var_from_non_decimal_integer_str(mcx, s, cp + 2, base));
        v.sign = sign;
        (v, end)
    };
    cp = end;

    // Should be nothing left but spaces. As above, throw any typmod error after
    // finishing the syntax check.
    while at(cp) != 0 {
        if !is_c_space(at(cp)) {
            soft!(invalid_syntax(s));
        }
        cp += 1;
    }

    soft_try!(convert::apply_typmod(&mut value, typmod));

    match convert::make_result_opt_error(mcx, &value)? {
        Some(res) => Ok(Some(res)),
        None => soft!(out_of_range()),
    }
}

/// Case-insensitive ASCII prefix test (mirrors C's `pg_strncasecmp(s, lit, n)`
/// returning 0, where `n == lit.len()`).
fn starts_with_ci(haystack: &str, lit: &str) -> bool {
    let h = haystack.as_bytes();
    let l = lit.as_bytes();
    if h.len() < l.len() {
        return false;
    }
    h[..l.len()].eq_ignore_ascii_case(l)
}

// ---------------------------------------------------------------------------
// Text output (numeric.c get_str_from_var / get_str_from_var_sci /
// numeric_out / numeric_out_sci).
// ---------------------------------------------------------------------------

/// `get_str_from_var(var)`: render `var` to its plain decimal string.
///
/// Faithful port of numeric.c:7613. FINITE values only (NaN/Inf are handled by
/// `numeric_out`/`numeric_out_sci`). Excess fractional digits produced by the
/// NBASE expansion are *truncated* (not rounded), exactly as C does. Leading
/// decimal zeroes in the most significant NBASE digit are suppressed, but at
/// least one integer digit is always emitted.
pub fn get_str_from_var(var: &NumericVar<'_>) -> String {
    let dscale = var.dscale;
    let ndigits = var.ndigits() as i32;
    let digits = var.logical_digits();

    let mut s = String::new();

    macro_rules! push_byte {
        ($b:expr) => {{
            s.push($b as char);
        }};
    }

    if var.sign == NumericSign::Neg {
        push_byte!(b'-');
    }

    // Output all digits before the decimal point. `d` continues into the
    // fractional loop, just like the C `int d`.
    let mut d: i32;
    if var.weight < 0 {
        d = var.weight + 1;
        push_byte!(b'0');
    } else {
        d = 0;
        while d <= var.weight {
            let mut dig: i32 = if d < ndigits {
                digits[d as usize] as i32
            } else {
                0
            };
            // In the first digit, suppress extra leading decimal zeroes.
            let mut putit = d > 0;

            let mut d1 = dig / 1000;
            dig -= d1 * 1000;
            putit |= d1 > 0;
            if putit {
                push_byte!(b'0' + d1 as u8);
            }
            d1 = dig / 100;
            dig -= d1 * 100;
            putit |= d1 > 0;
            if putit {
                push_byte!(b'0' + d1 as u8);
            }
            d1 = dig / 10;
            dig -= d1 * 10;
            putit |= d1 > 0;
            if putit {
                push_byte!(b'0' + d1 as u8);
            }
            push_byte!(b'0' + dig as u8);

            d += 1;
        }
    }

    // If requested, output a decimal point and the fractional digits. C emits a
    // multiple of DEC_DIGITS then truncates to exactly `dscale`; we instead push
    // only the `dscale` fractional chars that survive that truncation (each
    // NBASE digit yields DEC_DIGITS chars).
    if dscale > 0 {
        push_byte!(b'.');
        let mut emitted = 0i32;
        let mut i = 0;
        while i < dscale {
            let mut dig: i32 = if d >= 0 && d < ndigits {
                digits[d as usize] as i32
            } else {
                0
            };
            let mut block = [0u8; DEC_DIGITS as usize];
            let mut d1 = dig / 1000;
            dig -= d1 * 1000;
            block[0] = b'0' + d1 as u8;
            d1 = dig / 100;
            dig -= d1 * 100;
            block[1] = b'0' + d1 as u8;
            d1 = dig / 10;
            dig -= d1 * 10;
            block[2] = b'0' + d1 as u8;
            block[3] = b'0' + dig as u8;

            // Push only as many of this block's DEC_DIGITS chars as remain
            // before reaching exactly `dscale` (the C post-loop truncate).
            let want = (dscale - emitted).min(DEC_DIGITS) as usize;
            for &b in &block[..want] {
                push_byte!(b);
            }
            emitted += want as i32;

            d += 1;
            i += DEC_DIGITS;
        }
    }

    s
}

/// `power_ten_int(exp, result)` (numeric.c:11656): raise ten to the power of
/// `exp`. Constructs the result directly from `10^0 = 1`; no overflow/underflow
/// checking or rounding. Used solely by [`get_str_from_var_sci`], so it is
/// owned here.
fn power_ten_int<'mcx>(mcx: Mcx<'mcx>, exp: i32) -> PgResult<NumericVar<'mcx>> {
    // Construct the result directly, starting from 10^0 = 1.
    let mut result = kernel_var::const_one(mcx);

    let mut exp = exp;

    // Scale needed to represent the result exactly.
    result.dscale = if exp < 0 { -exp } else { 0 };

    // Base-NBASE weight of result and remaining exponent.
    result.weight = if exp >= 0 {
        exp / DEC_DIGITS
    } else {
        (exp + 1) / DEC_DIGITS - 1
    };

    exp -= result.weight * DEC_DIGITS;

    // Final adjustment of the result's single NBASE digit.
    // const_one has exactly one logical digit (value 1) at index `headroom`.
    while exp > 0 {
        result.digits[result.headroom] *= 10;
        exp -= 1;
    }

    Ok(result)
}

/// `get_str_from_var_sci(var, rscale)`: render `var` in scientific notation.
///
/// Faithful port of numeric.c:7766. `rscale < 0` is treated as zero. Zero is
/// displayed with exponent 0.
pub fn get_str_from_var_sci(var: &NumericVar<'_>, rscale: i32) -> PgResult<String> {
    let rscale = if rscale < 0 { 0 } else { rscale };

    // The transient working vars are charged to the same context that owns
    // `var`'s digit buffer (there is no ambient context).
    let mcx = *var.digits.allocator();

    // Determine the exponent of this number in normalised form: the exponent
    // required to represent the number with only one significant digit before
    // the decimal place.
    let exponent: i32 = if var.ndigits() > 0 {
        let mut e = (var.weight + 1) * DEC_DIGITS;
        // Compensate for leading decimal zeroes in the first numeric digit by
        // decrementing the exponent. `log10(digits[0])` floored.
        e -= DEC_DIGITS - (var.logical_digits()[0] as f64).log10() as i32;
        e
    } else {
        // Zero: display the exponent as zero for output consistency.
        0
    };

    // Divide var by 10^exponent to get the significand, rounding to rscale.
    let ten = power_ten_int(mcx, exponent)?;
    // div_var(var, &tmp_var, &tmp_var, rscale, true, true)
    let sig = kernel_var::div_var(mcx, var, &ten, rscale, true, true)?;
    let sig_out = get_str_from_var(&sig);

    // snprintf(str, len, "%se%+03d", sig_out, exponent)
    Ok(format!("{}e{}", sig_out, format_exp(exponent)))
}

/// Render an exponent as C's `%+03d`: always a sign, then at least two digits.
fn format_exp(exponent: i32) -> String {
    let sign = if exponent < 0 { '-' } else { '+' };
    let mag = (exponent as i64).unsigned_abs();
    format!("{}{:02}", sign, mag)
}

/// `numeric_out(num)`: SQL text-output of an on-disk byte image.
///
/// Faithful port of numeric.c:816 — handles NaN/Infinity, else decodes to a
/// `NumericVar` and renders via [`get_str_from_var`].
pub fn numeric_out<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<String> {
    // Handle NaN and infinities.
    if numeric_is_special(num) {
        if numeric_is_pinf(num) {
            return Ok("Infinity".to_string());
        } else if numeric_is_ninf(num) {
            return Ok("-Infinity".to_string());
        } else {
            return Ok("NaN".to_string());
        }
    }

    // Get the number in the variable format.
    let x = convert::set_var_from_num(mcx, num)?;
    Ok(get_str_from_var(&x))
}

/// `numeric_out_sci(num, scale)`: SQL scientific text-output.
///
/// Faithful port of numeric.c:992 — handles NaN/Infinity, else decodes and
/// renders via [`get_str_from_var_sci`].
pub fn numeric_out_sci<'mcx>(mcx: Mcx<'mcx>, num: &[u8], scale: i32) -> PgResult<String> {
    if numeric_is_special(num) {
        if numeric_is_pinf(num) {
            return Ok("Infinity".to_string());
        } else if numeric_is_ninf(num) {
            return Ok("-Infinity".to_string());
        } else {
            return Ok("NaN".to_string());
        }
    }

    let x = convert::set_var_from_num(mcx, num)?;
    get_str_from_var_sci(&x, scale)
}

// ---------------------------------------------------------------------------
// Binary wire protocol (numeric.c numeric_recv/send + numericvar_(de)serialize).
//
// The external binary format is a sequence of network-order (big-endian) ints.
// In C these go through `pq_getmsgint`/`pq_sendint16` over a `StringInfo`; here
// the same big-endian (de)serialization -- which IS the computational content
// of these functions -- is performed directly over a byte cursor (`&[u8]` read /
// `PgVec<u8>` write). A short read mirrors C's `pq_getmsgint` running off the
// message end (`pq_getmsgbytes`/`pq_copymsgbytes` => ERRCODE_PROTOCOL_VIOLATION).
// ---------------------------------------------------------------------------

/// Read a network-order (big-endian) `u16` from `buf` at `*pos`, advancing the
/// cursor. Mirrors `pq_getmsgint(buf, 2)`.
#[inline]
fn get_be_u16(buf: &[u8], pos: &mut usize) -> PgResult<u16> {
    let p = *pos;
    if p + 2 > buf.len() {
        return Err(insufficient_data());
    }
    let v = u16::from_be_bytes([buf[p], buf[p + 1]]);
    *pos = p + 2;
    Ok(v)
}

/// Read a network-order (big-endian) `u32` from `buf` at `*pos`, advancing the
/// cursor. Mirrors `pq_getmsgint(buf, 4)`.
#[inline]
fn get_be_u32(buf: &[u8], pos: &mut usize) -> PgResult<u32> {
    let p = *pos;
    if p + 4 > buf.len() {
        return Err(insufficient_data());
    }
    let v = u32::from_be_bytes([buf[p], buf[p + 1], buf[p + 2], buf[p + 3]]);
    *pos = p + 4;
    Ok(v)
}

/// Append a network-order (big-endian) `i16` to `buf`. Mirrors
/// `pq_sendint16(buf, i)`.
#[inline]
fn put_be_i16(buf: &mut PgVec<'_, u8>, v: i16) {
    buf.extend_from_slice(&v.to_be_bytes());
}

/// `numeric_recv(buf, typmod)`: decode the binary wire form into an on-disk
/// byte image.
///
/// Faithful port of numeric.c:1078. External format is a sequence of int16's:
/// ndigits, weight, sign, dscale, NumericDigits. Validates sign/scale/digit
/// ranges, truncates to dscale, applies typmod, then packs via `make_result`.
pub fn numeric_recv<'mcx>(mcx: Mcx<'mcx>, buf: &[u8], typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    let mut pos = 0usize;

    let len = get_be_u16(buf, &mut pos)? as usize;

    let mut value = kernel_var::alloc_var(mcx, len)?;

    // we allow any int16 for weight --- OK?
    value.weight = get_be_u16(buf, &mut pos)? as i16 as i32;

    let sign_word = get_be_u16(buf, &mut pos)?;
    let sign = NumericSign::from_numeric_word(sign_word).ok_or_else(|| {
        PgError::error("invalid sign in external \"numeric\" value")
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
    })?;
    value.sign = sign;

    let dscale = get_be_u16(buf, &mut pos)?;
    if (dscale & NUMERIC_DSCALE_MASK) != dscale {
        return Err(PgError::error("invalid scale in external \"numeric\" value")
            .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
    }
    value.dscale = dscale as i32;

    for i in 0..len {
        let d = get_be_u16(buf, &mut pos)? as NumericDigit;
        if d < 0 || d as i32 >= NBASE {
            return Err(PgError::error("invalid digit in external \"numeric\" value")
                .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION));
        }
        value.digits[value.headroom + i] = d;
    }

    // If the given dscale would hide any digits, truncate those digits away. Be
    // careful not to apply trunc_var to special values; make_result ignores all
    // but the sign field. After doing that, check the typmod restriction.
    let res = if value.sign == NumericSign::Pos || value.sign == NumericSign::Neg {
        let dscale = value.dscale;
        kernel_var::trunc_var(&mut value, dscale);
        convert::apply_typmod(&mut value, typmod)?;
        convert::make_result(mcx, &value)?
    } else {
        // apply_typmod_special wants us to make the Numeric first.
        let res = convert::make_result(mcx, &value)?;
        convert::apply_typmod_special(&res, typmod)?;
        res
    };

    Ok(res)
}

/// `numeric_send(num)`: encode an on-disk byte image to the binary wire form.
///
/// Faithful port of numeric.c:1163. Decodes `num` to a `NumericVar` and writes
/// the int16 stream `ndigits/weight/sign/dscale` + digits.
pub fn numeric_send<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let x = convert::set_var_from_num(mcx, num)?;

    let ndigits = x.ndigits();
    let digits = x.logical_digits();

    let mut buf: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    put_be_i16(&mut buf, ndigits as i16);
    put_be_i16(&mut buf, x.weight as i16);
    put_be_i16(&mut buf, x.sign.to_numeric_word() as i16);
    put_be_i16(&mut buf, x.dscale as i16);
    for &d in digits.iter() {
        put_be_i16(&mut buf, d);
    }

    Ok(buf)
}

/// `numericvar_serialize(buf, var)`: append the aggregate-serialization form of
/// `var` to `buf`.
///
/// Faithful port of numeric.c:7843. ndigits/weight/sign/dscale as int32, then
/// digits as int16. At variable level no checks are performed on weight or
/// dscale (intermediate values may exceed the numeric type's precision). Note:
/// incompatible with numeric_send/recv(), which use 16-bit int fields.
pub fn numericvar_serialize(buf: &mut PgVec<'_, u8>, var: &NumericVar<'_>) {
    let digits = var.logical_digits();

    // pq_sendint32 for ndigits/weight/sign/dscale.
    buf.extend_from_slice(&(var.ndigits() as i32).to_be_bytes());
    buf.extend_from_slice(&var.weight.to_be_bytes());
    buf.extend_from_slice(&(var.sign.to_numeric_word() as i32).to_be_bytes());
    buf.extend_from_slice(&var.dscale.to_be_bytes());
    for &d in digits.iter() {
        buf.extend_from_slice(&d.to_be_bytes());
    }
}

/// `numericvar_deserialize(buf, &pos)`: read a serialized `NumericVar` from
/// `buf` starting at `*pos`, advancing `*pos`.
///
/// Faithful port of numeric.c:7859, the inverse of [`numericvar_serialize`].
pub fn numericvar_deserialize<'mcx>(
    mcx: Mcx<'mcx>,
    buf: &[u8],
    pos: &mut usize,
) -> PgResult<NumericVar<'mcx>> {
    let len = get_be_u32(buf, pos)? as i32 as usize; // sets var->ndigits

    let mut var = kernel_var::alloc_var(mcx, len)?;

    var.weight = get_be_u32(buf, pos)? as i32;
    // C stores the raw int32 sign word with no validation; reconstruct the
    // `NumericSign` tag. This buffer is only ever produced by
    // `numericvar_serialize`, so the word always names a valid sign.
    let sign_word = get_be_u32(buf, pos)? as u16;
    var.sign = NumericSign::from_numeric_word(sign_word).ok_or_else(|| {
        PgError::error(
            "numericvar_deserialize: serialized NumericVar carries an invalid sign word",
        )
        .with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION)
    })?;
    var.dscale = get_be_u32(buf, pos)? as i32;
    for i in 0..len {
        var.digits[var.headroom + i] = get_be_u16(buf, pos)? as NumericDigit;
    }

    Ok(var)
}

// ---------------------------------------------------------------------------
// Owned seams (byte-image comparison reached from jsonb_util).
//
// These port `cmp_numerics` (numeric.c:2624) directly over the on-disk byte
// image: the special-value ordering (NaN > +Inf > finite > -Inf, all NaNs
// equal) is decided from the header word; the finite case delegates to
// `kernel_var::cmp_var_common` over the decoded digit run.
// ---------------------------------------------------------------------------

/// `cmp_numerics(num1, num2)` (numeric.c:2624): 3-way comparison over two whole
/// on-disk `numeric` byte images.
fn cmp_numerics(num1: &[u8], num2: &[u8]) -> i32 {
    // We consider all NANs to be equal and larger than any non-NAN (including
    // Infinity). This is somewhat arbitrary; the important thing is to have a
    // consistent sort order.
    if numeric_is_special(num1) {
        if numeric_is_nan(num1) {
            if numeric_is_nan(num2) {
                0 // NAN = NAN
            } else {
                1 // NAN > non-NAN
            }
        } else if numeric_is_pinf(num1) {
            if numeric_is_nan(num2) {
                -1 // PINF < NAN
            } else if numeric_is_pinf(num2) {
                0 // PINF = PINF
            } else {
                1 // PINF > anything else
            }
        } else {
            // num1 must be NINF
            if numeric_is_ninf(num2) {
                0 // NINF = NINF
            } else {
                -1 // NINF < anything else
            }
        }
    } else if numeric_is_special(num2) {
        if numeric_is_ninf(num2) {
            1 // normal > NINF
        } else {
            -1 // normal < NAN or PINF
        }
    } else {
        // Decode each value's NBASE digit run from its byte image and compare
        // via cmp_var_common (which honors sign/weight).
        let n1 = numeric_ndigits(num1, num1.len());
        let n2 = numeric_ndigits(num2, num2.len());
        let d1bytes = numeric_digits(num1);
        let d2bytes = numeric_digits(num2);
        let v1digits: Vec<NumericDigit> =
            (0..n1).map(|i| numeric_digit_at(d1bytes, i)).collect();
        let v2digits: Vec<NumericDigit> =
            (0..n2).map(|i| numeric_digit_at(d2bytes, i)).collect();

        let ord = kernel_var::cmp_var_common(
            &v1digits,
            numeric_weight(num1),
            numeric_sign(num1) as i32,
            &v2digits,
            numeric_weight(num2),
            numeric_sign(num2) as i32,
        );
        match ord {
            core::cmp::Ordering::Less => -1,
            core::cmp::Ordering::Equal => 0,
            core::cmp::Ordering::Greater => 1,
        }
    }
}

/// Implements the `numeric_eq` seam: value equality (scale-insensitive) over
/// two whole on-disk `numeric` byte images. Pure; infallible.
pub fn seam_numeric_eq(a: &[u8], b: &[u8]) -> bool {
    cmp_numerics(a, b) == 0
}

/// Implements the `numeric_cmp` seam: 3-way B-tree comparison (`-1`/`0`/`1`,
/// full special-value ordering) over two whole on-disk byte images. Pure;
/// infallible.
pub fn seam_numeric_cmp(a: &[u8], b: &[u8]) -> i32 {
    cmp_numerics(a, b)
}
