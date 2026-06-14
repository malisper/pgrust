#![allow(non_snake_case)]
//! Port of PostgreSQL `src/backend/utils/adt/cash.c`: the `money` (`Cash` =
//! `int64`) datatype.
//!
//! Every routine in `cash.c` is ported here with logic identical to PostgreSQL
//! 18.3 (branch order, message text, and SQLSTATE all cross-checked against the
//! C source).
//!
//! # Conventions
//!
//! * Text inputs are borrowed `&[u8]` (the parser walks raw database-encoded
//!   bytes, exactly as C walks `unsigned char`, never assuming UTF-8); text
//!   outputs are owned [`String`] (the fmgr layer wraps them into `cstring` /
//!   `text` datums, mirroring this repo's `numeric` text I/O).
//! * `cash_recv` / `cash_send` are the binary-wire format: their C bodies are
//!   nothing but `pq_getmsgint64` / `pq_sendint64` (the `libpq/pqformat.h` wire
//!   layer); there is no cash-specific logic to port, so they are documented
//!   but not re-implemented (the fmgr+pqformat layer wraps the raw `Cash`).
//!
//! # Genuine externals
//!
//! * `PGLC_localeconv()` (`pg_locale.c`): the monetary `struct lconv` snapshot,
//!   via the [`pg_locale`](backend_utils_adt_pg_locale_seams::pglc_localeconv)
//!   seam (owner not yet ported, panics until installed).
//! * `float8_mul` / `float8_div` (`utils/float.h`): via the
//!   [`float`](backend_utils_adt_float_seams) seam (owner not yet ported).
//! * the `numeric` arithmetic cores (`numeric.c`): the owner IS ported, called
//!   directly on the on-disk `numeric` varlena image.
//! * the high-bit branch of `pg_toupper` (`port/pgstrcasecmp.c`): called
//!   directly.

use backend_utils_adt_float_seams as float_seam;
use backend_utils_adt_numeric::convert::{int64_to_numeric, set_var_from_num};
use backend_utils_adt_numeric::kernel_transcendental::numericvar_to_int64;
use backend_utils_adt_numeric::ops_sql::{numeric_div, numeric_mul, numeric_round};
use backend_utils_adt_pg_locale_seams::pglc_localeconv as pglc_localeconv_seam;
use mcx::{Mcx, PgVec};
use port_pgstrcasecmp::pg_toupper as pg_toupper_full;
use types_cash::{Cash, CashLconv};
use types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_DIVISION_BY_ZERO, ERRCODE_FEATURE_NOT_SUPPORTED,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use types_numeric::var::NumericSign;

#[cfg(test)]
mod tests;

/// `PG_INT64_MIN` (`c.h`).
const PG_INT64_MIN: i64 = i64::MIN;

// ===========================================================================
// Private overflow-checked int64 helpers (common/int.h).
//
// These mirror `pg_{add,sub,mul}_s64_overflow`: each returns `true` on overflow
// and writes the (wrapping) result through `res`, exactly as the C macros do.
// ===========================================================================

#[inline]
fn pg_add_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = a.wrapping_add(b);
            true
        }
    }
}

#[inline]
fn pg_sub_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_sub(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = a.wrapping_sub(b);
            true
        }
    }
}

#[inline]
fn pg_mul_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_mul(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = a.wrapping_mul(b);
            true
        }
    }
}

/// `pg_abs_s64()` (common/int.h): absolute value of an `int64`, returned as
/// `uint64` so the most-negative value is representable.
#[inline]
fn pg_abs_s64(a: i64) -> u64 {
    a.unsigned_abs()
}

// ===========================================================================
// Float arithmetic guards (utils/float.h).
//
// `float8_mul` / `float8_div` cross the float seam; the IEEE-rounding (`rint`)
// and the int64 range check are pure inline macros, ported in-crate.
// ===========================================================================

/// `FLOAT8_FITS_IN_INT64(num)` (utils/float.h): true when converting `num` to
/// `int64` will not overflow. The exact PostgreSQL macro: `((num) >= (double)
/// PG_INT64_MIN && (num) < -((double) PG_INT64_MIN))`.
#[inline]
fn float8_fits_in_int64(num: f64) -> bool {
    // `-(double) PG_INT64_MIN` is exactly 2^63 (representable in f64).
    num >= (PG_INT64_MIN as f64) && num < -(PG_INT64_MIN as f64)
}

/// C `rint()`: round to nearest, ties to even.
#[inline]
fn rint(x: f64) -> f64 {
    x.round_ties_even()
}

// ===========================================================================
// ctype helpers: ASCII classification matching C `isspace`/`isdigit` on the
// `(unsigned char)` cast.
// ===========================================================================

/// C `isspace((unsigned char) c)` for the standard "C" classification: space,
/// tab, newline, vertical tab, form feed, carriage return.
#[inline]
fn is_space(c: u8) -> bool {
    matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

/// C `isdigit((unsigned char) c)`.
#[inline]
fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

// ===========================================================================
// Private inline arithmetic cores (cash.c:90-164).
// ===========================================================================

/// `cash_pl_cash()` (cash.c:90).
#[inline]
fn cash_pl_cash(c1: Cash, c2: Cash) -> PgResult<Cash> {
    let mut res: Cash = 0;
    if pg_add_s64_overflow(c1, c2, &mut res) {
        return Err(money_out_of_range());
    }
    Ok(res)
}

/// `cash_mi_cash()` (cash.c:103).
#[inline]
fn cash_mi_cash(c1: Cash, c2: Cash) -> PgResult<Cash> {
    let mut res: Cash = 0;
    if pg_sub_s64_overflow(c1, c2, &mut res) {
        return Err(money_out_of_range());
    }
    Ok(res)
}

/// `cash_mul_float8()` (cash.c:116).
#[inline]
fn cash_mul_float8(c: Cash, f: f64) -> PgResult<Cash> {
    let res = rint(float_seam::float8_mul::call(c as f64, f)?);

    if res.is_nan() || !float8_fits_in_int64(res) {
        return Err(money_out_of_range());
    }

    Ok(res as Cash)
}

/// `cash_div_float8()` (cash.c:129).
#[inline]
fn cash_div_float8(c: Cash, f: f64) -> PgResult<Cash> {
    let res = rint(float_seam::float8_div::call(c as f64, f)?);

    if res.is_nan() || !float8_fits_in_int64(res) {
        return Err(money_out_of_range());
    }

    Ok(res as Cash)
}

/// `cash_mul_int64()` (cash.c:142).
#[inline]
fn cash_mul_int64(c: Cash, i: i64) -> PgResult<Cash> {
    let mut res: Cash = 0;
    if pg_mul_s64_overflow(c, i, &mut res) {
        return Err(money_out_of_range());
    }
    Ok(res)
}

/// `cash_div_int64()` (cash.c:155).
#[inline]
fn cash_div_int64(c: Cash, i: i64) -> PgResult<Cash> {
    if i == 0 {
        return Err(division_by_zero());
    }

    Ok(c / i)
}

/// `ereport(ERROR, errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("money
/// out of range"))` — the shared hard error of the arithmetic cores.
#[inline]
fn money_out_of_range() -> PgError {
    PgError::error("money out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `ereport(ERROR, errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by
/// zero"))`.
#[inline]
fn division_by_zero() -> PgError {
    PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}

/// `int8mul()` (int8.c:489) CORE — overflow-checked `arg1 * arg2`, used by
/// `int4_cash` / `int8_cash`. Overflow -> "bigint out of range".
#[inline]
fn int8mul(arg1: i64, arg2: i64) -> PgResult<i64> {
    let mut result: i64 = 0;
    if pg_mul_s64_overflow(arg1, arg2, &mut result) {
        return Err(
            PgError::error("bigint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        );
    }
    Ok(result)
}

// ===========================================================================
// numeric_int8 (numeric.c:4654 / numeric_int8_opt_error 4664).
//
// `numeric.c` owns the rounding kernel; `cash.c`'s `numeric_cash` reaches
// `numeric_int8` through `DirectFunctionCall1`. The repo's `numeric` owner is
// ported but does not export this exact entry point, so its faithful body
// (the NaN/Inf rejection, the round-to-int64, the overflow rejection) is
// assembled here from the numeric owner's public kernels (`set_var_from_num` +
// `numericvar_to_int64`) — the rounding logic itself stays numeric-owned.
// ===========================================================================

/// `numeric_int8_opt_error(num, NULL)` (numeric.c:4664): convert the on-disk
/// `numeric` byte image to `int64`, rounding to nearest. Rejects NaN/Inf with
/// `ERRCODE_FEATURE_NOT_SUPPORTED` and overflow with
/// `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE`.
fn numeric_int8(mcx: Mcx<'_>, num: &[u8]) -> PgResult<i64> {
    let x = set_var_from_num(mcx, num)?;

    // NUMERIC_IS_SPECIAL(num): NaN / +Inf / -Inf cannot become a bigint.
    if x.sign.is_special() {
        if x.sign == NumericSign::NaN {
            return Err(
                PgError::error("cannot convert NaN to bigint").with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
            );
        }
        return Err(PgError::error("cannot convert infinity to bigint")
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    // numericvar_to_int64 rounds to nearest; `Ok(None)` is the C `false`
    // (overflow) return.
    match numericvar_to_int64(&x)? {
        Some(v) => Ok(v),
        None => Err(
            PgError::error("bigint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        ),
    }
}

// ===========================================================================
// Shared lconv-field extraction (the identical preamble of cash_in / cash_out /
// the *_cash conversions).
// ===========================================================================

/// `PGLC_localeconv()` — the monetary `struct lconv` snapshot, through the
/// seam. This is the exact call `cash.c` makes at the top of each text/numeric
/// entry point.
#[inline]
fn pglc_localeconv() -> CashLconv {
    pglc_localeconv_seam::call()
}

/// `fpoint = lconvert->frac_digits; if (fpoint < 0 || fpoint > 10) fpoint = 2;`
/// (the "see comments about frac_digits in cash_in()" block, repeated verbatim
/// in cash_out / cash_numeric / numeric_cash / int4_cash / int8_cash).
#[inline]
fn clamp_frac_digits(lconvert: &CashLconv) -> i32 {
    let fpoint = lconvert.frac_digits as i32;
    if !(0..=10).contains(&fpoint) {
        2
    } else {
        fpoint
    }
}

/// `dsymbol` selection (cash.c:206 / cash.c:420): a single byte, the monetary
/// decimal point if it is exactly one byte long, else `'.'`.
#[inline]
fn dsymbol_of(lconvert: &CashLconv) -> u8 {
    let bytes = lconvert.mon_decimal_point.as_bytes();
    if bytes.len() == 1 {
        bytes[0]
    } else {
        b'.'
    }
}

/// `ssymbol` selection (cash.c:211 / cash.c:425): the monetary thousands
/// separator if non-empty, else `","` unless `dsymbol == ','` in which case
/// `"."` (ssymbol must not equal dsymbol).
#[inline]
fn ssymbol_of(lconvert: &CashLconv, dsymbol: u8) -> &str {
    if !lconvert.mon_thousands_sep.is_empty() {
        &lconvert.mon_thousands_sep
    } else if dsymbol != b',' {
        ","
    } else {
        "."
    }
}

/// `csymbol` selection (cash.c:215 / cash.c:429): currency symbol if non-empty,
/// else `"$"`.
#[inline]
fn csymbol_of(lconvert: &CashLconv) -> &str {
    if !lconvert.currency_symbol.is_empty() {
        &lconvert.currency_symbol
    } else {
        "$"
    }
}

// ===========================================================================
// cash_in (cash.c:172)
// ===========================================================================

/// `cash_in()` (cash.c:172) — parse a string into `money`.
///
/// Format is `[$]###[,]###[.##]`. The C entry point takes a NUL-terminated
/// `char *str`; the port takes the same bytes as a `&[u8]`. Soft errors are
/// saved into `escontext` (`ereturn`) when present, otherwise raised hard
/// (`Err`).
pub fn cash_in(str: &[u8], escontext: Option<&mut SoftErrorContext>) -> PgResult<Cash> {
    // C builds `value` in the negative and flips the sign at the end.
    let mut value: Cash = 0;
    let mut dec: Cash = 0;
    let mut sgn: Cash = 1;
    let mut seen_dot = false;

    let lconvert = pglc_localeconv();
    let fpoint = clamp_frac_digits(&lconvert);
    let dsymbol = dsymbol_of(&lconvert);
    let ssymbol = ssymbol_of(&lconvert, dsymbol).as_bytes();
    let csymbol = csymbol_of(&lconvert).as_bytes();
    let psymbol: &[u8] = if !lconvert.positive_sign.is_empty() {
        lconvert.positive_sign.as_bytes()
    } else {
        b"+"
    };
    let nsymbol: &[u8] = if !lconvert.negative_sign.is_empty() {
        lconvert.negative_sign.as_bytes()
    } else {
        b"-"
    };

    // Operate over the input bytes; `s` is the cursor index.
    let bytes = str;
    let mut s: usize = 0;

    // strip leading whitespace and any leading currency symbol.
    while at(bytes, s).is_some_and(is_space) {
        s += 1;
    }
    if starts_with(bytes, s, csymbol) {
        s += csymbol.len();
    }
    while at(bytes, s).is_some_and(is_space) {
        s += 1;
    }

    // a leading minus or paren signifies a negative number.
    if starts_with(bytes, s, nsymbol) {
        sgn = -1;
        s += nsymbol.len();
    } else if at(bytes, s) == Some(b'(') {
        sgn = -1;
        s += 1;
    } else if starts_with(bytes, s, psymbol) {
        s += psymbol.len();
    }

    // allow whitespace and currency symbol after the sign, too.
    while at(bytes, s).is_some_and(is_space) {
        s += 1;
    }
    if starts_with(bytes, s, csymbol) {
        s += csymbol.len();
    }
    while at(bytes, s).is_some_and(is_space) {
        s += 1;
    }

    // Accumulate the absolute amount in `value`, in the negative.
    while let Some(c) = at(bytes, s) {
        if is_digit(c) && (!seen_dot || dec < fpoint as Cash) {
            let digit = (c - b'0') as i64;

            if pg_mul_s64_overflow(value, 10, &mut value)
                || pg_sub_s64_overflow(value, digit, &mut value)
            {
                return errsave(escontext, value_out_of_range_for_money(str)).map(|()| 0);
            }

            if seen_dot {
                dec += 1;
            }
            s += 1;
        } else if c == dsymbol && !seen_dot {
            seen_dot = true;
            s += 1;
        } else if starts_with(bytes, s, ssymbol) {
            // ignore "thousands" separator. (ssymbol_of never returns "".)
            s += ssymbol.len().max(1);
        } else {
            break;
        }
    }

    // round off if there's another digit.
    if at(bytes, s).is_some_and(|c| is_digit(c) && c >= b'5') {
        // remember we build the value in the negative.
        if pg_sub_s64_overflow(value, 1, &mut value) {
            return errsave(escontext, value_out_of_range_for_money(str)).map(|()| 0);
        }
    }

    // adjust for less than required decimal places.
    while dec < fpoint as Cash {
        if pg_mul_s64_overflow(value, 10, &mut value) {
            return errsave(escontext, value_out_of_range_for_money(str)).map(|()| 0);
        }
        dec += 1;
    }

    // skip trailing digits.
    while at(bytes, s).is_some_and(is_digit) {
        s += 1;
    }

    // only trailing whitespace, ')', sign, and/or currency symbol allowed.
    while let Some(c) = at(bytes, s) {
        if is_space(c) || c == b')' {
            s += 1;
        } else if starts_with(bytes, s, nsymbol) {
            sgn = -1;
            s += nsymbol.len();
        } else if starts_with(bytes, s, psymbol) {
            s += psymbol.len();
        } else if starts_with(bytes, s, csymbol) {
            s += csymbol.len();
        } else {
            return errsave(escontext, invalid_input_for_money(str)).map(|()| 0);
        }
    }

    // flip the sign if positive, catching most-negative-number overflow.
    let result: Cash = if sgn > 0 {
        if value == PG_INT64_MIN {
            return errsave(escontext, value_out_of_range_for_money(str)).map(|()| 0);
        }
        -value
    } else {
        value
    };

    Ok(result)
}

/// Render the input bytes for an error message the way C's `%s` does on a
/// `char *`: lossily, since the parser does not require valid UTF-8.
#[inline]
fn input_lossy(str: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(str)
}

#[inline]
fn value_out_of_range_for_money(str: &[u8]) -> PgError {
    PgError::error(format!(
        "value \"{}\" is out of range for type {}",
        input_lossy(str),
        "money"
    ))
    .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[inline]
fn invalid_input_for_money(str: &[u8]) -> PgError {
    PgError::error(format!(
        "invalid input syntax for type {}: \"{}\"",
        "money",
        input_lossy(str)
    ))
    .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

/// Byte at index `i`, or `None` past the end (the C NUL terminator).
#[inline]
fn at(bytes: &[u8], i: usize) -> Option<u8> {
    bytes.get(i).copied()
}

/// C `strncmp(&bytes[i], needle, strlen(needle)) == 0`: does the slice starting
/// at `i` begin with `needle`? An empty `needle` matches anywhere (as in C).
#[inline]
fn starts_with(bytes: &[u8], i: usize, needle: &[u8]) -> bool {
    bytes.len() >= i && bytes[i..].starts_with(needle)
}

/// C `ereturn(escontext, (Cash) 0, …)` value form: store the soft-error and
/// return `Ok(0)` when `escontext` requests soft handling, otherwise propagate
/// the hard error. The `.map(|()| 0)` callers expect a `PgResult<()>`, so
/// thread the C "return 0" through the unit value.
#[inline]
fn errsave(escontext: Option<&mut SoftErrorContext>, err: PgError) -> PgResult<()> {
    types_error::ereturn(escontext, (), err)
}

// ===========================================================================
// cash_out (cash.c:386)
// ===========================================================================

/// `cash_out()` (cash.c:386) — render `money` as text using the `lc_monetary`
/// locale's formatting. C returns a `psprintf`'d `cstring`; the port returns
/// the owned [`String`].
pub fn cash_out(value: Cash) -> String {
    let lconvert = pglc_localeconv();

    let points = clamp_frac_digits(&lconvert);

    // range check on mon_grouping to avoid variant CHAR_MAX values.
    let mut mon_group = lconvert.mon_grouping_first() as i32;
    if mon_group <= 0 || mon_group > 6 {
        mon_group = 3;
    }

    let dsymbol = dsymbol_of(&lconvert);
    let ssymbol = ssymbol_of(&lconvert, dsymbol).as_bytes();
    let csymbol = csymbol_of(&lconvert);

    let signsymbol: &str;
    let sign_posn: i8;
    let cs_precedes: i8;
    let sep_by_space: i8;
    if value < 0 {
        signsymbol = if !lconvert.negative_sign.is_empty() {
            &lconvert.negative_sign
        } else {
            "-"
        };
        sign_posn = lconvert.n_sign_posn;
        cs_precedes = lconvert.n_cs_precedes;
        sep_by_space = lconvert.n_sep_by_space;
    } else {
        signsymbol = &lconvert.positive_sign;
        sign_posn = lconvert.p_sign_posn;
        cs_precedes = lconvert.p_cs_precedes;
        sep_by_space = lconvert.p_sep_by_space;
    }

    // make the amount positive for the digit-reconstruction loop.
    let uvalue: u64 = pg_abs_s64(value);

    // Build digits + decimal point + separators right-to-left (the C `char
    // buf[128]` analog). A 64-bit `money` is at most 18 dollar digits + 2
    // fraction digits + separators + symbols, far below 128, so the buffer is
    // bounded (not data-derived).
    let bufstr = build_cash_out_buf(uvalue, points, mon_group, dsymbol, ssymbol);

    // Attach currency + sign in the order POSIX p/n_sign_posn dictates.
    let sp1 = |sep_by_space: i8| if sep_by_space == 1 { " " } else { "" };
    let sp2 = |sep_by_space: i8| if sep_by_space == 2 { " " } else { "" };

    match sign_posn {
        0 => {
            if cs_precedes != 0 {
                format!("({}{}{})", csymbol, sp1(sep_by_space), bufstr)
            } else {
                format!("({}{}{})", bufstr, sp1(sep_by_space), csymbol)
            }
        }
        2 => {
            if cs_precedes != 0 {
                format!(
                    "{}{}{}{}{}",
                    csymbol,
                    sp1(sep_by_space),
                    bufstr,
                    sp2(sep_by_space),
                    signsymbol
                )
            } else {
                format!(
                    "{}{}{}{}{}",
                    bufstr,
                    sp1(sep_by_space),
                    csymbol,
                    sp2(sep_by_space),
                    signsymbol
                )
            }
        }
        3 => {
            if cs_precedes != 0 {
                format!(
                    "{}{}{}{}{}",
                    signsymbol,
                    sp2(sep_by_space),
                    csymbol,
                    sp1(sep_by_space),
                    bufstr
                )
            } else {
                format!(
                    "{}{}{}{}{}",
                    bufstr,
                    sp1(sep_by_space),
                    signsymbol,
                    sp2(sep_by_space),
                    csymbol
                )
            }
        }
        4 => {
            if cs_precedes != 0 {
                format!(
                    "{}{}{}{}{}",
                    csymbol,
                    sp2(sep_by_space),
                    signsymbol,
                    sp1(sep_by_space),
                    bufstr
                )
            } else {
                format!(
                    "{}{}{}{}{}",
                    bufstr,
                    sp1(sep_by_space),
                    csymbol,
                    sp2(sep_by_space),
                    signsymbol
                )
            }
        }
        // case 1 and `default` in C.
        _ => {
            if cs_precedes != 0 {
                format!(
                    "{}{}{}{}{}",
                    signsymbol,
                    sp2(sep_by_space),
                    csymbol,
                    sp1(sep_by_space),
                    bufstr
                )
            } else {
                format!(
                    "{}{}{}{}{}",
                    signsymbol,
                    sp2(sep_by_space),
                    bufstr,
                    sp1(sep_by_space),
                    csymbol
                )
            }
        }
    }
}

/// Build the right-to-left digit/separator buffer of [`cash_out`] (the `char
/// buf[128]` analog), reversing it before returning the materialized string.
/// The buffer is bounded by the i64 range (not data-derived).
fn build_cash_out_buf(
    mut uvalue: u64,
    points: i32,
    mon_group: i32,
    dsymbol: u8,
    ssymbol: &[u8],
) -> String {
    let mut buf: Vec<u8> = Vec::new();
    let mut digit_pos = points;
    loop {
        if points != 0 && digit_pos == 0 {
            // insert decimal point, but not if value cannot be fractional.
            buf.push(dsymbol);
        } else if digit_pos < 0 && digit_pos % mon_group == 0 {
            // insert thousands sep, but only to left of radix point.
            for &b in ssymbol.iter().rev() {
                buf.push(b);
            }
        }

        buf.push((uvalue % 10) as u8 + b'0');
        uvalue /= 10;
        digit_pos -= 1;

        if uvalue == 0 && digit_pos < 0 {
            break;
        }
    }
    buf.reverse();
    String::from_utf8_lossy(&buf).into_owned()
}

// ===========================================================================
// cash_recv / cash_send (cash.c:591 / 602)
// ===========================================================================
//
// The C `cash_recv` / `cash_send` bodies are pure `libpq/pqformat.h` wire calls:
//
//     cash_recv: PG_RETURN_CASH((Cash) pq_getmsgint64(buf));
//     cash_send: pq_begintypsend(&buf); pq_sendint64(&buf, arg1);
//                PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
//
// There is no `cash`-specific logic to port: both delegate entirely to the
// `pqformat` wire layer (a separate, not-yet-ported subsystem) wrapping the raw
// `Cash` (an `int64`); they are documented rather than re-emitted as trivial
// pass-throughs.

// ===========================================================================
// Comparison functions (cash.c:617-683)
// ===========================================================================

/// `cash_eq()` (cash.c:617).
pub fn cash_eq(c1: Cash, c2: Cash) -> bool {
    c1 == c2
}

/// `cash_ne()` (cash.c:626).
pub fn cash_ne(c1: Cash, c2: Cash) -> bool {
    c1 != c2
}

/// `cash_lt()` (cash.c:635).
pub fn cash_lt(c1: Cash, c2: Cash) -> bool {
    c1 < c2
}

/// `cash_le()` (cash.c:644).
pub fn cash_le(c1: Cash, c2: Cash) -> bool {
    c1 <= c2
}

/// `cash_gt()` (cash.c:653).
pub fn cash_gt(c1: Cash, c2: Cash) -> bool {
    c1 > c2
}

/// `cash_ge()` (cash.c:662).
pub fn cash_ge(c1: Cash, c2: Cash) -> bool {
    c1 >= c2
}

/// `cash_cmp()` (cash.c:671).
pub fn cash_cmp(c1: Cash, c2: Cash) -> i32 {
    if c1 > c2 {
        1
    } else if c1 == c2 {
        0
    } else {
        -1
    }
}

// ===========================================================================
// Arithmetic functions (cash.c:689-924)
// ===========================================================================

/// `cash_pl()` (cash.c:689) — add two `money` values.
pub fn cash_pl(c1: Cash, c2: Cash) -> PgResult<Cash> {
    cash_pl_cash(c1, c2)
}

/// `cash_mi()` (cash.c:702) — subtract two `money` values.
pub fn cash_mi(c1: Cash, c2: Cash) -> PgResult<Cash> {
    cash_mi_cash(c1, c2)
}

/// `cash_div_cash()` (cash.c:715) — divide `money` by `money`, returns float8.
pub fn cash_div_cash(dividend: Cash, divisor: Cash) -> PgResult<f64> {
    if divisor == 0 {
        return Err(division_by_zero());
    }

    Ok(dividend as f64 / divisor as f64)
}

/// `cash_mul_flt8()` (cash.c:735).
pub fn cash_mul_flt8(c: Cash, f: f64) -> PgResult<Cash> {
    cash_mul_float8(c, f)
}

/// `flt8_mul_cash()` (cash.c:748).
pub fn flt8_mul_cash(f: f64, c: Cash) -> PgResult<Cash> {
    cash_mul_float8(c, f)
}

/// `cash_div_flt8()` (cash.c:761).
pub fn cash_div_flt8(c: Cash, f: f64) -> PgResult<Cash> {
    cash_div_float8(c, f)
}

/// `cash_mul_flt4()` (cash.c:774).
pub fn cash_mul_flt4(c: Cash, f: f32) -> PgResult<Cash> {
    cash_mul_float8(c, f as f64)
}

/// `flt4_mul_cash()` (cash.c:787).
pub fn flt4_mul_cash(f: f32, c: Cash) -> PgResult<Cash> {
    cash_mul_float8(c, f as f64)
}

/// `cash_div_flt4()` (cash.c:801).
pub fn cash_div_flt4(c: Cash, f: f32) -> PgResult<Cash> {
    cash_div_float8(c, f as f64)
}

/// `cash_mul_int8()` (cash.c:814).
pub fn cash_mul_int8(c: Cash, i: i64) -> PgResult<Cash> {
    cash_mul_int64(c, i)
}

/// `int8_mul_cash()` (cash.c:827).
pub fn int8_mul_cash(i: i64, c: Cash) -> PgResult<Cash> {
    cash_mul_int64(c, i)
}

/// `cash_div_int8()` (cash.c:839).
pub fn cash_div_int8(c: Cash, i: i64) -> PgResult<Cash> {
    cash_div_int64(c, i)
}

/// `cash_mul_int4()` (cash.c:852).
pub fn cash_mul_int4(c: Cash, i: i32) -> PgResult<Cash> {
    cash_mul_int64(c, i as i64)
}

/// `int4_mul_cash()` (cash.c:865).
pub fn int4_mul_cash(i: i32, c: Cash) -> PgResult<Cash> {
    cash_mul_int64(c, i as i64)
}

/// `cash_div_int4()` (cash.c:879).
pub fn cash_div_int4(c: Cash, i: i32) -> PgResult<Cash> {
    cash_div_int64(c, i as i64)
}

/// `cash_mul_int2()` (cash.c:892).
pub fn cash_mul_int2(c: Cash, s: i16) -> PgResult<Cash> {
    cash_mul_int64(c, s as i64)
}

/// `int2_mul_cash()` (cash.c:904).
pub fn int2_mul_cash(s: i16, c: Cash) -> PgResult<Cash> {
    cash_mul_int64(c, s as i64)
}

/// `cash_div_int2()` (cash.c:917).
pub fn cash_div_int2(c: Cash, s: i16) -> PgResult<Cash> {
    cash_div_int64(c, s as i64)
}

/// `cashlarger()` (cash.c:929) — larger of two `money` values.
pub fn cashlarger(c1: Cash, c2: Cash) -> Cash {
    if c1 > c2 {
        c1
    } else {
        c2
    }
}

/// `cashsmaller()` (cash.c:944) — smaller of two `money` values.
pub fn cashsmaller(c1: Cash, c2: Cash) -> Cash {
    if c1 < c2 {
        c1
    } else {
        c2
    }
}

// ===========================================================================
// cash_words (cash.c:38 append_num_word + cash.c:960)
// ===========================================================================

/// `small` / `big` lookup tables (cash.c:41). `big = small + 18`.
const SMALL: [&str; 28] = [
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
    "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen",
    "nineteen", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety",
];

/// `big[i] == small[i + 18]` (cash.c:47).
#[inline]
fn big(i: usize) -> &'static str {
    SMALL[i + 18]
}

/// `append_num_word()` (cash.c:38) — append the English words for `value`
/// (which is `< 1000`) to `buf`.
fn append_num_word(buf: &mut String, value: Cash) {
    let tu = (value % 100) as usize;
    let value = value as usize;

    // deal with the simple cases first.
    if value <= 20 {
        buf.push_str(SMALL[value]);
        return;
    }

    // is it an even multiple of 100?
    if tu == 0 {
        buf.push_str(SMALL[value / 100]);
        buf.push_str(" hundred");
        return;
    }

    // more than 99?
    if value > 99 {
        // is it an even multiple of 10 other than 10?
        if value % 10 == 0 && tu > 10 {
            buf.push_str(SMALL[value / 100]);
            buf.push_str(" hundred ");
            buf.push_str(big(tu / 10));
        } else if tu < 20 {
            buf.push_str(SMALL[value / 100]);
            buf.push_str(" hundred and ");
            buf.push_str(SMALL[tu]);
        } else {
            buf.push_str(SMALL[value / 100]);
            buf.push_str(" hundred ");
            buf.push_str(big(tu / 10));
            buf.push(' ');
            buf.push_str(SMALL[tu % 10]);
        }
    } else {
        // is it an even multiple of 10 other than 10?
        if value % 10 == 0 && tu > 10 {
            buf.push_str(big(tu / 10));
        } else if tu < 20 {
            buf.push_str(SMALL[tu]);
        } else {
            buf.push_str(big(tu / 10));
            buf.push(' ');
            buf.push_str(SMALL[tu % 10]);
        }
    }
}

/// `cash_words()` (cash.c:960) — an English-language description of an amount.
///
/// C builds a `StringInfo`, capitalizes `buf.data[0]`, and returns a `text`
/// datum via `cstring_to_text_with_len`. The port returns the owned [`String`].
pub fn cash_words(value: Cash) -> String {
    let mut buf = String::new();

    // work with positive numbers.
    let mut value = value;
    if value < 0 {
        // C does a plain `value = -value;` then relies on the subsequent
        // `(uint64) value` cast to recover the correct magnitude even at
        // INT64_MIN (where the negation overflows back to INT64_MIN). A plain
        // Rust negate would panic in debug at INT64_MIN, so use a wrapping
        // negate; the following `value as u64` then reproduces C's `(uint64)
        // value` exactly (INT64_MIN -> 9223372036854775808).
        value = value.wrapping_neg();
        buf.push_str("minus ");
    }

    // Now treat as unsigned, to avoid trouble at INT_MIN.
    let val = value as u64;

    let dollars: Cash = (val / 100) as Cash;
    let m0: Cash = (val % 100) as Cash; // cents
    let m1: Cash = ((val / 100) % 1000) as Cash; // hundreds
    let m2: Cash = ((val / 100_000) % 1000) as Cash; // thousands
    let m3: Cash = ((val / 100_000_000) % 1000) as Cash; // millions
    let m4: Cash = ((val / 100_000_000_000) % 1000) as Cash; // billions
    let m5: Cash = ((val / 100_000_000_000_000) % 1000) as Cash; // trillions
    let m6: Cash = ((val / 100_000_000_000_000_000) % 1000) as Cash; // quadrillions

    if m6 != 0 {
        append_num_word(&mut buf, m6);
        buf.push_str(" quadrillion ");
    }
    if m5 != 0 {
        append_num_word(&mut buf, m5);
        buf.push_str(" trillion ");
    }
    if m4 != 0 {
        append_num_word(&mut buf, m4);
        buf.push_str(" billion ");
    }
    if m3 != 0 {
        append_num_word(&mut buf, m3);
        buf.push_str(" million ");
    }
    if m2 != 0 {
        append_num_word(&mut buf, m2);
        buf.push_str(" thousand ");
    }
    if m1 != 0 {
        append_num_word(&mut buf, m1);
    }

    if dollars == 0 {
        buf.push_str("zero");
    }

    buf.push_str(if dollars == 1 {
        " dollar and "
    } else {
        " dollars and "
    });
    append_num_word(&mut buf, m0);
    buf.push_str(if m0 == 1 { " cent" } else { " cents" });

    capitalize_first(&mut buf);
    buf
}

/// `buf.data[0] = pg_toupper((unsigned char) buf.data[0])` (cash.c:1038).
///
/// `cash_words` always produces a non-empty buffer whose first byte is the
/// first byte of an ASCII English word, so `pg_toupper` takes its ASCII branch
/// and the in-place byte replacement preserves UTF-8 validity.
fn capitalize_first(buf: &mut String) {
    let Some(&first) = buf.as_bytes().first() else {
        return;
    };
    let upper = pg_toupper_full(first);
    if upper == first {
        return;
    }
    if first.is_ascii() && upper.is_ascii() {
        // The first char is a single ASCII byte; replace just that one char
        // with its uppercase form. `replace_range` keeps the String valid
        // without any unsafe byte poking.
        buf.replace_range(0..1, (upper as char).to_string().as_str());
    }
}

// ===========================================================================
// Numeric / integer conversions (cash.c:1050-1195)
// ===========================================================================

/// `cash_numeric()` (cash.c:1050) — convert `money` to `numeric`.
///
/// C returns a `numeric` `Datum`; the port returns the on-disk `numeric`
/// varlena byte image (charged to `mcx`). The `numeric` arithmetic cores
/// (`int64_to_numeric`, `numeric_round`, `numeric_div`) are called directly on
/// the on-disk image; `cash.c`'s own control flow (the `fpoint` guard, the
/// scale-factor computation, the round-the-divisor-before-dividing trick) is
/// ported here.
pub fn cash_numeric<'mcx>(mcx: Mcx<'mcx>, money: Cash) -> PgResult<PgVec<'mcx, u8>> {
    let lconvert = pglc_localeconv();
    let fpoint = clamp_frac_digits(&lconvert);

    // convert the integral money value to numeric.
    let mut result = int64_to_numeric(mcx, money)?;

    // scale appropriately, if needed.
    if fpoint > 0 {
        // compute required scale factor.
        let mut scale: i64 = 1;
        for _ in 0..fpoint {
            scale *= 10;
        }
        let mut numeric_scale = int64_to_numeric(mcx, scale)?;

        // Given integral inputs approaching INT64_MAX, select_div_scale() might
        // choose a result scale of zero, causing loss of fractional digits in
        // the quotient. Ensure an exact result by rounding the divisor's dscale
        // up to at least the desired result scale.
        numeric_scale = numeric_round(mcx, &numeric_scale, fpoint)?;

        // Now divide ...
        let quotient = numeric_div(mcx, &result, &numeric_scale)?;

        // ... and forcibly round to exactly the intended number of digits.
        result = numeric_round(mcx, &quotient, fpoint)?;
    }

    Ok(result)
}

/// `numeric_cash()` (cash.c:1106) — convert `numeric` to `money`.
///
/// `amount` is the on-disk input `numeric` byte image. C multiplies by the
/// scale factor and routes through `numeric_int8` (which rounds to nearest and
/// rejects NaN/Inf).
pub fn numeric_cash(mcx: Mcx<'_>, amount: &[u8]) -> PgResult<Cash> {
    let lconvert = pglc_localeconv();
    let fpoint = clamp_frac_digits(&lconvert);

    // compute required scale factor.
    let mut scale: i64 = 1;
    for _ in 0..fpoint {
        scale *= 10;
    }

    // multiply the input amount by scale factor.
    let numeric_scale = int64_to_numeric(mcx, scale)?;
    let product = numeric_mul(mcx, amount, &numeric_scale)?;

    // note that numeric_int8 will round to nearest integer for us (and reject
    // NaN/Inf -> ERRCODE_FEATURE_NOT_SUPPORTED, overflow -> bigint out of range).
    let result = numeric_int8(mcx, &product)?;

    Ok(result as Cash)
}

/// `int4_cash()` (cash.c:1140) — convert `int4` (`int`) to `money`.
pub fn int4_cash(amount: i32) -> PgResult<Cash> {
    let lconvert = pglc_localeconv();
    let fpoint = clamp_frac_digits(&lconvert);

    // compute required scale factor.
    let mut scale: i64 = 1;
    for _ in 0..fpoint {
        scale *= 10;
    }

    // compute amount * scale, checking for overflow (DirectFunctionCall2 int8mul).
    let result = int8mul(amount as i64, scale)?;

    Ok(result as Cash)
}

/// `int8_cash()` (cash.c:1170) — convert `int8` (`bigint`) to `money`.
pub fn int8_cash(amount: i64) -> PgResult<Cash> {
    let lconvert = pglc_localeconv();
    let fpoint = clamp_frac_digits(&lconvert);

    // compute required scale factor.
    let mut scale: i64 = 1;
    for _ in 0..fpoint {
        scale *= 10;
    }

    // compute amount * scale, checking for overflow (DirectFunctionCall2 int8mul).
    let result = int8mul(amount, scale)?;

    Ok(result as Cash)
}
