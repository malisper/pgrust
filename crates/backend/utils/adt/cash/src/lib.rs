//! cash.c: the money type over a 64-bit integer of lc_monetary fractional
//! units. Locale symbols come from pg_locale::pglc_localeconv (C-locale arm
//! live, non-C loud there).

pub mod builtins;
#[cfg(test)]
mod tests;

use adt_numeric::{Num, NumericImage};
use datum::{Bytea, Varlena};
use mcx::{Mcx, PgVec};
use pg_locale::{pglc_localeconv, PgLconv};
use stringinfo::StringInfo;
use types_error::{
    ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_DIVISION_BY_ZERO,
    ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub type Cash = i64;

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r')
}

#[cold]
#[inline(never)]
fn money_out_of_range() -> PgError {
    PgError::error("money out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn division_by_zero() -> PgError {
    PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}

#[cold]
#[inline(never)]
fn value_out_of_range_err(input: &str) -> PgError {
    PgError::error(format!("value \"{input}\" is out of range for type money"))
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn invalid_syntax_err(input: &str) -> PgError {
    PgError::error(format!("invalid input syntax for type money: \"{input}\""))
        .with_sqlstate(ERRCODE_INVALID_TEXT_REPRESENTATION)
}

// See the frac_digits plausibility-range comment in C cash_in.
fn clamp_fpoint(lconvert: &PgLconv) -> i32 {
    let fpoint = lconvert.frac_digits as i32;
    if !(0..=10).contains(&fpoint) {
        2
    } else {
        fpoint
    }
}

fn dsymbol_of(lconvert: &PgLconv) -> u8 {
    // dsymbol is restricted to a single byte, unlike the other symbols.
    if lconvert.mon_decimal_point.len() == 1 {
        lconvert.mon_decimal_point.as_bytes()[0]
    } else {
        b'.'
    }
}

fn ssymbol_of(lconvert: &PgLconv, dsymbol: u8) -> &'static str {
    if !lconvert.mon_thousands_sep.is_empty() {
        lconvert.mon_thousands_sep
    } else if dsymbol != b',' {
        ","
    } else {
        "."
    }
}

fn nonempty_or(sym: &'static str, default: &'static str) -> &'static str {
    if sym.is_empty() {
        default
    } else {
        sym
    }
}

pub fn cash_in(str: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<Cash> {
    let lconvert = pglc_localeconv()?;
    let fpoint = clamp_fpoint(lconvert);
    let dsymbol = dsymbol_of(lconvert);
    let ssymbol = ssymbol_of(lconvert, dsymbol).as_bytes();
    let csymbol = nonempty_or(lconvert.currency_symbol, "$").as_bytes();
    let psymbol = nonempty_or(lconvert.positive_sign, "+").as_bytes();
    let nsymbol = nonempty_or(lconvert.negative_sign, "-").as_bytes();

    let b = str.as_bytes();
    let mut i = 0usize;
    let mut sgn: i64 = 1;

    while i < b.len() && is_space(b[i]) {
        i += 1;
    }
    if b[i..].starts_with(csymbol) {
        i += csymbol.len();
    }
    while i < b.len() && is_space(b[i]) {
        i += 1;
    }

    if b[i..].starts_with(nsymbol) {
        sgn = -1;
        i += nsymbol.len();
    } else if i < b.len() && b[i] == b'(' {
        sgn = -1;
        i += 1;
    } else if b[i..].starts_with(psymbol) {
        i += psymbol.len();
    }

    while i < b.len() && is_space(b[i]) {
        i += 1;
    }
    if b[i..].starts_with(csymbol) {
        i += csymbol.len();
    }
    while i < b.len() && is_space(b[i]) {
        i += 1;
    }

    // The amount accumulates in the negative (larger range) and flips at the
    // end, catching most-negative-number overflow.
    let mut value: i64 = 0;
    let mut dec: i32 = 0;
    let mut seen_dot = false;

    while i < b.len() {
        let ch = b[i];
        if ch.is_ascii_digit() && (!seen_dot || dec < fpoint) {
            let digit = (ch - b'0') as i64;
            value = match value.checked_mul(10).and_then(|v| v.checked_sub(digit)) {
                Some(v) => v,
                None => return ereturn(escontext, 0, value_out_of_range_err(str)),
            };
            if seen_dot {
                dec += 1;
            }
        } else if ch == dsymbol && !seen_dot {
            seen_dot = true;
        } else if b[i..].starts_with(ssymbol) {
            i += ssymbol.len() - 1;
        } else {
            break;
        }
        i += 1;
    }

    if i < b.len() && b[i].is_ascii_digit() && b[i] >= b'5' {
        value = match value.checked_sub(1) {
            Some(v) => v,
            None => return ereturn(escontext, 0, value_out_of_range_err(str)),
        };
    }

    while dec < fpoint {
        value = match value.checked_mul(10) {
            Some(v) => v,
            None => return ereturn(escontext, 0, value_out_of_range_err(str)),
        };
        dec += 1;
    }

    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }

    while i < b.len() {
        if is_space(b[i]) || b[i] == b')' {
            i += 1;
        } else if b[i..].starts_with(nsymbol) {
            sgn = -1;
            i += nsymbol.len();
        } else if b[i..].starts_with(psymbol) {
            i += psymbol.len();
        } else if b[i..].starts_with(csymbol) {
            i += csymbol.len();
        } else {
            return ereturn(escontext, 0, invalid_syntax_err(str));
        }
    }

    if sgn > 0 {
        if value == i64::MIN {
            return ereturn(escontext, 0, value_out_of_range_err(str));
        }
        Ok(-value)
    } else {
        Ok(value)
    }
}

pub const CASH_OUT_BUFLEN: usize = 128;

pub fn cash_out_into(value: Cash, out: &mut [u8; CASH_OUT_BUFLEN]) -> PgResult<usize> {
    let lconvert = pglc_localeconv()?;
    let points = clamp_fpoint(lconvert);
    // As with frac_digits, mon_grouping gets a plausibility range check.
    let mon_group = {
        let g = lconvert.mon_grouping.as_bytes().first().copied().unwrap_or(0) as i32;
        if !(1..=6).contains(&g) {
            3
        } else {
            g
        }
    };
    let dsymbol = dsymbol_of(lconvert);
    let ssymbol = ssymbol_of(lconvert, dsymbol).as_bytes();
    let csymbol = nonempty_or(lconvert.currency_symbol, "$").as_bytes();

    let (signsymbol, sign_posn, cs_precedes, sep_by_space) = if value < 0 {
        (
            nonempty_or(lconvert.negative_sign, "-").as_bytes(),
            lconvert.n_sign_posn,
            lconvert.n_cs_precedes,
            lconvert.n_sep_by_space,
        )
    } else {
        (
            lconvert.positive_sign.as_bytes(),
            lconvert.p_sign_posn,
            lconvert.p_cs_precedes,
            lconvert.p_sep_by_space,
        )
    };

    let mut uvalue = value.unsigned_abs();
    let mut numbuf = [0u8; 64];
    let mut bp = numbuf.len();
    let mut digit_pos = points;
    loop {
        if points != 0 && digit_pos == 0 {
            bp -= 1;
            numbuf[bp] = dsymbol;
        } else if digit_pos < 0 && digit_pos % mon_group == 0 {
            bp -= ssymbol.len();
            numbuf[bp..bp + ssymbol.len()].copy_from_slice(ssymbol);
        }
        bp -= 1;
        numbuf[bp] = (uvalue % 10) as u8 + b'0';
        uvalue /= 10;
        digit_pos -= 1;
        if uvalue == 0 && digit_pos < 0 {
            break;
        }
    }
    let digits = &numbuf[bp..];

    let sep1: &[u8] = if sep_by_space == 1 { b" " } else { b"" };
    let sep2: &[u8] = if sep_by_space == 2 { b" " } else { b"" };
    let cs_precedes = cs_precedes != 0;

    let mut n = 0usize;
    let mut w = |part: &[u8]| {
        out[n..n + part.len()].copy_from_slice(part);
        n += part.len();
    };
    match sign_posn {
        0 => {
            if cs_precedes {
                w(b"(");
                w(csymbol);
                w(sep1);
                w(digits);
                w(b")");
            } else {
                w(b"(");
                w(digits);
                w(sep1);
                w(csymbol);
                w(b")");
            }
        }
        2 => {
            if cs_precedes {
                w(csymbol);
                w(sep1);
                w(digits);
                w(sep2);
                w(signsymbol);
            } else {
                w(digits);
                w(sep1);
                w(csymbol);
                w(sep2);
                w(signsymbol);
            }
        }
        3 => {
            if cs_precedes {
                w(signsymbol);
                w(sep2);
                w(csymbol);
                w(sep1);
                w(digits);
            } else {
                w(digits);
                w(sep1);
                w(signsymbol);
                w(sep2);
                w(csymbol);
            }
        }
        4 => {
            if cs_precedes {
                w(csymbol);
                w(sep2);
                w(signsymbol);
                w(sep1);
                w(digits);
            } else {
                w(digits);
                w(sep1);
                w(csymbol);
                w(sep2);
                w(signsymbol);
            }
        }
        _ => {
            if cs_precedes {
                w(signsymbol);
                w(sep2);
                w(csymbol);
                w(sep1);
                w(digits);
            } else {
                w(signsymbol);
                w(sep2);
                w(digits);
                w(sep1);
                w(csymbol);
            }
        }
    }
    Ok(n)
}

pub fn cash_recv(buf: &mut StringInfo<'_>) -> PgResult<Cash> {
    pqformat::pq_getmsgint64(buf)
}

pub fn cash_send<'mcx>(mcx: Mcx<'mcx>, arg1: Cash) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint64(&mut buf, arg1 as u64)?;
    Ok(pqformat::pq_endtypsend(buf))
}

pub const fn cash_eq(c1: Cash, c2: Cash) -> bool {
    c1 == c2
}

pub const fn cash_ne(c1: Cash, c2: Cash) -> bool {
    c1 != c2
}

pub const fn cash_lt(c1: Cash, c2: Cash) -> bool {
    c1 < c2
}

pub const fn cash_le(c1: Cash, c2: Cash) -> bool {
    c1 <= c2
}

pub const fn cash_gt(c1: Cash, c2: Cash) -> bool {
    c1 > c2
}

pub const fn cash_ge(c1: Cash, c2: Cash) -> bool {
    c1 >= c2
}

pub const fn cash_cmp(c1: Cash, c2: Cash) -> i32 {
    if c1 > c2 {
        1
    } else if c1 == c2 {
        0
    } else {
        -1
    }
}

pub fn cash_pl(c1: Cash, c2: Cash) -> PgResult<Cash> {
    match c1.checked_add(c2) {
        Some(res) => Ok(res),
        None => Err(money_out_of_range().into()),
    }
}

pub fn cash_mi(c1: Cash, c2: Cash) -> PgResult<Cash> {
    match c1.checked_sub(c2) {
        Some(res) => Ok(res),
        None => Err(money_out_of_range().into()),
    }
}

pub fn cash_mul_float8(c: Cash, f: f64) -> PgResult<Cash> {
    let res = adt_float::float8_mul(c as f64, f)?.round_ties_even();
    if res.is_nan() || !adt_float::float8_fits_in_int64(res) {
        return Err(money_out_of_range().into());
    }
    Ok(res as Cash)
}

pub fn cash_div_float8(c: Cash, f: f64) -> PgResult<Cash> {
    let res = adt_float::float8_div(c as f64, f)?.round_ties_even();
    if res.is_nan() || !adt_float::float8_fits_in_int64(res) {
        return Err(money_out_of_range().into());
    }
    Ok(res as Cash)
}

pub fn cash_mul_int64(c: Cash, i: i64) -> PgResult<Cash> {
    match c.checked_mul(i) {
        Some(res) => Ok(res),
        None => Err(money_out_of_range().into()),
    }
}

pub fn cash_div_int64(c: Cash, i: i64) -> PgResult<Cash> {
    if i == 0 {
        return Err(division_by_zero().into());
    }
    // Cash::MIN / -1 overflows i64 (the quotient is Cash::MAX + 1). C's
    // cash_div_int64 lacks this guard — its 2024 money-overflow sweep
    // (4f96281587) covered multiply and the float paths but not division,
    // so on aarch64 it silently returns Cash::MIN and on x86-64 it traps
    // into a "floating-point exception". int8div raises 22003 for the same
    // arithmetic; mirror that (ruling 2026-07-29; bug reported upstream).
    if i == -1 && c == Cash::MIN {
        return Err(money_out_of_range().into());
    }
    Ok(c / i)
}

pub fn cash_div_cash(dividend: Cash, divisor: Cash) -> PgResult<f64> {
    if divisor == 0 {
        return Err(division_by_zero().into());
    }
    Ok(dividend as f64 / divisor as f64)
}

pub const fn cashlarger(c1: Cash, c2: Cash) -> Cash {
    if c1 > c2 {
        c1
    } else {
        c2
    }
}

pub const fn cashsmaller(c1: Cash, c2: Cash) -> Cash {
    if c1 < c2 {
        c1
    } else {
        c2
    }
}

fn scale_factor() -> PgResult<i64> {
    let fpoint = clamp_fpoint(pglc_localeconv()?);
    let mut scale: i64 = 1;
    for _ in 0..fpoint {
        scale *= 10;
    }
    Ok(scale)
}

// C goes through DirectFunctionCall2(int8mul): its bigint-out-of-range error
// is the parity surface.
pub fn int4_cash(amount: i32) -> PgResult<Cash> {
    adt_int8::int8mul(amount as i64, scale_factor()?)
}

pub fn int8_cash(amount: i64) -> PgResult<Cash> {
    adt_int8::int8mul(amount, scale_factor()?)
}

const SMALL_WORDS: [&str; 28] = [
    "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten",
    "eleven", "twelve", "thirteen", "fourteen", "fifteen", "sixteen", "seventeen", "eighteen",
    "nineteen", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety",
];

// C: big = small + 18, so big[tu/10] = SMALL_WORDS[tu/10 + 18].
fn big_word(tens: i64) -> &'static str {
    SMALL_WORDS[(tens + 18) as usize]
}

fn append_num_word(buf: &mut PgVec<'_, u8>, value: i64) -> PgResult<()> {
    let mut push = |s: &str| mcx::vec_append_bytes(buf, s.as_bytes());
    let tu = value % 100;

    if value <= 20 {
        return push(SMALL_WORDS[value as usize]);
    }

    if tu == 0 {
        push(SMALL_WORDS[(value / 100) as usize])?;
        return push(" hundred");
    }

    if value > 99 {
        push(SMALL_WORDS[(value / 100) as usize])?;
        if value % 10 == 0 && tu > 10 {
            push(" hundred ")?;
            push(big_word(tu / 10))
        } else if tu < 20 {
            push(" hundred and ")?;
            push(SMALL_WORDS[tu as usize])
        } else {
            push(" hundred ")?;
            push(big_word(tu / 10))?;
            push(" ")?;
            push(SMALL_WORDS[(tu % 10) as usize])
        }
    } else if value % 10 == 0 && tu > 10 {
        push(big_word(tu / 10))
    } else if tu < 20 {
        push(SMALL_WORDS[tu as usize])
    } else {
        push(big_word(tu / 10))?;
        push(" ")?;
        push(SMALL_WORDS[(tu % 10) as usize])
    }
}

pub fn cash_words<'mcx>(mcx: Mcx<'mcx>, value: Cash) -> PgResult<Varlena<'mcx>> {
    let mut buf: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 64)?;

    // Negate-then-treat-as-unsigned keeps INT64_MIN out of trouble (C casts
    // the negated value to uint64).
    let val: u64 = if value < 0 {
        mcx::vec_append_bytes(&mut buf, b"minus ")?;
        value.wrapping_neg() as u64
    } else {
        value as u64
    };

    let dollars = (val / 100) as i64;
    let m0 = (val % 100) as i64;
    let m1 = ((val / 100) % 1000) as i64;
    let m2 = ((val / 100_000) % 1000) as i64;
    let m3 = ((val / 100_000_000) % 1000) as i64;
    let m4 = ((val / 100_000_000_000) % 1000) as i64;
    let m5 = ((val / 100_000_000_000_000) % 1000) as i64;
    let m6 = ((val / 100_000_000_000_000_000) % 1000) as i64;

    if m6 != 0 {
        append_num_word(&mut buf, m6)?;
        mcx::vec_append_bytes(&mut buf, b" quadrillion ")?;
    }
    if m5 != 0 {
        append_num_word(&mut buf, m5)?;
        mcx::vec_append_bytes(&mut buf, b" trillion ")?;
    }
    if m4 != 0 {
        append_num_word(&mut buf, m4)?;
        mcx::vec_append_bytes(&mut buf, b" billion ")?;
    }
    if m3 != 0 {
        append_num_word(&mut buf, m3)?;
        mcx::vec_append_bytes(&mut buf, b" million ")?;
    }
    if m2 != 0 {
        append_num_word(&mut buf, m2)?;
        mcx::vec_append_bytes(&mut buf, b" thousand ")?;
    }
    if m1 != 0 {
        append_num_word(&mut buf, m1)?;
    }
    if dollars == 0 {
        mcx::vec_append_bytes(&mut buf, b"zero")?;
    }
    mcx::vec_append_bytes(
        &mut buf,
        if dollars == 1 {
            b" dollar and ".as_slice()
        } else {
            b" dollars and ".as_slice()
        },
    )?;
    append_num_word(&mut buf, m0)?;
    mcx::vec_append_bytes(
        &mut buf,
        if m0 == 1 {
            b" cent".as_slice()
        } else {
            b" cents".as_slice()
        },
    )?;

    buf[0] = buf[0].to_ascii_uppercase();
    varlena::cstring_to_text(mcx, &buf)
}

pub fn cash_numeric(money: Cash) -> PgResult<NumericImage> {
    let fpoint = clamp_fpoint(pglc_localeconv()?);
    let mut result = adt_numeric::int64_to_numeric(money);

    if fpoint > 0 {
        // select_div_scale() can pick a zero result scale for near-INT64_MAX
        // inputs; rounding the scale operand's dscale up to fpoint forces an
        // exact quotient, then the explicit round trims it to fpoint.
        let scale = adt_numeric::numeric_round_common(
            adt_numeric::int64_to_numeric(scale_factor()?).num(),
            fpoint,
        )?;
        let quotient = adt_numeric::numeric_div_common(result.num(), scale.num())?;
        result = adt_numeric::numeric_round_common(quotient.num(), fpoint)?;
    }

    Ok(result)
}

pub fn numeric_cash(amount: Num<'_>) -> PgResult<Cash> {
    let scale = adt_numeric::int64_to_numeric(scale_factor()?);
    let scaled = adt_numeric::numeric_mul_common(amount, scale.num())?;
    adt_numeric::numeric_int8(scaled.num())
}
