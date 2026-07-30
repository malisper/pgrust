use core::cell::RefCell;

use datum::Bytea;
use mcx::Mcx;
use stringinfo::StringInfo;
use types_error::{ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_INVALID_BINARY_REPRESENTATION};

use crate::arith::{add_var, mul_var};
use crate::ops::{apply_typmod, apply_typmod_special};
use crate::var::{int64_to_var, make_result, make_result_opt_error, NumericImage, NumericVar, VarView};
use crate::{
    invalid_numeric_syntax, numeric_overflow_error, Num, NumericDigit, DEC_DIGITS, NBASE,
    NUMERIC_DSCALE_MASK, NUMERIC_NAN, NUMERIC_NEG, NUMERIC_NINF, NUMERIC_PINF, NUMERIC_POS,
    NUMERIC_WEIGHT_MAX,
};

// C's palloc'd decdigits scratch in set_var_from_str; retained TLS (rule 7).
std::thread_local! {
    static DECDIGITS: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

#[inline]
fn c_isspace(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

fn strncasecmp_eq(s: &[u8], lit: &[u8]) -> bool {
    s.len() >= lit.len() && s[..lit.len()].iter().zip(lit).all(|(a, b)| a.eq_ignore_ascii_case(b))
}

enum NumErr {
    InvalidSyntax,
    OutOfRange,
}

pub fn numeric_in(
    input: &str,
    typmod: i32,
    mut escontext: Option<&mut SoftErrorContext>,
) -> PgResult<Option<NumericImage>> {
    let s = input.as_bytes();
    let mut cp = 0usize;
    while cp < s.len() && c_isspace(s[cp]) {
        cp += 1;
    }

    let numstart = cp;
    let mut sign = NUMERIC_POS;
    if cp < s.len() && s[cp] == b'+' {
        cp += 1;
    } else if cp < s.len() && s[cp] == b'-' {
        sign = NUMERIC_NEG;
        cp += 1;
    }

    let res: NumericImage;
    if cp >= s.len() || (!s[cp].is_ascii_digit() && s[cp] != b'.') {
        // NaN mustn't have a sign; infinities may.
        if strncasecmp_eq(&s[numstart..], b"NaN") {
            res = NumericImage::nan();
            cp = numstart + 3;
        } else if strncasecmp_eq(&s[cp..], b"Infinity") {
            res = if sign == NUMERIC_POS {
                NumericImage::pinf()
            } else {
                NumericImage::ninf()
            };
            cp += 8;
        } else if strncasecmp_eq(&s[cp..], b"inf") {
            res = if sign == NUMERIC_POS {
                NumericImage::pinf()
            } else {
                NumericImage::ninf()
            };
            cp += 3;
        } else {
            return ereturn(escontext, None, invalid_numeric_syntax(input));
        }

        while cp < s.len() {
            if !c_isspace(s[cp]) {
                return ereturn(escontext, None, invalid_numeric_syntax(input));
            }
            cp += 1;
        }

        if !apply_typmod_special(res.num(), typmod, escontext)? {
            return Ok(None);
        }
        return Ok(Some(res));
    }

    let base = if s[cp] == b'0' && cp + 1 < s.len() {
        match s[cp + 1] {
            b'x' | b'X' => 16,
            b'o' | b'O' => 8,
            b'b' | b'B' => 2,
            _ => 10,
        }
    } else {
        10
    };

    let mut value = NumericVar::new();
    let endpos = if base == 10 {
        match set_var_from_str(s, cp, &mut value) {
            Ok(end) => {
                value.sign = sign;
                end
            }
            Err(NumErr::InvalidSyntax) => {
                return ereturn(escontext, None, invalid_numeric_syntax(input))
            }
            Err(NumErr::OutOfRange) => {
                return ereturn(escontext, None, numeric_overflow_error())
            }
        }
    } else {
        match set_var_from_non_decimal_integer_str(s, cp + 2, sign, base, &mut value) {
            Ok(end) => end,
            Err(NumErr::InvalidSyntax) => {
                return ereturn(escontext, None, invalid_numeric_syntax(input))
            }
            Err(NumErr::OutOfRange) => {
                return ereturn(escontext, None, numeric_overflow_error())
            }
        }
    };

    cp = endpos;
    while cp < s.len() {
        if !c_isspace(s[cp]) {
            return ereturn(escontext, None, invalid_numeric_syntax(input));
        }
        cp += 1;
    }

    if !apply_typmod(&mut value, typmod, escontext.as_deref_mut())? {
        return Ok(None);
    }

    match make_result_opt_error(value.view()) {
        Some(img) => Ok(Some(img)),
        None => ereturn(escontext, None, numeric_overflow_error()),
    }
}

pub fn numeric_recv(buf: &mut StringInfo<'_>, typmod: i32) -> PgResult<NumericImage> {
    let len = pqformat::pq_getmsgint(buf, 2)? as i32;
    let mut value = NumericVar::new();
    value.alloc(len);
    value.weight = pqformat::pq_getmsgint(buf, 2)? as u16 as i16 as i32;

    let sign = pqformat::pq_getmsgint(buf, 2)? as u16;
    if !(sign == NUMERIC_POS
        || sign == NUMERIC_NEG
        || sign == NUMERIC_NAN
        || sign == NUMERIC_PINF
        || sign == NUMERIC_NINF)
    {
        return Err(recv_error("invalid sign in external \"numeric\" value"));
    }
    value.sign = sign;

    let dscale = pqformat::pq_getmsgint(buf, 2)? as u16;
    if dscale & NUMERIC_DSCALE_MASK != dscale {
        return Err(recv_error("invalid scale in external \"numeric\" value"));
    }
    value.dscale = dscale as i32;

    for slot in value.digits_mut() {
        let d = pqformat::pq_getmsgint(buf, 2)? as u16 as NumericDigit;
        if d < 0 || d as i32 >= NBASE {
            return Err(recv_error("invalid digit in external \"numeric\" value"));
        }
        *slot = d;
    }

    if sign == NUMERIC_POS || sign == NUMERIC_NEG {
        let ds = value.dscale;
        value.trunc(ds);
        apply_typmod(&mut value, typmod, None)?;
        make_result(value.view())
    } else {
        let res = make_result(value.view())?;
        apply_typmod_special(res.num(), typmod, None)?;
        Ok(res)
    }
}

pub fn numeric_send<'mcx>(mcx: Mcx<'mcx>, num: Num<'_>) -> PgResult<Bytea<'mcx>> {
    let mut buf = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint16(&mut buf, num.ndigits() as u16)?;
    pqformat::pq_sendint16(&mut buf, num.weight() as i16 as u16)?;
    pqformat::pq_sendint16(&mut buf, num.sign())?;
    pqformat::pq_sendint16(&mut buf, num.dscale() as u16)?;
    for &d in num.digits() {
        pqformat::pq_sendint16(&mut buf, d as u16)?;
    }
    Ok(pqformat::pq_endtypsend(buf))
}

#[track_caller]
#[cold]
#[inline(never)]
fn recv_error(msg: &'static str) -> Box<PgError> {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_BINARY_REPRESENTATION).into()
}

fn set_var_from_str(s: &[u8], mut cp: usize, dest: &mut NumericVar) -> Result<usize, NumErr> {
    let mut have_dp = false;
    let mut sign = NUMERIC_POS;
    let mut dweight: i32 = -1;
    let mut dscale: i32 = 0;

    match s.get(cp) {
        Some(b'+') => {
            sign = NUMERIC_POS;
            cp += 1;
        }
        Some(b'-') => {
            sign = NUMERIC_NEG;
            cp += 1;
        }
        _ => {}
    }

    if s.get(cp) == Some(&b'.') {
        have_dp = true;
        cp += 1;
    }

    if !s.get(cp).is_some_and(|b| b.is_ascii_digit()) {
        return Err(NumErr::InvalidSyntax);
    }

    DECDIGITS.with(|dd| {
        let mut decdigits = dd.borrow_mut();
        let cap = s.len() - cp + DEC_DIGITS as usize * 2;
        decdigits.clear();
        decdigits.reserve(cap);
        // SAFETY: cap bounds every write below (<= digits remaining + padding);
        // the final set_len exposes only written bytes.
        let dd_ptr = decdigits.as_mut_ptr();
        let mut i = DEC_DIGITS as usize;
        unsafe {
            core::ptr::write_bytes(dd_ptr, 0, DEC_DIGITS as usize);

            while cp < s.len() {
                let b = *s.get_unchecked(cp);
                if b.is_ascii_digit() {
                    *dd_ptr.add(i) = b - b'0';
                    i += 1;
                    cp += 1;
                    if !have_dp {
                        dweight += 1;
                    } else {
                        dscale += 1;
                    }
                } else if b == b'.' {
                    if have_dp {
                        return Err(NumErr::InvalidSyntax);
                    }
                    have_dp = true;
                    cp += 1;
                    if s.get(cp) == Some(&b'_') {
                        return Err(NumErr::InvalidSyntax);
                    }
                } else if b == b'_' {
                    cp += 1;
                    if !s.get(cp).is_some_and(|b| b.is_ascii_digit()) {
                        return Err(NumErr::InvalidSyntax);
                    }
                } else {
                    break;
                }
            }

            core::ptr::write_bytes(dd_ptr.add(i), 0, DEC_DIGITS as usize - 1);
            decdigits.set_len(i + DEC_DIGITS as usize - 1);
        }
        let ddigits = i as i32 - DEC_DIGITS;

        if matches!(s.get(cp), Some(b'e') | Some(b'E')) {
            let mut exponent: i64 = 0;
            let mut neg = false;

            cp += 1;
            if s.get(cp) == Some(&b'+') {
                cp += 1;
            } else if s.get(cp) == Some(&b'-') {
                neg = true;
                cp += 1;
            }

            if !s.get(cp).is_some_and(|b| b.is_ascii_digit()) {
                return Err(NumErr::InvalidSyntax);
            }

            while cp < s.len() {
                let b = s[cp];
                if b.is_ascii_digit() {
                    exponent = exponent * 10 + (b - b'0') as i64;
                    if exponent > i32::MAX as i64 / 2 {
                        return Err(NumErr::OutOfRange);
                    }
                    cp += 1;
                } else if b == b'_' {
                    cp += 1;
                    if !s.get(cp).is_some_and(|b| b.is_ascii_digit()) {
                        return Err(NumErr::InvalidSyntax);
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

        let weight = if dweight >= 0 {
            (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1
        } else {
            -((-dweight - 1) / DEC_DIGITS + 1)
        };
        let offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
        let ndigits = (ddigits + offset + DEC_DIGITS - 1) / DEC_DIGITS;

        dest.alloc(ndigits);
        dest.sign = sign;
        dest.weight = weight;
        dest.dscale = dscale;

        let mut i = (DEC_DIGITS - offset) as usize;
        {
            let digits = dest.digits_mut();
            let dd = decdigits.as_ptr();
            debug_assert!(i + DEC_DIGITS as usize * (digits.len().max(1) - 1) + DEC_DIGITS as usize
                <= decdigits.len() + 1);
            for dig in digits.iter_mut() {
                // SAFETY: i + 3 stays within the padded decdigits buffer
                // (ndigits * DEC_DIGITS <= offset + ddigits + DEC_DIGITS - 1).
                unsafe {
                    *dig = (((*dd.add(i) as i32 * 10 + *dd.add(i + 1) as i32) * 10
                        + *dd.add(i + 2) as i32)
                        * 10
                        + *dd.add(i + 3) as i32) as NumericDigit;
                }
                i += DEC_DIGITS as usize;
            }
        }

        dest.strip();
        Ok(cp)
    })
}

fn set_var_from_non_decimal_integer_str(
    s: &[u8],
    mut cp: usize,
    sign: u16,
    base: i32,
    dest: &mut NumericVar,
) -> Result<usize, NumErr> {
    let firstdigit = cp;
    let mut tmp: i64 = 0;
    let mut mul: i64 = 1;
    dest.set_zero();

    let digit_ok = |b: u8| -> Option<i64> {
        match base {
            16 => (b as char).to_digit(16).map(|d| d as i64),
            8 => {
                if (b'0'..=b'7').contains(&b) {
                    Some((b - b'0') as i64)
                } else {
                    None
                }
            }
            _ => {
                if (b'0'..=b'1').contains(&b) {
                    Some((b - b'0') as i64)
                } else {
                    None
                }
            }
        }
    };

    while cp < s.len() {
        let b = s[cp];
        if let Some(d) = digit_ok(b) {
            if mul > i64::MAX / base as i64 {
                let mul_var_tmp = int64_to_var(mul);
                let mut prod = NumericVar::new();
                mul_var(dest.view(), mul_var_tmp.view(), &mut prod, 0);
                let add_tmp = int64_to_var(tmp);
                let mut sum = NumericVar::new();
                add_var(prod.view(), add_tmp.view(), &mut sum);
                *dest = sum;

                if dest.weight > NUMERIC_WEIGHT_MAX {
                    return Err(NumErr::OutOfRange);
                }

                tmp = 0;
                mul = 1;
            }
            tmp = tmp * base as i64 + d;
            mul *= base as i64;
            cp += 1;
        } else if b == b'_' {
            cp += 1;
            if !s.get(cp).is_some_and(|b| digit_ok(*b).is_some()) {
                return Err(NumErr::InvalidSyntax);
            }
        } else {
            break;
        }
    }

    if cp == firstdigit {
        return Err(NumErr::InvalidSyntax);
    }

    let mul_var_tmp = int64_to_var(mul);
    let mut prod = NumericVar::new();
    mul_var(dest.view(), mul_var_tmp.view(), &mut prod, 0);
    let add_tmp = int64_to_var(tmp);
    let mut sum = NumericVar::new();
    add_var(prod.view(), add_tmp.view(), &mut sum);
    *dest = sum;

    if dest.weight > NUMERIC_WEIGHT_MAX {
        return Err(NumErr::OutOfRange);
    }

    dest.sign = sign;
    Ok(cp)
}

pub fn numeric_out_into(num: Num<'_>, out: &mut Vec<u8>) {
    if num.is_special() {
        if num.is_pinf() {
            out.extend_from_slice(b"Infinity");
        } else if num.is_ninf() {
            out.extend_from_slice(b"-Infinity");
        } else {
            out.extend_from_slice(b"NaN");
        }
        return;
    }
    get_str_from_var(num.view(), out);
}

pub fn get_str_from_var(var: VarView<'_>, out: &mut Vec<u8>) {
    let dscale = var.dscale;

    let mut i = (var.weight + 1) * DEC_DIGITS;
    if i <= 0 {
        i = 1;
    }
    // C pallocs i + dscale + DEC_DIGITS + 2 and writes with a raw cursor; the
    // reserve below bounds every write, set_len exposes only written bytes.
    let base = out.len();
    out.reserve(i as usize + dscale as usize + DEC_DIGITS as usize + 2);
    let start = out.as_mut_ptr();
    let mut cp = base;
    let digits = var.digits.as_ptr();

    // SAFETY: writes stay below base + reserve (same arithmetic as C's palloc
    // size); digit reads are index-guarded to [0, ndigits).
    unsafe {
        if var.sign == NUMERIC_NEG {
            *start.add(cp) = b'-';
            cp += 1;
        }

        let mut d: i32;
        if var.weight < 0 {
            d = var.weight + 1;
            *start.add(cp) = b'0';
            cp += 1;
        } else {
            d = 0;
            while d <= var.weight {
                let mut dig = if d < var.ndigits {
                    *digits.add(d as usize) as i32
                } else {
                    0
                };
                let mut putit = d > 0;
                let mut d1 = dig / 1000;
                dig -= d1 * 1000;
                putit |= d1 > 0;
                if putit {
                    *start.add(cp) = d1 as u8 + b'0';
                    cp += 1;
                }
                d1 = dig / 100;
                dig -= d1 * 100;
                putit |= d1 > 0;
                if putit {
                    *start.add(cp) = d1 as u8 + b'0';
                    cp += 1;
                }
                d1 = dig / 10;
                dig -= d1 * 10;
                putit |= d1 > 0;
                if putit {
                    *start.add(cp) = d1 as u8 + b'0';
                    cp += 1;
                }
                *start.add(cp) = dig as u8 + b'0';
                cp += 1;
                d += 1;
            }
        }

        if dscale > 0 {
            *start.add(cp) = b'.';
            cp += 1;
            let end = cp + dscale as usize;
            let mut i = 0;
            while i < dscale {
                let mut dig = if d >= 0 && d < var.ndigits {
                    *digits.add(d as usize) as i32
                } else {
                    0
                };
                let d1 = dig / 1000;
                dig -= d1 * 1000;
                *start.add(cp) = d1 as u8 + b'0';
                let d1 = dig / 100;
                dig -= d1 * 100;
                *start.add(cp + 1) = d1 as u8 + b'0';
                let d1 = dig / 10;
                dig -= d1 * 10;
                *start.add(cp + 2) = d1 as u8 + b'0';
                *start.add(cp + 3) = dig as u8 + b'0';
                cp += 4;
                d += 1;
                i += DEC_DIGITS;
            }
            cp = end;
        }

        out.set_len(cp);
    }
}
