use types_error::{ereturn, PgError, PgResult, SoftErrorContext, ERRCODE_FEATURE_NOT_SUPPORTED};

use crate::arith::{add_var, cmp_var, cmp_var_common, div_var, mul_var, select_div_scale, sub_var};
use crate::io::get_str_from_var;
use crate::var::{
    int64_to_var, make_result, make_result_into, make_result_opt_error, set_var_from_int64,
    var_to_int32, var_to_int64, NumericImage, NumericVar, CONST_ONE, CONST_ZERO,
};
use crate::{
    division_by_zero_error, numeric_can_be_short, Num, NumericDigit, DEC_DIGITS,
    NUMERIC_DSCALE_MASK,
    NUMERIC_DSCALE_MAX, NUMERIC_INF_SIGN_MASK, NUMERIC_NEG, NUMERIC_POS,
    NUMERIC_SHORT_DSCALE_MASK, NUMERIC_SHORT_DSCALE_SHIFT, NUMERIC_SHORT_SIGN_MASK,
    NUMERIC_WEIGHT_MAX, VARHDRSZ,
};

#[inline]
/// C: numerictypmodin over already-decoded typmod integers.
pub fn numerictypmodin_core(tl: &[i32]) -> PgResult<i32> {
    use ::types_error::{PgError, ERRCODE_INVALID_PARAMETER_VALUE};

    #[cold]
    #[inline(never)]
    fn param_err(msg: String) -> PgResult<i32> {
        Err(Box::new(
            PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ))
    }

    match tl.len() {
        2 => {
            if tl[0] < 1 || tl[0] > crate::NUMERIC_MAX_PRECISION {
                return param_err(format!(
                    "NUMERIC precision {} must be between 1 and {}",
                    tl[0], crate::NUMERIC_MAX_PRECISION
                ));
            }
            if tl[1] < crate::NUMERIC_MIN_SCALE || tl[1] > crate::NUMERIC_MAX_SCALE {
                return param_err(format!(
                    "NUMERIC scale {} must be between {} and {}",
                    tl[1], crate::NUMERIC_MIN_SCALE, crate::NUMERIC_MAX_SCALE
                ));
            }
            Ok(make_numeric_typmod(tl[0], tl[1]))
        }
        1 => {
            if tl[0] < 1 || tl[0] > crate::NUMERIC_MAX_PRECISION {
                return param_err(format!(
                    "NUMERIC precision {} must be between 1 and {}",
                    tl[0], crate::NUMERIC_MAX_PRECISION
                ));
            }
            Ok(make_numeric_typmod(tl[0], 0))
        }
        _ => param_err(String::from("invalid NUMERIC type modifier")),
    }
}

pub fn make_numeric_typmod(precision: i32, scale: i32) -> i32 {
    ((precision << 16) | (scale & 0x7ff)) + VARHDRSZ as i32
}

#[inline]
pub fn is_valid_numeric_typmod(typmod: i32) -> bool {
    typmod >= VARHDRSZ as i32
}

#[inline]
pub fn numeric_typmod_precision(typmod: i32) -> i32 {
    ((typmod - VARHDRSZ as i32) >> 16) & 0xffff
}

#[inline]
pub fn numeric_typmod_scale(typmod: i32) -> i32 {
    (((typmod - VARHDRSZ as i32) & 0x7ff) ^ 1024) - 1024
}

#[cold]
#[inline(never)]
fn numeric_field_overflow(precision: i32, scale: i32, maxdigits: i32) -> PgError {
    PgError::error("numeric field overflow")
        .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .with_detail(format!(
            "A field with precision {precision}, scale {scale} must round to an absolute value less than {}{}.",
            if maxdigits != 0 { "10^" } else { "" },
            if maxdigits != 0 { maxdigits } else { 1 }
        ))
}

#[cold]
#[inline(never)]
fn numeric_field_overflow_inf(precision: i32, scale: i32) -> PgError {
    PgError::error("numeric field overflow")
        .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        .with_detail(format!(
            "A field with precision {precision}, scale {scale} cannot hold an infinite value."
        ))
}

pub fn apply_typmod(
    var: &mut NumericVar,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    if !is_valid_numeric_typmod(typmod) {
        return Ok(true);
    }

    let precision = numeric_typmod_precision(typmod);
    let scale = numeric_typmod_scale(typmod);
    let maxdigits = precision - scale;

    var.round(scale);

    if var.dscale < 0 {
        var.dscale = 0;
    }

    let mut ddigits = (var.weight + 1) * DEC_DIGITS;
    if ddigits > maxdigits {
        for i in 0..var.ndigits {
            let dig = var.digits()[i as usize];
            if dig != 0 {
                if dig < 10 {
                    ddigits -= 3;
                } else if dig < 100 {
                    ddigits -= 2;
                } else if dig < 1000 {
                    ddigits -= 1;
                }
                if ddigits > maxdigits {
                    return ereturn(
                        escontext,
                        false,
                        numeric_field_overflow(precision, scale, maxdigits),
                    );
                }
                break;
            }
            ddigits -= DEC_DIGITS;
        }
    }

    Ok(true)
}

pub fn apply_typmod_special(
    num: Num<'_>,
    typmod: i32,
    escontext: Option<&mut SoftErrorContext>,
) -> PgResult<bool> {
    debug_assert!(num.is_special());

    // NaN passes any typmod (longstanding behavior); Inf never fits one.
    if num.is_nan() {
        return Ok(true);
    }
    if !is_valid_numeric_typmod(typmod) {
        return Ok(true);
    }

    let precision = numeric_typmod_precision(typmod);
    let scale = numeric_typmod_scale(typmod);
    ereturn(
        escontext,
        false,
        numeric_field_overflow_inf(precision, scale),
    )
}

pub(crate) fn numeric_sign_internal(num: Num<'_>) -> i32 {
    if num.is_special() {
        debug_assert!(!num.is_nan());
        if num.is_pinf() {
            1
        } else {
            -1
        }
    } else if num.ndigits() == 0 {
        0
    } else if num.sign() == NUMERIC_NEG {
        -1
    } else {
        1
    }
}

pub fn numeric_add_into(num1: Num<'_>, num2: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num1.is_special() || num2.is_special() {
        let h = if num1.is_nan() || num2.is_nan() {
            crate::NUMERIC_NAN
        } else if num1.is_pinf() {
            if num2.is_ninf() {
                crate::NUMERIC_NAN
            } else {
                crate::NUMERIC_PINF
            }
        } else if num1.is_ninf() {
            if num2.is_pinf() {
                crate::NUMERIC_NAN
            } else {
                crate::NUMERIC_NINF
            }
        } else if num2.is_pinf() {
            crate::NUMERIC_PINF
        } else {
            crate::NUMERIC_NINF
        };
        out.set_special(h);
        return Ok(());
    }

    let mut result = NumericVar::new();
    add_var(num1.view(), num2.view(), &mut result);
    if !make_result_into(result.view(), out) {
        return Err(crate::numeric_overflow_error().into());
    }
    Ok(())
}

pub fn numeric_add_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_add_into(num1, num2, &mut out)?;
    Ok(out)
}

pub fn numeric_sub_into(num1: Num<'_>, num2: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num1.is_special() || num2.is_special() {
        let h = if num1.is_nan() || num2.is_nan() {
            crate::NUMERIC_NAN
        } else if num1.is_pinf() {
            if num2.is_pinf() {
                crate::NUMERIC_NAN
            } else {
                crate::NUMERIC_PINF
            }
        } else if num1.is_ninf() {
            if num2.is_ninf() {
                crate::NUMERIC_NAN
            } else {
                crate::NUMERIC_NINF
            }
        } else if num2.is_pinf() {
            crate::NUMERIC_NINF
        } else {
            crate::NUMERIC_PINF
        };
        out.set_special(h);
        return Ok(());
    }

    let mut result = NumericVar::new();
    sub_var(num1.view(), num2.view(), &mut result);
    if !make_result_into(result.view(), out) {
        return Err(crate::numeric_overflow_error().into());
    }
    Ok(())
}

pub fn numeric_sub_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_sub_into(num1, num2, &mut out)?;
    Ok(out)
}

fn inf_times_sign(pos: bool, sign: i32) -> PgResult<NumericImage> {
    Ok(match sign {
        0 => NumericImage::nan(),
        1 => {
            if pos {
                NumericImage::pinf()
            } else {
                NumericImage::ninf()
            }
        }
        _ => {
            if pos {
                NumericImage::ninf()
            } else {
                NumericImage::pinf()
            }
        }
    })
}

pub fn numeric_mul_into(num1: Num<'_>, num2: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num1.is_special() || num2.is_special() {
        let img = if num1.is_nan() || num2.is_nan() {
            NumericImage::nan()
        } else if num1.is_pinf() {
            inf_times_sign(true, numeric_sign_internal(num2))?
        } else if num1.is_ninf() {
            inf_times_sign(false, numeric_sign_internal(num2))?
        } else if num2.is_pinf() {
            inf_times_sign(true, numeric_sign_internal(num1))?
        } else {
            inf_times_sign(false, numeric_sign_internal(num1))?
        };
        out.set_from_num(img.num());
        return Ok(());
    }

    let arg1 = num1.view();
    let arg2 = num2.view();
    let mut result = NumericVar::new();
    mul_var(arg1, arg2, &mut result, arg1.dscale + arg2.dscale);
    if result.dscale > NUMERIC_DSCALE_MAX {
        result.round(NUMERIC_DSCALE_MAX);
    }
    if !make_result_into(result.view(), out) {
        return Err(crate::numeric_overflow_error().into());
    }
    Ok(())
}

pub fn numeric_mul_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_mul_into(num1, num2, &mut out)?;
    Ok(out)
}

pub fn numeric_div_into(num1: Num<'_>, num2: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num1.is_special() || num2.is_special() {
        if num1.is_nan() || num2.is_nan() {
            out.set_special(crate::NUMERIC_NAN);
            return Ok(());
        }
        if num1.is_pinf() || num1.is_ninf() {
            if num2.is_special() {
                out.set_special(crate::NUMERIC_NAN);
                return Ok(());
            }
            let pos = num1.is_pinf();
            let h = match numeric_sign_internal(num2) {
                0 => return Err(division_by_zero_error().into()),
                1 => {
                    if pos {
                        crate::NUMERIC_PINF
                    } else {
                        crate::NUMERIC_NINF
                    }
                }
                _ => {
                    if pos {
                        crate::NUMERIC_NINF
                    } else {
                        crate::NUMERIC_PINF
                    }
                }
            };
            out.set_special(h);
            return Ok(());
        }
        // num1 finite / [-]Inf: no underflow in numeric, return zero.
        make_result_into(CONST_ZERO, out);
        return Ok(());
    }

    let arg1 = num1.view();
    let arg2 = num2.view();
    let rscale = select_div_scale(arg1, arg2);
    let mut result = NumericVar::new();
    div_var(arg1, arg2, &mut result, rscale, true, true)?;
    if !make_result_into(result.view(), out) {
        return Err(crate::numeric_overflow_error().into());
    }
    Ok(())
}

pub fn numeric_div_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_div_into(num1, num2, &mut out)?;
    Ok(out)
}

pub fn numeric_div_trunc_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    if num1.is_special() || num2.is_special() {
        if num1.is_nan() || num2.is_nan() {
            return Ok(NumericImage::nan());
        }
        if num1.is_pinf() {
            if num2.is_special() {
                return Ok(NumericImage::nan());
            }
            return match numeric_sign_internal(num2) {
                0 => Err(division_by_zero_error().into()),
                1 => Ok(NumericImage::pinf()),
                _ => Ok(NumericImage::ninf()),
            };
        }
        if num1.is_ninf() {
            if num2.is_special() {
                return Ok(NumericImage::nan());
            }
            return match numeric_sign_internal(num2) {
                0 => Err(division_by_zero_error().into()),
                1 => Ok(NumericImage::ninf()),
                _ => Ok(NumericImage::pinf()),
            };
        }
        return make_result(CONST_ZERO);
    }

    let mut result = NumericVar::new();
    div_var(num1.view(), num2.view(), &mut result, 0, false, true)?;
    make_result(result.view())
}

pub fn cmp_numerics(num1: Num<'_>, num2: Num<'_>) -> i32 {
    if num1.is_special() {
        if num1.is_nan() {
            if num2.is_nan() {
                0
            } else {
                1
            }
        } else if num1.is_pinf() {
            if num2.is_nan() {
                -1
            } else if num2.is_pinf() {
                0
            } else {
                1
            }
        } else if num2.is_ninf() {
            0
        } else {
            -1
        }
    } else if num2.is_special() {
        if num2.is_ninf() {
            1
        } else {
            -1
        }
    } else {
        cmp_var_common(
            num1.digits(),
            num1.ndigits(),
            num1.weight(),
            num1.sign(),
            num2.digits(),
            num2.ndigits(),
            num2.weight(),
            num2.sign(),
        )
    }
}

#[inline]
pub fn numeric_eq(num1: Num<'_>, num2: Num<'_>) -> bool {
    cmp_numerics(num1, num2) == 0
}

#[inline]
pub fn numeric_ne(num1: Num<'_>, num2: Num<'_>) -> bool {
    cmp_numerics(num1, num2) != 0
}

#[inline]
pub fn numeric_gt(num1: Num<'_>, num2: Num<'_>) -> bool {
    cmp_numerics(num1, num2) > 0
}

#[inline]
pub fn numeric_ge(num1: Num<'_>, num2: Num<'_>) -> bool {
    cmp_numerics(num1, num2) >= 0
}

#[inline]
pub fn numeric_lt(num1: Num<'_>, num2: Num<'_>) -> bool {
    cmp_numerics(num1, num2) < 0
}

#[inline]
pub fn numeric_le(num1: Num<'_>, num2: Num<'_>) -> bool {
    cmp_numerics(num1, num2) <= 0
}

/// The `numeric(num, typmod)` length coercion.
pub fn numeric_apply_typmod(num: Num<'_>, typmod: i32) -> PgResult<NumericImage> {
    if num.is_special() {
        apply_typmod_special(num, typmod, None)?;
        return Ok(NumericImage::from_num(num));
    }

    if !is_valid_numeric_typmod(typmod) {
        return Ok(NumericImage::from_num(num));
    }

    let precision = numeric_typmod_precision(typmod);
    let scale = numeric_typmod_scale(typmod);
    let maxdigits = precision - scale;
    let dscale = scale.max(0);

    // In-bounds and no rounding needed: copy and patch the dscale field,
    // unless a larger dscale forces abandoning the short header.
    let ddigits = (num.weight() + 1) * DEC_DIGITS;
    if ddigits <= maxdigits
        && scale >= num.dscale()
        && (numeric_can_be_short(dscale, num.weight()) || !num.is_short())
    {
        let mut img = NumericImage::from_num(num);
        let hdr_word = num.header();
        let new_hdr = if num.is_short() {
            (hdr_word & !NUMERIC_SHORT_DSCALE_MASK) | ((dscale as u16) << NUMERIC_SHORT_DSCALE_SHIFT)
        } else {
            num.sign() | (dscale as u16 & NUMERIC_DSCALE_MASK)
        };
        img.set_header_word(new_hdr);
        return Ok(img);
    }

    let mut var = NumericVar::from_view(num.view());
    apply_typmod(&mut var, typmod, None)?;
    make_result(var.view())
}

pub fn numeric_round_common(num: Num<'_>, scale: i32) -> PgResult<NumericImage> {
    if num.is_special() {
        return Ok(NumericImage::from_num(num));
    }

    let scale = scale
        .max(-(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS - 1)
        .min(NUMERIC_DSCALE_MAX);

    let mut arg = NumericVar::from_view(num.view());
    arg.round(scale);
    if scale < 0 {
        arg.dscale = 0;
    }
    make_result(arg.view())
}

pub fn numeric_trunc_common(num: Num<'_>, scale: i32) -> PgResult<NumericImage> {
    if num.is_special() {
        return Ok(NumericImage::from_num(num));
    }

    let scale = scale
        .max(-(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS)
        .min(NUMERIC_DSCALE_MAX);

    let mut arg = NumericVar::from_view(num.view());
    arg.trunc(scale);
    if scale < 0 {
        arg.dscale = 0;
    }
    make_result(arg.view())
}

/// C: numeric_ceil (ceil_var).
pub fn numeric_ceil(num: Num<'_>) -> PgResult<NumericImage> {
    if num.is_special() {
        return Ok(NumericImage::from_num(num));
    }
    let mut tmp = NumericVar::from_view(num.view());
    tmp.trunc(0);
    if num.sign() == NUMERIC_POS && cmp_var(num.view(), tmp.view()) != 0 {
        let mut res = NumericVar::default();
        add_var(tmp.view(), CONST_ONE, &mut res);
        return make_result(res.view());
    }
    make_result(tmp.view())
}

/// C: numeric_floor (floor_var).
pub fn numeric_floor(num: Num<'_>) -> PgResult<NumericImage> {
    if num.is_special() {
        return Ok(NumericImage::from_num(num));
    }
    let mut tmp = NumericVar::from_view(num.view());
    tmp.trunc(0);
    if num.sign() == NUMERIC_NEG && cmp_var(num.view(), tmp.view()) != 0 {
        let mut res = NumericVar::default();
        sub_var(tmp.view(), CONST_ONE, &mut res);
        return make_result(res.view());
    }
    make_result(tmp.view())
}

pub fn numeric_abs(num: Num<'_>) -> NumericImage {
    let mut res = NumericImage::from_num(num);
    let h = num.header();
    if num.is_short() {
        res.set_header_word(h & !NUMERIC_SHORT_SIGN_MASK);
    } else if num.is_special() {
        // -Inf becomes Inf; NaN unaffected.
        res.set_header_word(h & !NUMERIC_INF_SIGN_MASK);
    } else {
        res.set_header_word(NUMERIC_POS | num.dscale() as u16);
    }
    res
}

pub fn numeric_uminus(num: Num<'_>) -> NumericImage {
    let mut res = NumericImage::from_num(num);
    let h = num.header();
    if num.is_special() {
        if !num.is_nan() {
            res.set_header_word(h ^ NUMERIC_INF_SIGN_MASK);
        }
    } else if num.ndigits() != 0 {
        if num.is_short() {
            res.set_header_word(h ^ NUMERIC_SHORT_SIGN_MASK);
        } else if num.sign() == NUMERIC_POS {
            res.set_header_word(NUMERIC_NEG | num.dscale() as u16);
        } else {
            res.set_header_word(NUMERIC_POS | num.dscale() as u16);
        }
    }
    res
}

pub fn numeric_uplus(num: Num<'_>) -> NumericImage {
    NumericImage::from_num(num)
}

#[cold]
#[inline(never)]
fn cannot_convert_special(is_nan: bool, target: &str) -> PgError {
    PgError::error(format!(
        "cannot convert {} to {target}",
        if is_nan { "NaN" } else { "infinity" }
    ))
    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED)
}

#[cold]
#[inline(never)]
fn out_of_range(target: &str) -> PgError {
    PgError::error(format!("{target} out of range"))
        .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

pub fn int64_to_numeric(val: i64) -> NumericImage {
    let mut var = NumericVar::new();
    set_var_from_int64(val, &mut var);
    make_result_opt_error(var.view()).expect("int64 always fits numeric")
}

pub fn int4_numeric(val: i32) -> NumericImage {
    int64_to_numeric(val as i64)
}

pub fn int8_numeric(val: i64) -> NumericImage {
    int64_to_numeric(val)
}

pub fn int2_numeric(val: i16) -> NumericImage {
    int64_to_numeric(val as i64)
}

pub fn numeric_int4(num: Num<'_>) -> PgResult<i32> {
    if num.is_special() {
        return Err(cannot_convert_special(num.is_nan(), "integer").into());
    }
    var_to_int32(num.view()).ok_or_else(|| out_of_range("integer").into())
}

pub fn numeric_int8(num: Num<'_>) -> PgResult<i64> {
    if num.is_special() {
        return Err(cannot_convert_special(num.is_nan(), "bigint").into());
    }
    var_to_int64(num.view()).ok_or_else(|| out_of_range("bigint").into())
}

pub fn numeric_int2(num: Num<'_>) -> PgResult<i16> {
    if num.is_special() {
        return Err(cannot_convert_special(num.is_nan(), "smallint").into());
    }
    let val = var_to_int64(num.view()).ok_or_else(|| out_of_range("smallint"))?;
    if val < i16::MIN as i64 || val > i16::MAX as i64 {
        return Err(out_of_range("smallint").into());
    }
    Ok(val as i16)
}

// %.*g significant-digit formatting (C's snprintf("%.*g", prec, val) for
// finite nonzero values): Rust's {:.*e} rounds correctly to prec digits.
fn format_g(val: f64, prec: usize, out: &mut Vec<u8>) {
    debug_assert!(val.is_finite());
    if val == 0.0 {
        out.push(b'0');
        return;
    }
    let sci = format!("{:.*e}", prec - 1, val);
    let (mant, exp) = sci.split_once('e').expect("{:e} always emits an exponent");
    let exp: i32 = exp.parse().expect("{:e} exponent is an integer");
    let mut digits: Vec<u8> = mant.bytes().filter(|b| b.is_ascii_digit()).collect();
    let neg = mant.starts_with('-');
    while digits.len() > 1 && digits[digits.len() - 1] == b'0' {
        digits.pop();
    }

    if neg {
        out.push(b'-');
    }
    if exp < -4 || exp >= prec as i32 {
        out.push(digits[0]);
        if digits.len() > 1 {
            out.push(b'.');
            out.extend_from_slice(&digits[1..]);
        }
        out.push(b'e');
        out.push(if exp < 0 { b'-' } else { b'+' });
        let e = exp.unsigned_abs();
        if e < 10 {
            out.push(b'0');
        }
        out.extend_from_slice(e.to_string().as_bytes());
    } else if exp < 0 {
        out.extend_from_slice(b"0.");
        for _ in 0..(-exp - 1) {
            out.push(b'0');
        }
        out.extend_from_slice(&digits);
    } else {
        let ip = (exp as usize) + 1;
        if digits.len() <= ip {
            out.extend_from_slice(&digits);
            for _ in 0..ip - digits.len() {
                out.push(b'0');
            }
        } else {
            out.extend_from_slice(&digits[..ip]);
            out.push(b'.');
            out.extend_from_slice(&digits[ip..]);
        }
    }
}

fn float_to_numeric(val: f64, prec: usize) -> PgResult<NumericImage> {
    if val.is_nan() {
        return Ok(NumericImage::nan());
    }
    if val.is_infinite() {
        return Ok(if val < 0.0 {
            NumericImage::ninf()
        } else {
            NumericImage::pinf()
        });
    }

    let mut buf = Vec::with_capacity(32);
    format_g(val, prec, &mut buf);
    let s = core::str::from_utf8(&buf).expect("format_g emits ASCII");
    match crate::io::numeric_in(s, -1, None)? {
        Some(img) => Ok(img),
        None => unreachable!("numeric_in on %g output cannot soft-fail"),
    }
}

pub fn float8_numeric(val: f64) -> PgResult<NumericImage> {
    float_to_numeric(val, 15)
}

pub fn float4_numeric(val: f32) -> PgResult<NumericImage> {
    float_to_numeric(val as f64, 6)
}

pub fn numeric_float8_no_overflow(num: Num<'_>) -> f64 {
    if num.is_special() {
        return if num.is_pinf() {
            f64::INFINITY
        } else if num.is_ninf() {
            f64::NEG_INFINITY
        } else {
            f64::NAN
        };
    }
    let mut buf = Vec::with_capacity(32);
    get_str_from_var(num.view(), &mut buf);
    let s = core::str::from_utf8(&buf).expect("numeric text is ASCII");
    // C strtod ignoring ERANGE: Rust parse saturates to +/-inf the same way.
    s.parse::<f64>().expect("numeric text parses as f64")
}

// Stats arrays hand out packed elements at arbitrary byte offsets; a 1-byte
// varlena header puts digits at odd addresses, so realign by copy before
// viewing (C: DatumGetNumeric's unpacking copy).
pub fn numeric_float8_no_overflow_any(payload: &[u8]) -> f64 {
    let num = Num::from_payload(payload);
    if num.is_special() || (payload.as_ptr() as usize + num.header_size()) % 2 == 0 {
        return numeric_float8_no_overflow(num);
    }
    let mut buf = vec![0u16; payload.len().div_ceil(2)];
    // SAFETY: the u16 buffer reinterpreted as bytes, sized to cover payload.
    let dst = unsafe {
        core::slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<u8>(), payload.len())
    };
    dst.copy_from_slice(payload);
    numeric_float8_no_overflow(Num::from_payload(dst))
}

pub fn numeric_float8(num: Num<'_>) -> PgResult<f64> {
    if num.is_special() {
        return Ok(if num.is_pinf() {
            f64::INFINITY
        } else if num.is_ninf() {
            f64::NEG_INFINITY
        } else {
            f64::NAN
        });
    }
    let mut buf = Vec::with_capacity(32);
    get_str_from_var(num.view(), &mut buf);
    let s = core::str::from_utf8(&buf).expect("numeric text is ASCII");
    adt_float::float8in(s, None)
}

pub fn numeric_float4(num: Num<'_>) -> PgResult<f32> {
    if num.is_special() {
        return Ok(if num.is_pinf() {
            f32::INFINITY
        } else if num.is_ninf() {
            f32::NEG_INFINITY
        } else {
            f32::NAN
        });
    }
    let mut buf = Vec::with_capacity(32);
    get_str_from_var(num.view(), &mut buf);
    let s = core::str::from_utf8(&buf).expect("numeric text is ASCII");
    adt_float::float4in(s, None)
}

pub fn numeric_avg_div(sum: Num<'_>, count: i64) -> PgResult<NumericImage> {
    let count_img = int64_to_numeric(count);
    numeric_div_common(sum, count_img.num())
}

/// `avg(int2/int4)` finalize core (`sum / count` over the int8 transarray):
/// `numeric_avg_div`'s exact result — same `select_div_scale`, same
/// round-half-away division, byte-identical image (differential tests pin
/// it against the materializing composition) — minus the two intermediate
/// `NumericImage` materializations (`int64_to_numeric` of sum and count)
/// AND (in the common range) the digit-loop `div_var` machinery: the
/// quotient computes as one i128 division. The hashed-agg emit's finalize
/// hot path (high-cardinality-group class: `avg(int)` over millions of groups).
pub fn int64_avg_div(sum: i64, count: i64) -> PgResult<NumericImage> {
    let mut v1 = NumericVar::new();
    set_var_from_int64(sum, &mut v1);
    int_var_avg_div(&v1, sum as i128, count)
}

/// `avg(int8)` finalize core (Int128AggState sum): as [`int64_avg_div`].
pub fn int128_avg_div(sum: i128, count: i64) -> PgResult<NumericImage> {
    let mut v1 = NumericVar::new();
    crate::var::int128_to_var(sum, &mut v1);
    int_var_avg_div(&v1, sum, count)
}

/// The shared division tail: `select_div_scale` on stack vars (C's exact
/// result-scale rule), then the integer fast quotient when `sum × 10^rscale`
/// fits i128, else `numeric_div_into`'s finite arm verbatim (both operands
/// are integers — the special-value arms are unreachable).
fn int_var_avg_div(v1: &NumericVar, sum: i128, count: i64) -> PgResult<NumericImage> {
    let mut v2 = NumericVar::new();
    set_var_from_int64(count, &mut v2);
    let rscale = select_div_scale(v1.view(), v2.view());
    if count != 0 {
        if let Some(r) = int_quotient_image(sum, count, rscale) {
            return r;
        }
    }
    let mut result = NumericVar::new();
    div_var(v1.view(), v2.view(), &mut result, rscale, true, true)?;
    let mut out = NumericImage::empty();
    if !make_result_into(result.view(), &mut out) {
        return Err(crate::numeric_overflow_error().into());
    }
    Ok(out)
}

/// Integer division producing `div_var`'s exact image: the round-half-away
/// (C `round_var`) quotient `q = sum × 10^rscale / count` computes in i128,
/// and the image constructs the `int64_div_fast_to_numeric` way (pad the
/// mantissa into base-10000 alignment, shift the weight, display at rscale)
/// followed by `strip()` — the digit canonicalization `div_var`'s integer
/// tails apply, so the stored digit form is identical. `None` = the scaled
/// numerator leaves i128 (or an extreme rscale): the caller falls back to
/// the digit-loop division, byte-identically.
fn int_quotient_image(sum: i128, count: i64, rscale: i32) -> Option<PgResult<NumericImage>> {
    debug_assert!(count != 0);
    if !(0..=37).contains(&rscale) {
        return None;
    }
    let scaled = sum.checked_mul(10i128.checked_pow(rscale as u32)?)?;
    let den = count as i128;
    let mut q = scaled / den;
    // Round half away from zero (C round_var's carry rule on the quotient).
    if (scaled % den).unsigned_abs() * 2 >= den.unsigned_abs() {
        q = q.checked_add(if (sum < 0) == (count < 0) { 1 } else { -1 })?;
    }
    // value = q × 10^-rscale displayed at rscale.
    let mut w = rscale / DEC_DIGITS;
    let m = rscale % DEC_DIGITS;
    let qv = if m > 0 {
        w += 1;
        q.checked_mul(10i128.pow((DEC_DIGITS - m) as u32))?
    } else {
        q
    };
    let mut var = NumericVar::new();
    crate::var::int128_to_var(qv, &mut var);
    var.weight -= w;
    var.dscale = rscale;
    var.strip();
    let mut out = NumericImage::empty();
    Some(if make_result_into(var.view(), &mut out) {
        Ok(out)
    } else {
        Err(crate::numeric_overflow_error().into())
    })
}

macro_rules! unported {
    ($($name:ident),* $(,)?) => {$(
        pub fn $name() -> ! {
            panic!(concat!("adt_numeric: ", stringify!($name), " not ported (deferred)"))
        }
    )*};
}

// Deferred loud (M3+ / other lanes): wire format, hash, in_range/series.
// numeric_sortsupport itself lives in sortsupport.rs, dispatched by proc oid
// in tuplesort::ssup (never through fmgr); this loud guards direct fmgr calls.
unported! {
    numeric_recv_unported,
    numeric_send_unported,
    numeric_sortsupport_unported,
    in_range_numeric_unported,
    generate_series_numeric_unported,
}

pub fn numeric_is_nan(num: Num<'_>) -> bool {
    num.is_nan()
}

pub fn numeric_is_inf(num: Num<'_>) -> bool {
    num.is_inf()
}

pub fn numeric_maximum_size(typmod: i32) -> i32 {
    if !is_valid_numeric_typmod(typmod) {
        return -1;
    }
    let precision = numeric_typmod_precision(typmod);
    let numeric_digits = (precision + 2 * (DEC_DIGITS - 1)) / DEC_DIGITS;
    crate::NUMERIC_HDRSZ as i32 + numeric_digits * 2
}

pub fn int64_div_fast_to_numeric(val1: i64, log10val2: i32) -> PgResult<NumericImage> {
    let rscale = if log10val2 < 0 { 0 } else { log10val2 };

    let mut w = log10val2 / DEC_DIGITS;
    let mut m = log10val2 % DEC_DIGITS;
    if m < 0 {
        m += DEC_DIGITS;
        w -= 1;
    }

    let mut result;
    if m > 0 {
        const POW10: [i64; 4] = [1, 10, 100, 1000];
        let factor = POW10[(DEC_DIGITS - m) as usize];
        match val1.checked_mul(factor) {
            Some(new_val1) => {
                result = int64_to_var(new_val1);
            }
            None => {
                let tmp = val1 as i128 * factor as i128;
                result = NumericVar::new();
                crate::var::int128_to_var(tmp, &mut result);
            }
        }
        w += 1;
    } else {
        result = int64_to_var(val1);
    }

    result.weight -= w;
    result.dscale = rscale;

    make_result(result.view())
}

fn get_min_scale(var: &NumericVar) -> i32 {
    let digits = var.digits();
    let mut last = var.ndigits - 1;
    while last >= 0 && digits[last as usize] == 0 {
        last -= 1;
    }
    if last < 0 {
        return 0;
    }
    let mut min_scale = (last - var.weight) * DEC_DIGITS;
    if min_scale > 0 {
        let mut last_digit = digits[last as usize];
        while last_digit % 10 == 0 {
            min_scale -= 1;
            last_digit /= 10;
        }
    } else {
        min_scale = 0;
    }
    min_scale
}

pub fn numeric_min_scale(num: Num<'_>) -> i32 {
    let var = NumericVar::from_view(num.view());
    get_min_scale(&var)
}

pub fn numeric_trim_scale(num: Num<'_>) -> PgResult<NumericImage> {
    if num.is_special() {
        return Ok(NumericImage::from_num(num));
    }
    let mut result = NumericVar::from_view(num.view());
    result.dscale = get_min_scale(&result);
    make_result(result.view())
}

// hash_numeric hashes the live digit span only (leading/trailing zeros and
// dscale excluded: equal numerics with different scales must hash equal).
fn live_digit_span(num: Num<'_>) -> Option<(&[NumericDigit], i32)> {
    let digits = num.digits();
    let mut weight = num.weight();
    let mut start = 0usize;
    while start < digits.len() && digits[start] == 0 {
        start += 1;
        weight -= 1;
    }
    if start == digits.len() {
        return None;
    }
    let mut end = digits.len();
    while digits[end - 1] == 0 {
        end -= 1;
    }
    Some((&digits[start..end], weight))
}

#[inline]
fn digit_bytes(d: &[NumericDigit]) -> &[u8] {
    // SAFETY: i16 -> u8 reinterpret of a live slice; alignment only loosens.
    unsafe { core::slice::from_raw_parts(d.as_ptr().cast::<u8>(), d.len() * 2) }
}

pub fn hash_numeric(key: Num<'_>) -> u32 {
    if key.is_special() {
        return 0;
    }
    match live_digit_span(key) {
        None => u32::MAX,
        Some((live, weight)) => ::hashfn::hash_bytes(digit_bytes(live)) ^ (weight as u32),
    }
}

pub fn hash_numeric_extended(key: Num<'_>, seed: u64) -> u64 {
    if key.is_special() {
        return seed;
    }
    match live_digit_span(key) {
        None => seed.wrapping_sub(1),
        Some((live, weight)) => {
            ::hashfn::hash_bytes_extended(digit_bytes(live), seed) ^ (weight as i64 as u64)
        }
    }
}

pub fn in_range_numeric_numeric(
    val: Num<'_>,
    base: Num<'_>,
    offset: Num<'_>,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    if offset.is_nan() || offset.is_ninf() || offset.sign() == NUMERIC_NEG {
        return Err(PgError::error(
            "invalid preceding or following size in window function",
        )
        .with_sqlstate(::types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
        .into());
    }
    // NaN sorts after non-NaN (cf cmp_numerics); the offset cannot change that.
    let result = if val.is_nan() {
        if base.is_nan() {
            true
        } else {
            !less
        }
    } else if base.is_nan() {
        less
    } else if offset.is_special() {
        debug_assert!(offset.is_pinf());
        if if sub { base.is_pinf() } else { base.is_ninf() } {
            // base +/- offset would be NaN: true for any val, per C.
            true
        } else if sub {
            if less {
                val.is_ninf()
            } else {
                true
            }
        } else if less {
            true
        } else {
            val.is_pinf()
        }
    } else if val.is_special() {
        if val.is_pinf() {
            if base.is_pinf() {
                true
            } else {
                !less
            }
        } else if base.is_ninf() {
            true
        } else {
            less
        }
    } else if base.is_special() {
        if base.is_ninf() {
            !less
        } else {
            less
        }
    } else {
        let mut sum = NumericVar::new();
        if sub {
            sub_var(base.view(), offset.view(), &mut sum);
        } else {
            add_var(base.view(), offset.view(), &mut sum);
        }
        if less {
            cmp_var(val.view(), sum.view()) <= 0
        } else {
            cmp_var(val.view(), sum.view()) >= 0
        }
    };
    Ok(result)
}

pub fn numeric_sign(num: Num<'_>) -> PgResult<NumericImage> {
    if num.is_nan() {
        return Ok(NumericImage::nan());
    }
    match numeric_sign_internal(num) {
        0 => make_result(CONST_ZERO),
        1 => make_result(CONST_ONE),
        _ => make_result(crate::var::CONST_MINUS_ONE),
    }
}

pub fn numeric_inc(num: Num<'_>) -> PgResult<NumericImage> {
    if num.is_special() {
        return Ok(NumericImage::from_num(num));
    }
    let mut result = NumericVar::new();
    add_var(num.view(), CONST_ONE, &mut result);
    make_result(result.view())
}
