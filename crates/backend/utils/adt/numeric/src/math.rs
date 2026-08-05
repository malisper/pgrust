//! Advanced math: sqrt/exp/ln/log/power kernels, mod/gcd family, factorial,
//! scientific-notation output, width_bucket.

use core::mem::swap;

use types_error::{
    PgError, PgResult, ERRCODE_INVALID_ARGUMENT_FOR_LOG,
    ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION,
    ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::arith::{add_var, cmp_abs, cmp_var, div_var, div_var_int, mul_var, sub_var};
use crate::io::get_str_from_var;
use crate::ops::{cmp_numerics, numeric_sign_internal};
use crate::var::{
    int128_to_var, int64_to_var, make_result, var_to_int32, var_to_int64, NumericImage,
    NumericVar, VarView, CONST_MINUS_ONE, CONST_ONE, CONST_ONE_POINT_ONE, CONST_TWO, CONST_ZERO,
    CONST_ZERO_POINT_NINE,
};
use crate::{
    division_by_zero_error, numeric_overflow_error, Num, DEC_DIGITS, NBASE,
    NUMERIC_MAX_DISPLAY_SCALE, NUMERIC_MAX_RESULT_SCALE, NUMERIC_MIN_DISPLAY_SCALE,
    NUMERIC_MIN_SIG_DIGITS, NUMERIC_NEG, NUMERIC_POS, NUMERIC_WEIGHT_MAX,
};

fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        panic!("CHECK_FOR_INTERRUPTS: ProcessInterrupts (tcop/postgres.c) unported");
    }
}

#[cold]
#[inline(never)]
fn sqrt_negative_error() -> PgError {
    PgError::error("cannot take square root of a negative number")
        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
}

#[cold]
#[inline(never)]
fn log_of_zero_error() -> PgError {
    PgError::error("cannot take logarithm of zero").with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
}

#[cold]
#[inline(never)]
fn log_of_negative_error() -> PgError {
    PgError::error("cannot take logarithm of a negative number")
        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
}

#[cold]
#[inline(never)]
fn zero_to_negative_power_error() -> PgError {
    PgError::error("zero raised to a negative power is undefined")
        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
}

#[cold]
#[inline(never)]
fn complex_power_error() -> PgError {
    PgError::error("a negative number raised to a non-integer power yields a complex result")
        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
}

#[cold]
#[inline(never)]
fn width_bucket_error(msg: &'static str) -> PgError {
    PgError::error(msg).with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION)
}

#[cold]
#[inline(never)]
fn integer_out_of_range_error() -> PgError {
    PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn factorial_negative_error() -> PgError {
    PgError::error("factorial of a negative number is undefined")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

// C's numericvar_to_double_no_overflow (strtod ignoring ERANGE; Rust parse
// yields inf/0 at the same thresholds). Retained TLS text scratch (rule 7).
pub(crate) fn var_to_f64(var: VarView<'_>) -> f64 {
    std::thread_local! {
        static F64_TEXT: core::cell::RefCell<Vec<u8>> =
            const { core::cell::RefCell::new(Vec::new()) };
    }
    F64_TEXT.with(|b| {
        let mut buf = b.borrow_mut();
        buf.clear();
        get_str_from_var(var, &mut buf);
        core::str::from_utf8(&buf)
            .expect("numeric text is ASCII")
            .parse()
            .expect("numeric text parses as f64")
    })
}

// C writes kernel results onto their own operands (mul_var(&x, &y, &x)) by
// reassigning the digit buffer. NumericVar's inline buffer makes a swap a
// ~384-byte move, so in-place update loops ping-pong two buffers by role
// instead — no digit or struct copies per iteration.
struct Flip {
    a: NumericVar,
    b: NumericVar,
    in_b: bool,
}

impl Flip {
    fn new(v: NumericVar) -> Flip {
        Flip {
            a: v,
            b: NumericVar::new(),
            in_b: false,
        }
    }

    #[inline]
    fn cur(&self) -> &NumericVar {
        if self.in_b {
            &self.b
        } else {
            &self.a
        }
    }

    // (current value, spare destination); caller flips after writing dst.
    #[inline]
    fn parts(&mut self) -> (VarView<'_>, &mut NumericVar) {
        if self.in_b {
            (self.b.view(), &mut self.a)
        } else {
            (self.a.view(), &mut self.b)
        }
    }

    #[inline]
    fn flip(&mut self) {
        self.in_b = !self.in_b;
    }

    fn take(self) -> NumericVar {
        if self.in_b {
            self.b
        } else {
            self.a
        }
    }
}

pub fn mod_var(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar) -> PgResult<()> {
    // mod(x,y) = x - trunc(x/y)*y
    let mut tmp = NumericVar::new();
    div_var(var1, var2, &mut tmp, 0, false, true)?;
    let mut prod = NumericVar::new();
    mul_var(var2, tmp.view(), &mut prod, var2.dscale);
    sub_var(var1, prod.view(), result);
    Ok(())
}

// Truncated integer quotient + remainder; remainder precise to var2's dscale.
pub fn div_mod_var(
    var1: VarView<'_>,
    var2: VarView<'_>,
    quot: &mut NumericVar,
    rem: &mut NumericVar,
) -> PgResult<()> {
    let mut q = NumericVar::new();
    let mut r = NumericVar::new();
    let mut tmp = NumericVar::new();

    // exact = false: initial estimate, corrected below.
    div_var(var1, var2, &mut q, 0, false, false)?;

    mul_var(var2, q.view(), &mut r, var2.dscale);
    sub_var(var1, r.view(), &mut tmp);
    swap(&mut r, &mut tmp);

    while r.ndigits != 0 && r.sign != var1.sign {
        if var1.sign == var2.sign {
            sub_var(q.view(), CONST_ONE, &mut tmp);
            swap(&mut q, &mut tmp);
            add_var(r.view(), var2, &mut tmp);
            swap(&mut r, &mut tmp);
        } else {
            add_var(q.view(), CONST_ONE, &mut tmp);
            swap(&mut q, &mut tmp);
            sub_var(r.view(), var2, &mut tmp);
            swap(&mut r, &mut tmp);
        }
    }

    while cmp_abs(r.view(), var2) >= 0 {
        if var1.sign == var2.sign {
            add_var(q.view(), CONST_ONE, &mut tmp);
            swap(&mut q, &mut tmp);
            sub_var(r.view(), var2, &mut tmp);
            swap(&mut r, &mut tmp);
        } else {
            sub_var(q.view(), CONST_ONE, &mut tmp);
            swap(&mut q, &mut tmp);
            add_var(r.view(), var2, &mut tmp);
            swap(&mut r, &mut tmp);
        }
    }

    swap(quot, &mut q);
    swap(rem, &mut r);
    Ok(())
}

pub fn gcd_var(var1: VarView<'_>, var2: VarView<'_>, result: &mut NumericVar) -> PgResult<()> {
    let res_dscale = var1.dscale.max(var2.dscale);

    // var1 = the greater absolute value (saves one modulo).
    let cmp = cmp_abs(var1, var2);
    let (var1, var2) = if cmp < 0 { (var2, var1) } else { (var1, var2) };

    if cmp == 0 || var2.ndigits == 0 {
        result.set_from_view(var1);
        result.sign = NUMERIC_POS;
        result.dscale = res_dscale;
        return Ok(());
    }

    let mut tmp_arg = NumericVar::from_view(var1);
    result.set_from_view(var2);
    let mut modv = NumericVar::new();

    loop {
        check_for_interrupts();

        mod_var(tmp_arg.view(), result.view(), &mut modv)?;
        if modv.ndigits == 0 {
            break;
        }
        swap(&mut tmp_arg, result);
        swap(result, &mut modv);
    }
    result.sign = NUMERIC_POS;
    result.dscale = res_dscale;
    Ok(())
}

fn extract_root_half(arg: VarView<'_>, src_idx: i32, blen: i32, dst: &mut NumericVar) {
    if src_idx < arg.ndigits {
        let tmp_len = blen.min(arg.ndigits - src_idx);
        dst.alloc(tmp_len);
        dst.digits_mut()
            .copy_from_slice(&arg.digits[src_idx as usize..(src_idx + tmp_len) as usize]);
        dst.weight = blen - 1;
        dst.sign = NUMERIC_POS;
        dst.dscale = 0;
        dst.strip();
    } else {
        dst.set_zero();
        dst.dscale = 0;
    }
}

// Karatsuba square root (Zimmermann, INRIA RR-3805); rscale < 0 allowed
// (rounding before the decimal point).
pub fn sqrt_var(arg: VarView<'_>, result: &mut NumericVar, rscale: i32) -> PgResult<()> {
    let stat = cmp_var(arg, CONST_ZERO);
    if stat == 0 {
        result.set_zero();
        result.dscale = rscale;
        return Ok(());
    }
    // SQL2003 defines sqrt() in terms of power: 2201F on negative input.
    if stat < 0 {
        return Err(sqrt_negative_error().into());
    }

    // res_weight = floor(arg->weight / 2)
    let res_weight = if arg.weight >= 0 {
        arg.weight / 2
    } else {
        -((-arg.weight - 1) / 2 + 1)
    };

    // At least 1 extra decimal digit for correct rounding; at least 1 NBASE
    // digit always.
    let mut res_ndigits = if rscale + 1 >= 0 {
        res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS
    } else {
        res_weight + 1 - (-rscale - 1) / DEC_DIGITS
    };
    res_ndigits = res_ndigits.max(1);

    let mut src_ndigits = arg.weight + 1 + (res_ndigits - res_weight - 1) * 2;
    src_ndigits = src_ndigits.max(1);

    // ndigits[] = input digits consumed at the end of each iteration, stored
    // outermost-first; each step roughly halves. Bounded by 32 steps (i32
    // digit counts).
    let mut ndigits = [0i32; 32];
    let mut step = 0usize;
    loop {
        ndigits[step] = src_ndigits;
        if src_ndigits <= 4 {
            break;
        }
        // Choose b = NBASE^blen so that a3 >= b/4.
        let mut blen = src_ndigits / 4;
        if blen * 4 == src_ndigits && (arg.digits[0] as i32) < NBASE / 4 {
            blen -= 1;
        }
        src_ndigits -= 2 * blen;
        step += 1;
    }

    // Innermost square root: input fits in an int64; f64 sqrt estimate plus
    // Newton correction.
    let mut arg_int64 = arg.digits[0] as i64;
    let mut src_idx = 1i32;
    while src_idx < src_ndigits {
        arg_int64 *= NBASE as i64;
        if src_idx < arg.ndigits {
            arg_int64 += arg.digits[src_idx as usize] as i64;
        }
        src_idx += 1;
    }

    let mut s_int64 = (arg_int64 as f64).sqrt() as i64;
    let mut r_int64 = arg_int64 - s_int64 * s_int64;

    while r_int64 < 0 || r_int64 > 2 * s_int64 {
        s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
        r_int64 = arg_int64 - s_int64 * s_int64;
    }

    // Iterations with src_ndigits <= 8: result still fits in an int64.
    let mut istep = step as i32 - 1;
    while istep >= 0 {
        src_ndigits = ndigits[istep as usize];
        if src_ndigits > 8 {
            break;
        }
        let blen = (src_ndigits - src_idx) / 2;

        let mut a0: i32 = 0;
        let mut a1: i32 = 0;
        let mut b: i32 = 1;

        for _ in 0..blen {
            b *= NBASE;
            a1 *= NBASE;
            if src_idx < arg.ndigits {
                a1 += arg.digits[src_idx as usize] as i32;
            }
            src_idx += 1;
        }
        for _ in 0..blen {
            a0 *= NBASE;
            if src_idx < arg.ndigits {
                a0 += arg.digits[src_idx as usize] as i32;
            }
            src_idx += 1;
        }

        let numer = r_int64 * b as i64 + a1 as i64;
        let denom = 2 * s_int64;
        let q = numer / denom;
        let u = numer - q * denom;

        s_int64 = s_int64 * b as i64 + q;
        r_int64 = u * b as i64 + a0 as i64 - q * q;

        if r_int64 < 0 {
            r_int64 += s_int64;
            s_int64 -= 1;
            r_int64 += s_int64;
        }

        debug_assert!(src_idx == src_ndigits);
        istep -= 1;
    }

    let mut s_var = NumericVar::new();
    let mut r_var = NumericVar::new();

    if istep >= 0 {
        // Iterations with src_ndigits <= 16: int128 stage (HAVE_INT128).
        let mut s_int128 = s_int64 as i128;
        let mut r_int128 = r_int64 as i128;

        while istep >= 0 {
            src_ndigits = ndigits[istep as usize];
            if src_ndigits > 16 {
                break;
            }
            let blen = ((src_ndigits - src_idx) / 2) as i64;

            let mut a0: i64 = 0;
            let mut a1: i64 = 0;
            let mut b: i64 = 1;

            for _ in 0..blen {
                b *= NBASE as i64;
                a1 *= NBASE as i64;
                if src_idx < arg.ndigits {
                    a1 += arg.digits[src_idx as usize] as i64;
                }
                src_idx += 1;
            }
            for _ in 0..blen {
                a0 *= NBASE as i64;
                if src_idx < arg.ndigits {
                    a0 += arg.digits[src_idx as usize] as i64;
                }
                src_idx += 1;
            }

            let numer = r_int128 * b as i128 + a1 as i128;
            let denom = 2 * s_int128;
            let q = numer / denom;
            let u = numer - q * denom;

            s_int128 = s_int128 * b as i128 + q;
            r_int128 = u * b as i128 + a0 as i128 - q * q;

            if r_int128 < 0 {
                r_int128 += s_int128;
                s_int128 -= 1;
                r_int128 += s_int128;
            }

            debug_assert!(src_idx == src_ndigits);
            istep -= 1;
        }

        int128_to_var(s_int128, &mut s_var);
        if istep >= 0 {
            int128_to_var(r_int128, &mut r_var);
        }
    } else {
        s_var = int64_to_var(s_int64);
    }

    // Remaining iterations use numeric variables.
    let mut a0_var = NumericVar::new();
    let mut a1_var = NumericVar::new();
    let mut q_var = NumericVar::new();
    let mut u_var = NumericVar::new();
    let mut tmp = NumericVar::new();

    while istep >= 0 {
        src_ndigits = ndigits[istep as usize];
        let blen = (src_ndigits - src_idx) / 2;

        extract_root_half(arg, src_idx, blen, &mut a1_var);
        src_idx += blen;
        extract_root_half(arg, src_idx, blen, &mut a0_var);
        src_idx += blen;

        // (q,u) = DivRem(r*b + a1, 2*s)
        q_var.set_from_view(r_var.view());
        q_var.weight += blen;
        add_var(q_var.view(), a1_var.view(), &mut tmp);
        swap(&mut q_var, &mut tmp);
        add_var(s_var.view(), s_var.view(), &mut u_var);
        let mut new_q = NumericVar::new();
        let mut new_u = NumericVar::new();
        div_mod_var(q_var.view(), u_var.view(), &mut new_q, &mut new_u)?;
        swap(&mut q_var, &mut new_q);
        swap(&mut u_var, &mut new_u);

        // s = s*b + q
        s_var.weight += blen;
        add_var(s_var.view(), q_var.view(), &mut tmp);
        swap(&mut s_var, &mut tmp);

        // r = u*b + a0 - q^2; final iteration only needs its sign.
        u_var.weight += blen;
        add_var(u_var.view(), a0_var.view(), &mut tmp);
        swap(&mut u_var, &mut tmp);
        mul_var(q_var.view(), q_var.view(), &mut tmp, 0);
        swap(&mut q_var, &mut tmp);

        if istep > 0 {
            sub_var(u_var.view(), q_var.view(), &mut r_var);
            if r_var.sign == NUMERIC_NEG {
                // s is too large by 1; r += s, s--, r += s
                add_var(r_var.view(), s_var.view(), &mut tmp);
                swap(&mut r_var, &mut tmp);
                sub_var(s_var.view(), CONST_ONE, &mut tmp);
                swap(&mut s_var, &mut tmp);
                add_var(r_var.view(), s_var.view(), &mut tmp);
                swap(&mut r_var, &mut tmp);
            }
        } else if cmp_var(u_var.view(), q_var.view()) < 0 {
            sub_var(s_var.view(), CONST_ONE, &mut tmp);
            swap(&mut s_var, &mut tmp);
        }

        debug_assert!(src_idx == src_ndigits);
        istep -= 1;
    }

    result.set_from_view(s_var.view());
    result.weight = res_weight;
    result.sign = NUMERIC_POS;
    result.round(rscale);
    result.strip();
    Ok(())
}

// e^x to rscale fractional digits.
pub fn exp_var(arg: VarView<'_>, result: &mut NumericVar, rscale: i32) -> PgResult<()> {
    let mut x = NumericVar::from_view(arg);
    let elem = NumericVar::new();
    let mut tmp = NumericVar::new();

    let val = var_to_f64(x.view());

    // Overflow guard; power_var()'s limit must match.
    if val.abs() >= (NUMERIC_MAX_RESULT_SCALE * 3) as f64 {
        if val > 0.0 {
            return Err(numeric_overflow_error().into());
        }
        result.set_zero();
        result.dscale = rscale;
        return Ok(());
    }

    // decimal weight = x * log10(e)
    let dweight = (val * 0.434294481903252) as i32;

    // Reduce x to (approximately) -0.01 <= x <= 0.01 by dividing by 2^ndiv2;
    // the guard above keeps ndiv2 <= 20.
    let mut ndiv2 = 0i32;
    if val.abs() > 0.01 {
        let mut val = val / 2.0;
        ndiv2 = 1;
        while val.abs() > 0.01 {
            ndiv2 += 1;
            val /= 2.0;
        }

        let local_rscale = x.dscale + ndiv2;
        div_var_int(x.view(), 1 << ndiv2, 0, &mut tmp, local_rscale, true)?;
        swap(&mut x, &mut tmp);
    }

    // Taylor scale: (dweight + rscale + 1) significant digits, plus
    // ~log10(2^ndiv2) for the squarings below, plus slop.
    let mut sig_digits = 1 + dweight + rscale + (ndiv2 as f64 * 0.301029995663981) as i32;
    sig_digits = sig_digits.max(0) + 8;

    let mut local_rscale = sig_digits - 1;

    // exp(x) = 1 + x + x^2/2! + x^3/3! + ...
    add_var(CONST_ONE, x.view(), result);
    let mut res = Flip::new(core::mem::take(result));
    let mut elem = Flip::new(elem);

    {
        let (_, dst) = elem.parts();
        mul_var(x.view(), x.view(), dst, local_rscale);
        elem.flip();
    }
    let mut ni = 2;
    {
        let (src, dst) = elem.parts();
        div_var_int(src, ni, 0, dst, local_rscale, true)?;
        elem.flip();
    }

    while elem.cur().ndigits != 0 {
        let (src, dst) = res.parts();
        add_var(src, elem.cur().view(), dst);
        res.flip();

        {
            let (src, dst) = elem.parts();
            mul_var(src, x.view(), dst, local_rscale);
            elem.flip();
        }
        ni += 1;
        let (src, dst) = elem.parts();
        div_var_int(src, ni, 0, dst, local_rscale, true)?;
        elem.flip();
    }

    // Undo the range reduction; the weight doubles with each squaring, so the
    // local rscale can shrink as we go.
    while ndiv2 > 0 {
        ndiv2 -= 1;
        local_rscale = sig_digits - res.cur().weight * 2 * DEC_DIGITS;
        local_rscale = local_rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
        let (src, dst) = res.parts();
        mul_var(src, src, dst, local_rscale);
        res.flip();
    }

    *result = res.take();
    result.round(rscale);
    Ok(())
}

// Estimate log10(abs(ln(var))); robust against invalid ln() inputs (returns 0).
pub fn estimate_ln_dweight(var: VarView<'_>) -> i32 {
    if var.sign != NUMERIC_POS {
        return 0;
    }

    if cmp_var(var, CONST_ZERO_POINT_NINE) >= 0 && cmp_var(var, CONST_ONE_POINT_ONE) <= 0 {
        // 0.9 <= var <= 1.1: estimate via ln(1+x) ~= x.
        let mut x = NumericVar::new();
        sub_var(var, CONST_ONE, &mut x);

        if x.ndigits > 0 {
            x.weight * DEC_DIGITS + (x.digits()[0] as f64).log10() as i32
        } else {
            0
        }
    } else if var.ndigits > 0 {
        let mut digits = var.digits[0] as i32;
        let mut dweight = var.weight * DEC_DIGITS;

        if var.ndigits > 1 {
            digits = digits * NBASE + var.digits[1] as i32;
            dweight -= DEC_DIGITS;
        }

        // var ~= digits * 10^dweight, so ln(var) ~= ln(digits) + dweight*ln(10)
        let ln_var = (digits as f64).ln() + dweight as f64 * 2.302585092994046;
        ln_var.abs().log10() as i32
    } else {
        0
    }
}

pub fn ln_var(arg: VarView<'_>, result: &mut NumericVar, rscale: i32) -> PgResult<()> {
    let cmp = cmp_var(arg, CONST_ZERO);
    if cmp == 0 {
        return Err(log_of_zero_error().into());
    }
    if cmp < 0 {
        return Err(log_of_negative_error().into());
    }

    let mut x = NumericVar::from_view(arg);
    let mut xx = NumericVar::new();
    let mut elem = NumericVar::new();
    let mut fact = NumericVar::from_view(CONST_TWO);
    let mut tmp = NumericVar::new();

    // Reduce into 0.9 < x < 1.1 with repeated sqrt; local_rscale < 0 is
    // allowed here (sqrt_var supports it) and cuts work on huge inputs.
    let mut nsqrt = 0;
    while cmp_var(x.view(), CONST_ZERO_POINT_NINE) <= 0 {
        let local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        sqrt_var(x.view(), &mut tmp, local_rscale)?;
        swap(&mut x, &mut tmp);
        mul_var(fact.view(), CONST_TWO, &mut tmp, 0);
        swap(&mut fact, &mut tmp);
        nsqrt += 1;
    }
    while cmp_var(x.view(), CONST_ONE_POINT_ONE) >= 0 {
        let local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        sqrt_var(x.view(), &mut tmp, local_rscale)?;
        swap(&mut x, &mut tmp);
        mul_var(fact.view(), CONST_TWO, &mut tmp, 0);
        swap(&mut fact, &mut tmp);
        nsqrt += 1;
    }

    // Taylor series for 0.5 * ln((1+z)/(1-z)): z + z^3/3 + z^5/5 + ...
    // with z = (x-1)/(x+1) in about (-0.053, 0.048). Result is multiplied by
    // 2^(nsqrt+1), so carry (nsqrt+1)*log10(2) extra digits.
    let local_rscale = rscale + ((nsqrt + 1) as f64 * 0.301029995663981) as i32 + 8;

    sub_var(x.view(), CONST_ONE, result);
    add_var(x.view(), CONST_ONE, &mut elem);
    div_var(result.view(), elem.view(), &mut tmp, local_rscale, true, false)?;
    swap(result, &mut tmp);
    xx.set_from_view(result.view());
    mul_var(result.view(), result.view(), &mut x, local_rscale);

    let mut res = Flip::new(core::mem::take(result));
    let mut xx = Flip::new(xx);
    let mut ni = 1;
    loop {
        ni += 2;
        {
            let (src, dst) = xx.parts();
            mul_var(src, x.view(), dst, local_rscale);
            xx.flip();
        }
        div_var_int(xx.cur().view(), ni, 0, &mut elem, local_rscale, true)?;

        if elem.ndigits == 0 {
            break;
        }

        {
            let (src, dst) = res.parts();
            add_var(src, elem.view(), dst);
            res.flip();
        }

        if elem.weight < res.cur().weight - local_rscale * 2 / DEC_DIGITS {
            break;
        }
    }

    // Undo the range reduction, rounding to rscale.
    {
        let (src, dst) = res.parts();
        mul_var(src, fact.view(), dst, rscale);
        res.flip();
    }
    *result = res.take();
    Ok(())
}

// log base of num; chooses the result dscale itself.
pub fn log_var(base: VarView<'_>, num: VarView<'_>, result: &mut NumericVar) -> PgResult<()> {
    let ln_base_dweight = estimate_ln_dweight(base);
    let ln_num_dweight = estimate_ln_dweight(num);
    let result_dweight = ln_num_dweight - ln_base_dweight;

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
    rscale = rscale.max(base.dscale);
    rscale = rscale.max(num.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let ln_base_rscale =
        (rscale + result_dweight - ln_base_dweight + 8).max(NUMERIC_MIN_DISPLAY_SCALE);
    let ln_num_rscale =
        (rscale + result_dweight - ln_num_dweight + 8).max(NUMERIC_MIN_DISPLAY_SCALE);

    let mut ln_base = NumericVar::new();
    let mut ln_num = NumericVar::new();
    ln_var(base, &mut ln_base, ln_base_rscale)?;
    ln_var(num, &mut ln_num, ln_num_rscale)?;

    div_var(ln_num.view(), ln_base.view(), result, rscale, true, false)
}

// base^exp; chooses the result dscale itself.
pub fn power_var(base: VarView<'_>, exp: VarView<'_>, result: &mut NumericVar) -> PgResult<()> {
    if exp.ndigits == 0 || exp.ndigits <= exp.weight + 1 {
        // exact integer exponent, if it fits in i32
        if let Some(expval64) = var_to_int64(exp) {
            if (i32::MIN as i64..=i32::MAX as i64).contains(&expval64) {
                return power_var_int(base, expval64 as i32, exp.dscale, result);
            }
        }
    }

    // Avoids ln(0) for 0 raised to a non-integer; 0^0 goes via power_var_int.
    if cmp_var(base, CONST_ZERO) == 0 {
        result.set_from_view(CONST_ZERO);
        result.dscale = NUMERIC_MIN_SIG_DIGITS;
        return Ok(());
    }

    let mut abs_base = NumericVar::new();
    let mut ln_base = NumericVar::new();
    let mut ln_num = NumericVar::new();

    // Negative base requires an integer exp (SQLSTATE per the SQL standard);
    // the result sign follows exp's parity.
    let res_sign;
    let base = if base.sign == NUMERIC_NEG {
        if exp.ndigits > 0 && exp.ndigits > exp.weight + 1 {
            return Err(complex_power_error().into());
        }
        res_sign = if exp.ndigits > 0
            && exp.ndigits == exp.weight + 1
            && (exp.digits[exp.ndigits as usize - 1] & 1) != 0
        {
            NUMERIC_NEG
        } else {
            NUMERIC_POS
        };
        abs_base.set_from_view(base);
        abs_base.sign = NUMERIC_POS;
        abs_base.view()
    } else {
        res_sign = NUMERIC_POS;
        base
    };

    // Low-precision exp * ln(base) sets the real calculation's scale and
    // pre-screens overflow (fuzzed; exp_var owns the exact threshold).
    let ln_dweight = estimate_ln_dweight(base);

    let mut local_rscale = (8 - ln_dweight).max(NUMERIC_MIN_DISPLAY_SCALE);

    ln_var(base, &mut ln_base, local_rscale)?;
    mul_var(ln_base.view(), exp, &mut ln_num, local_rscale);

    let mut val = var_to_f64(ln_num.view());

    if val.abs() > NUMERIC_MAX_RESULT_SCALE as f64 * 3.01 {
        if val > 0.0 {
            return Err(numeric_overflow_error().into());
        }
        result.set_zero();
        result.dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return Ok(());
    }

    val *= 0.434294481903252; // approximate decimal result weight

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - val as i32;
    rscale = rscale.max(base.dscale);
    rscale = rscale.max(exp.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let sig_digits = (rscale + val as i32).max(0);

    local_rscale = (sig_digits - ln_dweight + 8).max(NUMERIC_MIN_DISPLAY_SCALE);

    ln_var(base, &mut ln_base, local_rscale)?;
    mul_var(ln_base.view(), exp, &mut ln_num, local_rscale);
    exp_var(ln_num.view(), result, rscale)?;

    if res_sign == NUMERIC_NEG && result.ndigits > 0 {
        result.sign = NUMERIC_NEG;
    }
    Ok(())
}

pub fn power_var_int(
    base: VarView<'_>,
    exp: i32,
    exp_dscale: i32,
    result: &mut NumericVar,
) -> PgResult<()> {
    // base ~= f * 10^p; log10(result) ~= exp * (log10(f) + p)
    let f = if base.ndigits != 0 {
        let mut f = base.digits[0] as f64;
        let mut p = base.weight * DEC_DIGITS;
        let mut i = 1;
        while i < base.ndigits && i * DEC_DIGITS < 16 {
            f = f * NBASE as f64 + base.digits[i as usize] as f64;
            p -= DEC_DIGITS;
            i += 1;
        }
        exp as f64 * (f.log10() + p as f64)
    } else {
        0.0 // result is 0 or 1 (weight 0), or error
    };

    // overflow/underflow tests with fuzz factors
    if f > ((NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS) as f64 {
        return Err(numeric_overflow_error().into());
    }
    if f + 1.0 < -(NUMERIC_MAX_DISPLAY_SCALE as f64) {
        result.set_zero();
        result.dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return Ok(());
    }

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - f as i32;
    rscale = rscale.max(base.dscale);
    rscale = rscale.max(exp_dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    match exp {
        0 => {
            // 0^0 = 1 per SQL:2003
            result.set_from_view(CONST_ONE);
            result.dscale = rscale;
            return Ok(());
        }
        1 => {
            result.set_from_view(base);
            result.round(rscale);
            return Ok(());
        }
        -1 => {
            return div_var(CONST_ONE, base, result, rscale, true, true);
        }
        2 => {
            mul_var(base, base, result, rscale);
            return Ok(());
        }
        _ => {}
    }

    if base.ndigits == 0 {
        if exp < 0 {
            return Err(division_by_zero_error().into());
        }
        result.set_zero();
        result.dscale = rscale;
        return Ok(());
    }

    // Repeated squaring over exp's bit pattern; per-multiplication rscales
    // hold sig_digits significant digits without exceeding the operands' own
    // scales. The extra log10(abs(exp)) digits absorb accumulated error.
    let mut sig_digits = 1 + rscale + f as i32;
    sig_digits += (exp as f64).abs().ln() as i32 + 8;

    let mut neg = exp < 0;
    let mut mask = exp.unsigned_abs();

    let mut base_prod = Flip::new(NumericVar::from_view(base));

    if mask & 1 != 0 {
        result.set_from_view(base);
    } else {
        result.set_from_view(CONST_ONE);
    }
    let mut res = Flip::new(core::mem::take(result));

    loop {
        mask >>= 1;
        if mask == 0 {
            break;
        }

        {
            let bp = base_prod.cur();
            let mut local_rscale = sig_digits - 2 * bp.weight * DEC_DIGITS;
            local_rscale = local_rscale.min(2 * bp.dscale);
            local_rscale = local_rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
            let (src, dst) = base_prod.parts();
            mul_var(src, src, dst, local_rscale);
            base_prod.flip();
        }

        if mask & 1 != 0 {
            let bp = base_prod.cur();
            let mut local_rscale = sig_digits - (bp.weight + res.cur().weight) * DEC_DIGITS;
            local_rscale = local_rscale.min(bp.dscale + res.cur().dscale);
            local_rscale = local_rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
            let (src, dst) = res.parts();
            mul_var(bp.view(), src, dst, local_rscale);
            res.flip();
        }

        // Once a weight exceeds i16 the final result must overflow (or
        // underflow when exp < 0) — stop early.
        if base_prod.cur().weight > NUMERIC_WEIGHT_MAX || res.cur().weight > NUMERIC_WEIGHT_MAX {
            if !neg {
                return Err(numeric_overflow_error().into());
            }
            res.parts().1.set_zero();
            res.flip();
            neg = false;
            break;
        }
    }

    if neg {
        let (src, dst) = res.parts();
        div_var(CONST_ONE, src, dst, rscale, true, false)?;
        res.flip();
        *result = res.take();
    } else {
        *result = res.take();
        result.round(rscale);
    }
    Ok(())
}

// 10^exp exactly; no overflow/underflow checks or rounding (C caveat).
fn power_ten_int(exp: i32, result: &mut NumericVar) {
    result.set_from_view(CONST_ONE);
    result.dscale = if exp < 0 { -exp } else { 0 };
    result.weight = if exp >= 0 {
        exp / DEC_DIGITS
    } else {
        (exp + 1) / DEC_DIGITS - 1
    };

    let mut e = exp - result.weight * DEC_DIGITS;
    while e > 0 {
        result.digits_mut()[0] *= 10;
        e -= 1;
    }
}

// C's get_str_from_var_sci: significand to rscale digits, then "e%+03d".
pub fn get_str_from_var_sci(var: VarView<'_>, rscale: i32, out: &mut Vec<u8>) {
    let rscale = rscale.max(0);

    // Exponent putting one significant digit before the decimal point.
    let exponent = if var.ndigits > 0 {
        (var.weight + 1) * DEC_DIGITS - (DEC_DIGITS - (var.digits[0] as f64).log10() as i32)
    } else {
        0 // zero displays exponent 0 for consistency
    };

    let mut denom = NumericVar::new();
    power_ten_int(exponent, &mut denom);
    let mut sig = NumericVar::new();
    div_var(var, denom.view(), &mut sig, rscale, true, true)
        .expect("power of ten divisor is nonzero");
    get_str_from_var(sig.view(), out);

    out.push(b'e');
    out.push(if exponent < 0 { b'-' } else { b'+' });
    let e = exponent.unsigned_abs();
    // %+03d: at least two exponent digits.
    let ndig = if e < 10 {
        2
    } else {
        (e.ilog10() + 1).max(2) as usize
    };
    let base = out.len();
    out.resize(base + ndig, b'0');
    let mut e = e;
    for i in (0..ndig).rev() {
        out[base + i] = b'0' + (e % 10) as u8;
        e /= 10;
    }
}

fn make_result_checked(var: VarView<'_>, out: &mut NumericImage) -> PgResult<()> {
    if !crate::var::make_result_into(var, out) {
        return Err(numeric_overflow_error().into());
    }
    Ok(())
}

pub fn numeric_sqrt_into(num: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num.is_special() {
        // error must match sqrt_var()'s
        if num.is_ninf() {
            return Err(sqrt_negative_error().into());
        }
        out.set_from_num(num);
        return Ok(());
    }

    let arg = num.view();

    // result has at least sweight digits before the decimal point
    // (DEC_DIGITS is even: exact division, no round toward -inf needed)
    let sweight = arg.weight * DEC_DIGITS / 2 + 1;

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
    rscale = rscale.max(arg.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let mut result = NumericVar::new();
    sqrt_var(arg, &mut result, rscale)?;
    make_result_checked(result.view(), out)
}

pub fn numeric_sqrt(num: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_sqrt_into(num, &mut out)?;
    Ok(out)
}

pub fn numeric_exp_into(num: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num.is_special() {
        // Per POSIX, exp(-Inf) is zero
        if num.is_ninf() {
            return make_result_checked(CONST_ZERO, out);
        }
        out.set_from_num(num);
        return Ok(());
    }

    let arg = num.view();

    // log10(result) = num * log10(e) ~= the result's decimal weight; clamp so
    // the rscale arithmetic can't overflow.
    let mut val = var_to_f64(arg) * 0.434294481903252;
    val = val.max(-(NUMERIC_MAX_RESULT_SCALE as f64));
    val = val.min(NUMERIC_MAX_RESULT_SCALE as f64);

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - val as i32;
    rscale = rscale.max(arg.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let mut result = NumericVar::new();
    exp_var(arg, &mut result, rscale)?;
    make_result_checked(result.view(), out)
}

pub fn numeric_exp(num: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_exp_into(num, &mut out)?;
    Ok(out)
}

pub fn numeric_ln_into(num: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num.is_special() {
        if num.is_ninf() {
            return Err(log_of_negative_error().into());
        }
        out.set_from_num(num);
        return Ok(());
    }

    let arg = num.view();
    let ln_dweight = estimate_ln_dweight(arg);

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - ln_dweight;
    rscale = rscale.max(arg.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let mut result = NumericVar::new();
    ln_var(arg, &mut result, rscale)?;
    make_result_checked(result.view(), out)
}

pub fn numeric_ln(num: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_ln_into(num, &mut out)?;
    Ok(out)
}

pub fn numeric_log(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    if num1.is_special() || num2.is_special() {
        if num1.is_nan() || num2.is_nan() {
            return Ok(NumericImage::nan());
        }
        // fail on negative and zero inputs, as log_var would
        let sign1 = numeric_sign_internal(num1);
        let sign2 = numeric_sign_internal(num2);
        if sign1 < 0 || sign2 < 0 {
            return Err(log_of_negative_error().into());
        }
        if sign1 == 0 || sign2 == 0 {
            return Err(log_of_zero_error().into());
        }
        if num1.is_pinf() {
            // log(Inf, Inf) is Inf/Inf = NaN; log(Inf, finite-positive) is 0
            if num2.is_pinf() {
                return Ok(NumericImage::nan());
            }
            return make_result(CONST_ZERO);
        }
        debug_assert!(num2.is_pinf());
        return Ok(NumericImage::pinf());
    }

    let mut result = NumericVar::new();
    log_var(num1.view(), num2.view(), &mut result)?;
    make_result(result.view())
}

fn numeric_is_integral(num: Num<'_>) -> bool {
    if num.is_special() {
        // NaN is not integral; infinities are
        return !num.is_nan();
    }
    let arg = num.view();
    arg.ndigits == 0 || arg.ndigits <= arg.weight + 1
}

pub fn numeric_power_into(num1: Num<'_>, num2: Num<'_>, out: &mut NumericImage) -> PgResult<()> {
    if num1.is_special() || num2.is_special() {
        // POSIX pow(3): NaN^0 = 1 and 1^NaN = 1; other NaN inputs yield NaN.
        if num1.is_nan() {
            if !num2.is_special() && cmp_var(num2.view(), CONST_ZERO) == 0 {
                return make_result_checked(CONST_ONE, out);
            }
            out.set_special(crate::NUMERIC_NAN);
            return Ok(());
        }
        if num2.is_nan() {
            if !num1.is_special() && cmp_var(num1.view(), CONST_ONE) == 0 {
                return make_result_checked(CONST_ONE, out);
            }
            out.set_special(crate::NUMERIC_NAN);
            return Ok(());
        }
        // At least one input is infinite; error rules still apply.
        let sign1 = numeric_sign_internal(num1);
        let sign2 = numeric_sign_internal(num2);
        if sign1 == 0 && sign2 < 0 {
            return Err(zero_to_negative_power_error().into());
        }
        if sign1 < 0 && !numeric_is_integral(num2) {
            return Err(complex_power_error().into());
        }

        // POSIX rules for infinite inputs follow.
        if !num1.is_special() && cmp_var(num1.view(), CONST_ONE) == 0 {
            return make_result_checked(CONST_ONE, out);
        }
        if sign2 == 0 {
            return make_result_checked(CONST_ONE, out);
        }
        if sign1 == 0 && sign2 > 0 {
            return make_result_checked(CONST_ZERO, out);
        }
        if num2.is_inf() {
            let abs_x_gt_one = if num1.is_special() {
                true
            } else {
                let mut arg1 = NumericVar::from_view(num1.view());
                if cmp_var(arg1.view(), CONST_MINUS_ONE) == 0 {
                    return make_result_checked(CONST_ONE, out);
                }
                arg1.sign = NUMERIC_POS;
                cmp_var(arg1.view(), CONST_ONE) > 0
            };
            if abs_x_gt_one == (sign2 > 0) {
                out.set_special(crate::NUMERIC_PINF);
                return Ok(());
            }
            return make_result_checked(CONST_ZERO, out);
        }
        if num1.is_pinf() {
            if sign2 > 0 {
                out.set_special(crate::NUMERIC_PINF);
                return Ok(());
            }
            return make_result_checked(CONST_ZERO, out);
        }
        debug_assert!(num1.is_ninf());
        if sign2 < 0 {
            return make_result_checked(CONST_ZERO, out);
        }
        let arg2 = num2.view();
        if arg2.ndigits > 0
            && arg2.ndigits == arg2.weight + 1
            && (arg2.digits[arg2.ndigits as usize - 1] & 1) != 0
        {
            out.set_special(crate::NUMERIC_NINF);
        } else {
            out.set_special(crate::NUMERIC_PINF);
        }
        return Ok(());
    }

    // SQL requires 2201F (not division-by-zero) for 0 ^ negative; the
    // negative-base non-integer-exp case is checked in power_var().
    let sign1 = numeric_sign_internal(num1);
    let sign2 = numeric_sign_internal(num2);
    if sign1 == 0 && sign2 < 0 {
        return Err(zero_to_negative_power_error().into());
    }

    let mut result = NumericVar::new();
    power_var(num1.view(), num2.view(), &mut result)?;
    make_result_checked(result.view(), out)
}

pub fn numeric_power(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    let mut out = NumericImage::empty();
    numeric_power_into(num1, num2, &mut out)?;
    Ok(out)
}

pub fn numeric_mod_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    // POSIX fmod semantics, except y-is-zero always errors.
    if num1.is_special() || num2.is_special() {
        if num1.is_nan() || num2.is_nan() {
            return Ok(NumericImage::nan());
        }
        if num1.is_inf() {
            if numeric_sign_internal(num2) == 0 {
                return Err(division_by_zero_error().into());
            }
            return Ok(NumericImage::nan());
        }
        // num2 is [-]Inf; result is num1 regardless of num2's sign
        return Ok(NumericImage::from_num(num1));
    }

    let mut result = NumericVar::new();
    mod_var(num1.view(), num2.view(), &mut result)?;
    make_result(result.view())
}

pub fn numeric_gcd_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    if num1.is_special() || num2.is_special() {
        return Ok(NumericImage::nan());
    }

    let mut result = NumericVar::new();
    gcd_var(num1.view(), num2.view(), &mut result)?;
    make_result(result.view())
}

pub fn numeric_lcm_common(num1: Num<'_>, num2: Num<'_>) -> PgResult<NumericImage> {
    if num1.is_special() || num2.is_special() {
        return Ok(NumericImage::nan());
    }

    let arg1 = num1.view();
    let arg2 = num2.view();
    let mut result = NumericVar::new();

    // lcm(x, y) = abs(x / gcd(x, y) * y), zero if either input is zero; the
    // division is exact.
    if arg1.ndigits == 0 || arg2.ndigits == 0 {
        result.set_from_view(CONST_ZERO);
    } else {
        gcd_var(arg1, arg2, &mut result)?;
        let mut tmp = NumericVar::new();
        div_var(arg1, result.view(), &mut tmp, 0, false, true)?;
        swap(&mut result, &mut tmp);
        mul_var(arg2, result.view(), &mut tmp, arg2.dscale);
        swap(&mut result, &mut tmp);
        result.sign = NUMERIC_POS;
    }

    result.dscale = arg1.dscale.max(arg2.dscale);
    make_result(result.view())
}

pub fn numeric_fac(num: i64) -> PgResult<NumericImage> {
    if num < 0 {
        return Err(factorial_negative_error().into());
    }
    if num <= 1 {
        return make_result(CONST_ONE);
    }
    // 32178! overflows the numeric format
    if num > 32177 {
        return Err(numeric_overflow_error().into());
    }

    let mut res = Flip::new(int64_to_var(num));
    let mut fact = NumericVar::new();

    let mut n = num - 1;
    while n > 1 {
        check_for_interrupts();

        crate::var::set_var_from_int64(n, &mut fact);
        let (src, dst) = res.parts();
        mul_var(src, fact.view(), dst, 0);
        res.flip();
        n -= 1;
    }

    make_result(res.cur().view())
}

pub fn numeric_out_sci(num: Num<'_>, scale: i32, out: &mut Vec<u8>) {
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
    get_str_from_var_sci(num.view(), scale, out);
}

// ((operand - bound1) * count) / (bound2 - bound1) + 1, floor division;
// multiply before dividing to limit roundoff (per SQL2003).
fn compute_bucket(
    operand: Num<'_>,
    bound1: Num<'_>,
    bound2: Num<'_>,
    count_var: &NumericVar,
    result_var: &mut NumericVar,
) -> PgResult<()> {
    let mut operand_var = NumericVar::new();
    let mut bound2_var = NumericVar::new();
    let mut tmp = NumericVar::new();

    sub_var(operand.view(), bound1.view(), &mut operand_var);
    sub_var(bound2.view(), bound1.view(), &mut bound2_var);

    mul_var(
        operand_var.view(),
        count_var.view(),
        &mut tmp,
        operand_var.dscale + count_var.dscale,
    );
    swap(&mut operand_var, &mut tmp);
    div_var(
        operand_var.view(),
        bound2_var.view(),
        result_var,
        0,
        false,
        true,
    )?;
    add_var(result_var.view(), CONST_ONE, &mut tmp);
    swap(result_var, &mut tmp);
    Ok(())
}

// SQL2003 width_bucket(); operand below the range goes to bucket 0, at or
// above the upper bound to bucket count+1.
pub fn width_bucket_numeric(
    operand: Num<'_>,
    bound1: Num<'_>,
    bound2: Num<'_>,
    count: i32,
) -> PgResult<i32> {
    if count <= 0 {
        return Err(width_bucket_error("count must be greater than zero").into());
    }

    if operand.is_special() || bound1.is_special() || bound2.is_special() {
        if operand.is_nan() || bound1.is_nan() || bound2.is_nan() {
            return Err(
                width_bucket_error("operand, lower bound, and upper bound cannot be NaN").into(),
            );
        }
        // infinite operand is fine; cmp_numerics copes
        if bound1.is_inf() || bound2.is_inf() {
            return Err(width_bucket_error("lower and upper bounds must be finite").into());
        }
    }

    let count_var = int64_to_var(count as i64);
    let mut result_var = NumericVar::new();

    match cmp_numerics(bound1, bound2) {
        0 => {
            return Err(width_bucket_error("lower bound cannot equal upper bound").into());
        }
        -1 => {
            if cmp_numerics(operand, bound1) < 0 {
                result_var.set_from_view(CONST_ZERO);
            } else if cmp_numerics(operand, bound2) >= 0 {
                add_var(count_var.view(), CONST_ONE, &mut result_var);
            } else {
                compute_bucket(operand, bound1, bound2, &count_var, &mut result_var)?;
            }
        }
        _ => {
            if cmp_numerics(operand, bound1) > 0 {
                result_var.set_from_view(CONST_ZERO);
            } else if cmp_numerics(operand, bound2) <= 0 {
                add_var(count_var.view(), CONST_ONE, &mut result_var);
            } else {
                compute_bucket(operand, bound1, bound2, &count_var, &mut result_var)?;
            }
        }
    }

    var_to_int32(result_var.view()).ok_or_else(|| integer_out_of_range_error().into())
}
