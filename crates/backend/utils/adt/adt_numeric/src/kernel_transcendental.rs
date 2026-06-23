//! Family: transcendental kernels (numeric.c `sqrt_var`/`exp_var`/`ln_var`/
//! `log_var`/`power_var`/`power_var_int`) + the small-int<->`NumericVar`
//! helpers (`int64_to_numericvar`/`numericvar_to_int64`/`estimate_ln_dweight`)
//! they build on.
//!
//! All allocate digit buffers and so take an explicit `Mcx<'mcx>` and return
//! [`PgResult`] where the C `ereport`s.
//!
//! These mirror numeric.c (18.3) function-for-function. The base-NBASE kernels
//! they invoke (`cmp_var`/`add_var`/`sub_var`/`mul_var`/`div_var`/`div_mod_var`/
//! `round_var`/`strip_var`/`set_var_from_var` + the preinitialized constants)
//! live in the sibling `kernel_var` family, and the decimal-string renderer
//! (`get_str_from_var`, used by `numericvar_to_double_no_overflow`) lives in the
//! sibling `io` family — both are this unit's OWN logic, called directly here
//! (no seam, no stub).
//!
//! C's `div_var_int()` is an internal short-division fast path of `div_var()`
//! (numeric.c:9407-9424 delegates to it for <=2-digit divisors) and is not part
//! of the kernel_var contract, so the `exp_var`/`ln_var` Taylor-term divisions
//! that the C source writes as `div_var_int(&v, n, 0, &v, rscale, round=true)`
//! are expressed here as `div_var(mcx, &v, &<n as NumericVar>, rscale, true,
//! true)` — mathematically identical, since `div_var` itself routes small
//! divisors through that same fast path.

use core::cmp::{max, min, Ordering};

use ::mcx::Mcx;
use ::types_error::{
    PgError, PgResult, ERRCODE_DIVISION_BY_ZERO, ERRCODE_INVALID_ARGUMENT_FOR_LOG,
    ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};
use ::types_numeric::var::{NumericSign, NumericVar};
use ::types_numeric::{
    DEC_DIGITS, NBASE, NUMERIC_MAX_DISPLAY_SCALE, NUMERIC_MAX_RESULT_SCALE,
    NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MIN_SIG_DIGITS, NUMERIC_WEIGHT_MAX,
};

use crate::kernel_var;

// ---------------------------------------------------------------------------
// Error constructors (mirror the C ereport call sites).
// ---------------------------------------------------------------------------

#[inline]
fn err_value_overflow() -> PgError {
    PgError::error("value overflows numeric format")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[inline]
fn err_div_by_zero() -> PgError {
    PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}

// ---------------------------------------------------------------------------
// Small-int <-> NumericVar helpers.
// ---------------------------------------------------------------------------

/// `int64_to_numericvar(val, var)`: build a `NumericVar` from an `i64`.
///
/// numeric.c:8223. The returned value carries one spare leading carry-slack
/// digit (`headroom == 1`, mirroring C's `alloc_var(20/DEC_DIGITS)` then
/// `digits = ptr` placement). An `i64` needs at most 5 base-NBASE digits, so
/// these fixed-capacity buffers are not data-derived growth.
pub fn int64_to_numericvar<'mcx>(mcx: Mcx<'mcx>, val: i64) -> PgResult<NumericVar<'mcx>> {
    if val == 0 {
        // C: var->ndigits = 0; var->weight = 0; (sign POS, dscale 0).
        return Ok(NumericVar::zero(mcx));
    }

    let sign = if val < 0 {
        NumericSign::Neg
    } else {
        NumericSign::Pos
    };
    let mut uval = val.unsigned_abs();

    // Emit base-NBASE digits least-significant first, then reverse into the
    // logical buffer behind one leading carry-slack slot.
    let mut tmp: [i16; 6] = [0; 6];
    let mut n = 0usize;
    while uval != 0 {
        let newuval = uval / NBASE as u64;
        tmp[n] = (uval - newuval * NBASE as u64) as i16;
        n += 1;
        uval = newuval;
    }

    // alloc_var(20 / DEC_DIGITS) == 5 logical digits; we reserve n + 1 (one
    // leading carry-slack slot) and write the n significant digits.
    let mut digits = crate::alloc_digits(mcx, n + 1)?;
    digits[0] = 0; // spare carry-slack slot (headroom)
    for i in 0..n {
        digits[1 + i] = tmp[n - 1 - i];
    }

    Ok(NumericVar {
        sign,
        weight: n as i32 - 1,
        dscale: 0,
        digits,
        headroom: 1,
    })
}

/// `int128_to_numericvar(val, var)`: build a `NumericVar` from an `i128`
/// (numeric.c:8312, the `HAVE_INT128` helper used by `sqrt_var`). Carries one
/// leading carry-slack slot; an `i128` needs at most 10 base-NBASE digits.
fn int128_to_numericvar<'mcx>(mcx: Mcx<'mcx>, val: i128) -> PgResult<NumericVar<'mcx>> {
    if val == 0 {
        return Ok(NumericVar::zero(mcx));
    }
    let sign = if val < 0 {
        NumericSign::Neg
    } else {
        NumericSign::Pos
    };
    let mut uval = val.unsigned_abs();
    let mut tmp: [i16; 11] = [0; 11];
    let mut n = 0usize;
    while uval != 0 {
        let newuval = uval / NBASE as u128;
        tmp[n] = (uval - newuval * NBASE as u128) as i16;
        n += 1;
        uval = newuval;
    }
    let mut digits = crate::alloc_digits(mcx, n + 1)?;
    digits[0] = 0;
    for i in 0..n {
        digits[1 + i] = tmp[n - 1 - i];
    }
    Ok(NumericVar {
        sign,
        weight: n as i32 - 1,
        dscale: 0,
        digits,
        headroom: 1,
    })
}

/// `numericvar_to_int64(var)`: convert to `i64`, `Ok(None)` on the C `false`
/// (out of range / non-integral). numeric.c:8148.
///
/// The rounding clone is charged to a scratch `mcx`; the `PgResult` channel
/// reports only an OOM from that clone, matching the C function's `bool`
/// success/overflow result via `Ok(Some)`/`Ok(None)`.
pub fn numericvar_to_int64(var: &NumericVar<'_>) -> PgResult<Option<i64>> {
    // Round to nearest integer (operate on a clone in the same context).
    let mut rounded = var.clone();
    kernel_var::round_var(&mut rounded, 0);

    // Check for zero input.
    kernel_var::strip_var(&mut rounded);
    let ndigits = rounded.ndigits() as i32;
    if ndigits == 0 {
        return Ok(Some(0));
    }

    // weight+1 digits before the decimal point; loop assumes stripped trailing
    // digits are real zeroes.
    let weight = rounded.weight;
    // C Assert(weight >= 0 && ndigits <= weight + 1) — guaranteed by round(0)+strip.
    let digits = rounded.logical_digits();
    let neg = rounded.sign == NumericSign::Neg;

    // Accumulate as a negative number so INT64_MIN is representable.
    let mut val: i64 = -(digits[0] as i64);
    let mut i = 1i32;
    while i <= weight {
        val = match val.checked_mul(NBASE as i64) {
            Some(v) => v,
            None => return Ok(None),
        };
        if i < ndigits {
            val = match val.checked_sub(digits[i as usize] as i64) {
                Some(v) => v,
                None => return Ok(None),
            };
        }
        i += 1;
    }

    if !neg {
        if val == i64::MIN {
            return Ok(None);
        }
        val = -val;
    }
    Ok(Some(val))
}

/// `numericvar_to_double_no_overflow(var)` (numeric.c:8460): render `var` to its
/// decimal string and `strtod` it (here, parse it as `f64`, which is correctly
/// rounded like `strtod`). Out-of-range yields +/-inf, exactly as C ignores
/// `strtod`'s `ERANGE`.
///
/// `get_str_from_var` is this unit's OWN logic (the `io` family); we call it
/// directly. The string it produces is always a parseable decimal, so the parse
/// cannot fail (the C "shouldn't happen" branch).
pub(crate) fn numericvar_to_double_no_overflow(var: &NumericVar<'_>) -> f64 {
    let s = crate::io::get_str_from_var(var);
    s.parse::<f64>()
        .expect("get_str_from_var always yields a parseable float")
}

/// `estimate_ln_dweight(var)`: estimate the decimal weight of `ln(var)` —
/// essentially `log10(abs(ln(var)))`. numeric.c:11029. Robust against inputs
/// invalid for `ln()` (returns 0), as many callers invoke it pre-range-check.
pub fn estimate_ln_dweight(var: &NumericVar<'_>) -> PgResult<i32> {
    // Caller should fail on ln(negative); for now return zero.
    if var.sign != NumericSign::Pos {
        return Ok(0);
    }

    // We need the constants 0.9 and 1.1 / 1 in some context; reuse var's (the
    // memory context owning its digit buffer — these constants are local scratch
    // that never escape this function).
    let mcx = *var.digits.allocator();
    let zpn = kernel_var::const_zero_point_nine(mcx);
    let opo = kernel_var::const_one_point_one(mcx);

    if kernel_var::cmp_var(var, &zpn) != Ordering::Less
        && kernel_var::cmp_var(var, &opo) != Ordering::Greater
    {
        // 0.9 <= var <= 1.1: ln(var) has a (possibly very large) negative
        // weight; estimate via ln(1+x) ~= x where x = var - 1.
        let one = kernel_var::const_one(mcx);
        let x = kernel_var::sub_var(mcx, var, &one)?;
        if x.ndigits() > 0 {
            let x_digits = x.logical_digits();
            Ok(x.weight * DEC_DIGITS + (x_digits[0] as f64).log10() as i32)
        } else {
            // x = 0; ln(1) = 0 exactly, no extra digits needed.
            Ok(0)
        }
    } else if var.ndigits() > 0 {
        // Estimate from the first couple of digits: var ~= digits * 10^dweight,
        // so ln(var) ~= ln(digits) + dweight * ln(10).
        let var_digits = var.logical_digits();
        let mut digits = var_digits[0] as i64;
        let mut dweight = var.weight * DEC_DIGITS;
        if var.ndigits() > 1 {
            digits = digits * NBASE as i64 + var_digits[1] as i64;
            dweight -= DEC_DIGITS;
        }
        // C uses the literal ln(10) = 2.302585092994046 (numeric.c:11091).
        let ln_var = (digits as f64).ln() + dweight as f64 * 2.302585092994046;
        Ok(ln_var.abs().log10() as i32)
    } else {
        // Caller should fail on ln(0); for now return zero.
        Ok(0)
    }
}

// ---------------------------------------------------------------------------
// sqrt_var (numeric.c:10420) -- Karatsuba Square Root.
// ---------------------------------------------------------------------------

pub fn sqrt_var<'mcx>(
    mcx: Mcx<'mcx>,
    arg: &NumericVar<'_>,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let zero = kernel_var::const_zero(mcx);
    let stat = kernel_var::cmp_var(arg, &zero);
    if stat == Ordering::Equal {
        let mut result = NumericVar::zero(mcx);
        result.dscale = rscale;
        return Ok(result);
    }
    // SQL2003 defines sqrt() via power(), so negative -> power-function error.
    if stat == Ordering::Less {
        return Err(
            PgError::error("cannot take square root of a negative number")
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
        );
    }

    let arg_digits = arg.logical_digits();
    let arg_ndigits = arg.ndigits() as i32;

    // res_weight = floor(arg->weight / 2).
    let res_weight = if arg.weight >= 0 {
        arg.weight / 2
    } else {
        -((-arg.weight - 1) / 2 + 1)
    };

    // res_ndigits = res_weight + 1 + ceil((rscale + 1) / DEC_DIGITS), >= 1.
    let mut res_ndigits = if rscale + 1 >= 0 {
        res_weight + 1 + (rscale + DEC_DIGITS) / DEC_DIGITS
    } else {
        res_weight + 1 - (-rscale - 1) / DEC_DIGITS
    };
    res_ndigits = max(res_ndigits, 1);

    // Number of source NBASE digits logically required.
    let mut src_ndigits = arg.weight + 1 + (res_ndigits - res_weight - 1) * 2;
    src_ndigits = max(src_ndigits, 1);

    // Build the (reverse-ordered) per-step digit-count schedule. <32 steps.
    let mut ndigits = [0i32; 32];
    let mut step: i32 = 0;
    loop {
        ndigits[step as usize] = src_ndigits;
        if src_ndigits <= 4 {
            break;
        }
        // Choose b = NBASE^blen so that a3 >= b/4.
        let mut blen = src_ndigits / 4;
        if blen * 4 == src_ndigits && (arg_digits[0] as i32) < NBASE / 4 {
            blen -= 1;
        }
        src_ndigits -= 2 * blen;
        step += 1;
    }

    // First (innermost) iteration: src_ndigits <= 4, fits in i64; estimate the
    // root with double-precision and correct via Newton's method.
    let mut arg_int64: i64 = arg_digits[0] as i64;
    let mut src_idx = 1i32;
    while src_idx < src_ndigits {
        arg_int64 *= NBASE as i64;
        if src_idx < arg_ndigits {
            arg_int64 += arg_digits[src_idx as usize] as i64;
        }
        src_idx += 1;
    }

    let mut s_int64: i64 = (arg_int64 as f64).sqrt() as i64;
    let mut r_int64: i64 = arg_int64 - s_int64 * s_int64;

    while r_int64 < 0 || r_int64 > 2 * s_int64 {
        s_int64 = (s_int64 + arg_int64 / s_int64) / 2;
        r_int64 = arg_int64 - s_int64 * s_int64;
    }

    // Iterations with src_ndigits <= 8: still fits in i64.
    step -= 1;
    while step >= 0 && {
        src_ndigits = ndigits[step as usize];
        src_ndigits <= 8
    } {
        let blen = (src_ndigits - src_idx) / 2;
        let mut a0: i64 = 0;
        let mut a1: i64 = 0;
        let mut b: i64 = 1;

        for _ in 0..blen {
            b *= NBASE as i64;
            a1 *= NBASE as i64;
            if src_idx < arg_ndigits {
                a1 += arg_digits[src_idx as usize] as i64;
            }
            src_idx += 1;
        }
        for _ in 0..blen {
            a0 *= NBASE as i64;
            if src_idx < arg_ndigits {
                a0 += arg_digits[src_idx as usize] as i64;
            }
            src_idx += 1;
        }

        // (q,u) = DivRem(r*b + a1, 2*s)
        let numer = r_int64 * b + a1;
        let denom = 2 * s_int64;
        let q = numer / denom;
        let u = numer - q * denom;

        // s = s*b + q ; r = u*b + a0 - q^2
        s_int64 = s_int64 * b + q;
        r_int64 = u * b + a0 - q * q;

        if r_int64 < 0 {
            // s too large by 1.
            r_int64 += s_int64;
            s_int64 -= 1;
            r_int64 += s_int64;
        }

        debug_assert_eq!(src_idx, src_ndigits);
        step -= 1;
    }

    // On platforms with 128-bit integers (always, here), delay numeric vars.
    let mut s_var: NumericVar<'mcx>;
    let mut r_var: NumericVar<'mcx> = NumericVar::zero(mcx);

    if step >= 0 {
        let mut s_int128: i128 = s_int64 as i128;
        let mut r_int128: i128 = r_int64 as i128;

        // Iterations with src_ndigits <= 16: fits in i128.
        while step >= 0 && {
            src_ndigits = ndigits[step as usize];
            src_ndigits <= 16
        } {
            let blen = (src_ndigits - src_idx) / 2;
            let mut a0: i128 = 0;
            let mut a1: i128 = 0;
            let mut b: i128 = 1;

            for _ in 0..blen {
                b *= NBASE as i128;
                a1 *= NBASE as i128;
                if src_idx < arg_ndigits {
                    a1 += arg_digits[src_idx as usize] as i128;
                }
                src_idx += 1;
            }
            for _ in 0..blen {
                a0 *= NBASE as i128;
                if src_idx < arg_ndigits {
                    a0 += arg_digits[src_idx as usize] as i128;
                }
                src_idx += 1;
            }

            let numer = r_int128 * b + a1;
            let denom = 2 * s_int128;
            let q = numer / denom;
            let u = numer - q * denom;

            s_int128 = s_int128 * b + q;
            r_int128 = u * b + a0 - q * q;

            if r_int128 < 0 {
                r_int128 += s_int128;
                s_int128 -= 1;
                r_int128 += s_int128;
            }

            debug_assert_eq!(src_idx, src_ndigits);
            step -= 1;
        }

        // Convert to NumericVar to continue; final iteration doesn't need r.
        s_var = int128_to_numericvar(mcx, s_int128)?;
        if step >= 0 {
            r_var = int128_to_numericvar(mcx, r_int128)?;
        }
    } else {
        s_var = int64_to_numericvar(mcx, s_int64)?;
        // step < 0, so we certainly don't need r.
    }

    // Remaining iterations (src_ndigits > 16) use numeric variables.
    let one = kernel_var::const_one(mcx);
    while step >= 0 {
        src_ndigits = ndigits[step as usize];
        let blen = (src_ndigits - src_idx) / 2;

        // Extract a1, then a0.
        let a1_var = extract_chunk(mcx, arg, src_idx, blen)?;
        src_idx += blen;
        let a0_var = extract_chunk(mcx, arg, src_idx, blen)?;
        src_idx += blen;

        // (q,u) = DivRem(r*b + a1, 2*s)
        let mut q_var = r_var.clone();
        q_var.weight += blen;
        q_var = kernel_var::add_var(mcx, &q_var, &a1_var)?;
        let u_var0 = kernel_var::add_var(mcx, &s_var, &s_var)?;
        let (q_new, mut u_var) = kernel_var::div_mod_var(mcx, &q_var, &u_var0)?;
        q_var = q_new;

        // s = s*b + q
        let mut s_shift = s_var.clone();
        s_shift.weight += blen;
        s_var = kernel_var::add_var(mcx, &s_shift, &q_var)?;

        // u = u*b + a0 ; q^2
        u_var.weight += blen;
        u_var = kernel_var::add_var(mcx, &u_var, &a0_var)?;
        let q_sq = kernel_var::mul_var(mcx, &q_var, &q_var, 0)?;

        if step > 0 {
            // Need r for later iterations: r = u - q^2.
            r_var = kernel_var::sub_var(mcx, &u_var, &q_sq)?;
            if r_var.sign == NumericSign::Neg {
                // s too large by 1.
                r_var = kernel_var::add_var(mcx, &r_var, &s_var)?;
                s_var = kernel_var::sub_var(mcx, &s_var, &one)?;
                r_var = kernel_var::add_var(mcx, &r_var, &s_var)?;
            }
        } else if kernel_var::cmp_var(&u_var, &q_sq) == Ordering::Less {
            // Don't need r anymore; just test whether s is too large by 1.
            s_var = kernel_var::sub_var(mcx, &s_var, &one)?;
        }

        debug_assert_eq!(src_idx, src_ndigits);
        step -= 1;
    }

    // Construct the final result, rounding to the requested precision.
    let mut result = s_var.clone();
    result.weight = res_weight;
    result.sign = NumericSign::Pos;
    kernel_var::round_var(&mut result, rscale);
    kernel_var::strip_var(&mut result);
    Ok(result)
}

/// Extract `blen` source NBASE digits of `arg` starting at `src_idx` into a
/// fresh `NumericVar` with `weight = blen - 1` (mirrors the C `memcpy` + fixup
/// + `strip_var`), or a zero (dscale 0) if past the end of `arg`'s digits.
fn extract_chunk<'mcx>(
    mcx: Mcx<'mcx>,
    arg: &NumericVar<'_>,
    src_idx: i32,
    blen: i32,
) -> PgResult<NumericVar<'mcx>> {
    let arg_ndigits = arg.ndigits() as i32;
    if src_idx < arg_ndigits {
        let tmp_len = min(blen, arg_ndigits - src_idx) as usize;
        let arg_digits = arg.logical_digits();
        // alloc with one spare leading slot for rounding/strip headroom.
        let mut digits = crate::alloc_digits(mcx, tmp_len + 1)?;
        digits[0] = 0;
        for i in 0..tmp_len {
            digits[1 + i] = arg_digits[src_idx as usize + i];
        }
        let mut v = NumericVar {
            sign: NumericSign::Pos,
            weight: blen - 1,
            dscale: 0,
            digits,
            headroom: 1,
        };
        kernel_var::strip_var(&mut v);
        Ok(v)
    } else {
        let mut v = NumericVar::zero(mcx);
        v.dscale = 0;
        Ok(v)
    }
}

// ---------------------------------------------------------------------------
// exp_var (numeric.c:10900) -- e^arg via range-reduced Taylor series.
// ---------------------------------------------------------------------------

pub fn exp_var<'mcx>(
    mcx: Mcx<'mcx>,
    arg: &NumericVar<'_>,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let mut x = kernel_var::set_var_from_var(mcx, arg)?;

    // Estimate result dweight via double precision to choose the local rscale.
    let mut val = numericvar_to_double_no_overflow(&x);

    // Guard against overflow/underflow (if changed, see power_var()'s limit).
    if val.abs() >= (NUMERIC_MAX_RESULT_SCALE * 3) as f64 {
        if val > 0.0 {
            return Err(err_value_overflow());
        }
        let mut result = NumericVar::zero(mcx);
        result.dscale = rscale;
        return Ok(result);
    }

    // decimal weight = log10(e^x) = x * log10(e); literal per numeric.c:10936.
    #[allow(clippy::approx_constant)]
    let dweight = (val * 0.434294481903252) as i32;

    // Reduce x into ~[-0.01, 0.01] by dividing by 2^ndiv2 (ndiv2 <= 20).
    let ndiv2: i32;
    if val.abs() > 0.01 {
        let mut n = 1;
        val /= 2.0;
        while val.abs() > 0.01 {
            n += 1;
            val /= 2.0;
        }
        ndiv2 = n;
        let local_rscale = x.dscale + ndiv2;
        // div_var_int(&x, 1 << ndiv2, 0, &x, local_rscale, true).
        let divisor = int64_to_numericvar(mcx, 1i64 << ndiv2)?;
        x = kernel_var::div_var(mcx, &x, &divisor, local_rscale, true, true)?;
    } else {
        ndiv2 = 0;
    }

    // Scale for the Taylor series; literal log10(2) per numeric.c:10969.
    #[allow(clippy::approx_constant)]
    let mut sig_digits = 1 + dweight + rscale + (ndiv2 as f64 * 0.301029995663981) as i32;
    sig_digits = max(sig_digits, 0) + 8;
    let local_rscale = sig_digits - 1;

    // Taylor: exp(x) = 1 + x + x^2/2! + x^3/3! + ...
    let one = kernel_var::const_one(mcx);
    let mut result = kernel_var::add_var(mcx, &one, &x)?;

    let mut elem = kernel_var::mul_var(mcx, &x, &x, local_rscale)?;
    let mut ni = 2i32;
    elem = div_var_int_round(mcx, &elem, ni, local_rscale)?;

    while elem.ndigits() != 0 {
        result = kernel_var::add_var(mcx, &result, &elem)?;
        elem = kernel_var::mul_var(mcx, &elem, &x, local_rscale)?;
        ni += 1;
        elem = div_var_int_round(mcx, &elem, ni, local_rscale)?;
    }

    // Compensate for the range reduction (result weight doubles each square).
    let mut k = ndiv2;
    while k > 0 {
        k -= 1;
        let mut lr = sig_digits - result.weight * 2 * DEC_DIGITS;
        lr = max(lr, NUMERIC_MIN_DISPLAY_SCALE);
        result = kernel_var::mul_var(mcx, &result, &result, lr)?;
    }

    // Round to requested rscale.
    kernel_var::round_var(&mut result, rscale);
    Ok(result)
}

/// `div_var_int(var, ival, 0, result, rscale, round=true)` expressed via the
/// kernel_var `div_var` contract (see the module note). `ival` is a small
/// positive integer (Taylor-term denominator), so building it as a `NumericVar`
/// is bounded.
fn div_var_int_round<'mcx>(
    mcx: Mcx<'mcx>,
    var: &NumericVar<'_>,
    ival: i32,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let divisor = int64_to_numericvar(mcx, ival as i64)?;
    kernel_var::div_var(mcx, var, &divisor, rscale, true, true)
}

// ---------------------------------------------------------------------------
// ln_var (numeric.c:11111) -- natural log via range-reduced Taylor series.
// ---------------------------------------------------------------------------

pub fn ln_var<'mcx>(mcx: Mcx<'mcx>, arg: &NumericVar<'_>, rscale: i32) -> PgResult<NumericVar<'mcx>> {
    let zero = kernel_var::const_zero(mcx);
    let cmp = kernel_var::cmp_var(arg, &zero);
    if cmp == Ordering::Equal {
        return Err(PgError::error("cannot take logarithm of zero")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG));
    } else if cmp == Ordering::Less {
        return Err(PgError::error("cannot take logarithm of a negative number")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG));
    }

    let mut x = kernel_var::set_var_from_var(mcx, arg)?;
    let mut fact = kernel_var::const_two(mcx);
    let two = kernel_var::const_two(mcx);
    let zpn = kernel_var::const_zero_point_nine(mcx);
    let opo = kernel_var::const_one_point_one(mcx);
    let one = kernel_var::const_one(mcx);

    // Reduce x into 0.9 < x < 1.1 with repeated sqrt() (local_rscale may be < 0).
    let mut nsqrt = 0i32;
    while kernel_var::cmp_var(&x, &zpn) != Ordering::Greater {
        let local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        x = sqrt_var(mcx, &x, local_rscale)?;
        fact = kernel_var::mul_var(mcx, &fact, &two, 0)?;
        nsqrt += 1;
    }
    while kernel_var::cmp_var(&x, &opo) != Ordering::Less {
        let local_rscale = rscale - x.weight * DEC_DIGITS / 2 + 8;
        x = sqrt_var(mcx, &x, local_rscale)?;
        fact = kernel_var::mul_var(mcx, &fact, &two, 0)?;
        nsqrt += 1;
    }

    // Taylor series for 0.5*ln((1+z)/(1-z)) = z + z^3/3 + z^5/5 + ...
    // literal log10(2) per numeric.c:11186.
    #[allow(clippy::approx_constant)]
    let local_rscale = rscale + ((nsqrt + 1) as f64 * 0.301029995663981) as i32 + 8;

    // result = z = (x-1)/(x+1); xx = z; x = z^2.
    let mut result = kernel_var::sub_var(mcx, &x, &one)?;
    let denom = kernel_var::add_var(mcx, &x, &one)?;
    result = kernel_var::div_var(mcx, &result, &denom, local_rscale, true, false)?;
    let mut xx = result.clone();
    x = kernel_var::mul_var(mcx, &result, &result, local_rscale)?;

    let mut ni = 1i32;
    loop {
        ni += 2;
        xx = kernel_var::mul_var(mcx, &xx, &x, local_rscale)?;
        let elem = div_var_int_round(mcx, &xx, ni, local_rscale)?;

        if elem.ndigits() == 0 {
            break;
        }

        result = kernel_var::add_var(mcx, &result, &elem)?;

        if elem.weight < result.weight - local_rscale * 2 / DEC_DIGITS {
            break;
        }
    }

    // Compensate for range reduction, round to requested rscale.
    result = kernel_var::mul_var(mcx, &result, &fact, rscale)?;
    Ok(result)
}

// ---------------------------------------------------------------------------
// log_var (numeric.c:11229) -- logarithm of num in base. Chooses the dscale.
// ---------------------------------------------------------------------------

pub fn log_var<'mcx>(
    mcx: Mcx<'mcx>,
    base: &NumericVar<'_>,
    num: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    // Estimated dweights of ln(base), ln(num), and the result.
    let ln_base_dweight = estimate_ln_dweight(base)?;
    let ln_num_dweight = estimate_ln_dweight(num)?;
    let result_dweight = ln_num_dweight - ln_base_dweight;

    // Result scale: >= NUMERIC_MIN_SIG_DIGITS sig digits, >= either input dscale.
    let mut rscale = NUMERIC_MIN_SIG_DIGITS - result_dweight;
    rscale = max(rscale, base.dscale);
    rscale = max(rscale, num.dscale);
    rscale = max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    // ln(base), ln(num) computed with more digits than the result.
    let mut ln_base_rscale = rscale + result_dweight - ln_base_dweight + 8;
    ln_base_rscale = max(ln_base_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    let mut ln_num_rscale = rscale + result_dweight - ln_num_dweight + 8;
    ln_num_rscale = max(ln_num_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    let ln_base = ln_var(mcx, base, ln_base_rscale)?;
    let ln_num = ln_var(mcx, num, ln_num_rscale)?;

    // Divide and round to the required scale.
    kernel_var::div_var(mcx, &ln_num, &ln_base, rscale, true, false)
}

// ---------------------------------------------------------------------------
// power_var / power_var_int (numeric.c:11289 / :11451). Choose the dscale.
// ---------------------------------------------------------------------------

pub fn power_var<'mcx>(
    mcx: Mcx<'mcx>,
    base: &NumericVar<'_>,
    exp: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    // If exp is an exact integer that fits in i32, use power_var_int. The
    // rscale that power_var_int requires is computed there in C; here it is a
    // parameter, so we compute the same value and pass it (see power_var_int).
    if exp.ndigits() == 0 || (exp.ndigits() as i32) <= exp.weight + 1 {
        if let Some(expval64) = numericvar_to_int64(exp)? {
            if expval64 >= i32::MIN as i64 && expval64 <= i32::MAX as i64 {
                let rscale = power_var_int_rscale(base, expval64 as i32, exp.dscale);
                return power_var_int(mcx, base, expval64 as i32, exp.dscale, rscale);
            }
        }
    }

    // Avoid log(0) for 0 raised to a non-integer (0^0 handled by power_var_int).
    if kernel_var::cmp_var(base, &kernel_var::const_zero(mcx)) == Ordering::Equal {
        let mut result = kernel_var::const_zero(mcx);
        result.dscale = NUMERIC_MIN_SIG_DIGITS; // no need to round
        return Ok(result);
    }

    let mut res_sign = NumericSign::Pos;
    let mut abs_base_storage: NumericVar<'mcx>;
    let mut base_ref: &NumericVar<'_> = base;

    // Negative base: insist exp be an integer; result sign follows exp parity.
    if base.sign == NumericSign::Neg {
        if exp.ndigits() > 0 && (exp.ndigits() as i32) > exp.weight + 1 {
            return Err(PgError::error(
                "a negative number raised to a non-integer power yields a complex result",
            )
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION));
        }
        if exp.ndigits() > 0
            && (exp.ndigits() as i32) == exp.weight + 1
            && (exp.logical_digits()[exp.ndigits() - 1] & 1) != 0
        {
            res_sign = NumericSign::Neg;
        } else {
            res_sign = NumericSign::Pos;
        }
        abs_base_storage = kernel_var::set_var_from_var(mcx, base)?;
        abs_base_storage.sign = NumericSign::Pos;
        base_ref = &abs_base_storage;
    }

    // Low-precision estimate of the result weight via exp * ln(base).
    let ln_dweight = estimate_ln_dweight(base_ref)?;

    let mut local_rscale = 8 - ln_dweight;
    local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    let ln_base = ln_var(mcx, base_ref, local_rscale)?;
    let ln_num = kernel_var::mul_var(mcx, &ln_base, exp, local_rscale)?;

    let mut val = numericvar_to_double_no_overflow(&ln_num);

    // Initial overflow/underflow test with fuzz factor.
    if val.abs() > NUMERIC_MAX_RESULT_SCALE as f64 * 3.01 {
        if val > 0.0 {
            return Err(err_value_overflow());
        }
        let mut result = NumericVar::zero(mcx);
        result.dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return Ok(result);
    }

    // Approximate decimal result weight; literal log10(e) per numeric.c:11410.
    #[allow(clippy::approx_constant)]
    {
        val *= 0.434294481903252;
    }

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - val as i32;
    rscale = max(rscale, base_ref.dscale);
    rscale = max(rscale, exp.dscale);
    rscale = max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    let mut sig_digits = rscale + val as i32;
    sig_digits = max(sig_digits, 0);

    let mut local_rscale = sig_digits - ln_dweight + 8;
    local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

    // The real calculation.
    let ln_base = ln_var(mcx, base_ref, local_rscale)?;
    let ln_num = kernel_var::mul_var(mcx, &ln_base, exp, local_rscale)?;

    let mut result = exp_var(mcx, &ln_num, rscale)?;

    if res_sign == NumericSign::Neg && result.ndigits() > 0 {
        result.sign = NumericSign::Neg;
    }

    Ok(result)
}

/// Compute the result scale that C's `power_var_int` derives internally
/// (numeric.c:11521-11526), so `power_var` can supply it to the parameterized
/// `power_var_int` below. Mirrors the C `f`/rscale computation exactly.
fn power_var_int_rscale(base: &NumericVar<'_>, exp: i32, exp_dscale: i32) -> i32 {
    let f = power_var_int_f(base, exp);
    // C: `rscale = NUMERIC_MIN_SIG_DIGITS - (int) f`. `(int) f` of an
    // out-of-`int`-range `f` is UB in C but is immediately clamped by the
    // `max`/`min` below; saturate the cast and subtraction so an extreme
    // (under/overflowing) `f` cannot panic in debug builds.
    let mut rscale = NUMERIC_MIN_SIG_DIGITS.saturating_sub(f as i32);
    rscale = max(rscale, base.dscale);
    rscale = max(rscale, exp_dscale);
    rscale = max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = min(rscale, NUMERIC_MAX_DISPLAY_SCALE);
    rscale
}

/// The double-precision approximate decimal result weight `f` of `base^exp`
/// (numeric.c:11473-11497). Shared by `power_var_int` and the rscale helper.
fn power_var_int_f(base: &NumericVar<'_>, exp: i32) -> f64 {
    if base.ndigits() != 0 {
        // Choose f, p such that base ~= f * 10^p; f = exp * (log10(f) + p).
        let base_digits = base.logical_digits();
        let mut ff = base_digits[0] as f64;
        let mut p = base.weight * DEC_DIGITS;
        let mut i = 1usize;
        while i < base.ndigits() && (i as i32) * DEC_DIGITS < 16 {
            ff = ff * NBASE as f64 + base_digits[i] as f64;
            p -= DEC_DIGITS;
            i += 1;
        }
        exp as f64 * (ff.log10() + p as f64)
    } else {
        0.0 // result is 0 or 1 (weight 0), or error
    }
}

/// `power_var_int(base, exp, exp_dscale, result)`: `base^exp` for integer `exp`.
///
/// numeric.c:11451. C derives the result scale internally; the scaffold lifts
/// it to the `rscale` parameter, so the caller (`power_var`) computes it via
/// [`power_var_int_rscale`] and passes the identical value. The double-precision
/// `f` is still recomputed here for the overflow/underflow guards and the
/// `sig_digits` estimate, exactly as C does.
pub fn power_var_int<'mcx>(
    mcx: Mcx<'mcx>,
    base: &NumericVar<'_>,
    exp: i32,
    exp_dscale: i32,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let _ = exp_dscale; // folded into `rscale` by the caller (see doc).

    // Approximate decimal result weight (for overflow tests + sig_digits).
    let f = power_var_int_f(base, exp);

    // Overflow/underflow tests with fuzz factors.
    if f > (NUMERIC_WEIGHT_MAX + 1) as f64 * DEC_DIGITS as f64 {
        return Err(err_value_overflow());
    }
    if f + 1.0 < -(NUMERIC_MAX_DISPLAY_SCALE as f64) {
        let mut result = NumericVar::zero(mcx);
        result.dscale = NUMERIC_MAX_DISPLAY_SCALE;
        return Ok(result);
    }

    // Common/corner cases.
    match exp {
        0 => {
            // 0^0 == 1 (SQL:2003).
            let mut result = kernel_var::const_one(mcx);
            result.dscale = rscale; // no need to round
            return Ok(result);
        }
        1 => {
            let mut result = kernel_var::set_var_from_var(mcx, base)?;
            kernel_var::round_var(&mut result, rscale);
            return Ok(result);
        }
        -1 => {
            let one = kernel_var::const_one(mcx);
            return kernel_var::div_var(mcx, &one, base, rscale, true, true);
        }
        2 => {
            return kernel_var::mul_var(mcx, base, base, rscale);
        }
        _ => {}
    }

    // Base is zero (and exp not handled above).
    if base.ndigits() == 0 {
        if exp < 0 {
            return Err(err_div_by_zero());
        }
        let mut result = NumericVar::zero(mcx);
        result.dscale = rscale;
        return Ok(result);
    }

    // General case: square-and-multiply over the bit pattern of exp.
    let mut sig_digits = 1 + rscale + f as i32;
    // Mirror C's `(int) log(fabs((double) exp))` (numeric.c:11578).
    sig_digits += (exp as f64).abs().ln() as i32 + 8;

    let mut neg = exp < 0;
    let mut mask = exp.unsigned_abs();

    let mut base_prod = kernel_var::set_var_from_var(mcx, base)?;

    let mut result = if mask & 1 != 0 {
        kernel_var::set_var_from_var(mcx, base)?
    } else {
        kernel_var::const_one(mcx)
    };

    mask >>= 1;
    while mask > 0 {
        let mut local_rscale = sig_digits - 2 * base_prod.weight * DEC_DIGITS;
        local_rscale = min(local_rscale, 2 * base_prod.dscale);
        local_rscale = max(local_rscale, NUMERIC_MIN_DISPLAY_SCALE);

        base_prod = kernel_var::mul_var(mcx, &base_prod, &base_prod, local_rscale)?;

        if mask & 1 != 0 {
            let mut lr = sig_digits - (base_prod.weight + result.weight) * DEC_DIGITS;
            lr = min(lr, base_prod.dscale + result.dscale);
            lr = max(lr, NUMERIC_MIN_DISPLAY_SCALE);
            result = kernel_var::mul_var(mcx, &base_prod, &result, lr)?;
        }

        // Give up early once the weight is guaranteed to overflow.
        if base_prod.weight > NUMERIC_WEIGHT_MAX || result.weight > NUMERIC_WEIGHT_MAX {
            if !neg {
                return Err(err_value_overflow());
            }
            // neg: result underflows to 0.
            result = NumericVar::zero(mcx);
            neg = false;
            break;
        }

        mask >>= 1;
    }

    // Compensate for input sign, round to requested rscale.
    if neg {
        let one = kernel_var::const_one(mcx);
        result = kernel_var::div_var(mcx, &one, &result, rscale, true, false)?;
    } else {
        kernel_var::round_var(&mut result, rscale);
    }
    Ok(result)
}
