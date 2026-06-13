//! Family: base-NBASE arithmetic kernel over [`NumericVar`]`<'mcx>`.
//!
//! Mirrors numeric.c's `NumericVar` lifecycle + the core integer-ish kernels:
//! preinitialized constants, comparison (`cmp_var`/`cmp_abs`), add/sub
//! (`add_var`/`sub_var`/`add_abs`/`sub_abs`), rounding/truncation in place
//! (`round_var`/`trunc_var`/`strip_var`), multiply (`mul_var`), and divide/mod/
//! floor/ceil (`div_var`/`div_var_int`/`mod_var`/`div_mod_var`/`floor_var`/
//! `ceil_var`).
//!
//! Every fn that grows a digit buffer takes an explicit `Mcx<'mcx>` (no ambient
//! context) and returns [`PgResult`] where the C `ereport`s on overflow/OOM.
//!
//! # The `buf`/`digits` spare-digit model
//!
//! C's `NumericVar` keeps the palloc'd buffer (`buf`) separate from the logical
//! digit start (`digits = buf + 1`), reserving `buf[0]` as a zeroed spare for
//! rounding carry-out. Here the digit buffer is the charged `PgVec`
//! [`NumericVar::digits`]; the first [`NumericVar::headroom`] entries are that
//! reserved zeroed slack and the logical digits are `digits[headroom..]`. The
//! C idiom `var->digits--` (absorbing a carry-out into the spare) becomes
//! `headroom -= 1`; `digits++` (stripping a leading zero) becomes
//! `headroom += 1`.

use core::cmp::Ordering;

use mcx::{Mcx, PgVec};
use types_error::{PgResult, ERRCODE_DIVISION_BY_ZERO};
use types_numeric::var::{NumericSign, NumericVar};
use types_numeric::{
    NumericDigit, DEC_DIGITS, DIV_GUARD_DIGITS, HALF_NBASE, MUL_GUARD_DIGITS, NBASE, NBASE_SQR,
    NUMERIC_MAX_DISPLAY_SCALE, NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MIN_SIG_DIGITS,
};

// ---------------------------------------------------------------------------
// numeric.c round_powers[] (DEC_DIGITS == 4).
// ---------------------------------------------------------------------------

/// `round_powers[4]` (numeric.c:470) — used to round/truncate within the last
/// NBASE digit.
const ROUND_POWERS: [i32; 4] = [0, 1000, 100, 10];

// ---------------------------------------------------------------------------
// Internal allocation helper mirroring C's digitbuf_alloc(ndigits + 1) idiom:
// a buffer of `ndigits + 1` entries with one leading zeroed spare digit
// (headroom = 1) and `ndigits` logical digits.
// ---------------------------------------------------------------------------

/// Allocate a fresh result var with `ndigits` logical digits plus one leading
/// zeroed spare (headroom = 1), all digits zeroed. Mirrors `alloc_var`'s
/// `digitbuf_alloc(ndigits + 1); buf[0] = 0; digits = buf + 1`.
fn alloc_result<'mcx>(mcx: Mcx<'mcx>, ndigits: usize) -> PgResult<NumericVar<'mcx>> {
    let mut digits = mcx::vec_with_capacity_in::<NumericDigit>(mcx, ndigits + 1)?;
    digits.resize(ndigits + 1, 0);
    Ok(NumericVar {
        sign: NumericSign::Pos,
        weight: 0,
        dscale: 0,
        digits,
        headroom: 1,
    })
}

/// `zero_var` (numeric.c:7104): set a variable to ZERO. The C resets the digit
/// buffer; here we produce a fresh zero in `mcx`. Note: dscale is set by the
/// caller (C leaves it untouched on the reused var; our callers always overwrite
/// it right after).
#[inline]
fn zero_var(mcx: Mcx<'_>) -> NumericVar<'_> {
    NumericVar::zero(mcx)
}

// ---------------------------------------------------------------------------
// Preinitialized constants (numeric.c:424-466 const_zero..const_one_point_one).
// Allocated fresh in `mcx`. The C statics store {ndigits, weight, sign, dscale,
// buf=NULL, digits}; here we materialize the logical digits into a PgVec with
// headroom = 0 (the constants are never carry-extended in place).
// ---------------------------------------------------------------------------

fn const_var<'mcx>(
    mcx: Mcx<'mcx>,
    data: &[NumericDigit],
    weight: i32,
    sign: NumericSign,
    dscale: i32,
) -> NumericVar<'mcx> {
    let mut digits = PgVec::new_in(mcx);
    digits.extend_from_slice(data);
    NumericVar { sign, weight, dscale, digits, headroom: 0 }
}

pub fn const_zero(mcx: Mcx<'_>) -> NumericVar<'_> {
    // {0, 0, NUMERIC_POS, 0, NULL, {0}} — ndigits 0, so no logical digits.
    const_var(mcx, &[], 0, NumericSign::Pos, 0)
}

pub fn const_one(mcx: Mcx<'_>) -> NumericVar<'_> {
    // {1, 0, NUMERIC_POS, 0, NULL, {1}}
    const_var(mcx, &[1], 0, NumericSign::Pos, 0)
}

pub fn const_minus_one(mcx: Mcx<'_>) -> NumericVar<'_> {
    // {1, 0, NUMERIC_NEG, 0, NULL, {1}}
    const_var(mcx, &[1], 0, NumericSign::Neg, 0)
}

pub fn const_two(mcx: Mcx<'_>) -> NumericVar<'_> {
    // {1, 0, NUMERIC_POS, 0, NULL, {2}}
    const_var(mcx, &[2], 0, NumericSign::Pos, 0)
}

pub fn const_ten(mcx: Mcx<'_>) -> NumericVar<'_> {
    // numeric.c has no const_ten static; ten = {10} at weight 0 (DEC_DIGITS==4).
    const_var(mcx, &[10], 0, NumericSign::Pos, 0)
}

pub fn const_zero_point_nine(mcx: Mcx<'_>) -> NumericVar<'_> {
    // {1, -1, NUMERIC_POS, 1, NULL, {9000}} (DEC_DIGITS == 4)
    const_var(mcx, &[9000], -1, NumericSign::Pos, 1)
}

pub fn const_one_point_one(mcx: Mcx<'_>) -> NumericVar<'_> {
    // {2, 0, NUMERIC_POS, 1, NULL, {1, 1000}} (DEC_DIGITS == 4)
    const_var(mcx, &[1, 1000], 0, NumericSign::Pos, 1)
}

// ---------------------------------------------------------------------------
// Lifecycle helpers (alloc_var/set_var_from_var) — internal but shared across
// the kernel; sized in `mcx`.
// ---------------------------------------------------------------------------

/// `alloc_var(var, ndigits)` (numeric.c:7072): (re)allocate the digit buffer to
/// hold `ndigits` logical digits (plus a spare digit for rounding), charged to
/// `mcx`. The logical digits are left zeroed.
pub fn alloc_var<'mcx>(mcx: Mcx<'mcx>, ndigits: usize) -> PgResult<NumericVar<'mcx>> {
    alloc_result(mcx, ndigits)
}

/// `set_var_from_var(value, dest)` (numeric.c:7587): deep-copy `src` into a fresh
/// var in `mcx`. C allocates `ndigits + 1`, sets the spare digit to 0, and
/// memcpy's the logical digits — so the copy has headroom = 1.
pub fn set_var_from_var<'mcx>(mcx: Mcx<'mcx>, src: &NumericVar<'_>) -> PgResult<NumericVar<'mcx>> {
    let ndigits = src.ndigits();
    let mut newvar = alloc_result(mcx, ndigits)?;
    if ndigits > 0 {
        newvar.digits[1..].copy_from_slice(src.logical_digits());
    }
    newvar.sign = src.sign;
    newvar.weight = src.weight;
    newvar.dscale = src.dscale;
    Ok(newvar)
}

// ---------------------------------------------------------------------------
// Comparison (numeric.c cmp_var/cmp_var_common/cmp_abs/cmp_abs_common). Pure;
// no allocation. Returns are mapped from the C int (-1/0/1) to `Ordering`.
// ---------------------------------------------------------------------------

/// `cmp_var` (numeric.c:8492). We assume zeroes have been truncated to no
/// digits.
pub fn cmp_var(var1: &NumericVar<'_>, var2: &NumericVar<'_>) -> Ordering {
    cmp_var_common(
        var1.logical_digits(),
        var1.weight,
        var1.sign.to_numeric_word() as i32,
        var2.logical_digits(),
        var2.weight,
        var2.sign.to_numeric_word() as i32,
    )
}

/// `cmp_var_common` (numeric.c:8507): main routine of `cmp_var`. Usable by both
/// `NumericVar` and `Numeric`. `var{1,2}sign` are the raw `NUMERIC_*` sign words
/// (`NUMERIC_POS`/`NUMERIC_NEG`); only the negative-ness matters here.
pub fn cmp_var_common(
    var1digits: &[NumericDigit],
    var1weight: i32,
    var1sign: i32,
    var2digits: &[NumericDigit],
    var2weight: i32,
    var2sign: i32,
) -> Ordering {
    let var1ndigits = var1digits.len();
    let var2ndigits = var2digits.len();
    let neg = NumericSign::Neg.to_numeric_word() as i32;
    let pos = NumericSign::Pos.to_numeric_word() as i32;

    if var1ndigits == 0 {
        if var2ndigits == 0 {
            return Ordering::Equal;
        }
        if var2sign == neg {
            return Ordering::Greater;
        }
        return Ordering::Less;
    }
    if var2ndigits == 0 {
        if var1sign == pos {
            return Ordering::Greater;
        }
        return Ordering::Less;
    }

    if var1sign == pos {
        if var2sign == neg {
            return Ordering::Greater;
        }
        return cmp_abs_common(var1digits, var1weight, var2digits, var2weight);
    }

    if var2sign == pos {
        return Ordering::Less;
    }

    cmp_abs_common(var2digits, var2weight, var1digits, var1weight)
}

/// `cmp_abs` (numeric.c:11864): compare the absolute values of `var1` and
/// `var2`.
pub fn cmp_abs(var1: &NumericVar<'_>, var2: &NumericVar<'_>) -> Ordering {
    cmp_abs_common(
        var1.logical_digits(),
        var1.weight,
        var2.logical_digits(),
        var2.weight,
    )
}

/// `cmp_abs_common` (numeric.c:11878): main routine of `cmp_abs`.
pub fn cmp_abs_common(
    var1digits: &[NumericDigit],
    var1weight: i32,
    var2digits: &[NumericDigit],
    var2weight: i32,
) -> Ordering {
    let var1ndigits = var1digits.len();
    let var2ndigits = var2digits.len();
    let mut i1: usize = 0;
    let mut i2: usize = 0;
    let mut var1weight = var1weight;
    let mut var2weight = var2weight;

    /* Check any digits before the first common digit */
    while var1weight > var2weight && i1 < var1ndigits {
        if var1digits[i1] != 0 {
            return Ordering::Greater;
        }
        i1 += 1;
        var1weight -= 1;
    }
    while var2weight > var1weight && i2 < var2ndigits {
        if var2digits[i2] != 0 {
            return Ordering::Less;
        }
        i2 += 1;
        var2weight -= 1;
    }

    /* At this point, either w1 == w2 or we've run out of digits */
    if var1weight == var2weight {
        while i1 < var1ndigits && i2 < var2ndigits {
            let stat = var1digits[i1] as i32 - var2digits[i2] as i32;
            i1 += 1;
            i2 += 1;
            if stat != 0 {
                if stat > 0 {
                    return Ordering::Greater;
                }
                return Ordering::Less;
            }
        }
    }

    /*
     * At this point, we've run out of digits on one side or the other; so any
     * remaining nonzero digits imply that side is larger.
     */
    while i1 < var1ndigits {
        if var1digits[i1] != 0 {
            return Ordering::Greater;
        }
        i1 += 1;
    }
    while i2 < var2ndigits {
        if var2digits[i2] != 0 {
            return Ordering::Less;
        }
        i2 += 1;
    }

    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Add / subtract (numeric.c add_var/sub_var/add_abs/sub_abs).
// ---------------------------------------------------------------------------

/// `add_var` (numeric.c:8550): full version of add functionality on variable
/// level (handling signs).
pub fn add_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    /* Decide on the signs of the two variables what to do */
    if var1.sign == NumericSign::Pos {
        if var2.sign == NumericSign::Pos {
            /* Both are positive result = +(ABS(var1) + ABS(var2)) */
            let mut result = add_abs(mcx, var1, var2)?;
            result.sign = NumericSign::Pos;
            Ok(result)
        } else {
            /* var1 is positive, var2 is negative: compare absolute values */
            match cmp_abs(var1, var2) {
                Ordering::Equal => {
                    let mut result = zero_var(mcx);
                    result.dscale = var1.dscale.max(var2.dscale);
                    Ok(result)
                }
                Ordering::Greater => {
                    let mut result = sub_abs(mcx, var1, var2)?;
                    result.sign = NumericSign::Pos;
                    Ok(result)
                }
                Ordering::Less => {
                    let mut result = sub_abs(mcx, var2, var1)?;
                    result.sign = NumericSign::Neg;
                    Ok(result)
                }
            }
        }
    } else if var2.sign == NumericSign::Pos {
        /* var1 is negative, var2 is positive: compare absolute values */
        match cmp_abs(var1, var2) {
            Ordering::Equal => {
                let mut result = zero_var(mcx);
                result.dscale = var1.dscale.max(var2.dscale);
                Ok(result)
            }
            Ordering::Greater => {
                let mut result = sub_abs(mcx, var1, var2)?;
                result.sign = NumericSign::Neg;
                Ok(result)
            }
            Ordering::Less => {
                let mut result = sub_abs(mcx, var2, var1)?;
                result.sign = NumericSign::Pos;
                Ok(result)
            }
        }
    } else {
        /* Both are negative result = -(ABS(var1) + ABS(var2)) */
        let mut result = add_abs(mcx, var1, var2)?;
        result.sign = NumericSign::Neg;
        Ok(result)
    }
}

/// `sub_var` (numeric.c:8667): full version of sub functionality on variable
/// level (handling signs).
pub fn sub_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    /* Decide on the signs of the two variables what to do */
    if var1.sign == NumericSign::Pos {
        if var2.sign == NumericSign::Neg {
            /* var1 positive, var2 negative: result = +(ABS(var1) + ABS(var2)) */
            let mut result = add_abs(mcx, var1, var2)?;
            result.sign = NumericSign::Pos;
            Ok(result)
        } else {
            /* Both positive: compare absolute values */
            match cmp_abs(var1, var2) {
                Ordering::Equal => {
                    let mut result = zero_var(mcx);
                    result.dscale = var1.dscale.max(var2.dscale);
                    Ok(result)
                }
                Ordering::Greater => {
                    let mut result = sub_abs(mcx, var1, var2)?;
                    result.sign = NumericSign::Pos;
                    Ok(result)
                }
                Ordering::Less => {
                    let mut result = sub_abs(mcx, var2, var1)?;
                    result.sign = NumericSign::Neg;
                    Ok(result)
                }
            }
        }
    } else if var2.sign == NumericSign::Neg {
        /* Both negative: compare absolute values */
        match cmp_abs(var1, var2) {
            Ordering::Equal => {
                let mut result = zero_var(mcx);
                result.dscale = var1.dscale.max(var2.dscale);
                Ok(result)
            }
            Ordering::Greater => {
                let mut result = sub_abs(mcx, var1, var2)?;
                result.sign = NumericSign::Neg;
                Ok(result)
            }
            Ordering::Less => {
                let mut result = sub_abs(mcx, var2, var1)?;
                result.sign = NumericSign::Pos;
                Ok(result)
            }
        }
    } else {
        /* var1 negative, var2 positive: result = -(ABS(var1) + ABS(var2)) */
        let mut result = add_abs(mcx, var1, var2)?;
        result.sign = NumericSign::Neg;
        Ok(result)
    }
}

/// `add_abs` (numeric.c:11942): add the absolute values of two variables.
pub fn add_abs<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let var1digits = var1.logical_digits();
    let var2digits = var2.logical_digits();
    let var1ndigits = var1digits.len() as i32;
    let var2ndigits = var2digits.len() as i32;

    let res_weight = var1.weight.max(var2.weight) + 1;
    let res_dscale = var1.dscale.max(var2.dscale);

    /* Note: here we are figuring rscale in base-NBASE digits */
    let rscale1 = var1ndigits - var1.weight - 1;
    let rscale2 = var2ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);

    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    let mut result = alloc_result(mcx, res_ndigits as usize)?;
    let res_digits = &mut result.digits[1..]; // logical digits (headroom == 1)

    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    let mut carry: i32 = 0;
    for i in (0..res_ndigits).rev() {
        i1 -= 1;
        i2 -= 1;
        if i1 >= 0 && i1 < var1ndigits {
            carry += var1digits[i1 as usize] as i32;
        }
        if i2 >= 0 && i2 < var2ndigits {
            carry += var2digits[i2 as usize] as i32;
        }

        if carry >= NBASE {
            res_digits[i as usize] = (carry - NBASE) as NumericDigit;
            carry = 1;
        } else {
            res_digits[i as usize] = carry as NumericDigit;
            carry = 0;
        }
    }

    debug_assert_eq!(carry, 0); /* else we failed to allow for carry out */

    result.weight = res_weight;
    result.dscale = res_dscale;

    /* Remove leading/trailing zeroes */
    strip_var(&mut result);
    Ok(result)
}

/// `sub_abs` (numeric.c:12027): subtract ABS(var2) from ABS(var1). ABS(var1)
/// MUST BE GREATER OR EQUAL ABS(var2).
pub fn sub_abs<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let var1digits = var1.logical_digits();
    let var2digits = var2.logical_digits();
    let var1ndigits = var1digits.len() as i32;
    let var2ndigits = var2digits.len() as i32;

    let res_weight = var1.weight;
    let res_dscale = var1.dscale.max(var2.dscale);

    /* Note: here we are figuring rscale in base-NBASE digits */
    let rscale1 = var1ndigits - var1.weight - 1;
    let rscale2 = var2ndigits - var2.weight - 1;
    let res_rscale = rscale1.max(rscale2);

    let mut res_ndigits = res_rscale + res_weight + 1;
    if res_ndigits <= 0 {
        res_ndigits = 1;
    }

    let mut result = alloc_result(mcx, res_ndigits as usize)?;
    let res_digits = &mut result.digits[1..]; // logical digits (headroom == 1)

    let mut i1 = res_rscale + var1.weight + 1;
    let mut i2 = res_rscale + var2.weight + 1;
    let mut borrow: i32 = 0;
    for i in (0..res_ndigits).rev() {
        i1 -= 1;
        i2 -= 1;
        if i1 >= 0 && i1 < var1ndigits {
            borrow += var1digits[i1 as usize] as i32;
        }
        if i2 >= 0 && i2 < var2ndigits {
            borrow -= var2digits[i2 as usize] as i32;
        }

        if borrow < 0 {
            res_digits[i as usize] = (borrow + NBASE) as NumericDigit;
            borrow = -1;
        } else {
            res_digits[i as usize] = borrow as NumericDigit;
            borrow = 0;
        }
    }

    debug_assert_eq!(borrow, 0); /* else caller gave us var1 < var2 */

    result.weight = res_weight;
    result.dscale = res_dscale;

    /* Remove leading/trailing zeroes */
    strip_var(&mut result);
    Ok(result)
}

// ---------------------------------------------------------------------------
// Round / truncate / strip in place (numeric.c round_var/trunc_var/strip_var).
// ---------------------------------------------------------------------------

/// `round_var(var, rscale)` (numeric.c:12109): round in place to `rscale`
/// decimal digits after the point. `rscale < 0` rounds before the point.
pub fn round_var(var: &mut NumericVar<'_>, rscale: i32) {
    var.dscale = rscale;

    /* decimal digits wanted */
    let mut di = (var.weight + 1) * DEC_DIGITS + rscale;

    /*
     * If di = 0, the value loses all digits, but could round up to 1 if its
     * first extra digit is >= 5. If di < 0 the result must be 0.
     */
    if di < 0 {
        var.digits.truncate(var.headroom);
        var.weight = 0;
        var.sign = NumericSign::Pos;
        return;
    }

    /* NBASE digits wanted */
    let mut ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

    /* 0, or number of decimal digits to keep in last NBASE digit */
    di %= DEC_DIGITS;

    let cur_ndigits = var.ndigits() as i32;
    if ndigits < cur_ndigits || (ndigits == cur_ndigits && di > 0) {
        /*
         * C sets `var->ndigits = ndigits` here; the dropped digits remain in
         * the underlying buffer so the round decision can read `digits[ndigits]`
         * (the first discarded digit). We mirror that by reading/writing against
         * the still-full buffer and only fixing the logical length at the end.
         * `final_ndigits` is the logical run length (== C's var->ndigits); the
         * local `ndigits` is decremented for the within-digit/carry passes.
         */
        let final_ndigits = ndigits;

        // `digits[base + k]` indexes the logical digit run.
        let base = var.headroom;
        let mut carry: i32;

        if di == 0 {
            carry = if var.digits[base + ndigits as usize] >= HALF_NBASE as NumericDigit {
                1
            } else {
                0
            };
        } else {
            /* Must round within last NBASE digit */
            let pow10 = ROUND_POWERS[di as usize];
            ndigits -= 1;
            let extra = var.digits[base + ndigits as usize] as i32 % pow10;
            var.digits[base + ndigits as usize] -= extra as NumericDigit;
            carry = 0;
            if extra >= pow10 / 2 {
                let mut p = pow10 + var.digits[base + ndigits as usize] as i32;
                if p >= NBASE {
                    p -= NBASE;
                    carry = 1;
                }
                var.digits[base + ndigits as usize] = p as NumericDigit;
            }
        }

        /*
         * Propagate carry if needed. C accesses `digits[--ndigits]`, which when
         * ndigits reaches 0 reads/writes `digits[-1]` == the spare slot
         * (`buf[0]`); in our model that is `var.digits[base - 1]` (index 0 when
         * headroom == 1). The carry can propagate at most into that spare digit
         * (ndigits == -1), guaranteed by alloc_var reserving it.
         */
        while carry != 0 {
            ndigits -= 1;
            debug_assert!(ndigits >= -1);
            let idx = (base as i32 + ndigits) as usize;
            carry += var.digits[idx] as i32;
            if carry >= NBASE {
                var.digits[idx] = (carry - NBASE) as NumericDigit;
                carry = 1;
            } else {
                var.digits[idx] = carry as NumericDigit;
                carry = 0;
            }
            if ndigits < 0 {
                break;
            }
        }

        if ndigits < 0 {
            debug_assert_eq!(ndigits, -1); /* better not have added > 1 digit */
            debug_assert!(var.headroom > 0); /* Assert(var->digits > var->buf) */
            /*
             * C: var->digits--; var->ndigits++; var->weight++ — the spare digit
             * (now holding the carry-out) becomes the leading logical digit.
             */
            var.headroom -= 1;
            var.weight += 1;
            /* logical run is now final_ndigits + 1, starting one earlier */
            var.digits.truncate(var.headroom + final_ndigits as usize + 1);
        } else {
            /* Logical run length is the rounded count (C's var->ndigits). */
            var.digits.truncate(var.headroom + final_ndigits as usize);
        }
    }
}

/// `trunc_var(var, rscale)` (numeric.c:12215): truncate (towards zero) in place
/// at `rscale` decimal digits after the point. `rscale < 0` truncates before
/// the point.
pub fn trunc_var(var: &mut NumericVar<'_>, rscale: i32) {
    var.dscale = rscale;

    /* decimal digits wanted */
    let mut di = (var.weight + 1) * DEC_DIGITS + rscale;

    /* If di <= 0, the value loses all digits. */
    if di <= 0 {
        var.digits.truncate(var.headroom);
        var.weight = 0;
        var.sign = NumericSign::Pos;
        return;
    }

    /* NBASE digits wanted */
    let ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;

    if ndigits <= var.ndigits() as i32 {
        var.digits.truncate(var.headroom + ndigits as usize);

        /* 0, or number of decimal digits to keep in last NBASE digit */
        di %= DEC_DIGITS;

        if di > 0 {
            /* Must truncate within last NBASE digit */
            let pow10 = ROUND_POWERS[di as usize];
            let last = var.headroom + ndigits as usize - 1;
            let extra = var.digits[last] as i32 % pow10;
            var.digits[last] -= extra as NumericDigit;
        }
    }
}

/// `strip_var(var)` (numeric.c:12277): strip leading and trailing zero digits.
pub fn strip_var(var: &mut NumericVar<'_>) {
    /* Strip leading zeroes (C: digits++; weight--; ndigits--) */
    while var.ndigits() > 0 && var.digits[var.headroom] == 0 {
        var.headroom += 1;
        var.weight -= 1;
    }

    /* Strip trailing zeroes */
    while var.ndigits() > 0 && var.digits[var.digits.len() - 1] == 0 {
        var.digits.pop();
    }

    /* If it's zero, normalize the sign and weight */
    if var.ndigits() == 0 {
        var.sign = NumericSign::Pos;
        var.weight = 0;
    }
}

// ---------------------------------------------------------------------------
// Multiply (numeric.c mul_var + mul_var_short helper).
// ---------------------------------------------------------------------------

/// `mul_var` (numeric.c:8788): product of `var1 * var2`, rounded to no more than
/// `rscale` fractional digits.
pub fn mul_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    /*
     * Arrange for var1 to be the shorter of the two numbers. This improves
     * performance because the inner multiplication loop is much simpler than
     * the outer loop.
     */
    let (var1, var2) = if var1.ndigits() > var2.ndigits() {
        (var2, var1)
    } else {
        (var1, var2)
    };

    let var1ndigits = var1.ndigits() as i32;
    let var2ndigits = var2.ndigits() as i32;
    let var1digits = var1.logical_digits();
    let var2digits = var2.logical_digits();

    if var1ndigits == 0 {
        /* one or both inputs is zero; so is result */
        let mut result = zero_var(mcx);
        result.dscale = rscale;
        return Ok(result);
    }

    /*
     * If var1 has 1-6 digits and the exact result was requested, delegate to
     * mul_var_short() which uses a faster direct multiplication algorithm.
     */
    if var1ndigits <= 6 && rscale == var1.dscale + var2.dscale {
        return mul_var_short(mcx, var1, var2);
    }

    /* Determine result sign */
    let res_sign = if var1.sign == var2.sign {
        NumericSign::Pos
    } else {
        NumericSign::Neg
    };

    /* digit pairs in each input */
    let mut var1ndigitpairs = (var1ndigits + 1) / 2;
    let mut var2ndigitpairs = (var2ndigits + 1) / 2;

    /* digits in exact result */
    let mut res_ndigits = var1ndigits + var2ndigits;

    /* digit pairs in exact result with at least one extra output digit */
    let mut res_ndigitpairs = res_ndigits / 2 + 1;

    /* pair offset to align result to end of dig[] */
    let pair_offset = res_ndigitpairs - var1ndigitpairs - var2ndigitpairs + 1;

    /* maximum possible result weight (odd-length inputs shifted up below) */
    let res_weight = var1.weight + var2.weight + 1 + 2 * res_ndigitpairs
        - res_ndigits
        - (var1ndigits & 1)
        - (var2ndigits & 1);

    /* rscale-based truncation with at least one extra output digit */
    let maxdigits =
        res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS + MUL_GUARD_DIGITS;
    let maxdigitpairs = maxdigits / 2 + 1;

    res_ndigitpairs = res_ndigitpairs.min(maxdigitpairs);
    res_ndigits = 2 * res_ndigitpairs;

    if res_ndigitpairs <= pair_offset {
        /* All input digits will be ignored; so result is zero */
        let mut result = zero_var(mcx);
        result.dscale = rscale;
        return Ok(result);
    }
    var1ndigitpairs = var1ndigitpairs.min(res_ndigitpairs - pair_offset);
    var2ndigitpairs = var2ndigitpairs.min(res_ndigitpairs - pair_offset);

    /*
     * Arithmetic in an array `dig[]` of unsigned 64-bit integers, in base
     * NBASE^2. `var2digitpairs` holds var2 converted to base NBASE^2.
     */
    let res_ndigitpairs_u = res_ndigitpairs as usize;
    let var2ndigitpairs_u = var2ndigitpairs as usize;
    let mut dig: PgVec<u64> = mcx::vec_with_capacity_in(mcx, res_ndigitpairs_u)?;
    dig.resize(res_ndigitpairs_u, 0);
    let mut var2digitpairs: PgVec<u32> = mcx::vec_with_capacity_in(mcx, var2ndigitpairs_u)?;
    var2digitpairs.resize(var2ndigitpairs_u, 0);

    /* convert var2 to base NBASE^2, shifting up if its length is odd */
    {
        let mut i2 = 0;
        while i2 < var2ndigitpairs - 1 {
            var2digitpairs[i2 as usize] = var2digits[2 * i2 as usize] as u32 * NBASE as u32
                + var2digits[2 * i2 as usize + 1] as u32;
            i2 += 1;
        }
        if 2 * i2 + 1 < var2ndigits {
            var2digitpairs[i2 as usize] = var2digits[2 * i2 as usize] as u32 * NBASE as u32
                + var2digits[2 * i2 as usize + 1] as u32;
        } else {
            var2digitpairs[i2 as usize] = var2digits[2 * i2 as usize] as u32 * NBASE as u32;
        }
    }

    /*
     * Start by multiplying var2 by the least significant contributing digit
     * pair from var1, storing the results at the end of dig[].
     */
    let mut i1 = var1ndigitpairs - 1;
    let mut var1digitpair: u32 = if 2 * i1 + 1 < var1ndigits {
        var1digits[2 * i1 as usize] as u32 * NBASE as u32
            + var1digits[2 * i1 as usize + 1] as u32
    } else {
        var1digits[2 * i1 as usize] as u32 * NBASE as u32
    };
    let mut maxdig: u64 = var1digitpair as u64;

    let mut i2limit = var2ndigitpairs.min(res_ndigitpairs - i1 - pair_offset);
    let dig_i1_off_base = (i1 + pair_offset) as usize;

    /* memset(dig, 0, (i1 + pair_offset) * sizeof(uint64)) — already zeroed */
    for i2 in 0..i2limit {
        dig[dig_i1_off_base + i2 as usize] =
            var1digitpair as u64 * var2digitpairs[i2 as usize] as u64;
    }

    /*
     * Next, multiply var2 by the remaining digit pairs from var1, adding the
     * results to dig[] at the appropriate offsets, normalizing as needed.
     */
    i1 -= 1;
    while i1 >= 0 {
        var1digitpair = var1digits[2 * i1 as usize] as u32 * NBASE as u32
            + var1digits[2 * i1 as usize + 1] as u32;
        if var1digitpair == 0 {
            i1 -= 1;
            continue;
        }

        /* Time to normalize? */
        maxdig += var1digitpair as u64;
        if maxdig > (u64::MAX - u64::MAX / NBASE_SQR as u64) / (NBASE_SQR as u64 - 1) {
            /* Yes, do it (to base NBASE^2) */
            let mut carry: u64 = 0;
            for i in (0..res_ndigitpairs as usize).rev() {
                let mut newdig = dig[i] + carry;
                if newdig >= NBASE_SQR as u64 {
                    carry = newdig / NBASE_SQR as u64;
                    newdig -= carry * NBASE_SQR as u64;
                } else {
                    carry = 0;
                }
                dig[i] = newdig;
            }
            debug_assert_eq!(carry, 0);
            /* Reset maxdig to indicate new worst-case */
            maxdig = 1 + var1digitpair as u64;
        }

        /* Multiply and add */
        i2limit = var2ndigitpairs.min(res_ndigitpairs - i1 - pair_offset);
        let base = (i1 + pair_offset) as usize;
        for i2 in 0..i2limit {
            dig[base + i2 as usize] +=
                var1digitpair as u64 * var2digitpairs[i2 as usize] as u64;
        }

        i1 -= 1;
    }

    /*
     * Final carry propagation pass to normalize back to base NBASE^2, and
     * construct the base-NBASE result digits.
     */
    let mut result = alloc_result(mcx, res_ndigits as usize)?;
    {
        let res_digits = &mut result.digits[1..]; // logical (headroom == 1)
        let mut carry: u64 = 0;
        for i in (0..res_ndigitpairs as usize).rev() {
            let mut newdig = dig[i] + carry;
            if newdig >= NBASE_SQR as u64 {
                carry = newdig / NBASE_SQR as u64;
                newdig -= carry * NBASE_SQR as u64;
            } else {
                carry = 0;
            }
            res_digits[2 * i + 1] = ((newdig as u32) % NBASE as u32) as NumericDigit;
            res_digits[2 * i] = ((newdig as u32) / NBASE as u32) as NumericDigit;
        }
        debug_assert_eq!(carry, 0);
    }

    /* Finally, round the result to the requested precision. */
    result.weight = res_weight;
    result.sign = res_sign;

    /* Round to target rscale (and set result->dscale) */
    round_var(&mut result, rscale);

    /* Strip leading and trailing zeroes */
    strip_var(&mut result);
    Ok(result)
}

/// `mul_var_short` (numeric.c:9079): special-case multiplication used when var1
/// has 1-6 digits, var2 has at least as many digits as var1, and the exact
/// product is requested.
fn mul_var_short<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let var1ndigits = var1.ndigits();
    let var2ndigits = var2.ndigits();
    let var1digits = var1.logical_digits();
    let var2digits = var2.logical_digits();

    debug_assert!(var1ndigits >= 1);
    debug_assert!(var1ndigits <= 6);
    debug_assert!(var2ndigits >= var1ndigits);

    let res_sign = if var1.sign == var2.sign {
        NumericSign::Pos
    } else {
        NumericSign::Neg
    };
    let res_weight = var1.weight + var2.weight + 1;
    let res_ndigits = var1ndigits + var2ndigits;

    /* Allocate result digit array (res_ndigits + 1, spare digit at [0]) */
    let mut result = alloc_result(mcx, res_ndigits)?;

    // Helper indexers into the source digit slices (matching the C PRODSUMn).
    let v1 = |i: usize| var1digits[i] as u32;
    let v2 = |i: usize| var2digits[i] as u32;
    macro_rules! prodsum1 {
        ($i1:expr, $i2:expr) => {
            v1($i1) * v2($i2)
        };
    }
    macro_rules! prodsum2 {
        ($i1:expr, $i2:expr) => {
            prodsum1!($i1, $i2) + v1($i1 + 1) * v2($i2 - 1)
        };
    }
    macro_rules! prodsum3 {
        ($i1:expr, $i2:expr) => {
            prodsum2!($i1, $i2) + v1($i1 + 2) * v2($i2 - 2)
        };
    }
    macro_rules! prodsum4 {
        ($i1:expr, $i2:expr) => {
            prodsum3!($i1, $i2) + v1($i1 + 3) * v2($i2 - 3)
        };
    }
    macro_rules! prodsum5 {
        ($i1:expr, $i2:expr) => {
            prodsum4!($i1, $i2) + v1($i1 + 4) * v2($i2 - 4)
        };
    }
    macro_rules! prodsum6 {
        ($i1:expr, $i2:expr) => {
            prodsum5!($i1, $i2) + v1($i1 + 5) * v2($i2 - 5)
        };
    }

    let nbase = NBASE as u32;
    let mut carry: u32 = 0;
    let mut term: u32;

    // res_digits = result.digits[1..] (headroom == 1). We index it via `rd`.
    {
        let rd = &mut result.digits[1..];

        match var1ndigits {
            1 => {
                for i in (0..var2ndigits as i64).rev() {
                    let i = i as usize;
                    term = prodsum1!(0, i) + carry;
                    rd[i + 1] = (term % nbase) as NumericDigit;
                    carry = term / nbase;
                }
                rd[0] = carry as NumericDigit;
            }
            2 => {
                term = prodsum1!(1, var2ndigits - 1);
                rd[res_ndigits - 1] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                let mut i = var2ndigits as i64 - 1;
                while i >= 1 {
                    let iu = i as usize;
                    term = prodsum2!(0, iu) + carry;
                    rd[iu + 1] = (term % nbase) as NumericDigit;
                    carry = term / nbase;
                    i -= 1;
                }
            }
            3 => {
                term = prodsum1!(2, var2ndigits - 1);
                rd[res_ndigits - 1] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum2!(1, var2ndigits - 1) + carry;
                rd[res_ndigits - 2] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                let mut i = var2ndigits as i64 - 1;
                while i >= 2 {
                    let iu = i as usize;
                    term = prodsum3!(0, iu) + carry;
                    rd[iu + 1] = (term % nbase) as NumericDigit;
                    carry = term / nbase;
                    i -= 1;
                }
            }
            4 => {
                term = prodsum1!(3, var2ndigits - 1);
                rd[res_ndigits - 1] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum2!(2, var2ndigits - 1) + carry;
                rd[res_ndigits - 2] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum3!(1, var2ndigits - 1) + carry;
                rd[res_ndigits - 3] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                let mut i = var2ndigits as i64 - 1;
                while i >= 3 {
                    let iu = i as usize;
                    term = prodsum4!(0, iu) + carry;
                    rd[iu + 1] = (term % nbase) as NumericDigit;
                    carry = term / nbase;
                    i -= 1;
                }
            }
            5 => {
                term = prodsum1!(4, var2ndigits - 1);
                rd[res_ndigits - 1] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum2!(3, var2ndigits - 1) + carry;
                rd[res_ndigits - 2] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum3!(2, var2ndigits - 1) + carry;
                rd[res_ndigits - 3] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum4!(1, var2ndigits - 1) + carry;
                rd[res_ndigits - 4] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                let mut i = var2ndigits as i64 - 1;
                while i >= 4 {
                    let iu = i as usize;
                    term = prodsum5!(0, iu) + carry;
                    rd[iu + 1] = (term % nbase) as NumericDigit;
                    carry = term / nbase;
                    i -= 1;
                }
            }
            6 => {
                term = prodsum1!(5, var2ndigits - 1);
                rd[res_ndigits - 1] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum2!(4, var2ndigits - 1) + carry;
                rd[res_ndigits - 2] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum3!(3, var2ndigits - 1) + carry;
                rd[res_ndigits - 3] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum4!(2, var2ndigits - 1) + carry;
                rd[res_ndigits - 4] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                term = prodsum5!(1, var2ndigits - 1) + carry;
                rd[res_ndigits - 5] = (term % nbase) as NumericDigit;
                carry = term / nbase;
                let mut i = var2ndigits as i64 - 1;
                while i >= 5 {
                    let iu = i as usize;
                    term = prodsum6!(0, iu) + carry;
                    rd[iu + 1] = (term % nbase) as NumericDigit;
                    carry = term / nbase;
                    i -= 1;
                }
            }
            _ => unreachable!(),
        }

        /*
         * Finally, for var1ndigits > 1, compute the remaining var1ndigits most
         * significant result digits (fallthrough cascade).
         */
        if var1ndigits >= 6 {
            term = prodsum5!(0, 4) + carry;
            rd[5] = (term % nbase) as NumericDigit;
            carry = term / nbase;
        }
        if var1ndigits >= 5 {
            term = prodsum4!(0, 3) + carry;
            rd[4] = (term % nbase) as NumericDigit;
            carry = term / nbase;
        }
        if var1ndigits >= 4 {
            term = prodsum3!(0, 2) + carry;
            rd[3] = (term % nbase) as NumericDigit;
            carry = term / nbase;
        }
        if var1ndigits >= 3 {
            term = prodsum2!(0, 1) + carry;
            rd[2] = (term % nbase) as NumericDigit;
            carry = term / nbase;
        }
        if var1ndigits >= 2 {
            term = prodsum1!(0, 0) + carry;
            rd[1] = (term % nbase) as NumericDigit;
            rd[0] = (term / nbase) as NumericDigit;
        }
    }

    /* Store the product in result */
    result.weight = res_weight;
    result.sign = res_sign;
    result.dscale = var1.dscale + var2.dscale;

    /* Strip leading and trailing zeroes */
    strip_var(&mut result);
    Ok(result)
}

// ---------------------------------------------------------------------------
// Divide / mod / floor / ceil (numeric.c div_var/div_var_int/div_var_int64/
// mod_var/div_mod_var/floor_var/ceil_var/select_div_scale).
// ---------------------------------------------------------------------------

/// `div_var` (numeric.c:9366): compute `var1 / var2` to `rscale` fractional
/// digits. `round` chooses round vs truncate; `exact` chooses the exact vs
/// approximate (guard-digit) algorithm. Mirrors the C, including delegation to
/// the short-division helpers for 1-2 (and, with int128, 3-4) digit divisors.
pub fn div_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
    rscale: i32,
    round: bool,
    exact: bool,
) -> PgResult<NumericVar<'mcx>> {
    let var1ndigits = var1.ndigits() as i32;
    let var2ndigits = var2.ndigits() as i32;
    let var1digits = var1.logical_digits();
    let var2digits = var2.logical_digits();

    /*
     * First of all division by zero check; we must not be handed an
     * unnormalized divisor.
     */
    if var2ndigits == 0 || var2digits[0] == 0 {
        return Err(division_by_zero());
    }

    /*
     * If the divisor has just one or two digits, delegate to div_var_int(),
     * which uses fast short division. On platforms with 128-bit integer
     * support, delegate to div_var_int64() for divisors with three or four
     * digits.
     */
    if var2ndigits <= 2 {
        let mut idivisor = var2digits[0] as i32;
        let mut idivisor_weight = var2.weight;
        if var2ndigits == 2 {
            idivisor = idivisor * NBASE + var2digits[1] as i32;
            idivisor_weight -= 1;
        }
        if var2.sign == NumericSign::Neg {
            idivisor = -idivisor;
        }
        return div_var_int(mcx, var1, idivisor, idivisor_weight, rscale, round);
    }
    // HAVE_INT128 path: div_var_int64 for 3-4 digit divisors.
    if var2ndigits <= 4 {
        let mut idivisor: i64 = var2digits[0] as i64;
        let mut idivisor_weight = var2.weight;
        for i in 1..var2ndigits {
            idivisor = idivisor * NBASE as i64 + var2digits[i as usize] as i64;
            idivisor_weight -= 1;
        }
        if var2.sign == NumericSign::Neg {
            idivisor = -idivisor;
        }
        return div_var_int64(mcx, var1, idivisor, idivisor_weight, rscale, round);
    }

    /* Otherwise, perform full long division. */

    /* Result zero check */
    if var1ndigits == 0 {
        let mut result = zero_var(mcx);
        result.dscale = rscale;
        return Ok(result);
    }

    /*
     * Decide whether to do an exact computation. When var2 is shorter than the
     * threshold, the exact computation is faster.
     */
    let mut exact = exact;
    if var2ndigits <= 2 * (DIV_GUARD_DIGITS + 2) {
        exact = true;
    }

    /* Determine the result sign, weight and number of digits to calculate. */
    let res_sign = if var1.sign == var2.sign {
        NumericSign::Pos
    } else {
        NumericSign::Neg
    };
    let res_weight = var1.weight - var2.weight + 1;
    /* The number of accurate result digits we need to produce: */
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    /* ... but always at least 1 */
    res_ndigits = res_ndigits.max(1);
    /* If rounding needed, figure one more digit to ensure correct result */
    if round {
        res_ndigits += 1;
    }
    /* Add guard digits for roundoff error when producing approx result */
    if !exact {
        res_ndigits += DIV_GUARD_DIGITS;
    }

    /* Process input digits in pairs (base NBASE^2). */
    let mut var1ndigitpairs = (var1ndigits + 1) / 2;
    let mut var2ndigitpairs = (var2ndigits + 1) / 2;
    let res_ndigitpairs = (res_ndigits + 1) / 2;
    res_ndigits = 2 * res_ndigitpairs;

    let div_ndigitpairs;
    if exact {
        div_ndigitpairs = res_ndigitpairs + var2ndigitpairs;
        var1ndigitpairs = var1ndigitpairs.min(div_ndigitpairs);
    } else {
        div_ndigitpairs = res_ndigitpairs;
        var1ndigitpairs = var1ndigitpairs.min(div_ndigitpairs);
        var2ndigitpairs = var2ndigitpairs.min(div_ndigitpairs);
    }

    /*
     * Working dividend: `div_ndigitpairs + 1` 64-bit digits (one extra zeroed
     * digit so the main loop can read/write [qi+1] in the approximate case).
     */
    let div_len = (div_ndigitpairs + 1) as usize;
    let mut dividend: PgVec<i64> = mcx::vec_with_capacity_in(mcx, div_len)?;
    dividend.resize(div_len, 0);
    let var2pairs_u = var2ndigitpairs as usize;
    let mut divisor: PgVec<i32> = mcx::vec_with_capacity_in(mcx, var2pairs_u)?;
    divisor.resize(var2pairs_u, 0);

    /* load var1 into dividend[0 .. var1ndigitpairs-1], zeroing the rest */
    {
        let mut i = 0;
        while i < var1ndigitpairs - 1 {
            dividend[i as usize] = var1digits[2 * i as usize] as i64 * NBASE as i64
                + var1digits[2 * i as usize + 1] as i64;
            i += 1;
        }
        if 2 * i + 1 < var1ndigits {
            dividend[i as usize] = var1digits[2 * i as usize] as i64 * NBASE as i64
                + var1digits[2 * i as usize + 1] as i64;
        } else {
            dividend[i as usize] = var1digits[2 * i as usize] as i64 * NBASE as i64;
        }
        /* the rest is already zeroed (resize) */
    }

    /* load var2 into divisor[0 .. var2ndigitpairs-1] */
    {
        let mut i = 0;
        while i < var2ndigitpairs - 1 {
            divisor[i as usize] = var2digits[2 * i as usize] as i32 * NBASE
                + var2digits[2 * i as usize + 1] as i32;
            i += 1;
        }
        if 2 * i + 1 < var2ndigits {
            divisor[i as usize] = var2digits[2 * i as usize] as i32 * NBASE
                + var2digits[2 * i as usize + 1] as i32;
        } else {
            divisor[i as usize] = var2digits[2 * i as usize] as i32 * NBASE;
        }
    }

    /* Estimate quotient digits in floating point. */
    let mut fdivisor = divisor[0] as f64 * NBASE_SQR as f64;
    if var2ndigitpairs > 1 {
        fdivisor += divisor[1] as f64;
    }
    let fdivisorinverse = 1.0 / fdivisor;

    let mut maxdiv: i64 = 1;

    /* Outer loop computes next quotient digit, which goes in dividend[qi]. */
    for qi in 0..res_ndigitpairs as usize {
        /* Approximate the current dividend value */
        let mut fdividend = dividend[qi] as f64 * NBASE_SQR as f64;
        fdividend += dividend[qi + 1] as f64;

        /* Compute the (approximate) quotient digit */
        let mut fquotient = fdividend * fdivisorinverse;
        let mut qdigit: i32 = if fquotient >= 0.0 {
            fquotient as i32
        } else {
            (fquotient as i32) - 1 /* truncate towards -infinity */
        };

        if qdigit != 0 {
            /* Do we need to normalize now? */
            maxdiv += (qdigit as i64).abs();
            if maxdiv
                > (i64::MAX - i64::MAX / NBASE_SQR as i64 - 1) / (NBASE_SQR as i64 - 1)
            {
                let mut carry: i64 = 0;
                let start = (qi as i64 + var2ndigitpairs as i64 - 2)
                    .min(div_ndigitpairs as i64 - 1);
                let mut i = start;
                while i > qi as i64 {
                    let mut newdig = dividend[i as usize] + carry;
                    if newdig < 0 {
                        carry = -((-newdig - 1) / NBASE_SQR as i64) - 1;
                        newdig -= carry * NBASE_SQR as i64;
                    } else if newdig >= NBASE_SQR as i64 {
                        carry = newdig / NBASE_SQR as i64;
                        newdig -= carry * NBASE_SQR as i64;
                    } else {
                        carry = 0;
                    }
                    dividend[i as usize] = newdig;
                    i -= 1;
                }
                dividend[qi] += carry;

                /* reset maxdiv to 1 */
                maxdiv = 1;

                /* Recompute the quotient digit */
                fdividend = dividend[qi] as f64 * NBASE_SQR as f64;
                fdividend += dividend[qi + 1] as f64;
                fquotient = fdividend * fdivisorinverse;
                qdigit = if fquotient >= 0.0 {
                    fquotient as i32
                } else {
                    (fquotient as i32) - 1
                };

                maxdiv += (qdigit as i64).abs();
            }

            /* Subtract off the appropriate multiple of the divisor. */
            if qdigit != 0 {
                let istop = var2ndigitpairs.min(div_ndigitpairs - qi as i32);
                for i in 0..istop as usize {
                    dividend[qi + i] -= qdigit as i64 * divisor[i] as i64;
                }
            }
        }

        /*
         * The dividend digit we are about to replace might still be nonzero.
         * Fold it into the next digit position.
         */
        dividend[qi + 1] += dividend[qi].wrapping_mul(NBASE_SQR as i64);

        dividend[qi] = qdigit as i64;
    }

    /*
     * If an exact result was requested, use the remainder to correct the
     * approximate quotient.
     */
    if exact {
        let qi = res_ndigitpairs as usize; /* remainder starts at dividend[qi] */

        /* Normalize the remainder, expanding it down by one digit */
        {
            let mut carry: i64 = 0;
            let mut i = var2ndigitpairs - 2;
            while i >= 0 {
                let mut newdig = dividend[qi + i as usize] + carry;
                if newdig < 0 {
                    carry = -((-newdig - 1) / NBASE_SQR as i64) - 1;
                    newdig -= carry * NBASE_SQR as i64;
                } else if newdig >= NBASE_SQR as i64 {
                    carry = newdig / NBASE_SQR as i64;
                    newdig -= carry * NBASE_SQR as i64;
                } else {
                    carry = 0;
                }
                dividend[qi + i as usize + 1] = newdig;
                i -= 1;
            }
            dividend[qi] = carry;
        }

        if dividend[qi] < 0 {
            /*
             * The remainder is negative; the approximate quotient is too
             * large. Reduce the quotient by one and add the divisor to the
             * remainder until the remainder is positive.
             */
            loop {
                /* Add the divisor to the remainder */
                let mut carry: i64 = 0;
                let mut i = var2ndigitpairs - 1;
                while i > 0 {
                    let newdig = dividend[qi + i as usize] + divisor[i as usize] as i64 + carry;
                    if newdig >= NBASE_SQR as i64 {
                        dividend[qi + i as usize] = newdig - NBASE_SQR as i64;
                        carry = 1;
                    } else {
                        dividend[qi + i as usize] = newdig;
                        carry = 0;
                    }
                    i -= 1;
                }
                dividend[qi] += divisor[0] as i64 + carry;

                /* Subtract 1 from the quotient (propagating carries later) */
                dividend[qi - 1] -= 1;

                if dividend[qi] >= 0 {
                    break;
                }
            }
        } else {
            /*
             * The remainder is nonnegative. If it's >= the divisor, the
             * approximate quotient is too small and must be corrected.
             */
            loop {
                let mut less = false;
                /* Is remainder < divisor? */
                for i in 0..var2ndigitpairs as usize {
                    if dividend[qi + i] < divisor[i] as i64 {
                        less = true;
                        break;
                    }
                    if dividend[qi + i] > divisor[i] as i64 {
                        break; /* remainder > divisor */
                    }
                }
                if less {
                    break; /* quotient is correct */
                }

                /* Subtract the divisor from the remainder */
                let mut carry: i64 = 0;
                let mut i = var2ndigitpairs - 1;
                while i > 0 {
                    let newdig = dividend[qi + i as usize] - divisor[i as usize] as i64 + carry;
                    if newdig < 0 {
                        dividend[qi + i as usize] = newdig + NBASE_SQR as i64;
                        carry = -1;
                    } else {
                        dividend[qi + i as usize] = newdig;
                        carry = 0;
                    }
                    i -= 1;
                }
                dividend[qi] = dividend[qi] - divisor[0] as i64 + carry;

                /* Add 1 to the quotient (propagating carries later) */
                dividend[qi - 1] += 1;
            }
        }
    }

    /*
     * Final carry propagation pass to normalize back to base NBASE^2, and
     * construct the base-NBASE result digits.
     */
    let mut result = alloc_result(mcx, res_ndigits as usize)?;
    {
        let res_digits = &mut result.digits[1..]; // logical (headroom == 1)
        let mut carry: i64 = 0;
        for i in (0..res_ndigitpairs as usize).rev() {
            let mut newdig = dividend[i] + carry;
            if newdig < 0 {
                carry = -((-newdig - 1) / NBASE_SQR as i64) - 1;
                newdig -= carry * NBASE_SQR as i64;
            } else if newdig >= NBASE_SQR as i64 {
                carry = newdig / NBASE_SQR as i64;
                newdig -= carry * NBASE_SQR as i64;
            } else {
                carry = 0;
            }
            res_digits[2 * i + 1] = ((newdig as u32) % NBASE as u32) as NumericDigit;
            res_digits[2 * i] = ((newdig as u32) / NBASE as u32) as NumericDigit;
        }
        debug_assert_eq!(carry, 0);
    }

    /* Finally, round or truncate the result to the requested precision. */
    result.weight = res_weight;
    result.sign = res_sign;

    if round {
        round_var(&mut result, rscale);
    } else {
        trunc_var(&mut result, rscale);
    }

    /* Strip leading and trailing zeroes */
    strip_var(&mut result);
    Ok(result)
}

/// `div_var_int` (numeric.c:9907): divide a numeric by a 32-bit integer
/// `ival * NBASE^ival_weight`, with short division.
fn div_var_int<'mcx>(
    mcx: Mcx<'mcx>,
    var: &NumericVar<'_>,
    ival: i32,
    ival_weight: i32,
    rscale: i32,
    round: bool,
) -> PgResult<NumericVar<'mcx>> {
    let var_digits = var.logical_digits();
    let var_ndigits = var.ndigits() as i32;

    /* Guard against division by zero */
    if ival == 0 {
        return Err(division_by_zero());
    }

    /* Result zero check */
    if var_ndigits == 0 {
        let mut result = zero_var(mcx);
        result.dscale = rscale;
        return Ok(result);
    }

    /* Determine the result sign, weight and number of digits to calculate. */
    let res_sign = if var.sign == NumericSign::Pos {
        if ival > 0 {
            NumericSign::Pos
        } else {
            NumericSign::Neg
        }
    } else if ival > 0 {
        NumericSign::Neg
    } else {
        NumericSign::Pos
    };
    let res_weight = var.weight - ival_weight;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = res_ndigits.max(1);
    if round {
        res_ndigits += 1;
    }

    let mut result = alloc_result(mcx, res_ndigits as usize)?;

    /*
     * Compute the quotient digits (Knuth short division, divisor may exceed
     * the internal base). The carry may need 32 or 64 bits.
     */
    let divisor: u32 = ival.unsigned_abs();

    {
        let res_digits = &mut result.digits[1..]; // logical (headroom == 1)
        if divisor <= u32::MAX / NBASE as u32 {
            /* carry cannot overflow 32 bits */
            let mut carry: u32 = 0;
            for i in 0..res_ndigits as usize {
                carry = carry * NBASE as u32
                    + if (i as i32) < var_ndigits {
                        var_digits[i] as u32
                    } else {
                        0
                    };
                res_digits[i] = (carry / divisor) as NumericDigit;
                carry %= divisor;
            }
        } else {
            /* carry may exceed 32 bits */
            let mut carry: u64 = 0;
            let divisor = divisor as u64;
            for i in 0..res_ndigits as usize {
                carry = carry * NBASE as u64
                    + if (i as i32) < var_ndigits {
                        var_digits[i] as u64
                    } else {
                        0
                    };
                res_digits[i] = (carry / divisor) as NumericDigit;
                carry %= divisor;
            }
        }
    }

    result.weight = res_weight;
    result.sign = res_sign;

    /* Round or truncate to target rscale (and set result->dscale) */
    if round {
        round_var(&mut result, rscale);
    } else {
        trunc_var(&mut result, rscale);
    }

    /* Strip leading/trailing zeroes */
    strip_var(&mut result);
    Ok(result)
}

/// `div_var_int64` (numeric.c:10023, HAVE_INT128): divide a numeric by a 64-bit
/// integer `ival * NBASE^ival_weight`, with short division. Duplicates the
/// logic of `div_var_int` for the 3-4 digit divisor fast path.
fn div_var_int64<'mcx>(
    mcx: Mcx<'mcx>,
    var: &NumericVar<'_>,
    ival: i64,
    ival_weight: i32,
    rscale: i32,
    round: bool,
) -> PgResult<NumericVar<'mcx>> {
    let var_digits = var.logical_digits();
    let var_ndigits = var.ndigits() as i32;

    /* Guard against division by zero */
    if ival == 0 {
        return Err(division_by_zero());
    }

    /* Result zero check */
    if var_ndigits == 0 {
        let mut result = zero_var(mcx);
        result.dscale = rscale;
        return Ok(result);
    }

    /* Determine the result sign, weight and number of digits to calculate. */
    let res_sign = if var.sign == NumericSign::Pos {
        if ival > 0 {
            NumericSign::Pos
        } else {
            NumericSign::Neg
        }
    } else if ival > 0 {
        NumericSign::Neg
    } else {
        NumericSign::Pos
    };
    let res_weight = var.weight - ival_weight;
    let mut res_ndigits = res_weight + 1 + (rscale + DEC_DIGITS - 1) / DEC_DIGITS;
    res_ndigits = res_ndigits.max(1);
    if round {
        res_ndigits += 1;
    }

    let mut result = alloc_result(mcx, res_ndigits as usize)?;

    let divisor: u64 = ival.unsigned_abs();

    {
        let res_digits = &mut result.digits[1..]; // logical (headroom == 1)
        if divisor <= u64::MAX / NBASE as u64 {
            /* carry cannot overflow 64 bits */
            let mut carry: u64 = 0;
            for i in 0..res_ndigits as usize {
                carry = carry * NBASE as u64
                    + if (i as i32) < var_ndigits {
                        var_digits[i] as u64
                    } else {
                        0
                    };
                res_digits[i] = (carry / divisor) as NumericDigit;
                carry %= divisor;
            }
        } else {
            /* carry may exceed 64 bits */
            let mut carry: u128 = 0;
            let divisor = divisor as u128;
            for i in 0..res_ndigits as usize {
                carry = carry * NBASE as u128
                    + if (i as i32) < var_ndigits {
                        var_digits[i] as u128
                    } else {
                        0
                    };
                res_digits[i] = (carry / divisor) as NumericDigit;
                carry %= divisor;
            }
        }
    }

    result.weight = res_weight;
    result.sign = res_sign;

    if round {
        round_var(&mut result, rscale);
    } else {
        trunc_var(&mut result, rscale);
    }

    strip_var(&mut result);
    Ok(result)
}

/// `select_div_scale` (numeric.c:10135): default result scale for a division.
pub fn select_div_scale(var1: &NumericVar<'_>, var2: &NumericVar<'_>) -> i32 {
    let var1digits = var1.logical_digits();
    let var2digits = var2.logical_digits();

    /* Get the actual (normalized) weight and first digit of each input */
    let mut weight1 = 0; /* values to use if var1 is zero */
    let mut firstdigit1: NumericDigit = 0;
    for i in 0..var1digits.len() {
        firstdigit1 = var1digits[i];
        if firstdigit1 != 0 {
            weight1 = var1.weight - i as i32;
            break;
        }
    }

    let mut weight2 = 0; /* values to use if var2 is zero */
    let mut firstdigit2: NumericDigit = 0;
    for i in 0..var2digits.len() {
        firstdigit2 = var2digits[i];
        if firstdigit2 != 0 {
            weight2 = var2.weight - i as i32;
            break;
        }
    }

    /*
     * Estimate weight of quotient. If the two first digits are equal, we
     * can't be sure, but assume that var1 is less than var2.
     */
    let mut qweight = weight1 - weight2;
    if firstdigit1 <= firstdigit2 {
        qweight -= 1;
    }

    /* Select result scale */
    let mut rscale = NUMERIC_MIN_SIG_DIGITS - qweight * DEC_DIGITS;
    rscale = rscale.max(var1.dscale);
    rscale = rscale.max(var2.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    rscale
}

/// `mod_var` (numeric.c:10204): modulo of two numerics via
/// `mod(x,y) = x - trunc(x/y)*y`.
pub fn mod_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    /* div_var with round=false, exact=true gives us trunc(x/y) directly. */
    let tmp = div_var(mcx, var1, var2, 0, false, true)?;

    let tmp = mul_var(mcx, var2, &tmp, var2.dscale)?;

    sub_var(mcx, var1, &tmp)
}

/// `div_mod_var` (numeric.c:10233): truncated integer quotient and numeric
/// remainder; remainder precise to var2's dscale.
pub fn div_mod_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<(NumericVar<'mcx>, NumericVar<'mcx>)> {
    /*
     * Use div_var() with exact = false to get an initial estimate for the
     * integer quotient (truncated towards zero).
     */
    let mut q = div_var(mcx, var1, var2, 0, false, false)?;

    /* Compute initial estimate of remainder using the quotient estimate. */
    let r = mul_var(mcx, var2, &q, var2.dscale)?;
    let mut r = sub_var(mcx, var1, &r)?;

    let const_one = const_one(mcx);

    /*
     * Adjust the results if necessary --- the remainder should have the same
     * sign as var1, and its absolute value should be less than var2's.
     */
    while r.ndigits() != 0 && r.sign != var1.sign {
        /* The absolute value of the quotient is too large */
        if var1.sign == var2.sign {
            q = sub_var(mcx, &q, &const_one)?;
            r = add_var(mcx, &r, var2)?;
        } else {
            q = add_var(mcx, &q, &const_one)?;
            r = sub_var(mcx, &r, var2)?;
        }
    }

    while cmp_abs(&r, var2) != Ordering::Less {
        /* The absolute value of the quotient is too small */
        if var1.sign == var2.sign {
            q = add_var(mcx, &q, &const_one)?;
            r = sub_var(mcx, &r, var2)?;
        } else {
            q = sub_var(mcx, &q, &const_one)?;
            r = add_var(mcx, &r, var2)?;
        }
    }

    let quot = set_var_from_var(mcx, &q)?;
    let rem = set_var_from_var(mcx, &r)?;
    Ok((quot, rem))
}

/// `ceil_var` (numeric.c:10303): smallest integer >= the argument.
pub fn ceil_var<'mcx>(mcx: Mcx<'mcx>, var: &NumericVar<'_>) -> PgResult<NumericVar<'mcx>> {
    let mut tmp = set_var_from_var(mcx, var)?;

    trunc_var(&mut tmp, 0);

    if var.sign == NumericSign::Pos && cmp_var(var, &tmp) != Ordering::Equal {
        let const_one = const_one(mcx);
        tmp = add_var(mcx, &tmp, &const_one)?;
    }

    set_var_from_var(mcx, &tmp)
}

/// `floor_var` (numeric.c:10327): largest integer <= the argument.
pub fn floor_var<'mcx>(mcx: Mcx<'mcx>, var: &NumericVar<'_>) -> PgResult<NumericVar<'mcx>> {
    let mut tmp = set_var_from_var(mcx, var)?;

    trunc_var(&mut tmp, 0);

    if var.sign == NumericSign::Neg && cmp_var(var, &tmp) != Ordering::Equal {
        let const_one = const_one(mcx);
        tmp = sub_var(mcx, &tmp, &const_one)?;
    }

    set_var_from_var(mcx, &tmp)
}

// ---------------------------------------------------------------------------
// Errors mirroring the C ereport sites.
// ---------------------------------------------------------------------------

/// `ereport(ERROR, errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by
/// zero"))` (numeric.c div_var/div_var_int/div_var_int64).
fn division_by_zero() -> types_error::PgError {
    types_error::PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}
