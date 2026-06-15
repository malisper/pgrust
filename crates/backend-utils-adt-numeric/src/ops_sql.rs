//! Family: SQL operator/function cores + special functions + scale/typmod/
//! series helpers.
//!
//! Mirrors numeric.c's `numeric_add`/`sub`/`mul`/`div`/`div_trunc`/`mod`,
//! `numeric_abs`/`uminus`/`uplus`/`inc`/`sign`, the comparison wrappers
//! (`numeric_cmp`/`eq`/`ne`/`lt`/`le`/`gt`/`ge`), `numeric_round`/`trunc`/
//! `ceil`/`floor`, `numeric_sqrt`/`exp`/`ln`/`log`/`power`; the special
//! functions `gcd`/`lcm`/`factorial`/`min`/`max`/`width_bucket`; and the
//! scale/typmod/series helpers (`get_min_scale`/`numeric_trim_scale`/
//! `numeric_scale`/`numeric_normalize`/`numeric_maximum_size`/`numerictypmodin`/
//! `numerictypmodout`/`in_range`/`generate_series`).
//!
//! These cores operate over on-disk byte images (the SQL fmgr boundary): they
//! decode to `NumericVar` in `mcx`, compute, and re-encode. They allocate and
//! so take an explicit `Mcx<'mcx>` and return [`PgResult`] where C `ereport`s.

extern crate alloc;

use core::cmp::Ordering;

use mcx::{Mcx, PgVec};
use types_tuple::Datum;
use types_error::{PgError, PgResult};

use types_numeric::var::{NumericSign, NumericVar};
use types_numeric::{
    numeric_is_inf, numeric_is_nan, numeric_is_ninf, numeric_is_pinf, numeric_is_special,
    numeric_sign as numeric_sign_word, NumericDigit, DEC_DIGITS, NUMERIC_DSCALE_MAX,
    NUMERIC_HDRSZ, NUMERIC_MAX_DISPLAY_SCALE, NUMERIC_MAX_PRECISION, NUMERIC_MAX_RESULT_SCALE,
    NUMERIC_MAX_SCALE, NUMERIC_MIN_DISPLAY_SCALE, NUMERIC_MIN_SCALE, NUMERIC_MIN_SIG_DIGITS,
    NUMERIC_NEG, NUMERIC_POS, NUMERIC_WEIGHT_MAX,
};

use crate::convert::{make_result, numeric_to_float8, set_var_from_num};
use crate::kernel_transcendental::{
    estimate_ln_dweight, exp_var, int64_to_numericvar, ln_var, log_var, numericvar_to_int64,
    power_var, sqrt_var,
};
use crate::kernel_var::{
    add_var, ceil_var, cmp_abs, cmp_var, div_var, floor_var, mod_var, mul_var, round_var,
    select_div_scale, set_var_from_var, sub_var, trunc_var,
};

// ---------------------------------------------------------------------------
// Local value constructors for the preinitialized constants used by the SQL
// cores (numeric.c const_zero/const_one/const_minus_one/const_nan/const_pinf/
// const_ninf). These are trivial literal builders, not the NBASE kernel — they
// construct a fixed NumericVar in `mcx`, so they live here rather than calling
// the kernel_var const_* family.
// ---------------------------------------------------------------------------

/// `const_zero` (numeric.c): the value 0, weight 0, dscale 0, no digits.
#[inline]
fn const_zero(mcx: Mcx<'_>) -> NumericVar<'_> {
    NumericVar::zero(mcx)
}

/// Build a finite single-NBASE-digit constant (`digit` in [1, NBASE)) with the
/// given sign, weight 0 and dscale 0. Used for const_one / const_minus_one.
#[inline]
fn const_single_digit<'mcx>(
    mcx: Mcx<'mcx>,
    sign: NumericSign,
    digit: NumericDigit,
) -> PgResult<NumericVar<'mcx>> {
    let mut digits = crate::alloc_digits(mcx, 1)?;
    digits[0] = digit;
    Ok(NumericVar {
        sign,
        weight: 0,
        dscale: 0,
        digits,
        headroom: 0,
    })
}

/// `const_one` (numeric.c): the value 1.
#[inline]
fn const_one(mcx: Mcx<'_>) -> PgResult<NumericVar<'_>> {
    const_single_digit(mcx, NumericSign::Pos, 1)
}

/// `const_minus_one` (numeric.c): the value -1.
#[inline]
fn const_minus_one(mcx: Mcx<'_>) -> PgResult<NumericVar<'_>> {
    const_single_digit(mcx, NumericSign::Neg, 1)
}

/// `make_result(&const_nan)` — encode a NaN on-disk image.
#[inline]
fn make_nan(mcx: Mcx<'_>) -> PgResult<PgVec<'_, u8>> {
    make_result(mcx, &NumericVar::special(mcx, NumericSign::NaN))
}

/// `make_result(&const_pinf)` — encode a +Inf on-disk image.
#[inline]
fn make_pinf(mcx: Mcx<'_>) -> PgResult<PgVec<'_, u8>> {
    make_result(mcx, &NumericVar::special(mcx, NumericSign::PInf))
}

/// `make_result(&const_ninf)` — encode a -Inf on-disk image.
#[inline]
fn make_ninf(mcx: Mcx<'_>) -> PgResult<PgVec<'_, u8>> {
    make_result(mcx, &NumericVar::special(mcx, NumericSign::NInf))
}

/// `make_result(&const_zero)` — encode a 0 on-disk image.
#[inline]
fn make_zero(mcx: Mcx<'_>) -> PgResult<PgVec<'_, u8>> {
    make_result(mcx, &const_zero(mcx))
}

/// Allocate a zeroed, **charged** `PgVec<'mcx, u8>` of length `n`, OOM-safely
/// (validated bound + fallible reserve), mirroring [`crate::alloc_digits`] for
/// raw byte buffers. OOM surfaces as `numeric value out of range`.
fn alloc_bytes<'mcx>(mcx: Mcx<'mcx>, n: usize) -> PgResult<PgVec<'mcx, u8>> {
    let mut v = mcx::vec_with_capacity_in::<u8>(mcx, n).map_err(|_| {
        PgError::error("value overflows numeric format")
            .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
    })?;
    v.resize(n, 0);
    Ok(v)
}

/// `duplicate_numeric(num)` (numeric.c:7882): a verbatim byte copy of the
/// on-disk image (`palloc(VARSIZE); memcpy`).
fn duplicate_numeric<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let mut out = alloc_bytes(mcx, num.len())?;
    out.copy_from_slice(num);
    Ok(out)
}

// ---------------------------------------------------------------------------
// numeric_sign_internal / numeric_is_integral (numeric.c:1478, 873) — small
// classifiers over the on-disk image used by several cores.
// ---------------------------------------------------------------------------

/// `numeric_sign_internal(num)` (numeric.c:1478): -1 / 0 / 1. The NaN case must
/// have been handled by the caller; this copes with infinities.
fn numeric_sign_internal(num: &[u8]) -> i32 {
    if numeric_is_special(num) {
        // Must be Inf or -Inf (caller handled NaN).
        if numeric_is_pinf(num) {
            1
        } else {
            -1
        }
    } else if numeric_ndigits(num) == 0 {
        // The packed format is always zero-digit-trimmed, so no digits == 0.
        0
    } else if numeric_sign_word(num) == NUMERIC_NEG {
        -1
    } else {
        1
    }
}

/// `numeric_is_integral(num)` (numeric.c:873). Rejects NaN; infinities are
/// considered integral.
fn numeric_is_integral(mcx: Mcx<'_>, num: &[u8]) -> PgResult<bool> {
    if numeric_is_special(num) {
        if numeric_is_nan(num) {
            return Ok(false);
        }
        return Ok(true);
    }
    let arg = set_var_from_num(mcx, num)?;
    Ok(arg.ndigits() == 0 || (arg.ndigits() as i32) <= arg.weight + 1)
}

/// `NUMERIC_NDIGITS(num)` over a whole on-disk byte image (the slice length is
/// the value's VARSIZE).
#[inline]
fn numeric_ndigits(num: &[u8]) -> usize {
    types_numeric::numeric_ndigits(num, num.len())
}

// ---------------------------------------------------------------------------
// cmp_numerics (numeric.c:2624) — the comparison engine behind cmp/eq/.../
// width_bucket. Pure; works directly on the on-disk byte images.
// ---------------------------------------------------------------------------

/// `cmp_numerics(num1, num2)` (numeric.c:2624): full 3-way comparison with
/// the special-value ordering NaN > +Inf > finite > -Inf.
fn cmp_numerics(num1: &[u8], num2: &[u8]) -> i32 {
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
        cmp_var_common_bytes(num1, num2)
    }
}

/// `cmp_var_common(...)` invoked with the on-disk digit/weight/sign of two
/// finite values. Bridges the byte image onto [`cmp_var`] by decoding both
/// operands to a `NumericVar`; both are finite here so this cannot allocate
/// beyond the digit copies. Returns -1/0/1.
fn cmp_var_common_bytes(num1: &[u8], num2: &[u8]) -> i32 {
    // Both operands are finite; decode and compare via cmp_var. cmp_var only
    // reads digits/weight/sign so the (charged) decode is the only allocation;
    // the C path is alloc-free but the result is identical.
    //
    // Use a private transient context for the decode scratch.
    let ctx = mcx::MemoryContext::new("numeric cmp scratch");
    let mcx = ctx.mcx();
    // Decoding a finite numeric never errors (no overflow possible from a
    // stored value), so unwrap is safe here.
    let v1 = set_var_from_num(mcx, num1).expect("decode finite numeric");
    let v2 = set_var_from_num(mcx, num2).expect("decode finite numeric");
    match cmp_var(&v1, &v2) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

// ---------------------------------------------------------------------------
// Arithmetic operator cores (on-disk byte images in/out).
// ---------------------------------------------------------------------------

pub fn numeric_add<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_add_opt_error(num1, num2, NULL) (numeric.c:2986).
    if numeric_is_special(a) || numeric_is_special(b) {
        if numeric_is_nan(a) || numeric_is_nan(b) {
            return make_nan(mcx);
        }
        if numeric_is_pinf(a) {
            if numeric_is_ninf(b) {
                return make_nan(mcx); // Inf + -Inf
            }
            return make_pinf(mcx);
        }
        if numeric_is_ninf(a) {
            if numeric_is_pinf(b) {
                return make_nan(mcx); // -Inf + Inf
            }
            return make_ninf(mcx);
        }
        // num1 finite, so num2 is not.
        if numeric_is_pinf(b) {
            return make_pinf(mcx);
        }
        return make_ninf(mcx);
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    let result = add_var(mcx, &arg1, &arg2)?;
    encode_opt_error(mcx, &result)
}

pub fn numeric_sub<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_sub_opt_error (numeric.c:3064).
    if numeric_is_special(a) || numeric_is_special(b) {
        if numeric_is_nan(a) || numeric_is_nan(b) {
            return make_nan(mcx);
        }
        if numeric_is_pinf(a) {
            if numeric_is_pinf(b) {
                return make_nan(mcx); // Inf - Inf
            }
            return make_pinf(mcx);
        }
        if numeric_is_ninf(a) {
            if numeric_is_ninf(b) {
                return make_nan(mcx); // -Inf - -Inf
            }
            return make_ninf(mcx);
        }
        // num1 finite, so num2 is not.
        if numeric_is_pinf(b) {
            return make_ninf(mcx);
        }
        return make_pinf(mcx);
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    let result = sub_var(mcx, &arg1, &arg2)?;
    encode_opt_error(mcx, &result)
}

pub fn numeric_mul<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_mul_opt_error (numeric.c:3142).
    if numeric_is_special(a) || numeric_is_special(b) {
        if numeric_is_nan(a) || numeric_is_nan(b) {
            return make_nan(mcx);
        }
        if numeric_is_pinf(a) {
            return match numeric_sign_internal(b) {
                0 => make_nan(mcx), // Inf * 0
                1 => make_pinf(mcx),
                _ => make_ninf(mcx),
            };
        }
        if numeric_is_ninf(a) {
            return match numeric_sign_internal(b) {
                0 => make_nan(mcx), // -Inf * 0
                1 => make_ninf(mcx),
                _ => make_pinf(mcx),
            };
        }
        // num1 finite, so num2 is not.
        if numeric_is_pinf(b) {
            return match numeric_sign_internal(a) {
                0 => make_nan(mcx), // 0 * Inf
                1 => make_pinf(mcx),
                _ => make_ninf(mcx),
            };
        }
        // num2 must be NINF.
        return match numeric_sign_internal(a) {
            0 => make_nan(mcx), // 0 * -Inf
            1 => make_ninf(mcx),
            _ => make_pinf(mcx),
        };
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    let mut result = mul_var(mcx, &arg1, &arg2, arg1.dscale + arg2.dscale)?;
    if result.dscale > NUMERIC_DSCALE_MAX as i32 {
        round_var(&mut result, NUMERIC_DSCALE_MAX as i32);
    }
    encode_opt_error(mcx, &result)
}

pub fn numeric_div<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_div_opt_error (numeric.c:3263), have_error == NULL path.
    if numeric_is_special(a) || numeric_is_special(b) {
        if numeric_is_nan(a) || numeric_is_nan(b) {
            return make_nan(mcx);
        }
        if numeric_is_pinf(a) {
            if numeric_is_special(b) {
                return make_nan(mcx); // Inf / [-]Inf
            }
            return match numeric_sign_internal(b) {
                0 => Err(division_by_zero()),
                1 => make_pinf(mcx),
                _ => make_ninf(mcx),
            };
        }
        if numeric_is_ninf(a) {
            if numeric_is_special(b) {
                return make_nan(mcx); // -Inf / [-]Inf
            }
            return match numeric_sign_internal(b) {
                0 => Err(division_by_zero()),
                1 => make_ninf(mcx),
                _ => make_pinf(mcx),
            };
        }
        // num1 finite, so num2 is not; the numeric type doesn't underflow, so
        // we just return zero.
        return make_zero(mcx);
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    let rscale = select_div_scale(&arg1, &arg2);
    let result = div_var(mcx, &arg1, &arg2, rscale, true, true)?;
    encode_opt_error(mcx, &result)
}

pub fn numeric_div_trunc<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_div_trunc (numeric.c:3378).
    if numeric_is_special(a) || numeric_is_special(b) {
        if numeric_is_nan(a) || numeric_is_nan(b) {
            return make_nan(mcx);
        }
        if numeric_is_pinf(a) {
            if numeric_is_special(b) {
                return make_nan(mcx); // Inf / [-]Inf
            }
            return match numeric_sign_internal(b) {
                0 => Err(division_by_zero()),
                1 => make_pinf(mcx),
                _ => make_ninf(mcx),
            };
        }
        if numeric_is_ninf(a) {
            if numeric_is_special(b) {
                return make_nan(mcx); // -Inf / [-]Inf
            }
            return match numeric_sign_internal(b) {
                0 => Err(division_by_zero()),
                1 => make_ninf(mcx),
                _ => make_pinf(mcx),
            };
        }
        return make_zero(mcx);
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    let result = div_var(mcx, &arg1, &arg2, 0, false, true)?;
    make_result(mcx, &result)
}

pub fn numeric_mod<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_mod_opt_error (numeric.c:3487), have_error == NULL path.
    if numeric_is_special(a) || numeric_is_special(b) {
        if numeric_is_nan(a) || numeric_is_nan(b) {
            return make_nan(mcx);
        }
        if numeric_is_inf(a) {
            if numeric_sign_internal(b) == 0 {
                return Err(division_by_zero());
            }
            // Inf % any nonzero = NaN
            return make_nan(mcx);
        }
        // num2 must be [-]Inf; result is num1 regardless of sign of num2.
        return duplicate_numeric(mcx, a);
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    let result = mod_var(mcx, &arg1, &arg2)?;
    // numeric_mod_opt_error passes have_error == NULL to make_result_opt_error.
    make_result(mcx, &result)
}

/// `make_result_opt_error(var, NULL)` is just `make_result`. Kept as a named
/// bridge so the opt-error sites read like the C.
#[inline]
fn encode_opt_error<'mcx>(mcx: Mcx<'mcx>, var: &NumericVar<'_>) -> PgResult<PgVec<'mcx, u8>> {
    make_result(mcx, var)
}

/// The `ereport(ERROR, ERRCODE_DIVISION_BY_ZERO, "division by zero")` raised by
/// the division cores.
fn division_by_zero() -> PgError {
    PgError::error("division by zero").with_sqlstate(types_error::ERRCODE_DIVISION_BY_ZERO)
}

// ---------------------------------------------------------------------------
// Unary ops.
// ---------------------------------------------------------------------------

pub fn numeric_abs<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_abs (numeric.c:1393): operate directly on the packed format.
    let mut res = duplicate_numeric(mcx, num)?;
    use types_numeric::{numeric_is_short, NUMERIC_INF_SIGN_MASK, NUMERIC_SHORT_SIGN_MASK};
    let hdr_word = u16::from_ne_bytes([res[VARHDRSZ_U], res[VARHDRSZ_U + 1]]);
    if numeric_is_short(num) {
        write_header_word(&mut res, hdr_word & !NUMERIC_SHORT_SIGN_MASK);
    } else if numeric_is_special(num) {
        // Changes -Inf to Inf, doesn't affect NaN.
        write_header_word(&mut res, hdr_word & !NUMERIC_INF_SIGN_MASK);
    } else {
        // n_long.n_sign_dscale = NUMERIC_POS | NUMERIC_DSCALE(num)
        let dscale = types_numeric::numeric_dscale(num);
        write_header_word(&mut res, NUMERIC_POS | dscale);
    }
    Ok(res)
}

pub fn numeric_uminus<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_uminus (numeric.c:1420).
    let mut res = duplicate_numeric(mcx, num)?;
    use types_numeric::{numeric_is_short, NUMERIC_INF_SIGN_MASK, NUMERIC_SHORT_SIGN_MASK};
    let hdr_word = u16::from_ne_bytes([res[VARHDRSZ_U], res[VARHDRSZ_U + 1]]);

    if numeric_is_special(num) {
        // Flip the sign if it's Inf or -Inf.
        if !numeric_is_nan(num) {
            write_header_word(&mut res, hdr_word ^ NUMERIC_INF_SIGN_MASK);
        }
    } else if numeric_ndigits(num) != 0 {
        // Else, flip the sign of a nonzero value.
        if numeric_is_short(num) {
            write_header_word(&mut res, hdr_word ^ NUMERIC_SHORT_SIGN_MASK);
        } else if numeric_sign_word(num) == NUMERIC_POS {
            let dscale = types_numeric::numeric_dscale(num);
            write_header_word(&mut res, NUMERIC_NEG | dscale);
        } else {
            let dscale = types_numeric::numeric_dscale(num);
            write_header_word(&mut res, NUMERIC_POS | dscale);
        }
    }
    Ok(res)
}

pub fn numeric_uplus<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_uplus (numeric.c:1462): just duplicate.
    duplicate_numeric(mcx, num)
}

pub fn numeric_inc<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_inc (numeric.c:3556).
    if numeric_is_special(num) {
        return duplicate_numeric(mcx, num);
    }
    let arg = set_var_from_num(mcx, num)?;
    let one = const_one(mcx)?;
    let res = add_var(mcx, &arg, &one)?;
    make_result(mcx, &res)
}

pub fn numeric_sign<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_sign (numeric.c:1510).
    if numeric_is_nan(num) {
        return make_nan(mcx);
    }
    match numeric_sign_internal(num) {
        0 => make_zero(mcx),
        1 => make_result(mcx, &const_one(mcx)?),
        _ => make_result(mcx, &const_minus_one(mcx)?),
    }
}

// ---------------------------------------------------------------------------
// Comparison cores (pure; infallible).
// ---------------------------------------------------------------------------

pub fn numeric_cmp(a: &[u8], b: &[u8]) -> Ordering {
    // numeric_cmp (numeric.c:2518): cmp_numerics.
    match cmp_numerics(a, b) {
        d if d < 0 => Ordering::Less,
        0 => Ordering::Equal,
        _ => Ordering::Greater,
    }
}

pub fn numeric_eq(a: &[u8], b: &[u8]) -> bool {
    cmp_numerics(a, b) == 0
}

pub fn numeric_ne(a: &[u8], b: &[u8]) -> bool {
    cmp_numerics(a, b) != 0
}

pub fn numeric_lt(a: &[u8], b: &[u8]) -> bool {
    cmp_numerics(a, b) < 0
}

pub fn numeric_le(a: &[u8], b: &[u8]) -> bool {
    cmp_numerics(a, b) <= 0
}

pub fn numeric_gt(a: &[u8], b: &[u8]) -> bool {
    cmp_numerics(a, b) > 0
}

pub fn numeric_ge(a: &[u8], b: &[u8]) -> bool {
    cmp_numerics(a, b) >= 0
}

// ---------------------------------------------------------------------------
// Round / trunc / ceil / floor + transcendental SQL wrappers.
// ---------------------------------------------------------------------------

pub fn numeric_round<'mcx>(mcx: Mcx<'mcx>, num: &[u8], scale: i32) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_round (numeric.c:1543).
    if numeric_is_special(num) {
        return duplicate_numeric(mcx, num);
    }

    // Limit the scale value to avoid possible overflow in calculations; allow
    // one extra digit before the decimal point for round-up carry.
    let mut scale = scale.max(-(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS - 1);
    scale = scale.min(NUMERIC_DSCALE_MAX as i32);

    let mut arg = set_var_from_num(mcx, num)?;
    round_var(&mut arg, scale);

    // We don't allow negative output dscale.
    if scale < 0 {
        arg.dscale = 0;
    }
    make_result(mcx, &arg)
}

pub fn numeric_trunc<'mcx>(mcx: Mcx<'mcx>, num: &[u8], scale: i32) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_trunc (numeric.c:1597).
    if numeric_is_special(num) {
        return duplicate_numeric(mcx, num);
    }

    let mut scale = scale.max(-(NUMERIC_WEIGHT_MAX + 1) * DEC_DIGITS);
    scale = scale.min(NUMERIC_DSCALE_MAX as i32);

    let mut arg = set_var_from_num(mcx, num)?;
    trunc_var(&mut arg, scale);

    if scale < 0 {
        arg.dscale = 0;
    }
    make_result(mcx, &arg)
}

pub fn numeric_ceil<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_ceil (numeric.c:1647).
    if numeric_is_special(num) {
        return duplicate_numeric(mcx, num);
    }
    let arg = set_var_from_num(mcx, num)?;
    let result = ceil_var(mcx, &arg)?;
    make_result(mcx, &result)
}

pub fn numeric_floor<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_floor (numeric.c:1675).
    if numeric_is_special(num) {
        return duplicate_numeric(mcx, num);
    }
    let arg = set_var_from_num(mcx, num)?;
    let result = floor_var(mcx, &arg)?;
    make_result(mcx, &result)
}

pub fn numeric_sqrt<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_sqrt (numeric.c:3795).
    if numeric_is_special(num) {
        if numeric_is_ninf(num) {
            return Err(PgError::error("cannot take square root of a negative number")
                .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION));
        }
        // For NAN or PINF, duplicate the input.
        return duplicate_numeric(mcx, num);
    }

    let arg = set_var_from_num(mcx, num)?;

    // sweight = floor(arg.weight * DEC_DIGITS / 2 + 1). DEC_DIGITS == 4 is even,
    // so the division is exact.
    let sweight = arg.weight * DEC_DIGITS / 2 + 1;

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
    rscale = rscale.max(arg.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let result = sqrt_var(mcx, &arg, rscale)?;
    make_result(mcx, &result)
}

pub fn numeric_exp<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_exp (numeric.c:3867).
    if numeric_is_special(num) {
        // Per POSIX, exp(-Inf) is zero.
        if numeric_is_ninf(num) {
            return make_zero(mcx);
        }
        // For NAN or PINF, duplicate the input.
        return duplicate_numeric(mcx, num);
    }

    let arg = set_var_from_num(mcx, num)?;

    // val = numericvar_to_double_no_overflow(&arg); log10(result) ~= num*log10(e)
    let mut val = numericvar_to_double_no_overflow(mcx, &arg)?;
    val *= 0.434294481903252;

    // Limit to something that won't cause integer overflow.
    val = val.max(-(NUMERIC_MAX_RESULT_SCALE as f64));
    val = val.min(NUMERIC_MAX_RESULT_SCALE as f64);

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - val as i32;
    rscale = rscale.max(arg.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let result = exp_var(mcx, &arg, rscale)?;
    make_result(mcx, &result)
}

pub fn numeric_ln<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_ln (numeric.c:3934).
    if numeric_is_special(num) {
        if numeric_is_ninf(num) {
            return Err(PgError::error("cannot take logarithm of a negative number")
                .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_LOG));
        }
        // For NAN or PINF, duplicate the input.
        return duplicate_numeric(mcx, num);
    }

    let arg = set_var_from_num(mcx, num)?;
    let ln_dweight = estimate_ln_dweight(&arg)?;

    let mut rscale = NUMERIC_MIN_SIG_DIGITS - ln_dweight;
    rscale = rscale.max(arg.dscale);
    rscale = rscale.max(NUMERIC_MIN_DISPLAY_SCALE);
    rscale = rscale.min(NUMERIC_MAX_DISPLAY_SCALE);

    let result = ln_var(mcx, &arg, rscale)?;
    make_result(mcx, &result)
}

pub fn numeric_log<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_log (numeric.c:3983): a is the base (num1), b is num2.
    if numeric_is_special(a) || numeric_is_special(b) {
        if numeric_is_nan(a) || numeric_is_nan(b) {
            return make_nan(mcx);
        }
        // Fail on negative inputs including -Inf, as log_var would.
        let sign1 = numeric_sign_internal(a);
        let sign2 = numeric_sign_internal(b);
        if sign1 < 0 || sign2 < 0 {
            return Err(PgError::error("cannot take logarithm of a negative number")
                .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_LOG));
        }
        if sign1 == 0 || sign2 == 0 {
            return Err(PgError::error("cannot take logarithm of zero")
                .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_LOG));
        }
        if numeric_is_pinf(a) {
            // log(Inf, Inf) reduces to Inf/Inf -> NaN.
            if numeric_is_pinf(b) {
                return make_nan(mcx);
            }
            // log(Inf, finite-positive) is zero.
            return make_zero(mcx);
        }
        // num2 must be PINF: log(finite-positive, Inf) is Inf.
        return make_pinf(mcx);
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    // log_var handles scale selection itself.
    let result = log_var(mcx, &arg1, &arg2)?;
    make_result(mcx, &result)
}

pub fn numeric_power<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_power (numeric.c:4054): a is num1 (base), b is num2 (exponent).
    if numeric_is_special(a) || numeric_is_special(b) {
        // POSIX pow(3): NaN^0 = 1, 1^NaN = 1, else NaN for NaN inputs.
        if numeric_is_nan(a) {
            if !numeric_is_special(b) {
                let arg2 = set_var_from_num(mcx, b)?;
                if cmp_var(&arg2, &const_zero(mcx)) == Ordering::Equal {
                    return make_result(mcx, &const_one(mcx)?);
                }
            }
            return make_nan(mcx);
        }
        if numeric_is_nan(b) {
            if !numeric_is_special(a) {
                let arg1 = set_var_from_num(mcx, a)?;
                if cmp_var(&arg1, &const_one(mcx)?) == Ordering::Equal {
                    return make_result(mcx, &const_one(mcx)?);
                }
            }
            return make_nan(mcx);
        }

        // At least one input is infinite; error rules still apply.
        let sign1 = numeric_sign_internal(a);
        let sign2 = numeric_sign_internal(b);
        if sign1 == 0 && sign2 < 0 {
            return Err(zero_to_negative_power());
        }
        if sign1 < 0 && !numeric_is_integral(mcx, b)? {
            return Err(negative_to_non_integer_power());
        }

        // For any value of y, if x is +1, 1.0 shall be returned.
        if !numeric_is_special(a) {
            let arg1 = set_var_from_num(mcx, a)?;
            if cmp_var(&arg1, &const_one(mcx)?) == Ordering::Equal {
                return make_result(mcx, &const_one(mcx)?);
            }
        }

        // For any value of x, if y is [-]0, 1.0 shall be returned.
        if sign2 == 0 {
            return make_result(mcx, &const_one(mcx)?);
        }

        // For x [-]0 and y > 0, [-]0 (i.e. +0) shall be returned.
        if sign1 == 0 && sign2 > 0 {
            return make_zero(mcx);
        }

        // y is [-]Inf cases.
        if numeric_is_inf(b) {
            let abs_x_gt_one;
            if numeric_is_special(a) {
                abs_x_gt_one = true; // x is either Inf or -Inf
            } else {
                let mut arg1 = set_var_from_num(mcx, a)?;
                if cmp_var(&arg1, &const_minus_one(mcx)?) == Ordering::Equal {
                    return make_result(mcx, &const_one(mcx)?);
                }
                arg1.sign = NumericSign::Pos; // now arg1 = abs(x)
                abs_x_gt_one = cmp_var(&arg1, &const_one(mcx)?) == Ordering::Greater;
            }
            if abs_x_gt_one == (sign2 > 0) {
                return make_pinf(mcx);
            }
            return make_zero(mcx);
        }

        // x is +Inf cases.
        if numeric_is_pinf(a) {
            if sign2 > 0 {
                return make_pinf(mcx);
            }
            return make_zero(mcx);
        }

        // x must be NINF.
        // For y < 0, -Inf -> +0.
        if sign2 < 0 {
            return make_zero(mcx);
        }
        // For y an odd integer > 0, -Inf -> -Inf; else +Inf.
        let arg2 = set_var_from_num(mcx, b)?;
        let nd = arg2.ndigits();
        let logical = arg2.logical_digits();
        if nd > 0 && nd as i32 == arg2.weight + 1 && (logical[nd - 1] & 1) != 0 {
            return make_ninf(mcx);
        }
        return make_pinf(mcx);
    }

    // We don't return divide-by-zero for 0 ^ -1; that and negative^non-integer
    // are handled here / in power_var().
    let sign1 = numeric_sign_internal(a);
    let sign2 = numeric_sign_internal(b);
    if sign1 == 0 && sign2 < 0 {
        return Err(zero_to_negative_power());
    }

    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    // power_var handles scale selection itself.
    let result = power_var(mcx, &arg1, &arg2)?;
    make_result(mcx, &result)
}

fn zero_to_negative_power() -> PgError {
    PgError::error("zero raised to a negative power is undefined")
        .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
}

fn negative_to_non_integer_power() -> PgError {
    PgError::error("a negative number raised to a non-integer power yields a complex result")
        .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
}

// ---------------------------------------------------------------------------
// Special functions (numeric.c gcd_var/numeric_gcd/lcm/factorial/min/max/
// width_bucket_numeric).
// ---------------------------------------------------------------------------

/// `gcd_var(var1, var2, result)` (numeric.c:10350): Euclidean GCD at variable
/// level. Returns a fresh result in `mcx`.
fn gcd_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let res_dscale = var1.dscale.max(var2.dscale);

    // Arrange for var1 to be the number with the greater absolute value.
    let cmp = cmp_abs(var1, var2);
    let (var1, var2) = if cmp == Ordering::Less {
        (var2, var1)
    } else {
        (var1, var2)
    };

    // Avoid the modulo if the inputs have the same absolute value, or if the
    // smaller input is zero.
    if cmp == Ordering::Equal || var2.ndigits() == 0 {
        let mut result = set_var_from_var(mcx, var1)?;
        result.sign = NumericSign::Pos;
        result.dscale = res_dscale;
        return Ok(result);
    }

    // Use the Euclidean algorithm to find the GCD.
    let mut tmp_arg = set_var_from_var(mcx, var1)?;
    let mut result = set_var_from_var(mcx, var2)?;

    loop {
        let modv = mod_var(mcx, &tmp_arg, &result)?;
        if modv.ndigits() == 0 {
            break;
        }
        tmp_arg = set_var_from_var(mcx, &result)?;
        result = modv;
    }
    result.sign = NumericSign::Pos;
    result.dscale = res_dscale;
    Ok(result)
}

pub fn numeric_gcd<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_gcd (numeric.c:3640).
    if numeric_is_special(a) || numeric_is_special(b) {
        return make_nan(mcx);
    }
    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;
    let result = gcd_var(mcx, &arg1, &arg2)?;
    make_result(mcx, &result)
}

pub fn numeric_lcm<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_lcm (numeric.c:3683).
    if numeric_is_special(a) || numeric_is_special(b) {
        return make_nan(mcx);
    }
    let arg1 = set_var_from_num(mcx, a)?;
    let arg2 = set_var_from_num(mcx, b)?;

    // lcm(x, y) = abs(x / gcd(x, y) * y), zero if either input is zero.
    let mut result;
    if arg1.ndigits() == 0 || arg2.ndigits() == 0 {
        result = set_var_from_var(mcx, &const_zero(mcx))?;
    } else {
        let g = gcd_var(mcx, &arg1, &arg2)?;
        let q = div_var(mcx, &arg1, &g, 0, false, true)?;
        result = mul_var(mcx, &arg2, &q, arg2.dscale)?;
        result.sign = NumericSign::Pos;
    }
    result.dscale = arg1.dscale.max(arg2.dscale);
    make_result(mcx, &result)
}

pub fn numeric_factorial<'mcx>(mcx: Mcx<'mcx>, n: i64) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_fac (numeric.c:3743).
    if n < 0 {
        return Err(PgError::error("factorial of a negative number is undefined")
            .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }
    if n <= 1 {
        return make_result(mcx, &const_one(mcx)?);
    }
    // Fail immediately if the result would overflow.
    if n > 32177 {
        return Err(PgError::error("value overflows numeric format")
            .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }

    let mut result = int64_to_numericvar(mcx, n)?;
    let mut k = n - 1;
    while k > 1 {
        // CHECK_FOR_INTERRUPTS(): no interrupt model here.
        let fact = int64_to_numericvar(mcx, k)?;
        result = mul_var(mcx, &result, &fact, 0)?;
        k -= 1;
    }
    make_result(mcx, &result)
}

pub fn numeric_min<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_smaller (numeric.c:3589): returns the smaller per cmp_numerics.
    if cmp_numerics(a, b) < 0 {
        duplicate_numeric(mcx, a)
    } else {
        duplicate_numeric(mcx, b)
    }
}

pub fn numeric_max<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_larger (numeric.c:3611).
    if cmp_numerics(a, b) > 0 {
        duplicate_numeric(mcx, a)
    } else {
        duplicate_numeric(mcx, b)
    }
}

pub fn width_bucket_numeric(
    operand: &[u8],
    bound1: &[u8],
    bound2: &[u8],
    count: &[u8],
) -> PgResult<i32> {
    // width_bucket_numeric (numeric.c:1967). `count` is an int32 supplied as a
    // numeric byte image here; the C function takes int32 directly. Decode it.
    let ctx = mcx::MemoryContext::new("width_bucket scratch");
    let mcx = ctx.mcx();

    let count_i32 = {
        let count_var = set_var_from_num(mcx, count)?;
        match numericvar_to_int32(&count_var)? {
            Some(v) => v,
            None => {
                return Err(PgError::error("integer out of range")
                    .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
            }
        }
    };

    if count_i32 <= 0 {
        return Err(PgError::error("count must be greater than zero")
            .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION));
    }

    if numeric_is_special(operand) || numeric_is_special(bound1) || numeric_is_special(bound2) {
        if numeric_is_nan(operand) || numeric_is_nan(bound1) || numeric_is_nan(bound2) {
            return Err(PgError::error(
                "operand, lower bound, and upper bound cannot be NaN",
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION));
        }
        // operand may be infinite; cmp_numerics copes. Bounds must be finite.
        if numeric_is_inf(bound1) || numeric_is_inf(bound2) {
            return Err(PgError::error("lower and upper bounds must be finite")
                .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION));
        }
    }

    // Convert 'count' to a numeric for ease of use later.
    let count_var = int64_to_numericvar(mcx, count_i32 as i64)?;
    let one = const_one(mcx)?;

    let result_var: NumericVar;
    match cmp_numerics(bound1, bound2) {
        0 => {
            return Err(PgError::error("lower bound cannot equal upper bound")
                .with_sqlstate(types_error::ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION));
        }
        d if d < 0 => {
            // bound1 < bound2
            if cmp_numerics(operand, bound1) < 0 {
                result_var = const_zero(mcx);
            } else if cmp_numerics(operand, bound2) >= 0 {
                result_var = add_var(mcx, &count_var, &one)?;
            } else {
                result_var = compute_bucket(mcx, operand, bound1, bound2, &count_var)?;
            }
        }
        _ => {
            // bound1 > bound2
            if cmp_numerics(operand, bound1) > 0 {
                result_var = const_zero(mcx);
            } else if cmp_numerics(operand, bound2) <= 0 {
                result_var = add_var(mcx, &count_var, &one)?;
            } else {
                result_var = compute_bucket(mcx, operand, bound1, bound2, &count_var)?;
            }
        }
    }

    // If result exceeds the range of a legal int4, ereport.
    match numericvar_to_int32(&result_var)? {
        Some(result) => Ok(result),
        None => Err(PgError::error("integer out of range")
            .with_sqlstate(types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)),
    }
}

/// `compute_bucket(operand, bound1, bound2, count_var, result_var)`
/// (numeric.c:2055). result = ((operand - bound1) * count) / (bound2 - bound1)
/// + 1, using floor division.
fn compute_bucket<'mcx>(
    mcx: Mcx<'mcx>,
    operand: &[u8],
    bound1: &[u8],
    bound2: &[u8],
    count_var: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let bound1_var = set_var_from_num(mcx, bound1)?;
    let bound2_var = set_var_from_num(mcx, bound2)?;
    let operand_var = set_var_from_num(mcx, operand)?;

    // operand_var = operand - bound1
    let operand_var = sub_var(mcx, &operand_var, &bound1_var)?;
    // bound2_var = bound2 - bound1
    let bound2_var = sub_var(mcx, &bound2_var, &bound1_var)?;

    // operand_var = operand_var * count, rscale = sum of dscales
    let operand_var = mul_var(
        mcx,
        &operand_var,
        count_var,
        operand_var.dscale + count_var.dscale,
    )?;

    // result = operand_var / bound2_var, floor division (rscale 0, no round, exact)
    let result_var = div_var(mcx, &operand_var, &bound2_var, 0, false, true)?;
    // result += 1
    let one = const_one(mcx)?;
    add_var(mcx, &result_var, &one)
}

// ---------------------------------------------------------------------------
// Scale / typmod / normalize helpers.
// ---------------------------------------------------------------------------

pub fn get_min_scale(num: &[u8]) -> i32 {
    // numeric_min_scale (numeric.c:4310) wrapper over get_min_scale (4255).
    // Special values yield NULL in C; the caller is expected to have screened
    // those, but we mirror the inner get_min_scale over a finite value.
    let ctx = mcx::MemoryContext::new("get_min_scale scratch");
    let mcx = ctx.mcx();
    let var = set_var_from_num(mcx, num).expect("decode finite numeric");
    get_min_scale_var(&var)
}

/// `get_min_scale(var)` (numeric.c:4255): minimum scale required to represent
/// `var` without loss.
fn get_min_scale_var(var: &NumericVar<'_>) -> i32 {
    let digits = var.logical_digits();
    let ndigits = var.ndigits() as i32;

    // Explicitly find the last nonzero digit (the value should be stripped,
    // but don't loop forever if it isn't).
    let mut last_digit_pos = ndigits - 1;
    while last_digit_pos >= 0 && digits[last_digit_pos as usize] == 0 {
        last_digit_pos -= 1;
    }

    if last_digit_pos >= 0 {
        // Min scale assuming the last ndigit has no zeroes.
        let mut min_scale = (last_digit_pos - var.weight) * DEC_DIGITS;
        if min_scale > 0 {
            // Reduce min_scale for trailing zeroes in the last NumericDigit.
            let mut last_digit = digits[last_digit_pos as usize];
            while last_digit % 10 == 0 {
                min_scale -= 1;
                last_digit /= 10;
            }
            min_scale
        } else {
            0
        }
    } else {
        0 // result if input is zero
    }
}

pub fn numeric_trim_scale<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    // numeric_trim_scale (numeric.c:4326).
    if numeric_is_special(num) {
        return duplicate_numeric(mcx, num);
    }
    let mut result = set_var_from_num(mcx, num)?;
    result.dscale = get_min_scale_var(&result);
    make_result(mcx, &result)
}

/// `numeric_normalize(num)` (numeric.c:1026): produce the canonical decimal
/// string for `num`, stripping trailing fractional zeroes (and a now-dangling
/// decimal point). Used by hash-partition pruning. `num` is the whole on-disk
/// byte image. Pure computation; no error path.
pub fn numeric_normalize(num: &[u8]) -> alloc::string::String {
    use alloc::string::ToString;

    // Handle NaN and infinities.
    if numeric_is_special(num) {
        if numeric_is_pinf(num) {
            return "Infinity".to_string();
        } else if numeric_is_ninf(num) {
            return "-Infinity".to_string();
        } else {
            return "NaN".to_string();
        }
    }

    let ctx = mcx::MemoryContext::new("numeric_normalize scratch");
    let mcx = ctx.mcx();
    let x = set_var_from_num(mcx, num).expect("decode finite numeric");
    let mut str = crate::io::get_str_from_var(&x);

    // If there's no decimal point, there's certainly nothing to remove.
    if str.contains('.') {
        let bytes = str.as_bytes();
        // Back up over trailing fractional zeroes. Since there is a decimal
        // point, this loop terminates safely.
        let mut last = bytes.len() - 1;
        while bytes[last] == b'0' {
            last -= 1;
        }
        // We want to get rid of the decimal point too, if it's now last.
        if bytes[last] == b'.' {
            last -= 1;
        }
        // Delete whatever we backed up over.
        str.truncate(last + 1);
    }

    str
}

/// `in_range_numeric_numeric(val, base, offset, sub, less)` (numeric.c:2681):
/// the window-function `RANGE` offset predicate over `numeric`. `val`/`base`/
/// `offset` are whole on-disk byte images. `Err` carries the
/// `ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE` ereport for a negative/NaN
/// offset.
pub fn in_range_numeric_numeric(
    val: &[u8],
    base: &[u8],
    offset: &[u8],
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    // Reject negative (including -Inf) or NaN offset.
    if numeric_is_nan(offset)
        || numeric_is_ninf(offset)
        || numeric_sign_word(offset) == NUMERIC_NEG
    {
        return Err(PgError::error(
            "invalid preceding or following size in window function",
        )
        .with_sqlstate(types_error::ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE));
    }

    let result;
    // Deal with cases where val and/or base is NaN (NaN sorts after non-NaN).
    if numeric_is_nan(val) {
        if numeric_is_nan(base) {
            result = true; // NAN = NAN
        } else {
            result = !less; // NAN > non-NAN
        }
    } else if numeric_is_nan(base) {
        result = less; // non-NAN < NAN
    }
    // Deal with infinite offset (necessarily +Inf, at this point).
    else if numeric_is_special(offset) {
        debug_assert!(numeric_is_pinf(offset));
        if if sub {
            numeric_is_pinf(base)
        } else {
            numeric_is_ninf(base)
        } {
            // base +/- offset would produce NaN, so return true for any val.
            result = true;
        } else if sub {
            // base - offset must be -inf
            if less {
                result = numeric_is_ninf(val); // only -inf is <= sum
            } else {
                result = true; // any val is >= sum
            }
        } else {
            // base + offset must be +inf
            if less {
                result = true; // any val is <= sum
            } else {
                result = numeric_is_pinf(val); // only +inf is >= sum
            }
        }
    }
    // Deal with cases where val and/or base is infinite (offset now finite).
    else if numeric_is_special(val) {
        if numeric_is_pinf(val) {
            if numeric_is_pinf(base) {
                result = true; // PINF = PINF
            } else {
                result = !less; // PINF > any other non-NAN
            }
        } else {
            // val must be NINF
            if numeric_is_ninf(base) {
                result = true; // NINF = NINF
            } else {
                result = less; // NINF < anything else
            }
        }
    } else if numeric_is_special(base) {
        if numeric_is_ninf(base) {
            result = !less; // normal > NINF
        } else {
            result = less; // normal < PINF
        }
    } else {
        // Otherwise compute base +/- offset and compare against val.
        let ctx = mcx::MemoryContext::new("in_range_numeric scratch");
        let mcx = ctx.mcx();
        let valv = set_var_from_num(mcx, val)?;
        let basev = set_var_from_num(mcx, base)?;
        let offsetv = set_var_from_num(mcx, offset)?;
        let sum = if sub {
            sub_var(mcx, &basev, &offsetv)?
        } else {
            add_var(mcx, &basev, &offsetv)?
        };
        if less {
            result = cmp_var(&valv, &sum) != core::cmp::Ordering::Greater;
        } else {
            result = cmp_var(&valv, &sum) != core::cmp::Ordering::Less;
        }
    }

    Ok(result)
}

pub fn numeric_scale(num: &[u8]) -> PgResult<i32> {
    // numeric_scale (numeric.c:4241): special -> NULL (caller's concern); here
    // return the display scale. C returns NULL for specials; we mirror by
    // returning the dscale field which for specials is meaningless but the
    // caller screens specials. We follow the C and treat specials as the SQL
    // NULL upstream; for the in-crate contract we report the stored dscale.
    Ok(types_numeric::numeric_dscale(num) as i32)
}

pub fn numerictypmodin(typmod_parts: &[i32]) -> PgResult<i32> {
    // numerictypmodin (numeric.c:1324). `typmod_parts` is ArrayGetIntegerTypmods.
    let tl = typmod_parts;
    let n = tl.len();
    use types_numeric::make_numeric_typmod;

    if n == 2 {
        if tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION {
            return Err(precision_range_error(tl[0]));
        }
        if tl[1] < NUMERIC_MIN_SCALE || tl[1] > NUMERIC_MAX_SCALE {
            return Err(PgError::error(alloc::format!(
                "NUMERIC scale {} must be between {} and {}",
                tl[1],
                NUMERIC_MIN_SCALE,
                NUMERIC_MAX_SCALE
            ))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE));
        }
        Ok(make_numeric_typmod(tl[0], tl[1]))
    } else if n == 1 {
        if tl[0] < 1 || tl[0] > NUMERIC_MAX_PRECISION {
            return Err(precision_range_error(tl[0]));
        }
        // scale defaults to zero
        Ok(make_numeric_typmod(tl[0], 0))
    } else {
        Err(PgError::error("invalid NUMERIC type modifier")
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE))
    }
}

fn precision_range_error(prec: i32) -> PgError {
    PgError::error(alloc::format!(
        "NUMERIC precision {} must be between 1 and {}",
        prec,
        NUMERIC_MAX_PRECISION
    ))
    .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE)
}

pub fn numerictypmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    // numerictypmodout (numeric.c:1369): cstring "(prec,scale)" or "".
    use types_numeric::{is_valid_numeric_typmod, numeric_typmod_precision, numeric_typmod_scale};
    let s = if is_valid_numeric_typmod(typmod) {
        alloc::format!(
            "({},{})",
            numeric_typmod_precision(typmod),
            numeric_typmod_scale(typmod)
        )
    } else {
        alloc::string::String::new()
    };
    // PG_RETURN_CSTRING: produce a NUL-terminated cstring byte buffer.
    let bytes = s.as_bytes();
    let mut out = alloc_bytes(mcx, bytes.len() + 1)?;
    out[..bytes.len()].copy_from_slice(bytes);
    out[bytes.len()] = 0;
    Ok(out)
}

/// `numeric_maximum_size(typmod)`: the maximum on-disk size of a `numeric` with
/// the given typmod, or -1 if indeterminate. Pure arithmetic; infallible.
pub fn numeric_maximum_size(typmod: i32) -> i32 {
    // numeric_maximum_size (numeric.c:953).
    use types_numeric::{is_valid_numeric_typmod, numeric_typmod_precision};
    if !is_valid_numeric_typmod(typmod) {
        return -1;
    }
    let precision = numeric_typmod_precision(typmod);

    // Worst-case digit count: see comment in numeric.c.
    let numeric_digits = (precision + 2 * (DEC_DIGITS - 1)) / DEC_DIGITS;

    NUMERIC_HDRSZ as i32 + numeric_digits * core::mem::size_of::<NumericDigit>() as i32
}

// ---------------------------------------------------------------------------
// numericvar_to_int32 / numericvar_to_double_no_overflow — internal helpers
// used by the width_bucket / exp cores. (numericvar_to_int32 lives in the
// convert family; width_bucket needs it, so we bridge via numericvar_to_int64
// where appropriate, matching the C int32 conversion semantics.)
// ---------------------------------------------------------------------------

/// `numericvar_to_int32(var)` (numeric.c): convert to i32, `None` on out of
/// range / non-integral. Implemented via the int64 conversion + range check,
/// mirroring numericvar_to_int32 which rounds then range-checks the i64.
fn numericvar_to_int32(var: &NumericVar<'_>) -> PgResult<Option<i32>> {
    match numericvar_to_int64(var)? {
        Some(v) => {
            if v < i32::MIN as i64 || v > i32::MAX as i64 {
                Ok(None)
            } else {
                Ok(Some(v as i32))
            }
        }
        None => Ok(None),
    }
}

/// `numericvar_to_double_no_overflow(var)` (numeric.c:8460): render to string
/// then strtod, ignoring ERANGE. We round-trip through the convert family's
/// `numeric_to_float8` over the encoded image (equivalent: get_str_from_var +
/// strtod), which mirrors the C semantics of ignoring overflow to +/-Inf.
fn numericvar_to_double_no_overflow(mcx: Mcx<'_>, var: &NumericVar<'_>) -> PgResult<f64> {
    let img = make_result(mcx, var)?;
    numeric_to_float8(&img)
}

// ---------------------------------------------------------------------------
// Header-word write helpers over an owned on-disk byte image.
// ---------------------------------------------------------------------------

/// `VARHDRSZ` as a `usize` index into the byte image.
const VARHDRSZ_U: usize = types_datum::VARHDRSZ;

/// Write the first 16-bit header word (`choice.n_header`) into a numeric byte
/// image, native-endian, at byte offset `VARHDRSZ`.
#[inline]
fn write_header_word(num: &mut [u8], word: u16) {
    let b = word.to_ne_bytes();
    num[VARHDRSZ_U] = b[0];
    num[VARHDRSZ_U + 1] = b[1];
}

// ---------------------------------------------------------------------------
// Owned seams.
// ---------------------------------------------------------------------------

/// Implements the `numeric_maximum_size` seam (thin wrapper over the core).
pub fn seam_numeric_maximum_size(typmod: i32) -> i32 {
    numeric_maximum_size(typmod)
}

/// Implements the `numeric_subdiff` seam: the `numrange_subdiff` body —
/// `numeric_float8(numeric_sub(v1, v2))`, the subtype distance `v1 - v2` as a
/// `float8`. `Err` carries the `numeric_sub`/`numeric_float8` `ereport`s.
///
/// `v1`/`v2` are `numeric` `Datum`s — pointer words to detoasted `numeric`
/// varlena images (rangetypes passes already-detoasted Datums through
/// `FunctionCall2Coll`, exactly as C's `DirectFunctionCall2` dereferences a
/// `Numeric` pointer). We read each image's VARSIZE from its 4-byte header to
/// recover the `&[u8]`, run `numeric_sub` in a transient context, and convert
/// the result to `f64`.
pub fn seam_numeric_subdiff(v1: Datum<'_>, v2: Datum<'_>) -> PgResult<f64> {
    let ctx = mcx::MemoryContext::new("numrange_subdiff scratch");
    let mcx = ctx.mcx();

    // SAFETY: v1/v2 are pointer-bearing numeric Datums (detoasted by the
    // caller), so the word points to a numeric varlena whose 4-byte header
    // encodes the total size. This mirrors C's DirectFunctionCall2(numeric_sub,
    // v1, v2), which likewise treats the Datum as a `Numeric` pointer.
    let a = unsafe { numeric_bytes_from_datum(v1) };
    let b = unsafe { numeric_bytes_from_datum(v2) };

    let diff = numeric_sub(mcx, a, b)?;
    numeric_to_float8(&diff)
}

/// Implements the `numeric_subdiff_bytes` seam: the owned-bytes counterpart of
/// [`seam_numeric_subdiff`] — `numeric_float8(numeric_sub(a, b))` over two
/// on-disk `numeric` byte images, with no pointer-deref. `Err` carries the
/// `numeric_sub` / `numeric_float8` `ereport`s.
pub fn seam_numeric_subdiff_bytes(a: &[u8], b: &[u8]) -> PgResult<f64> {
    let ctx = mcx::MemoryContext::new("numeric_subdiff_bytes scratch");
    let mcx = ctx.mcx();
    let diff = numeric_sub(mcx, a, b)?;
    numeric_to_float8(&diff)
}

/// Recover the on-disk `numeric` byte image a pointer-bearing `Datum` refers
/// to. Reads `VARSIZE_4B` from the varlena header to determine the length.
///
/// # Safety
/// The Datum must be a valid pointer to a 4-byte-header (`VARATT_IS_4B_U`)
/// `numeric` varlena that lives at least as long as the returned slice.
unsafe fn numeric_bytes_from_datum<'a>(d: Datum<'_>) -> &'a [u8] {
    let ptr = d.as_usize() as *const u8;
    // VARSIZE_4B: native header word >> 2, low 30 bits (little-endian build).
    let header = core::slice::from_raw_parts(ptr, VARHDRSZ_U);
    let word = u32::from_ne_bytes([header[0], header[1], header[2], header[3]]);
    #[cfg(target_endian = "little")]
    let len = ((word >> 2) & 0x3FFF_FFFF) as usize;
    #[cfg(target_endian = "big")]
    let len = (word & 0x3FFF_FFFF) as usize;
    core::slice::from_raw_parts(ptr, len)
}
