//! Conversions, transcendental math, trigonometric (radian and degree),
//! hyperbolic, error/gamma functions, mixed-precision arithmetic, width_bucket
//! and in_range cores from float.c.
//!
//! Error/overflow semantics: Rust's `std` math functions do not set `errno`, so
//! we use the result-inspection branch that the C code itself supports for
//! platforms that "will not set errno but just return Inf or zero to report
//! overflow/underflow" (see e.g. float.c:1665 in `dexp`). Domain errors that C
//! checks explicitly are checked here the same way and raise the same SQLSTATE
//! + message.
//!
//! The four C-standard-library special functions Rust's `std` does not expose
//! (`erf`, `erfc`, `tgamma`, `lgamma`) are reached through the
//! `backend-utils-adt-float-seams` outward libm seams.

use std::sync::OnceLock;

use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_ARGUMENT_FOR_LOG,
    ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION, ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION,
    ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::{
    float8_div, float8_mul, float_overflow_error, float_underflow_error, get_float8_infinity,
    get_float8_nan, FLOAT4_FITS_IN_INT16, FLOAT4_FITS_IN_INT32, FLOAT8_FITS_IN_INT16,
    FLOAT8_FITS_IN_INT32, M_PI, RADIANS_PER_DEGREE,
};

/// Shared helper for the `input is out of range` errors raised by the trig and
/// hyperbolic domain checks (e.g. float.c:1772, 2703).
fn input_out_of_range() -> PgError {
    PgError::error("input is out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `integer out of range` (float.c:1229) for the float->int conversions.
fn integer_out_of_range() -> PgError {
    PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `smallint out of range` (float.c:1254).
fn smallint_out_of_range() -> PgError {
    PgError::error("smallint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

// ===========================================================================
// CONVERSION ROUTINES (float.c:1173-1355).
// ===========================================================================

/// `ftod()` core (float.c:1182): float4 -> float8.
#[inline]
pub fn ftod(num: f32) -> f64 {
    num as f64
}

/// `dtof()` core (float.c:1194): float8 -> float4, with overflow/underflow.
pub fn dtof(num: f64) -> PgResult<f32> {
    let result = num as f32;
    if result.is_infinite() && !num.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && num != 0.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `dtoi4()` core (float.c:1213): float8 -> int4 (rint, range-checked).
pub fn dtoi4(num: f64) -> PgResult<i32> {
    let num = num.round_ties_even();
    if num.is_nan() || !FLOAT8_FITS_IN_INT32(num) {
        return Err(integer_out_of_range());
    }
    Ok(num as i32)
}

/// `dtoi2()` core (float.c:1238): float8 -> int2 (rint, range-checked).
pub fn dtoi2(num: f64) -> PgResult<i16> {
    let num = num.round_ties_even();
    if num.is_nan() || !FLOAT8_FITS_IN_INT16(num) {
        return Err(smallint_out_of_range());
    }
    Ok(num as i16)
}

/// `i4tod()` core (float.c:1263): int4 -> float8.
#[inline]
pub fn i4tod(num: i32) -> f64 {
    num as f64
}

/// `i2tod()` core (float.c:1275): int2 -> float8.
#[inline]
pub fn i2tod(num: i16) -> f64 {
    num as f64
}

/// `ftoi4()` core (float.c:1287): float4 -> int4 (rint, range-checked).
pub fn ftoi4(num: f32) -> PgResult<i32> {
    let num = num.round_ties_even();
    if num.is_nan() || !FLOAT4_FITS_IN_INT32(num) {
        return Err(integer_out_of_range());
    }
    Ok(num as i32)
}

/// `ftoi2()` core (float.c:1312): float4 -> int2 (rint, range-checked).
pub fn ftoi2(num: f32) -> PgResult<i16> {
    let num = num.round_ties_even();
    if num.is_nan() || !FLOAT4_FITS_IN_INT16(num) {
        return Err(smallint_out_of_range());
    }
    Ok(num as i16)
}

/// `i4tof()` core (float.c:1337): int4 -> float4.
#[inline]
pub fn i4tof(num: i32) -> f32 {
    num as f32
}

/// `i2tof()` core (float.c:1349): int2 -> float4.
#[inline]
pub fn i2tof(num: i16) -> f32 {
    num as f32
}

// ===========================================================================
// RANDOM FLOAT8 OPERATORS (float.c:1364-1481).
// ===========================================================================

/// `dround()` core (float.c:1367): ROUND(arg1) = rint(arg1).
#[inline]
pub fn dround(arg1: f64) -> f64 {
    arg1.round_ties_even()
}

/// `dceil()` core (float.c:1379).
#[inline]
pub fn dceil(arg1: f64) -> f64 {
    arg1.ceil()
}

/// `dfloor()` core (float.c:1391).
#[inline]
pub fn dfloor(arg1: f64) -> f64 {
    arg1.floor()
}

/// `dsign()` core (float.c:1404): -1/0/1. NaN falls into the `else` branch,
/// yielding 0.0, exactly as C.
#[inline]
pub fn dsign(arg1: f64) -> f64 {
    if arg1 > 0.0 {
        1.0
    } else if arg1 < 0.0 {
        -1.0
    } else {
        0.0
    }
}

/// `dtrunc()` core (float.c:1427): truncation towards zero.
#[inline]
pub fn dtrunc(arg1: f64) -> f64 {
    if arg1 >= 0.0 {
        arg1.floor()
    } else {
        -((-arg1).floor())
    }
}

/// `dsqrt()` core (float.c:1445).
pub fn dsqrt(arg1: f64) -> PgResult<f64> {
    if arg1 < 0.0 {
        return Err(
            PgError::error("cannot take square root of a negative number")
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
        );
    }
    let result = arg1.sqrt();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && arg1 != 0.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `dcbrt()` core (float.c:1469).
pub fn dcbrt(arg1: f64) -> PgResult<f64> {
    let result = arg1.cbrt();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && arg1 != 0.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `dpow()` core (float.c:1488): pow(arg1, arg2) with POSIX corner cases handled
/// explicitly and overflow/underflow checks.
pub fn dpow(arg1: f64, arg2: f64) -> PgResult<f64> {
    // NaN ^ 0 = 1, 1 ^ NaN = 1, all other NaN inputs -> NaN.
    if arg1.is_nan() {
        if arg2.is_nan() || arg2 != 0.0 {
            return Ok(get_float8_nan());
        }
        return Ok(1.0);
    }
    if arg2.is_nan() {
        if arg1 != 1.0 {
            return Ok(get_float8_nan());
        }
        return Ok(1.0);
    }

    if arg1 == 0.0 && arg2 < 0.0 {
        return Err(
            PgError::error("zero raised to a negative power is undefined")
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
        );
    }
    if arg1 < 0.0 && arg2.floor() != arg2 {
        return Err(PgError::error(
            "a negative number raised to a non-integer power yields a complex result",
        )
        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION));
    }

    let result;
    if arg2.is_infinite() {
        let absx = arg1.abs();
        if absx == 1.0 {
            result = 1.0;
        } else if arg2 > 0.0 {
            // y = +Inf
            result = if absx > 1.0 { arg2 } else { 0.0 };
        } else {
            // y = -Inf
            result = if absx > 1.0 { 0.0 } else { -arg2 };
        }
    } else if arg1.is_infinite() {
        if arg2 == 0.0 {
            result = 1.0;
        } else if arg1 > 0.0 {
            // x = +Inf
            result = if arg2 > 0.0 { arg1 } else { 0.0 };
        } else {
            // x = -Inf; y is known to be an integer (from the domain check).
            let halfy = arg2 / 2.0;
            let yisoddinteger = halfy.floor() != halfy;
            if arg2 > 0.0 {
                result = if yisoddinteger { arg1 } else { -arg1 };
            } else {
                result = if yisoddinteger { -0.0 } else { 0.0 };
            }
        }
    } else {
        // Finite case. Rust's powf does not set errno, so use the
        // result-inspection branch (the C code supports this path).
        let r = arg1.powf(arg2);
        if r.is_nan() {
            // C: handle the old-glibc x86 bug (abs(y) > 2^63). All real domain
            // errors were handled above, so reaching here means y is large/even.
            if arg1 == 0.0 {
                result = 0.0;
            } else {
                let absx = arg1.abs();
                if absx == 1.0 {
                    result = 1.0;
                } else if if arg2 >= 0.0 { absx > 1.0 } else { absx < 1.0 } {
                    return Err(float_overflow_error());
                } else {
                    return Err(float_underflow_error());
                }
            }
        } else if r.is_infinite() {
            return Err(float_overflow_error());
        } else if r == 0.0 && arg1 != 0.0 {
            return Err(float_underflow_error());
        } else {
            result = r;
        }
    }

    Ok(result)
}

/// `dexp()` core (float.c:1643).
pub fn dexp(arg1: f64) -> PgResult<f64> {
    let result;
    if arg1.is_nan() {
        result = arg1;
    } else if arg1.is_infinite() {
        // Per POSIX, exp(-Inf) is 0.
        result = if arg1 > 0.0 { arg1 } else { 0.0 };
    } else {
        let r = arg1.exp();
        if r.is_infinite() {
            return Err(float_overflow_error());
        }
        if r == 0.0 {
            return Err(float_underflow_error());
        }
        result = r;
    }
    Ok(result)
}

/// `dlog1()` core (float.c:1689): natural logarithm.
pub fn dlog1(arg1: f64) -> PgResult<f64> {
    if arg1 == 0.0 {
        return Err(PgError::error("cannot take logarithm of zero")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG));
    }
    if arg1 < 0.0 {
        return Err(PgError::error("cannot take logarithm of a negative number")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG));
    }
    let result = arg1.ln();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && arg1 != 1.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `dlog10()` core (float.c:1721): base-10 logarithm.
pub fn dlog10(arg1: f64) -> PgResult<f64> {
    if arg1 == 0.0 {
        return Err(PgError::error("cannot take logarithm of zero")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG));
    }
    if arg1 < 0.0 {
        return Err(PgError::error("cannot take logarithm of a negative number")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG));
    }
    let result = arg1.log10();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && arg1 != 1.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

// ===========================================================================
// RADIAN TRIGONOMETRIC FUNCTIONS (float.c:1754-1984).
// ===========================================================================

/// `dacos()` core (float.c:1754).
pub fn dacos(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range());
    }
    let result = arg1.acos();
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dasin()` core (float.c:1786).
pub fn dasin(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range());
    }
    let result = arg1.asin();
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `datan()` core (float.c:1817).
pub fn datan(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.atan();
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `datan2()` core (float.c:1843): atan(arg1/arg2).
pub fn datan2(arg1: f64, arg2: f64) -> PgResult<f64> {
    if arg1.is_nan() || arg2.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.atan2(arg2);
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dcos()` core (float.c:1869).
pub fn dcos(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.cos();
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dcot()` core (float.c:1909): cotangent = 1/tan.
pub fn dcot(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.tan();
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    let result = 1.0 / result;
    // Not checking for overflow because cot(0) == Inf.
    Ok(result)
}

/// `dsin()` core (float.c:1937).
pub fn dsin(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.sin();
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dtan()` core (float.c:1964).
pub fn dtan(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.tan();
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    // Not checking for overflow because tan(pi/2) == Inf.
    Ok(result)
}

// ===========================================================================
// DEGREE-BASED TRIGONOMETRIC FUNCTIONS (float.c:1987-2588).
//
// The cached scaling constants make the degree functions return exact values at
// the cardinal angles. In C they are computed once from "non-const" globals
// purely to defeat compile-time constant folding of sin(constant); at runtime
// computing them directly gives the identical IEEE values. We memoize them.
// ===========================================================================

/// The cached constants from `init_degree_constants()` (float.c:2018).
struct DegreeConsts {
    sin_30: f64,
    one_minus_cos_60: f64,
    asin_0_5: f64,
    acos_0_5: f64,
    atan_1_0: f64,
    tan_45: f64,
    cot_45: f64,
}

fn degree_consts() -> &'static DegreeConsts {
    static CONSTS: OnceLock<DegreeConsts> = OnceLock::new();
    CONSTS.get_or_init(|| {
        let sin_30 = (30.0_f64 * RADIANS_PER_DEGREE).sin();
        let one_minus_cos_60 = 1.0 - (60.0_f64 * RADIANS_PER_DEGREE).cos();
        let asin_0_5 = 0.5_f64.asin();
        let acos_0_5 = 0.5_f64.acos();
        let atan_1_0 = 1.0_f64.atan();
        let partial = DegreeConsts {
            sin_30,
            one_minus_cos_60,
            asin_0_5,
            acos_0_5,
            atan_1_0,
            tan_45: 0.0,
            cot_45: 0.0,
        };
        let tan_45 = sind_q1_with(&partial, 45.0) / cosd_q1_with(&partial, 45.0);
        let cot_45 = cosd_q1_with(&partial, 45.0) / sind_q1_with(&partial, 45.0);
        DegreeConsts {
            sin_30,
            one_minus_cos_60,
            asin_0_5,
            acos_0_5,
            atan_1_0,
            tan_45,
            cot_45,
        }
    })
}

/// `asind_q1()` (float.c:2047): inverse sine in degrees for x in [0,1].
fn asind_q1(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 0.5 {
        let asin_x = x.asin();
        (asin_x / c.asin_0_5) * 30.0
    } else {
        let acos_x = x.acos();
        90.0 - (acos_x / c.acos_0_5) * 60.0
    }
}

/// `acosd_q1()` (float.c:2081): inverse cosine in degrees for x in [0,1].
fn acosd_q1(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 0.5 {
        let asin_x = x.asin();
        90.0 - (asin_x / c.asin_0_5) * 30.0
    } else {
        let acos_x = x.acos();
        (acos_x / c.acos_0_5) * 60.0
    }
}

/// `sind_0_to_30()` (float.c:2251).
fn sind_0_to_30(c: &DegreeConsts, x: f64) -> f64 {
    let sin_x = (x * RADIANS_PER_DEGREE).sin();
    (sin_x / c.sin_30) / 2.0
}

/// `cosd_0_to_60()` (float.c:2265).
fn cosd_0_to_60(c: &DegreeConsts, x: f64) -> f64 {
    let one_minus_cos_x = 1.0 - (x * RADIANS_PER_DEGREE).cos();
    1.0 - (one_minus_cos_x / c.one_minus_cos_60) / 2.0
}

/// `sind_q1()` (float.c:2278).
fn sind_q1_with(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 30.0 {
        sind_0_to_30(c, x)
    } else {
        cosd_0_to_60(c, 90.0 - x)
    }
}

/// `cosd_q1()` (float.c:2298).
fn cosd_q1_with(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 60.0 {
        cosd_0_to_60(c, x)
    } else {
        sind_0_to_30(c, 90.0 - x)
    }
}

/// `dacosd()` core (float.c:2107).
pub fn dacosd(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range());
    }
    let result = if arg1 >= 0.0 {
        acosd_q1(c, arg1)
    } else {
        90.0 + asind_q1(c, -arg1)
    };
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dasind()` core (float.c:2144).
pub fn dasind(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range());
    }
    let result = if arg1 >= 0.0 {
        asind_q1(c, arg1)
    } else {
        -asind_q1(c, -arg1)
    };
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `datand()` core (float.c:2181).
pub fn datand(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    let atan_arg1 = arg1.atan();
    let result = (atan_arg1 / c.atan_1_0) * 45.0;
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `datan2d()` core (float.c:2213).
pub fn datan2d(arg1: f64, arg2: f64) -> PgResult<f64> {
    if arg1.is_nan() || arg2.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    let atan2_arg1_arg2 = arg1.atan2(arg2);
    let result = (atan2_arg1_arg2 / c.atan_1_0) * 45.0;
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dcosd()` core (float.c:2317).
pub fn dcosd(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    let c = degree_consts();
    let mut arg1 = arg1 % 360.0;
    let mut sign = 1.0_f64;
    if arg1 < 0.0 {
        arg1 = -arg1;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
        sign = -sign;
    }
    let result = sign * cosd_q1_with(c, arg1);
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dcotd()` core (float.c:2372).
pub fn dcotd(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    let c = degree_consts();
    let mut arg1 = arg1 % 360.0;
    let mut sign = 1.0_f64;
    if arg1 < 0.0 {
        arg1 = -arg1;
        sign = -sign;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
        sign = -sign;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
        sign = -sign;
    }
    let cot_arg1 = cosd_q1_with(c, arg1) / sind_q1_with(c, arg1);
    let mut result = sign * (cot_arg1 / c.cot_45);
    // Force a plain zero (avoid minus zero); see float.c:2426.
    if result == 0.0 {
        result = 0.0;
    }
    // Not checking for overflow because cotd(0) == Inf.
    Ok(result)
}

/// `dsind()` core (float.c:2438).
pub fn dsind(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    let c = degree_consts();
    let mut arg1 = arg1 % 360.0;
    let mut sign = 1.0_f64;
    if arg1 < 0.0 {
        arg1 = -arg1;
        sign = -sign;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
        sign = -sign;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
    }
    let result = sign * sind_q1_with(c, arg1);
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dtand()` core (float.c:2494).
pub fn dtand(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range());
    }
    let c = degree_consts();
    let mut arg1 = arg1 % 360.0;
    let mut sign = 1.0_f64;
    if arg1 < 0.0 {
        arg1 = -arg1;
        sign = -sign;
    }
    if arg1 > 180.0 {
        arg1 = 360.0 - arg1;
        sign = -sign;
    }
    if arg1 > 90.0 {
        arg1 = 180.0 - arg1;
        sign = -sign;
    }
    let tan_arg1 = sind_q1_with(c, arg1) / cosd_q1_with(c, arg1);
    let mut result = sign * (tan_arg1 / c.tan_45);
    if result == 0.0 {
        result = 0.0;
    }
    // Not checking for overflow because tand(90) == Inf.
    Ok(result)
}

/// `degrees()` core (float.c:2560): radians -> degrees.
pub fn degrees(arg1: f64) -> PgResult<f64> {
    float8_div(arg1, RADIANS_PER_DEGREE)
}

/// `dpi()` core (float.c:2572).
#[inline]
pub fn dpi() -> f64 {
    M_PI
}

/// `radians()` core (float.c:2582): degrees -> radians.
pub fn radians(arg1: f64) -> PgResult<f64> {
    float8_mul(arg1, RADIANS_PER_DEGREE)
}

// ===========================================================================
// HYPERBOLIC FUNCTIONS (float.c:2591-2742).
// ===========================================================================

/// `dsinh()` core (float.c:2597). sinh overflow yields +-Inf, the same value
/// C produces from its ERANGE handling, so we leave the result as-is.
pub fn dsinh(arg1: f64) -> f64 {
    arg1.sinh()
}

/// `dcosh()` core (float.c:2627). cosh overflow yields +Inf; underflow is
/// impossible (cosh >= 1), but C still guards `result == 0.0`.
pub fn dcosh(arg1: f64) -> PgResult<f64> {
    let result = arg1.cosh();
    if result == 0.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `dtanh()` core (float.c:2651).
pub fn dtanh(arg1: f64) -> PgResult<f64> {
    let result = arg1.tanh();
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `dasinh()` core (float.c:2671).
#[inline]
pub fn dasinh(arg1: f64) -> f64 {
    arg1.asinh()
}

/// `dacosh()` core (float.c:2688): domain x >= 1.
pub fn dacosh(arg1: f64) -> PgResult<f64> {
    if arg1 < 1.0 {
        return Err(input_out_of_range());
    }
    Ok(arg1.acosh())
}

/// `datanh()` core (float.c:2713): domain (-1, 1), endpoints -> +-Inf.
pub fn datanh(arg1: f64) -> PgResult<f64> {
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range());
    }
    let result = if arg1 == -1.0 {
        -get_float8_infinity()
    } else if arg1 == 1.0 {
        get_float8_infinity()
    } else {
        arg1.atanh()
    };
    Ok(result)
}

// ===========================================================================
// ERROR FUNCTIONS (float.c:2745-2786).
// ===========================================================================

/// `derf()` core (float.c:2751).
pub fn derf(arg1: f64) -> PgResult<f64> {
    let result = erf(arg1);
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `derfc()` core (float.c:2771).
pub fn derfc(arg1: f64) -> PgResult<f64> {
    let result = erfc(arg1);
    if result.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

// ===========================================================================
// GAMMA FUNCTIONS (float.c:2789-2874).
// ===========================================================================

/// `dgamma()` core (float.c:2795): the gamma function (C's `tgamma`).
pub fn dgamma(arg1: f64) -> PgResult<f64> {
    let result;
    if arg1.is_nan() {
        result = arg1;
    } else if arg1.is_infinite() {
        if arg1 < 0.0 {
            // Per POSIX, -Inf is a domain error.
            return Err(float_overflow_error());
        }
        result = arg1;
    } else {
        let r = tgamma(arg1);
        // No errno; inspect the result (C's fallback path). tgamma has no
        // zeros, so a 0 result means underflow.
        if r.is_infinite() || r.is_nan() {
            if r != 0.0 {
                return Err(float_overflow_error());
            }
            return Err(float_underflow_error());
        }
        if r == 0.0 {
            return Err(float_underflow_error());
        }
        result = r;
    }
    Ok(result)
}

/// `dlgamma()` core (float.c:2849): natural log of |gamma(arg1)|.
pub fn dlgamma(arg1: f64) -> PgResult<f64> {
    let result = lgamma(arg1);
    // ERANGE means overflow or a pole (zero / negative-integer input -> +Inf).
    // No errno; an infinite result from a finite input is the signal.
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

// ===========================================================================
// MIXED-PRECISION ARITHMETIC (float.c:3855-3937).
// ===========================================================================

/// `float48pl()` core (float.c:3861).
pub fn float48pl(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_pl(arg1 as f64, arg2)
}

/// `float48mi()` core (float.c:3870).
pub fn float48mi(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_mi(arg1 as f64, arg2)
}

/// `float48mul()` core (float.c:3879).
pub fn float48mul(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_mul(arg1 as f64, arg2)
}

/// `float48div()` core (float.c:3888).
pub fn float48div(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_div(arg1 as f64, arg2)
}

/// `float84pl()` core (float.c:3903).
pub fn float84pl(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_pl(arg1, arg2 as f64)
}

/// `float84mi()` core (float.c:3912).
pub fn float84mi(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_mi(arg1, arg2 as f64)
}

/// `float84mul()` core (float.c:3921).
pub fn float84mul(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_mul(arg1, arg2 as f64)
}

/// `float84div()` core (float.c:3930).
pub fn float84div(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_div(arg1, arg2 as f64)
}

// ===========================================================================
// MIXED-PRECISION COMPARISON (float.c:3948-4057).
// ===========================================================================

/// `float48eq()` core (float.c:3949).
pub fn float48eq(arg1: f32, arg2: f64) -> bool {
    crate::float8_eq(arg1 as f64, arg2)
}
/// `float48ne()` core (float.c:3957).
pub fn float48ne(arg1: f32, arg2: f64) -> bool {
    crate::float8_ne(arg1 as f64, arg2)
}
/// `float48lt()` core (float.c:3966).
pub fn float48lt(arg1: f32, arg2: f64) -> bool {
    crate::float8_lt(arg1 as f64, arg2)
}
/// `float48le()` core (float.c:3975).
pub fn float48le(arg1: f32, arg2: f64) -> bool {
    crate::float8_le(arg1 as f64, arg2)
}
/// `float48gt()` core (float.c:3984).
pub fn float48gt(arg1: f32, arg2: f64) -> bool {
    crate::float8_gt(arg1 as f64, arg2)
}
/// `float48ge()` core (float.c:3993).
pub fn float48ge(arg1: f32, arg2: f64) -> bool {
    crate::float8_ge(arg1 as f64, arg2)
}

/// `float84eq()` core (float.c:4005).
pub fn float84eq(arg1: f64, arg2: f32) -> bool {
    crate::float8_eq(arg1, arg2 as f64)
}
/// `float84ne()` core (float.c:4014).
pub fn float84ne(arg1: f64, arg2: f32) -> bool {
    crate::float8_ne(arg1, arg2 as f64)
}
/// `float84lt()` core (float.c:4023).
pub fn float84lt(arg1: f64, arg2: f32) -> bool {
    crate::float8_lt(arg1, arg2 as f64)
}
/// `float84le()` core (float.c:4032).
pub fn float84le(arg1: f64, arg2: f32) -> bool {
    crate::float8_le(arg1, arg2 as f64)
}
/// `float84gt()` core (float.c:4041).
pub fn float84gt(arg1: f64, arg2: f32) -> bool {
    crate::float8_gt(arg1, arg2 as f64)
}
/// `float84ge()` core (float.c:4050).
pub fn float84ge(arg1: f64, arg2: f32) -> bool {
    crate::float8_ge(arg1, arg2 as f64)
}

// ===========================================================================
// in_range support (float.c:1026-1170).
// ===========================================================================

/// `in_range_float8_float8()` core (float.c:1026).
pub fn in_range_float8_float8(
    val: f64,
    base: f64,
    offset: f64,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    if offset.is_nan() || offset < 0.0 {
        return Err(
            PgError::error("invalid preceding or following size in window function")
                .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
        );
    }

    if val.is_nan() {
        return Ok(if base.is_nan() { true } else { !less });
    } else if base.is_nan() {
        return Ok(less);
    }

    if offset.is_infinite() && base.is_infinite() && (if sub { base > 0.0 } else { base < 0.0 }) {
        return Ok(true);
    }

    let sum = if sub { base - offset } else { base + offset };

    Ok(if less { val <= sum } else { val >= sum })
}

/// `in_range_float4_float8()` core (float.c:1102).
pub fn in_range_float4_float8(
    val: f32,
    base: f32,
    offset: f64,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    if offset.is_nan() || offset < 0.0 {
        return Err(
            PgError::error("invalid preceding or following size in window function")
                .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
        );
    }

    if val.is_nan() {
        return Ok(if base.is_nan() { true } else { !less });
    } else if base.is_nan() {
        return Ok(less);
    }

    if offset.is_infinite() && base.is_infinite() && (if sub { base > 0.0 } else { base < 0.0 }) {
        return Ok(true);
    }

    let base = base as f64;
    let sum = if sub { base - offset } else { base + offset };
    let val = val as f64;

    Ok(if less { val <= sum } else { val >= sum })
}

// ===========================================================================
// width_bucket_float8 (float.c:4073).
// ===========================================================================

/// `width_bucket_float8()` core (float.c:4073).
pub fn width_bucket_float8(operand: f64, bound1: f64, bound2: f64, count: i32) -> PgResult<i32> {
    if count <= 0 {
        return Err(PgError::error("count must be greater than zero")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION));
    }

    if operand.is_nan() || bound1.is_nan() || bound2.is_nan() {
        return Err(
            PgError::error("operand, lower bound, and upper bound cannot be NaN")
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION),
        );
    }

    if bound1.is_infinite() || bound2.is_infinite() {
        return Err(PgError::error("lower and upper bounds must be finite")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION));
    }

    let result: i32;
    if bound1 < bound2 {
        if operand < bound1 {
            result = 0;
        } else if operand >= bound2 {
            result = count.checked_add(1).ok_or_else(integer_out_of_range)?;
        } else {
            let mut r: i32;
            if !(bound2 - bound1).is_infinite() {
                r = (count as f64 * ((operand - bound1) / (bound2 - bound1))) as i32;
            } else {
                r = (count as f64
                    * ((operand / 2.0 - bound1 / 2.0) / (bound2 / 2.0 - bound1 / 2.0)))
                    as i32;
            }
            if r >= count {
                r = count - 1;
            }
            result = r + 1;
        }
    } else if bound1 > bound2 {
        if operand > bound1 {
            result = 0;
        } else if operand <= bound2 {
            result = count.checked_add(1).ok_or_else(integer_out_of_range)?;
        } else {
            let mut r: i32;
            if !(bound1 - bound2).is_infinite() {
                r = (count as f64 * ((bound1 - operand) / (bound1 - bound2))) as i32;
            } else {
                r = (count as f64
                    * ((bound1 / 2.0 - operand / 2.0) / (bound1 / 2.0 - bound2 / 2.0)))
                    as i32;
            }
            if r >= count {
                r = count - 1;
            }
            result = r + 1;
        }
    } else {
        return Err(PgError::error("lower bound cannot equal upper bound")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION));
    }

    Ok(result)
}

// ===========================================================================
// libm special functions not exposed by Rust std (erf/erfc/tgamma/lgamma),
// reached through the float-seams outward libm seams.
// ===========================================================================

#[inline]
fn erf(x: f64) -> f64 {
    float_seams::erf::call(x)
}

#[inline]
fn erfc(x: f64) -> f64 {
    float_seams::erfc::call(x)
}

#[inline]
fn tgamma(x: f64) -> f64 {
    float_seams::tgamma::call(x)
}

#[inline]
fn lgamma(x: f64) -> f64 {
    float_seams::lgamma::call(x)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_libm_seams() {
        use std::sync::Once;
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            float_seams::erf::set(test_erf);
            float_seams::erfc::set(|x| 1.0 - test_erf(x));
            float_seams::tgamma::set(test_tgamma);
            float_seams::lgamma::set(|x| test_tgamma(x).abs().ln());
        });
    }

    fn test_erf(x: f64) -> f64 {
        if x == 0.0 {
            return 0.0;
        }
        // Abramowitz & Stegun 7.1.26 approximation (|error| < 1.5e-7).
        let t = 1.0 / (1.0 + 0.3275911 * x.abs());
        let y = 1.0
            - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t
                + 0.254829592)
                * t
                * (-x * x).exp();
        if x < 0.0 {
            -y
        } else {
            y
        }
    }

    fn test_tgamma(x: f64) -> f64 {
        if x.is_infinite() {
            return x;
        }
        if x > 0.0 && x.fract() == 0.0 && x <= 20.0 {
            let mut acc = 1.0;
            let mut k = x - 1.0;
            while k > 1.0 {
                acc *= k;
                k -= 1.0;
            }
            return acc;
        }
        if x == 0.0 {
            return f64::INFINITY;
        }
        (2.0 * std::f64::consts::PI / x).sqrt() * (x / std::f64::consts::E).powf(x)
    }

    #[test]
    fn conversions_round_and_range_check() {
        assert_eq!(dtoi4(2.5).unwrap(), 2);
        assert_eq!(dtoi4(3.5).unwrap(), 4);
        assert_eq!(dtoi4(-2.5).unwrap(), -2);
        assert_eq!(dtoi4(2147483647.0).unwrap(), 2147483647);
        assert!(dtoi4(2147483648.0).is_err());
        assert!(dtoi4(f64::NAN).is_err());
        assert_eq!(dtoi2(100.4).unwrap(), 100);
        assert!(dtoi2(40000.0).is_err());

        assert!(dtof(1e40).is_err());
        let err = dtof(1e40).unwrap_err();
        assert_eq!(err.message(), "value out of range: overflow");
        assert!(dtof(1e-50).is_err());
        assert_eq!(dtof(1.5).unwrap(), 1.5_f32);
        assert_eq!(dtof(f64::INFINITY).unwrap(), f32::INFINITY);
    }

    #[test]
    fn sqrt_cbrt_pow_domains() {
        assert_eq!(dsqrt(4.0).unwrap(), 2.0);
        let err = dsqrt(-1.0).unwrap_err();
        assert_eq!(err.message(), "cannot take square root of a negative number");
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION);

        assert_eq!(dcbrt(27.0).unwrap(), 3.0);
        assert_eq!(dcbrt(-8.0).unwrap(), -2.0);

        assert_eq!(dpow(2.0, 10.0).unwrap(), 1024.0);
        assert_eq!(dpow(f64::NAN, 0.0).unwrap(), 1.0);
        assert_eq!(dpow(1.0, f64::NAN).unwrap(), 1.0);
        assert!(dpow(f64::NAN, 2.0).unwrap().is_nan());
        let err = dpow(0.0, -1.0).unwrap_err();
        assert_eq!(err.message(), "zero raised to a negative power is undefined");
        let err = dpow(-2.0, 0.5).unwrap_err();
        assert_eq!(
            err.message(),
            "a negative number raised to a non-integer power yields a complex result"
        );
        assert_eq!(dpow(2.0, f64::INFINITY).unwrap(), f64::INFINITY);
        assert_eq!(dpow(0.5, f64::INFINITY).unwrap(), 0.0);
        assert_eq!(dpow(f64::INFINITY, 0.0).unwrap(), 1.0);
        assert_eq!(dpow(f64::NEG_INFINITY, 2.0).unwrap(), f64::INFINITY);
        assert_eq!(dpow(f64::NEG_INFINITY, 3.0).unwrap(), f64::NEG_INFINITY);
        assert!(dpow(10.0, 400.0).is_err());
    }

    #[test]
    fn log_exp_domains() {
        assert_eq!(dlog1(std::f64::consts::E).unwrap(), 1.0);
        let err = dlog1(0.0).unwrap_err();
        assert_eq!(err.message(), "cannot take logarithm of zero");
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_ARGUMENT_FOR_LOG);
        let err = dlog1(-1.0).unwrap_err();
        assert_eq!(err.message(), "cannot take logarithm of a negative number");
        assert_eq!(dlog10(1000.0).unwrap(), 3.0);

        assert_eq!(dexp(0.0).unwrap(), 1.0);
        assert!(dexp(f64::NAN).unwrap().is_nan());
        assert_eq!(dexp(f64::NEG_INFINITY).unwrap(), 0.0);
        assert_eq!(dexp(f64::INFINITY).unwrap(), f64::INFINITY);
        assert!(dexp(1000.0).is_err());
    }

    #[test]
    fn trig_radian_domains_and_nan() {
        assert!(dacos(2.0).is_err());
        assert!(dacos(f64::NAN).unwrap().is_nan());
        assert!(dasin(f64::NAN).unwrap().is_nan());
        assert_eq!(dacos(1.0).unwrap(), 0.0);
        assert!((dasin(1.0).unwrap() - std::f64::consts::FRAC_PI_2).abs() < 1e-15);
        assert!(dsin(f64::INFINITY).is_err());
        assert!(dcos(f64::INFINITY).is_err());
        assert!(dtan(f64::INFINITY).is_err());
        assert!((datan(f64::INFINITY).unwrap() - std::f64::consts::FRAC_PI_2).abs() < 1e-15);
    }

    #[test]
    fn degree_trig_exact_cardinals() {
        assert_eq!(dsind(30.0).unwrap(), 0.5);
        assert_eq!(dsind(90.0).unwrap(), 1.0);
        assert_eq!(dsind(0.0).unwrap(), 0.0);
        assert_eq!(dsind(180.0).unwrap(), 0.0);
        assert_eq!(dcosd(0.0).unwrap(), 1.0);
        assert_eq!(dcosd(60.0).unwrap(), 0.5);
        assert_eq!(dcosd(90.0).unwrap(), 0.0);
        assert_eq!(dtand(45.0).unwrap(), 1.0);
        assert_eq!(dasind(0.5).unwrap(), 30.0);
        assert_eq!(dacosd(0.5).unwrap(), 60.0);
        assert_eq!(datand(1.0).unwrap(), 45.0);
        assert!(dsind(f64::INFINITY).is_err());
        assert!(dsind(f64::NAN).unwrap().is_nan());
    }

    #[test]
    fn degrees_radians_pi() {
        assert_eq!(dpi(), std::f64::consts::PI);
        assert!((radians(180.0).unwrap() - M_PI).abs() < 1e-12);
        assert!((degrees(M_PI).unwrap() - 180.0).abs() < 1e-12);
    }

    #[test]
    fn hyperbolic_and_special() {
        assert_eq!(dsinh(0.0), 0.0);
        assert_eq!(dcosh(0.0).unwrap(), 1.0);
        assert_eq!(dtanh(0.0).unwrap(), 0.0);
        assert!(datanh(1.0).unwrap().is_infinite());
        assert!(datanh(-1.0).unwrap() == f64::NEG_INFINITY);
        assert!(dacosh(0.5).is_err());
        assert_eq!(dacosh(1.0).unwrap(), 0.0);
        assert_eq!(dsinh(1000.0), f64::INFINITY);
        assert_eq!(dcosh(1000.0).unwrap(), f64::INFINITY);
    }

    #[test]
    fn erf_gamma() {
        install_libm_seams();
        assert!((derf(0.0).unwrap() - 0.0).abs() < 1e-15);
        assert!((derfc(0.0).unwrap() - 1.0).abs() < 1e-15);
        assert!((dgamma(5.0).unwrap() - 24.0).abs() < 1e-9);
        assert_eq!(dgamma(f64::INFINITY).unwrap(), f64::INFINITY);
        assert!(dgamma(f64::NEG_INFINITY).is_err());
        assert!((dlgamma(1.0).unwrap() - 0.0).abs() < 1e-12);
        assert!(dlgamma(0.0).is_err());
    }

    #[test]
    fn width_bucket_basic() {
        assert_eq!(width_bucket_float8(5.0, 0.0, 10.0, 5).unwrap(), 3);
        assert_eq!(width_bucket_float8(-1.0, 0.0, 10.0, 5).unwrap(), 0);
        assert_eq!(width_bucket_float8(100.0, 0.0, 10.0, 5).unwrap(), 6);
        assert_eq!(width_bucket_float8(5.0, 10.0, 0.0, 5).unwrap(), 3);
        assert!(width_bucket_float8(5.0, 0.0, 10.0, 0).is_err());
        assert!(width_bucket_float8(f64::NAN, 0.0, 10.0, 5).is_err());
        assert!(width_bucket_float8(5.0, 0.0, 0.0, 5).is_err());
        assert!(width_bucket_float8(5.0, f64::INFINITY, 10.0, 5).is_err());
    }

    #[test]
    fn in_range_float8() {
        assert!(in_range_float8_float8(5.0, 3.0, 2.0, false, false).unwrap());
        assert!(!in_range_float8_float8(6.0, 3.0, 2.0, false, true).unwrap());
        assert!(in_range_float8_float8(1.0, 1.0, -1.0, false, false).is_err());
        assert!(in_range_float8_float8(f64::NAN, f64::NAN, 1.0, false, true).unwrap());
    }

    #[test]
    fn mixed_precision() {
        assert_eq!(float48pl(1.5_f32, 2.5).unwrap(), 4.0);
        assert_eq!(float84mul(2.0, 3.0_f32).unwrap(), 6.0);
        assert!(float48eq(1.0_f32, 1.0));
        assert!(float84lt(1.0, 2.0_f32));
        assert_eq!(crate::btfloat48cmp(1.0_f32, 2.0), -1);
        assert_eq!(crate::btfloat84cmp(2.0, 1.0_f32), 1);
    }
}
