use std::sync::OnceLock;

use ::types_error::{
    PgError, PgResult, ERRCODE_INVALID_ARGUMENT_FOR_LOG,
    ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION, ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION,
    ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

use crate::{
    float4_fits_in_int16, float4_fits_in_int32, float8_div, float8_fits_in_int16,
    float8_fits_in_int32, float8_mul, float_overflow_error, float_underflow_error,
    get_float8_nan, M_PI, RADIANS_PER_DEGREE,
};

// erf/erfc/tgamma are absent from Rust std; C's own libm is the parity
// reference, so bind it directly. lgamma_r avoids libm's signgam global.
// atanh rides here too: Rust std computes it as 0.5*ln_1p(2x/(1-x)), which
// lands one ulp off libm's atanh on some inputs — C calls libm directly and
// the fnconf byte-diff caught the drift (OID 2467; e.g. C 18.3
// atanh(-1.3990760221756862e-5) → -1.399076022266972e-05, the formula gives
// ...669721e-05).
mod libm {
    extern "C" {
        pub fn erf(x: f64) -> f64;
        pub fn erfc(x: f64) -> f64;
        pub fn tgamma(x: f64) -> f64;
        pub fn lgamma_r(x: f64, signp: *mut core::ffi::c_int) -> f64;
        pub fn atanh(x: f64) -> f64;
    }
}

#[inline]
fn erf(x: f64) -> f64 {
    // SAFETY: pure libm function, no preconditions.
    unsafe { libm::erf(x) }
}

#[inline]
fn erfc(x: f64) -> f64 {
    // SAFETY: pure libm function, no preconditions.
    unsafe { libm::erfc(x) }
}

#[inline]
fn tgamma(x: f64) -> f64 {
    // SAFETY: pure libm function, no preconditions.
    unsafe { libm::tgamma(x) }
}

#[inline]
fn lgamma(x: f64) -> f64 {
    let mut sign: core::ffi::c_int = 0;
    // SAFETY: sign is a valid out-pointer for the reentrant variant.
    unsafe { libm::lgamma_r(x, &mut sign) }
}

#[cold]
#[inline(never)]
fn input_out_of_range() -> PgError {
    PgError::error("input is out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn integer_out_of_range() -> PgError {
    PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
fn smallint_out_of_range() -> PgError {
    PgError::error("smallint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[inline]
pub fn ftod(num: f32) -> f64 {
    num as f64
}

pub fn dtof(num: f64) -> PgResult<f32> {
    let result = num as f32;
    if result.is_infinite() && !num.is_infinite() {
        return Err(float_overflow_error().into());
    }
    if result == 0.0 && num != 0.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result)
}

pub fn dtoi4(num: f64) -> PgResult<i32> {
    let num = num.round_ties_even();
    if num.is_nan() || !float8_fits_in_int32(num) {
        return Err(integer_out_of_range().into());
    }
    Ok(num as i32)
}

pub fn dtoi2(num: f64) -> PgResult<i16> {
    let num = num.round_ties_even();
    if num.is_nan() || !float8_fits_in_int16(num) {
        return Err(smallint_out_of_range().into());
    }
    Ok(num as i16)
}

#[inline]
pub fn i4tod(num: i32) -> f64 {
    num as f64
}

#[inline]
pub fn i2tod(num: i16) -> f64 {
    num as f64
}

pub fn ftoi4(num: f32) -> PgResult<i32> {
    let num = num.round_ties_even();
    if num.is_nan() || !float4_fits_in_int32(num) {
        return Err(integer_out_of_range().into());
    }
    Ok(num as i32)
}

pub fn ftoi2(num: f32) -> PgResult<i16> {
    let num = num.round_ties_even();
    if num.is_nan() || !float4_fits_in_int16(num) {
        return Err(smallint_out_of_range().into());
    }
    Ok(num as i16)
}

#[inline]
pub fn i4tof(num: i32) -> f32 {
    num as f32
}

#[inline]
pub fn i2tof(num: i16) -> f32 {
    num as f32
}

#[inline]
pub fn dround(arg1: f64) -> f64 {
    arg1.round_ties_even()
}

#[inline]
pub fn dceil(arg1: f64) -> f64 {
    arg1.ceil()
}

#[inline]
pub fn dfloor(arg1: f64) -> f64 {
    arg1.floor()
}

// NaN yields the else branch: 0.0, exactly as C.
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

#[inline]
pub fn dtrunc(arg1: f64) -> f64 {
    if arg1 >= 0.0 {
        arg1.floor()
    } else {
        -((-arg1).floor())
    }
}

pub fn dsqrt(arg1: f64) -> PgResult<f64> {
    if arg1 < 0.0 {
        return Err(PgError::error("cannot take square root of a negative number")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
            .into());
    }
    let result = arg1.sqrt();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error().into());
    }
    if result == 0.0 && arg1 != 0.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result)
}

pub fn dcbrt(arg1: f64) -> PgResult<f64> {
    let result = arg1.cbrt();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error().into());
    }
    if result == 0.0 && arg1 != 0.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result)
}

pub fn dpow(arg1: f64, arg2: f64) -> PgResult<f64> {
    // NaN ^ 0 = 1, 1 ^ NaN = 1, all other NaN inputs -> NaN (POSIX).
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
        return Err(PgError::error("zero raised to a negative power is undefined")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
            .into());
    }
    if arg1 < 0.0 && arg2.floor() != arg2 {
        return Err(PgError::error(
            "a negative number raised to a non-integer power yields a complex result",
        )
        .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION)
        .into());
    }

    let result;
    if arg2.is_infinite() {
        let absx = arg1.abs();
        if absx == 1.0 {
            result = 1.0;
        } else if arg2 > 0.0 {
            result = if absx > 1.0 { arg2 } else { 0.0 };
        } else {
            result = if absx > 1.0 { 0.0 } else { -arg2 };
        }
    } else if arg1.is_infinite() {
        if arg2 == 0.0 {
            result = 1.0;
        } else if arg1 > 0.0 {
            result = if arg2 > 0.0 { arg1 } else { 0.0 };
        } else {
            // arg1 = -Inf; arg2 is an integer per the domain check above.
            let halfy = arg2 / 2.0;
            let yisoddinteger = halfy.floor() != halfy;
            if arg2 > 0.0 {
                result = if yisoddinteger { arg1 } else { -arg1 };
            } else {
                result = if yisoddinteger { -0.0 } else { 0.0 };
            }
        }
    } else {
        let r = arg1.powf(arg2);
        if r.is_nan() {
            // C's old-glibc |y| > 2^63 fallback; real domain errors are gone.
            if arg1 == 0.0 {
                result = 0.0;
            } else {
                let absx = arg1.abs();
                if absx == 1.0 {
                    result = 1.0;
                } else if if arg2 >= 0.0 { absx > 1.0 } else { absx < 1.0 } {
                    return Err(float_overflow_error().into());
                } else {
                    return Err(float_underflow_error().into());
                }
            }
        } else if r.is_infinite() {
            return Err(float_overflow_error().into());
        } else if r == 0.0 && arg1 != 0.0 {
            return Err(float_underflow_error().into());
        } else {
            result = r;
        }
    }

    Ok(result)
}

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
            return Err(float_overflow_error().into());
        }
        if r == 0.0 {
            return Err(float_underflow_error().into());
        }
        result = r;
    }
    Ok(result)
}

pub fn dlog1(arg1: f64) -> PgResult<f64> {
    if arg1 == 0.0 {
        return Err(PgError::error("cannot take logarithm of zero")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
            .into());
    }
    if arg1 < 0.0 {
        return Err(PgError::error("cannot take logarithm of a negative number")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
            .into());
    }
    let result = arg1.ln();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error().into());
    }
    if result == 0.0 && arg1 != 1.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result)
}

pub fn dlog10(arg1: f64) -> PgResult<f64> {
    if arg1 == 0.0 {
        return Err(PgError::error("cannot take logarithm of zero")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
            .into());
    }
    if arg1 < 0.0 {
        return Err(PgError::error("cannot take logarithm of a negative number")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_LOG)
            .into());
    }
    let result = arg1.log10();
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error().into());
    }
    if result == 0.0 && arg1 != 1.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result)
}

pub fn dacos(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range().into());
    }
    let result = arg1.acos();
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dasin(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range().into());
    }
    let result = arg1.asin();
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn datan(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.atan();
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn datan2(arg1: f64, arg2: f64) -> PgResult<f64> {
    if arg1.is_nan() || arg2.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.atan2(arg2);
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dcos(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.cos();
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
    }
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dcot(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.tan();
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
    }
    // No overflow check: cot(0) == Inf.
    Ok(1.0 / result)
}

pub fn dsin(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.sin();
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
    }
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dtan(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let result = arg1.tan();
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
    }
    // No overflow check: tan(pi/2) == Inf.
    Ok(result)
}

// init_degree_constants() (float.c): cached scaling constants that make the
// degree-based functions exact at cardinal angles. C computes them from
// volatile globals only to defeat constant folding; runtime evaluation here
// produces the identical IEEE values.
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
        let partial = DegreeConsts {
            sin_30: (30.0_f64 * RADIANS_PER_DEGREE).sin(),
            one_minus_cos_60: 1.0 - (60.0_f64 * RADIANS_PER_DEGREE).cos(),
            asin_0_5: 0.5_f64.asin(),
            acos_0_5: 0.5_f64.acos(),
            atan_1_0: 1.0_f64.atan(),
            tan_45: 0.0,
            cot_45: 0.0,
        };
        DegreeConsts {
            tan_45: sind_q1(&partial, 45.0) / cosd_q1(&partial, 45.0),
            cot_45: cosd_q1(&partial, 45.0) / sind_q1(&partial, 45.0),
            ..partial
        }
    })
}

fn asind_q1(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 0.5 {
        (x.asin() / c.asin_0_5) * 30.0
    } else {
        90.0 - (x.acos() / c.acos_0_5) * 60.0
    }
}

fn acosd_q1(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 0.5 {
        90.0 - (x.asin() / c.asin_0_5) * 30.0
    } else {
        (x.acos() / c.acos_0_5) * 60.0
    }
}

fn sind_0_to_30(c: &DegreeConsts, x: f64) -> f64 {
    ((x * RADIANS_PER_DEGREE).sin() / c.sin_30) / 2.0
}

fn cosd_0_to_60(c: &DegreeConsts, x: f64) -> f64 {
    1.0 - ((1.0 - (x * RADIANS_PER_DEGREE).cos()) / c.one_minus_cos_60) / 2.0
}

fn sind_q1(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 30.0 {
        sind_0_to_30(c, x)
    } else {
        cosd_0_to_60(c, 90.0 - x)
    }
}

fn cosd_q1(c: &DegreeConsts, x: f64) -> f64 {
    if x <= 60.0 {
        cosd_0_to_60(c, x)
    } else {
        sind_0_to_30(c, 90.0 - x)
    }
}

pub fn dacosd(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range().into());
    }
    let result = if arg1 >= 0.0 {
        acosd_q1(c, arg1)
    } else {
        90.0 + asind_q1(c, -arg1)
    };
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dasind(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range().into());
    }
    let result = if arg1 >= 0.0 {
        asind_q1(c, arg1)
    } else {
        -asind_q1(c, -arg1)
    };
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn datand(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    let result = (arg1.atan() / c.atan_1_0) * 45.0;
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn datan2d(arg1: f64, arg2: f64) -> PgResult<f64> {
    if arg1.is_nan() || arg2.is_nan() {
        return Ok(get_float8_nan());
    }
    let c = degree_consts();
    let result = (arg1.atan2(arg2) / c.atan_1_0) * 45.0;
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dcosd(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
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
    let result = sign * cosd_q1(c, arg1);
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dcotd(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
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
    let cot_arg1 = cosd_q1(c, arg1) / sind_q1(c, arg1);
    let mut result = sign * (cot_arg1 / c.cot_45);
    if result == 0.0 {
        result = 0.0; // force plain zero, never -0 (float.c)
    }
    // No overflow check: cotd(0) == Inf.
    Ok(result)
}

pub fn dsind(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
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
    let result = sign * sind_q1(c, arg1);
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dtand(arg1: f64) -> PgResult<f64> {
    if arg1.is_nan() {
        return Ok(get_float8_nan());
    }
    if arg1.is_infinite() {
        return Err(input_out_of_range().into());
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
    let tan_arg1 = sind_q1(c, arg1) / cosd_q1(c, arg1);
    let mut result = sign * (tan_arg1 / c.tan_45);
    if result == 0.0 {
        result = 0.0; // force plain zero, never -0 (float.c)
    }
    // No overflow check: tand(90) == Inf.
    Ok(result)
}

pub fn degrees(arg1: f64) -> PgResult<f64> {
    float8_div(arg1, RADIANS_PER_DEGREE)
}

#[inline]
pub fn dpi() -> f64 {
    M_PI
}

pub fn radians(arg1: f64) -> PgResult<f64> {
    float8_mul(arg1, RADIANS_PER_DEGREE)
}

// sinh overflow yields +-Inf, the same value C's ERANGE handling produces.
pub fn dsinh(arg1: f64) -> f64 {
    arg1.sinh()
}

pub fn dcosh(arg1: f64) -> PgResult<f64> {
    let result = arg1.cosh();
    if result == 0.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result)
}

pub fn dtanh(arg1: f64) -> PgResult<f64> {
    let result = arg1.tanh();
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

#[inline]
pub fn dasinh(arg1: f64) -> f64 {
    arg1.asinh()
}

pub fn dacosh(arg1: f64) -> PgResult<f64> {
    if arg1 < 1.0 {
        return Err(input_out_of_range().into());
    }
    Ok(arg1.acosh())
}

pub fn datanh(arg1: f64) -> PgResult<f64> {
    if arg1 < -1.0 || arg1 > 1.0 {
        return Err(input_out_of_range().into());
    }
    Ok(if arg1 == -1.0 {
        f64::NEG_INFINITY
    } else if arg1 == 1.0 {
        f64::INFINITY
    } else {
        // C (float.c datanh): atanh(arg1) — platform libm, not Rust std's
        // ln_1p formula (one-ulp drift, fnconf OID 2467).
        // SAFETY: pure libm function, no preconditions.
        unsafe { libm::atanh(arg1) }
    })
}

pub fn derf(arg1: f64) -> PgResult<f64> {
    let result = erf(arg1);
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn derfc(arg1: f64) -> PgResult<f64> {
    let result = erfc(arg1);
    if result.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn dgamma(arg1: f64) -> PgResult<f64> {
    let result;
    if arg1.is_nan() {
        result = arg1;
    } else if arg1.is_infinite() {
        if arg1 < 0.0 {
            // Per POSIX, tgamma(-Inf) is a domain error.
            return Err(float_overflow_error().into());
        }
        result = arg1;
    } else {
        let r = tgamma(arg1);
        // No errno here; tgamma has no zeros, so 0/Inf/NaN signal range error.
        if r.is_infinite() || r.is_nan() {
            return Err(float_overflow_error().into());
        }
        if r == 0.0 {
            return Err(float_underflow_error().into());
        }
        result = r;
    }
    Ok(result)
}

pub fn dlgamma(arg1: f64) -> PgResult<f64> {
    let result = lgamma(arg1);
    // Infinite result from finite input = overflow or a pole.
    if result.is_infinite() && !arg1.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result)
}

pub fn float48pl(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_pl(arg1 as f64, arg2)
}

pub fn float48mi(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_mi(arg1 as f64, arg2)
}

pub fn float48mul(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_mul(arg1 as f64, arg2)
}

pub fn float48div(arg1: f32, arg2: f64) -> PgResult<f64> {
    crate::float8_div(arg1 as f64, arg2)
}

pub fn float84pl(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_pl(arg1, arg2 as f64)
}

pub fn float84mi(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_mi(arg1, arg2 as f64)
}

pub fn float84mul(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_mul(arg1, arg2 as f64)
}

pub fn float84div(arg1: f64, arg2: f32) -> PgResult<f64> {
    crate::float8_div(arg1, arg2 as f64)
}

pub fn float48eq(arg1: f32, arg2: f64) -> bool {
    crate::float8_eq(arg1 as f64, arg2)
}

pub fn float48ne(arg1: f32, arg2: f64) -> bool {
    crate::float8_ne(arg1 as f64, arg2)
}

pub fn float48lt(arg1: f32, arg2: f64) -> bool {
    crate::float8_lt(arg1 as f64, arg2)
}

pub fn float48le(arg1: f32, arg2: f64) -> bool {
    crate::float8_le(arg1 as f64, arg2)
}

pub fn float48gt(arg1: f32, arg2: f64) -> bool {
    crate::float8_gt(arg1 as f64, arg2)
}

pub fn float48ge(arg1: f32, arg2: f64) -> bool {
    crate::float8_ge(arg1 as f64, arg2)
}

pub fn float84eq(arg1: f64, arg2: f32) -> bool {
    crate::float8_eq(arg1, arg2 as f64)
}

pub fn float84ne(arg1: f64, arg2: f32) -> bool {
    crate::float8_ne(arg1, arg2 as f64)
}

pub fn float84lt(arg1: f64, arg2: f32) -> bool {
    crate::float8_lt(arg1, arg2 as f64)
}

pub fn float84le(arg1: f64, arg2: f32) -> bool {
    crate::float8_le(arg1, arg2 as f64)
}

pub fn float84gt(arg1: f64, arg2: f32) -> bool {
    crate::float8_gt(arg1, arg2 as f64)
}

pub fn float84ge(arg1: f64, arg2: f32) -> bool {
    crate::float8_ge(arg1, arg2 as f64)
}

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
                .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
                .into(),
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
                .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE)
                .into(),
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

pub fn width_bucket_float8(operand: f64, bound1: f64, bound2: f64, count: i32) -> PgResult<i32> {
    if count <= 0 {
        return Err(PgError::error("count must be greater than zero")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION)
            .into());
    }

    if operand.is_nan() || bound1.is_nan() || bound2.is_nan() {
        return Err(
            PgError::error("operand, lower bound, and upper bound cannot be NaN")
                .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION)
                .into(),
        );
    }

    if bound1.is_infinite() || bound2.is_infinite() {
        return Err(PgError::error("lower and upper bounds must be finite")
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION)
            .into());
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
            .with_sqlstate(ERRCODE_INVALID_ARGUMENT_FOR_WIDTH_BUCKET_FUNCTION)
            .into());
    }

    Ok(result)
}
