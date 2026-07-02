//! float.c — float4/float8 I/O, arithmetic, comparisons, math, aggregates.

extern crate alloc;

pub mod aggregates;
pub mod builtins;
pub mod funcs;
pub mod io;

#[cfg(test)]
mod tests;

use ::types_error::{
    PgError, PgResult, ERRCODE_DIVISION_BY_ZERO, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub use funcs::*;
pub use io::{
    float4in, float4in_internal, float4out, float4out_with, float4recv, float4send, float8in,
    float8in_internal, float8out, float8out_internal, float8out_internal_with, float8recv,
    float8send, MAXDOUBLEWIDTH,
};

pub const M_PI: f64 = core::f64::consts::PI;
// Exact decimal literal from <utils/float.h>, NOT recomputed (bit parity).
pub const RADIANS_PER_DEGREE: f64 = 0.0174532925199432957692;
pub const FLT_DIG: i32 = 6;
pub const DBL_DIG: i32 = 15;

pub fn init_seams() {
    guc_tables::vars::extra_float_digits.install(guc_tables::GucVarAccessors {
        get: get_extra_float_digits,
        set: set_extra_float_digits,
    });
}

std::thread_local! {
    static EXTRA_FLOAT_DIGITS: core::cell::Cell<i32> = const { core::cell::Cell::new(1) };
}

pub fn get_extra_float_digits() -> i32 {
    EXTRA_FLOAT_DIGITS.with(|c| c.get())
}

pub fn set_extra_float_digits(v: i32) {
    EXTRA_FLOAT_DIGITS.with(|c| c.set(v));
}

#[inline]
pub fn get_float4_infinity() -> f32 {
    f32::INFINITY
}

#[inline]
pub fn get_float8_infinity() -> f64 {
    f64::INFINITY
}

#[inline]
pub fn get_float4_nan() -> f32 {
    f32::NAN
}

#[inline]
pub fn get_float8_nan() -> f64 {
    f64::NAN
}

#[cold]
#[inline(never)]
pub fn float_overflow_error() -> PgError {
    PgError::error("value out of range: overflow").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
pub fn float_underflow_error() -> PgError {
    PgError::error("value out of range: underflow")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

#[cold]
#[inline(never)]
pub fn float_zero_divide_error() -> PgError {
    PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}

#[inline]
pub fn is_infinite(val: f64) -> i32 {
    if !val.is_infinite() {
        0
    } else if val > 0.0 {
        1
    } else {
        -1
    }
}

// FLOAT{4,8}_FITS_IN_INT{16,32,64} (c.h): [MIN, -MIN) half-open in float space.
#[inline]
pub fn float4_fits_in_int16(num: f32) -> bool {
    num >= (i16::MIN as f32) && num < -(i16::MIN as f32)
}

#[inline]
pub fn float4_fits_in_int32(num: f32) -> bool {
    num >= (i32::MIN as f32) && num < -(i32::MIN as f32)
}

#[inline]
pub fn float4_fits_in_int64(num: f32) -> bool {
    num >= (i64::MIN as f32) && num < -(i64::MIN as f32)
}

#[inline]
pub fn float8_fits_in_int16(num: f64) -> bool {
    num >= (i16::MIN as f64) && num < -(i16::MIN as f64)
}

#[inline]
pub fn float8_fits_in_int32(num: f64) -> bool {
    num >= (i32::MIN as f64) && num < -(i32::MIN as f64)
}

#[inline]
pub fn float8_fits_in_int64(num: f64) -> bool {
    num >= (i64::MIN as f64) && num < -(i64::MIN as f64)
}

// Cold tails take `result` and return Result<bits, Box<PgError>> (ScalarPair
// -> two registers, no sret, nothing live across the call), so the happy
// paths compile to C's unlikely() shape: one isinf test, leaf, shrink-wrapped.
type ColdBits32 = Result<u32, alloc::boxed::Box<PgError>>;
type ColdBits64 = Result<u64, alloc::boxed::Box<PgError>>;

#[cold]
#[inline(never)]
fn f4_inf_cold(val1: f32, val2: f32, result: f32) -> ColdBits32 {
    if !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn f8_inf_cold(val1: f64, val2: f64, result: f64) -> ColdBits64 {
    if !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn f4_mul_zero_cold(val1: f32, val2: f32, result: f32) -> ColdBits32 {
    if val1 != 0.0 && val2 != 0.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn f8_mul_zero_cold(val1: f64, val2: f64, result: f64) -> ColdBits64 {
    if val1 != 0.0 && val2 != 0.0 {
        return Err(float_underflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn f4_div_inf_cold(val1: f32, result: f32) -> ColdBits32 {
    if !val1.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn f8_div_inf_cold(val1: f64, result: f64) -> ColdBits64 {
    if !val1.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn f4_div_zero_cold(val1: f32, val2: f32, result: f32) -> ColdBits32 {
    if val1 != 0.0 && !val2.is_infinite() {
        return Err(float_underflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn f8_div_zero_cold(val1: f64, val2: f64, result: f64) -> ColdBits64 {
    if val1 != 0.0 && !val2.is_infinite() {
        return Err(float_underflow_error().into());
    }
    Ok(result.to_bits())
}

#[cold]
#[inline(never)]
fn zero_divide_boxed() -> alloc::boxed::Box<PgError> {
    alloc::boxed::Box::new(float_zero_divide_error())
}

#[inline(always)]
fn bits4(r: ColdBits32) -> PgResult<f32> {
    match r {
        Ok(b) => Ok(f32::from_bits(b)),
        Err(e) => Err(e),
    }
}

#[inline(always)]
fn bits8(r: ColdBits64) -> PgResult<f64> {
    match r {
        Ok(b) => Ok(f64::from_bits(b)),
        Err(e) => Err(e),
    }
}

#[inline]
pub fn float4_pl(val1: f32, val2: f32) -> PgResult<f32> {
    let result = val1 + val2;
    if result.is_infinite() {
        return bits4(f4_inf_cold(val1, val2, result));
    }
    Ok(result)
}

#[inline]
pub fn float8_pl(val1: f64, val2: f64) -> PgResult<f64> {
    let result = val1 + val2;
    if result.is_infinite() {
        return bits8(f8_inf_cold(val1, val2, result));
    }
    Ok(result)
}

#[inline]
pub fn float4_mi(val1: f32, val2: f32) -> PgResult<f32> {
    let result = val1 - val2;
    if result.is_infinite() {
        return bits4(f4_inf_cold(val1, val2, result));
    }
    Ok(result)
}

#[inline]
pub fn float8_mi(val1: f64, val2: f64) -> PgResult<f64> {
    let result = val1 - val2;
    if result.is_infinite() {
        return bits8(f8_inf_cold(val1, val2, result));
    }
    Ok(result)
}

#[inline]
pub fn float4_mul(val1: f32, val2: f32) -> PgResult<f32> {
    let result = val1 * val2;
    if result.is_infinite() {
        return bits4(f4_inf_cold(val1, val2, result));
    }
    if result == 0.0 {
        return bits4(f4_mul_zero_cold(val1, val2, result));
    }
    Ok(result)
}

#[inline]
pub fn float8_mul(val1: f64, val2: f64) -> PgResult<f64> {
    let result = val1 * val2;
    if result.is_infinite() {
        return bits8(f8_inf_cold(val1, val2, result));
    }
    if result == 0.0 {
        return bits8(f8_mul_zero_cold(val1, val2, result));
    }
    Ok(result)
}

#[inline]
pub fn float4_div(val1: f32, val2: f32) -> PgResult<f32> {
    if val2 == 0.0 && !val1.is_nan() {
        return Err(zero_divide_boxed());
    }
    let result = val1 / val2;
    if result.is_infinite() {
        return bits4(f4_div_inf_cold(val1, result));
    }
    if result == 0.0 {
        return bits4(f4_div_zero_cold(val1, val2, result));
    }
    Ok(result)
}

#[inline]
pub fn float8_div(val1: f64, val2: f64) -> PgResult<f64> {
    if val2 == 0.0 && !val1.is_nan() {
        return Err(zero_divide_boxed());
    }
    let result = val1 / val2;
    if result.is_infinite() {
        return bits8(f8_div_inf_cold(val1, result));
    }
    if result == 0.0 {
        return bits8(f8_div_zero_cold(val1, val2, result));
    }
    Ok(result)
}

// NaN-aware comparisons (float.h): all NaNs equal, NaN > every non-NaN.
#[inline]
pub fn float4_eq(val1: f32, val2: f32) -> bool {
    // == is already NaN-false; the disjunct form drops a select vs C.
    val1 == val2 || (val1.is_nan() && val2.is_nan())
}

#[inline]
pub fn float8_eq(val1: f64, val2: f64) -> bool {
    // == is already NaN-false; the disjunct form drops a select vs C.
    val1 == val2 || (val1.is_nan() && val2.is_nan())
}

#[inline]
pub fn float4_ne(val1: f32, val2: f32) -> bool {
    if val1.is_nan() {
        !val2.is_nan()
    } else {
        val2.is_nan() || val1 != val2
    }
}

#[inline]
pub fn float8_ne(val1: f64, val2: f64) -> bool {
    if val1.is_nan() {
        !val2.is_nan()
    } else {
        val2.is_nan() || val1 != val2
    }
}

#[inline]
pub fn float4_lt(val1: f32, val2: f32) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}

#[inline]
pub fn float8_lt(val1: f64, val2: f64) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}

#[inline]
pub fn float4_le(val1: f32, val2: f32) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}

#[inline]
pub fn float8_le(val1: f64, val2: f64) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}

#[inline]
pub fn float4_gt(val1: f32, val2: f32) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}

#[inline]
pub fn float8_gt(val1: f64, val2: f64) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}

#[inline]
pub fn float4_ge(val1: f32, val2: f32) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}

#[inline]
pub fn float8_ge(val1: f64, val2: f64) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}

#[inline]
pub fn float4_min(val1: f32, val2: f32) -> f32 {
    if float4_lt(val1, val2) {
        val1
    } else {
        val2
    }
}

#[inline]
pub fn float8_min(val1: f64, val2: f64) -> f64 {
    if float8_lt(val1, val2) {
        val1
    } else {
        val2
    }
}

#[inline]
pub fn float4_max(val1: f32, val2: f32) -> f32 {
    if float4_gt(val1, val2) {
        val1
    } else {
        val2
    }
}

#[inline]
pub fn float8_max(val1: f64, val2: f64) -> f64 {
    if float8_gt(val1, val2) {
        val1
    } else {
        val2
    }
}

#[inline]
pub fn float4abs(arg1: f32) -> f32 {
    arg1.abs()
}

#[inline]
pub fn float8abs(arg1: f64) -> f64 {
    arg1.abs()
}

#[inline]
pub fn float4um(arg1: f32) -> f32 {
    -arg1
}

#[inline]
pub fn float8um(arg1: f64) -> f64 {
    -arg1
}

#[inline]
pub fn float4up(arg: f32) -> f32 {
    arg
}

#[inline]
pub fn float8up(arg: f64) -> f64 {
    arg
}

#[inline]
pub fn float4larger(arg1: f32, arg2: f32) -> f32 {
    if float4_gt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

#[inline]
pub fn float4smaller(arg1: f32, arg2: f32) -> f32 {
    if float4_lt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

#[inline]
pub fn float8larger(arg1: f64, arg2: f64) -> f64 {
    if float8_gt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

#[inline]
pub fn float8smaller(arg1: f64, arg2: f64) -> f64 {
    if float8_lt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

#[inline]
pub fn float4_cmp_internal(a: f32, b: f32) -> i32 {
    if float4_gt(a, b) {
        1
    } else if float4_lt(a, b) {
        -1
    } else {
        0
    }
}

#[inline]
pub fn float8_cmp_internal(a: f64, b: f64) -> i32 {
    if float8_gt(a, b) {
        1
    } else if float8_lt(a, b) {
        -1
    } else {
        0
    }
}

#[inline]
pub fn btfloat4cmp(arg1: f32, arg2: f32) -> i32 {
    float4_cmp_internal(arg1, arg2)
}

#[inline]
pub fn btfloat8cmp(arg1: f64, arg2: f64) -> i32 {
    float8_cmp_internal(arg1, arg2)
}

#[inline]
pub fn btfloat48cmp(arg1: f32, arg2: f64) -> i32 {
    float8_cmp_internal(arg1 as f64, arg2)
}

#[inline]
pub fn btfloat84cmp(arg1: f64, arg2: f32) -> i32 {
    float8_cmp_internal(arg1, arg2 as f64)
}

// SortSupport comparators installed by btfloat{4,8}sortsupport.
#[inline]
pub fn btfloat4fastcmp(arg1: f32, arg2: f32) -> i32 {
    float4_cmp_internal(arg1, arg2)
}

#[inline]
pub fn btfloat8fastcmp(arg1: f64, arg2: f64) -> i32 {
    float8_cmp_internal(arg1, arg2)
}
