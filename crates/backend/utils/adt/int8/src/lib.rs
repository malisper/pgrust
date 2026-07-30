// std (not no_std): the out-wrapper scratch is a const-init thread_local.
extern crate alloc;

pub mod builtins;

#[cfg(test)]
mod tests;

use alloc::boxed::Box;

use ::numutils::{pg_lltoa, pg_strtoint64_safe};
use ::types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_DIVISION_BY_ZERO,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub use ::numutils::MAXINT8LEN as MAX_INT8_LEN;

const PG_INT64_MIN: i64 = i64::MIN;
const PG_INT32_MIN: i64 = i32::MIN as i64;
const PG_INT32_MAX: i64 = i32::MAX as i64;
const PG_INT16_MIN: i64 = i16::MIN as i64;
const PG_INT16_MAX: i64 = i16::MAX as i64;
const PG_UINT32_MAX: i64 = u32::MAX as i64;

macro_rules! overflow_fns {
    ($add:ident, $sub:ident, $mul:ident, $t:ty) => {
        #[inline(always)]
        pub(crate) fn $add(a: $t, b: $t, result: &mut $t) -> bool {
            let (v, o) = a.overflowing_add(b);
            *result = v;
            o
        }
        #[inline(always)]
        pub(crate) fn $sub(a: $t, b: $t, result: &mut $t) -> bool {
            let (v, o) = a.overflowing_sub(b);
            *result = v;
            o
        }
        #[inline(always)]
        pub(crate) fn $mul(a: $t, b: $t, result: &mut $t) -> bool {
            let (v, o) = a.overflowing_mul(b);
            *result = v;
            o
        }
    };
}

overflow_fns!(pg_add_s64_overflow, pg_sub_s64_overflow, pg_mul_s64_overflow, i64);

#[track_caller]
#[cold]
#[inline(never)]
fn bigint_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("bigint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn integer_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn smallint_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("smallint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn oid_out_of_range() -> Box<PgError> {
    Box::new(PgError::error("OID out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE))
}

#[track_caller]
#[cold]
#[inline(never)]
fn division_by_zero() -> Box<PgError> {
    Box::new(PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO))
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_preceding_following() -> Box<PgError> {
    Box::new(
        PgError::error("invalid preceding or following size in window function")
            .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE),
    )
}

// FLOAT8_FITS_IN_INT64 / FLOAT4_FITS_IN_INT64 (utils/float.h): num >= MIN and
// num < -MIN (both exact powers of two in the float domain).
#[inline]
fn float8_fits_in_int64(num: f64) -> bool {
    let min = PG_INT64_MIN as f64;
    num >= min && num < -min
}

#[inline]
fn float4_fits_in_int64(num: f32) -> bool {
    let min = PG_INT64_MIN as f32;
    num >= min && num < -min
}

pub fn int8in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i64> {
    pg_strtoint64_safe(num, escontext)
}

// Encodes into the caller's buffer (AGENTS rule 8); the fmgr-shaped cstring
// wrapper lives in `builtins`.
#[inline]
pub fn int8out(val: i64, a: &mut [u8]) -> usize {
    pg_lltoa(val, a)
}

pub fn int8recv(buf: &mut ::stringinfo::StringInfo<'_>) -> PgResult<i64> {
    pqformat::pq_getmsgint64(buf)
}

pub fn int8send<'mcx>(mcx: ::mcx::Mcx<'mcx>, arg1: i64) -> PgResult<::datum::Bytea<'mcx>> {
    let mut b = pqformat::pq_begintypsend(mcx)?;
    pqformat::pq_sendint64(&mut b, arg1 as u64)?;
    Ok(pqformat::pq_endtypsend(b))
}

macro_rules! relops {
    ($($name:ident($ta:ty, $tb:ty): $op:tt;)*) => {$(
        #[inline]
        pub fn $name(val1: $ta, val2: $tb) -> bool {
            (val1 as i64) $op (val2 as i64)
        }
    )*};
}

relops! {
    int8eq(i64, i64): ==; int8ne(i64, i64): !=;
    int8lt(i64, i64): <;  int8gt(i64, i64): >;
    int8le(i64, i64): <=; int8ge(i64, i64): >=;
    int84eq(i64, i32): ==; int84ne(i64, i32): !=;
    int84lt(i64, i32): <;  int84gt(i64, i32): >;
    int84le(i64, i32): <=; int84ge(i64, i32): >=;
    int48eq(i32, i64): ==; int48ne(i32, i64): !=;
    int48lt(i32, i64): <;  int48gt(i32, i64): >;
    int48le(i32, i64): <=; int48ge(i32, i64): >=;
    int82eq(i64, i16): ==; int82ne(i64, i16): !=;
    int82lt(i64, i16): <;  int82gt(i64, i16): >;
    int82le(i64, i16): <=; int82ge(i64, i16): >=;
    int28eq(i16, i64): ==; int28ne(i16, i64): !=;
    int28lt(i16, i64): <;  int28gt(i16, i64): >;
    int28le(i16, i64): <=; int28ge(i16, i64): >=;
}

pub fn in_range_int8_int8(
    val: i64,
    base: i64,
    mut offset: i64,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    if offset < 0 {
        return Err(invalid_preceding_following());
    }
    if sub {
        offset = -offset; // cannot overflow
    }
    let mut sum = 0i64;
    if pg_add_s64_overflow(base, offset, &mut sum) {
        // Overflowed sum is certainly past val in step's direction.
        return Ok(if sub { !less } else { less });
    }
    Ok(if less { val <= sum } else { val >= sum })
}

#[inline]
pub fn int8um(arg: i64) -> PgResult<i64> {
    if arg == PG_INT64_MIN {
        return Err(bigint_out_of_range());
    }
    Ok(-arg)
}

#[inline]
pub fn int8up(arg: i64) -> i64 {
    arg
}

#[inline]
pub fn int8pl(arg1: i64, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int8mi(arg1: i64, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int8mul(arg1: i64, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int8div(arg1: i64, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // INT64_MIN / -1 traps; division by -1 is negation.
    if arg2 == -1 {
        if arg1 == PG_INT64_MIN {
            return Err(bigint_out_of_range());
        }
        return Ok(-arg1);
    }
    Ok(arg1 / arg2)
}

#[inline]
pub fn int8abs(arg1: i64) -> PgResult<i64> {
    if arg1 == PG_INT64_MIN {
        return Err(bigint_out_of_range());
    }
    Ok(if arg1 < 0 { -arg1 } else { arg1 })
}

#[inline]
pub fn int8mod(arg1: i64, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // INT64_MIN % -1 traps on some machines; the answer is zero.
    if arg2 == -1 {
        return Ok(0);
    }
    Ok(arg1 % arg2)
}

fn int8gcd_internal(mut arg1: i64, mut arg2: i64) -> PgResult<i64> {
    // Compare absolute values in negative space (INT64_MIN-safe).
    let a1 = if arg1 < 0 { arg1 } else { -arg1 };
    let a2 = if arg2 < 0 { arg2 } else { -arg2 };
    if a1 > a2 {
        core::mem::swap(&mut arg1, &mut arg2);
    }

    if arg1 == PG_INT64_MIN {
        if arg2 == 0 || arg2 == PG_INT64_MIN {
            return Err(bigint_out_of_range());
        }
        // gcd(INT64_MIN, -1): dodge the INT64_MIN % -1 trap.
        if arg2 == -1 {
            return Ok(1);
        }
    }

    while arg2 != 0 {
        let swap = arg2;
        arg2 = arg1 % arg2;
        arg1 = swap;
    }

    if arg1 < 0 {
        arg1 = -arg1;
    }
    Ok(arg1)
}

pub fn int8gcd(arg1: i64, arg2: i64) -> PgResult<i64> {
    int8gcd_internal(arg1, arg2)
}

pub fn int8lcm(mut arg1: i64, arg2: i64) -> PgResult<i64> {
    if arg1 == 0 || arg2 == 0 {
        return Ok(0);
    }
    let gcd = int8gcd_internal(arg1, arg2)?;
    arg1 /= gcd;
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    if result == PG_INT64_MIN {
        return Err(bigint_out_of_range());
    }
    if result < 0 {
        result = -result;
    }
    Ok(result)
}

// int8 is pass-by-value in this build (USE_FLOAT8_BYVAL), so C's aggregate
// modify-in-place branch is compiled out; this is the only branch.
#[inline]
pub fn int8inc(arg: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg, 1, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int8dec(arg: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg, 1, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

// C bodies of the *_any / float8 variants are `return int8inc(fcinfo)`.
#[inline]
pub fn int8inc_any(arg: i64) -> PgResult<i64> {
    int8inc(arg)
}

#[inline]
pub fn int8inc_float8_float8(arg: i64) -> PgResult<i64> {
    int8inc(arg)
}

#[inline]
pub fn int8dec_any(arg: i64) -> PgResult<i64> {
    int8dec(arg)
}

#[inline]
pub fn int8larger(arg1: i64, arg2: i64) -> i64 {
    if arg1 > arg2 { arg1 } else { arg2 }
}

#[inline]
pub fn int8smaller(arg1: i64, arg2: i64) -> i64 {
    if arg1 < arg2 { arg1 } else { arg2 }
}

#[inline]
pub fn int84pl(arg1: i64, arg2: i32) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int84mi(arg1: i64, arg2: i32) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int84mul(arg1: i64, arg2: i32) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int84div(arg1: i64, arg2: i32) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    if arg2 == -1 {
        if arg1 == PG_INT64_MIN {
            return Err(bigint_out_of_range());
        }
        return Ok(-arg1);
    }
    Ok(arg1 / arg2 as i64)
}

#[inline]
pub fn int48pl(arg1: i32, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int48mi(arg1: i32, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int48mul(arg1: i32, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int48div(arg1: i32, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // No overflow is possible.
    Ok(arg1 as i64 / arg2)
}

#[inline]
pub fn int82pl(arg1: i64, arg2: i16) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int82mi(arg1: i64, arg2: i16) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int82mul(arg1: i64, arg2: i16) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int82div(arg1: i64, arg2: i16) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    if arg2 == -1 {
        if arg1 == PG_INT64_MIN {
            return Err(bigint_out_of_range());
        }
        return Ok(-arg1);
    }
    Ok(arg1 / arg2 as i64)
}

#[inline]
pub fn int28pl(arg1: i16, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int28mi(arg1: i16, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int28mul(arg1: i16, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

#[inline]
pub fn int28div(arg1: i16, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // No overflow is possible.
    Ok(arg1 as i64 / arg2)
}

#[inline]
pub fn int8and(arg1: i64, arg2: i64) -> i64 {
    arg1 & arg2
}

#[inline]
pub fn int8or(arg1: i64, arg2: i64) -> i64 {
    arg1 | arg2
}

#[inline]
pub fn int8xor(arg1: i64, arg2: i64) -> i64 {
    arg1 ^ arg2
}

#[inline]
pub fn int8not(arg1: i64) -> i64 {
    !arg1
}

// C `arg1 << arg2` is UB past 63; hardware (and this port) masks the count.
#[inline]
pub fn int8shl(arg1: i64, arg2: i32) -> i64 {
    arg1.wrapping_shl(arg2 as u32)
}

#[inline]
pub fn int8shr(arg1: i64, arg2: i32) -> i64 {
    arg1.wrapping_shr(arg2 as u32)
}

#[inline]
pub fn int48(arg: i32) -> i64 {
    arg as i64
}

#[inline]
pub fn int84(arg: i64) -> PgResult<i32> {
    if arg < PG_INT32_MIN || arg > PG_INT32_MAX {
        return Err(integer_out_of_range());
    }
    Ok(arg as i32)
}

#[inline]
pub fn int28(arg: i16) -> i64 {
    arg as i64
}

#[inline]
pub fn int82(arg: i64) -> PgResult<i16> {
    if arg < PG_INT16_MIN || arg > PG_INT16_MAX {
        return Err(smallint_out_of_range());
    }
    Ok(arg as i16)
}

#[inline]
pub fn i8tod(arg: i64) -> f64 {
    arg as f64
}

// C rint(): round half to even, NaN/Inf pass through.
#[inline]
pub fn dtoi8(num: f64) -> PgResult<i64> {
    let num = num.round_ties_even();
    if num.is_nan() || !float8_fits_in_int64(num) {
        return Err(bigint_out_of_range());
    }
    Ok(num as i64)
}

#[inline]
pub fn i8tof(arg: i64) -> f32 {
    arg as f32
}

#[inline]
pub fn ftoi8(num: f32) -> PgResult<i64> {
    let num = num.round_ties_even();
    if num.is_nan() || !float4_fits_in_int64(num) {
        return Err(bigint_out_of_range());
    }
    Ok(num as i64)
}

#[inline]
pub fn i8tooid(arg: i64) -> PgResult<::types_core::Oid> {
    if arg < 0 || arg > PG_UINT32_MAX {
        return Err(oid_out_of_range());
    }
    Ok(arg as ::types_core::Oid)
}

#[inline]
pub fn oidtoi8(arg: ::types_core::Oid) -> i64 {
    arg as i64
}

// generate_series_step_int8's cross-call state; the funcapi SRF frame that
// owns it is backend-utils-fmgr-funcapi's unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerateSeriesInt8 {
    pub current: i64,
    pub finish: i64,
    pub step: i64,
}

impl GenerateSeriesInt8 {
    pub fn new(start: i64, finish: i64, step: i64) -> PgResult<Self> {
        if step == 0 {
            return Err(Box::new(
                PgError::error("step size cannot equal zero")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
            ));
        }
        Ok(GenerateSeriesInt8 {
            current: start,
            finish,
            step,
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<i64> {
        let result = self.current;
        if (self.step > 0 && self.current <= self.finish)
            || (self.step < 0 && self.current >= self.finish)
        {
            let mut nextval = 0;
            if pg_add_s64_overflow(self.current, self.step, &mut nextval) {
                self.step = 0;
            } else {
                self.current = nextval;
            }
            Some(result)
        } else {
            None
        }
    }
}

// generate_series_int8_support's SupportRequestRows estimate.
pub fn generate_series_int8_rows(start: f64, finish: f64, step: f64) -> Option<f64> {
    if step != 0.0 {
        Some(((finish - start + step) / step).floor())
    } else {
        None
    }
}
