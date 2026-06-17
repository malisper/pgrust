#![allow(non_snake_case)]
//! Port of PostgreSQL `src/backend/utils/adt/int8.c`: internal 64-bit integer
//! (`int8` / `bigint`) operations.
//!
//! Every routine in `int8.c` is ported here with logic identical to PostgreSQL
//! 18.3 (branch order, message text, and SQLSTATE all cross-checked against the
//! C source).
//!
//! # Conventions
//!
//! These are the SQL-callable bodies stripped of their `PG_FUNCTION_ARGS`
//! fmgr wrapper: each takes its `int64`/`int32`/`int16`/`Oid`/`float8`/`float4`
//! arguments by value and returns the result (or [`PgResult`] when the C body
//! can `ereport(ERROR)`), exactly as the sibling `cash.c` port does.  The fmgr
//! layer marshals the `Datum` arguments and wraps the return into a `Datum`.
//!
//! # Genuine externals (reached by the fmgr/executor layer, not re-implemented)
//!
//! A handful of `int8.c` entry points are nothing but a thin adapter over an
//! fmgr call frame or a planner/executor construct that this leaf data-type
//! crate cannot — and in C does not — own:
//!
//! * `int8recv` / `int8send` are the binary-wire format: their C bodies are
//!   only `pq_getmsgint64` / `pq_sendint64` (`libpq/pqformat.h`), with no int8
//!   specific logic.  As in the `cash.c` port, they are documented here but the
//!   wire marshaling is the `pqformat` layer's job.
//! * the aggregate fast path of `int8inc` / `int8dec` (the
//!   `AggCheckCallContext` in-place branch, `#ifndef USE_FLOAT8_BYVAL`) needs
//!   the live fmgr call frame.  The portable non-aggregate branch — the one
//!   that runs whenever int8 is pass-by-value, which is this build's model — is
//!   [`int8inc`] / [`int8dec`] below.  `int8inc_any`, `int8inc_float8_float8`
//!   and `int8dec_any` are exact aliases (their C bodies just `return
//!   int8inc(fcinfo);`).
//! * `int8inc_support` and `generate_series_int8_support` are planner
//!   *prosupport* functions whose bodies inspect `SupportRequest*` /
//!   `FuncExpr` / `Const` planner `Node`s; `generate_series_int8` /
//!   `generate_series_step_int8` drive the set-returning-function call frame
//!   (`FuncCallContext` / `SRF_*`).  Those are the executor/planner's frames
//!   (mirrored for `numeric` in `backend-utils-adt-numeric::series_srf`); the
//!   pure-int8 arithmetic they perform is the overflow-checked addition already
//!   ported here.

use backend_utils_adt_numutils::{pg_lltoa, pg_strtoint64_safe, MAX_INT64_DIGITS};
use types_error::{
    PgError, PgResult, SoftErrorContext, ERRCODE_DIVISION_BY_ZERO,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub mod fmgr_builtins;

#[cfg(test)]
mod tests;

/// Install this crate's seams. Registers every scalar `int8.c` builtin into the
/// fmgr-core builtin table (C: `fmgr_builtins[]`) so by-OID dispatch resolves
/// them. Called by `seams-init::init_all`.
pub fn init_seams() {
    fmgr_builtins::register_int8_builtins();
}

/// `PG_INT64_MIN` (`c.h`).
const PG_INT64_MIN: i64 = i64::MIN;
/// `PG_INT32_MIN` / `PG_INT32_MAX` (`c.h`).
const PG_INT32_MIN: i64 = i32::MIN as i64;
const PG_INT32_MAX: i64 = i32::MAX as i64;
/// `PG_INT16_MIN` / `PG_INT16_MAX` (`c.h`).
const PG_INT16_MIN: i64 = i16::MIN as i64;
const PG_INT16_MAX: i64 = i16::MAX as i64;
/// `PG_UINT32_MAX` (`c.h`).
const PG_UINT32_MAX: i64 = u32::MAX as i64;

// ===========================================================================
// Private overflow-checked int64 helpers (common/int.h).
//
// These mirror `pg_{add,sub,mul}_s64_overflow`: each returns `true` on overflow
// and writes the (wrapping) result through `res`, exactly as the C macros do.
// ===========================================================================

#[inline]
fn pg_add_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_add(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = a.wrapping_add(b);
            true
        }
    }
}

#[inline]
fn pg_sub_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_sub(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = a.wrapping_sub(b);
            true
        }
    }
}

#[inline]
fn pg_mul_s64_overflow(a: i64, b: i64, res: &mut i64) -> bool {
    match a.checked_mul(b) {
        Some(v) => {
            *res = v;
            false
        }
        None => {
            *res = a.wrapping_mul(b);
            true
        }
    }
}

/// The shared hard error of the int8 arithmetic cores: `ereport(ERROR,
/// (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), errmsg("bigint out of
/// range")))`.
#[inline]
fn bigint_out_of_range() -> PgError {
    PgError::error("bigint out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

// ===========================================================================
// Float-fits-in-int64 guards (utils/float.h), used by dtoi8 / ftoi8.
// ===========================================================================

/// `FLOAT8_FITS_IN_INT64(num)` (utils/float.h):
/// `((num) >= (double) PG_INT64_MIN && (num) < -((double) PG_INT64_MIN))`.
#[inline]
fn float8_fits_in_int64(num: f64) -> bool {
    let min = PG_INT64_MIN as f64;
    num >= min && num < -min
}

/// `FLOAT4_FITS_IN_INT64(num)` (utils/float.h):
/// `((num) >= (float) PG_INT64_MIN && (num) < -((float) PG_INT64_MIN))`.
#[inline]
fn float4_fits_in_int64(num: f32) -> bool {
    let min = PG_INT64_MIN as f32;
    num >= min && num < -min
}

// ===========================================================================
// Formatting and conversion routines.
// ===========================================================================

/// `int8in()`: parse text into an `int8`.
pub fn int8in(num: &str, escontext: Option<&mut SoftErrorContext>) -> PgResult<i64> {
    pg_strtoint64_safe(num, escontext)
}

/// `int8out()`: render an `int8` as decimal text.
pub fn int8out(val: i64) -> String {
    let mut buf = [0u8; MAX_INT64_DIGITS];
    let len = pg_lltoa(val, &mut buf);
    // SAFETY/CORRECTNESS: pg_lltoa emits only ASCII digits and an optional '-'.
    String::from_utf8(buf[..len].to_vec()).expect("pg_lltoa emits ASCII")
}

// ===========================================================================
// Relational operators (including cross-data-type comparisons).
// ===========================================================================

// int8relop(): val1 relop val2 (both int64)
pub fn int8eq(val1: i64, val2: i64) -> bool {
    val1 == val2
}
pub fn int8ne(val1: i64, val2: i64) -> bool {
    val1 != val2
}
pub fn int8lt(val1: i64, val2: i64) -> bool {
    val1 < val2
}
pub fn int8gt(val1: i64, val2: i64) -> bool {
    val1 > val2
}
pub fn int8le(val1: i64, val2: i64) -> bool {
    val1 <= val2
}
pub fn int8ge(val1: i64, val2: i64) -> bool {
    val1 >= val2
}

// int84relop(): int64 val1 relop int32 val2
pub fn int84eq(val1: i64, val2: i32) -> bool {
    val1 == val2 as i64
}
pub fn int84ne(val1: i64, val2: i32) -> bool {
    val1 != val2 as i64
}
pub fn int84lt(val1: i64, val2: i32) -> bool {
    val1 < val2 as i64
}
pub fn int84gt(val1: i64, val2: i32) -> bool {
    val1 > val2 as i64
}
pub fn int84le(val1: i64, val2: i32) -> bool {
    val1 <= val2 as i64
}
pub fn int84ge(val1: i64, val2: i32) -> bool {
    val1 >= val2 as i64
}

// int48relop(): int32 val1 relop int64 val2
pub fn int48eq(val1: i32, val2: i64) -> bool {
    val1 as i64 == val2
}
pub fn int48ne(val1: i32, val2: i64) -> bool {
    val1 as i64 != val2
}
pub fn int48lt(val1: i32, val2: i64) -> bool {
    (val1 as i64) < val2
}
pub fn int48gt(val1: i32, val2: i64) -> bool {
    val1 as i64 > val2
}
pub fn int48le(val1: i32, val2: i64) -> bool {
    val1 as i64 <= val2
}
pub fn int48ge(val1: i32, val2: i64) -> bool {
    val1 as i64 >= val2
}

// int82relop(): int64 val1 relop int16 val2
pub fn int82eq(val1: i64, val2: i16) -> bool {
    val1 == val2 as i64
}
pub fn int82ne(val1: i64, val2: i16) -> bool {
    val1 != val2 as i64
}
pub fn int82lt(val1: i64, val2: i16) -> bool {
    val1 < val2 as i64
}
pub fn int82gt(val1: i64, val2: i16) -> bool {
    val1 > val2 as i64
}
pub fn int82le(val1: i64, val2: i16) -> bool {
    val1 <= val2 as i64
}
pub fn int82ge(val1: i64, val2: i16) -> bool {
    val1 >= val2 as i64
}

// int28relop(): int16 val1 relop int64 val2
pub fn int28eq(val1: i16, val2: i64) -> bool {
    val1 as i64 == val2
}
pub fn int28ne(val1: i16, val2: i64) -> bool {
    val1 as i64 != val2
}
pub fn int28lt(val1: i16, val2: i64) -> bool {
    (val1 as i64) < val2
}
pub fn int28gt(val1: i16, val2: i64) -> bool {
    val1 as i64 > val2
}
pub fn int28le(val1: i16, val2: i64) -> bool {
    val1 as i64 <= val2
}
pub fn int28ge(val1: i16, val2: i64) -> bool {
    val1 as i64 >= val2
}

/// `in_range_int8_int8()`: in_range support function for int8.
pub fn in_range_int8_int8(
    val: i64,
    base: i64,
    mut offset: i64,
    sub: bool,
    less: bool,
) -> PgResult<bool> {
    if offset < 0 {
        return Err(PgError::error(
            "invalid preceding or following size in window function",
        )
        .with_sqlstate(ERRCODE_INVALID_PRECEDING_OR_FOLLOWING_SIZE));
    }

    if sub {
        offset = -offset; // cannot overflow
    }

    let mut sum = 0i64;
    if pg_add_s64_overflow(base, offset, &mut sum) {
        // If sub is false, the true sum is surely more than val, so correct
        // answer is the same as "less".  If sub is true, the true sum is surely
        // less than val, so the answer is "!less".
        return Ok(if sub { !less } else { less });
    }

    if less {
        Ok(val <= sum)
    } else {
        Ok(val >= sum)
    }
}

// ===========================================================================
// Arithmetic operators on 64-bit integers.
// ===========================================================================

/// `int8um()`: unary minus.
pub fn int8um(arg: i64) -> PgResult<i64> {
    if arg == PG_INT64_MIN {
        return Err(bigint_out_of_range());
    }
    Ok(-arg)
}

/// `int8up()`: unary plus.
pub fn int8up(arg: i64) -> i64 {
    arg
}

/// `int8pl()`: addition.
pub fn int8pl(arg1: i64, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

/// `int8mi()`: subtraction.
pub fn int8mi(arg1: i64, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

/// `int8mul()`: multiplication.
pub fn int8mul(arg1: i64, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

/// The shared division-by-zero error: `ereport(ERROR,
/// (errcode(ERRCODE_DIVISION_BY_ZERO), errmsg("division by zero")))`.
#[inline]
fn division_by_zero() -> PgError {
    PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}

/// `int8div()`: division.
pub fn int8div(arg1: i64, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }

    // INT64_MIN / -1 is problematic; recognize division by -1 as negation.
    if arg2 == -1 {
        if arg1 == PG_INT64_MIN {
            return Err(bigint_out_of_range());
        }
        return Ok(-arg1);
    }

    // No overflow is possible
    Ok(arg1 / arg2)
}

/// `int8abs()`: absolute value.
pub fn int8abs(arg1: i64) -> PgResult<i64> {
    if arg1 == PG_INT64_MIN {
        return Err(bigint_out_of_range());
    }
    Ok(if arg1 < 0 { -arg1 } else { arg1 })
}

/// `int8mod()`: modulo.
pub fn int8mod(arg1: i64, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }

    // Some machines throw FPE for INT64_MIN % -1; the answer is zero.
    if arg2 == -1 {
        return Ok(0);
    }

    // No overflow is possible
    Ok(arg1 % arg2)
}

/// `int8gcd_internal()`: greatest common divisor (file-static helper).
fn int8gcd_internal(mut arg1: i64, mut arg2: i64) -> PgResult<i64> {
    // Put the greater absolute value in arg1, working in negative space to
    // handle INT64_MIN.
    let a1 = if arg1 < 0 { arg1 } else { -arg1 };
    let a2 = if arg2 < 0 { arg2 } else { -arg2 };
    if a1 > a2 {
        std::mem::swap(&mut arg1, &mut arg2);
    }

    // Special care needs to be taken with INT64_MIN.
    if arg1 == PG_INT64_MIN {
        if arg2 == 0 || arg2 == PG_INT64_MIN {
            return Err(bigint_out_of_range());
        }

        // gcd(INT64_MIN, -1) = 1; guard against the FPE for INT64_MIN % -1.
        if arg2 == -1 {
            return Ok(1);
        }
    }

    // Use the Euclidean algorithm to find the GCD.
    while arg2 != 0 {
        let swap = arg2;
        arg2 = arg1 % arg2;
        arg1 = swap;
    }

    // Make sure the result is positive (we know it isn't INT64_MIN anymore).
    if arg1 < 0 {
        arg1 = -arg1;
    }

    Ok(arg1)
}

/// `int8gcd()`: greatest common divisor.
pub fn int8gcd(arg1: i64, arg2: i64) -> PgResult<i64> {
    int8gcd_internal(arg1, arg2)
}

/// `int8lcm()`: least common multiple.
pub fn int8lcm(mut arg1: i64, arg2: i64) -> PgResult<i64> {
    // lcm(x, 0) = lcm(0, x) = 0.
    if arg1 == 0 || arg2 == 0 {
        return Ok(0);
    }

    // lcm(x, y) = abs(x / gcd(x, y) * y)
    let gcd = int8gcd_internal(arg1, arg2)?;
    arg1 /= gcd;

    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }

    // If the result is INT64_MIN, it cannot be represented.
    if result == PG_INT64_MIN {
        return Err(bigint_out_of_range());
    }

    if result < 0 {
        result = -result;
    }

    Ok(result)
}

/// `int8inc()`: increment (the portable, non-aggregate branch — the only one
/// that runs when int8 is pass-by-value, which is this build's model).
pub fn int8inc(arg: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg, 1, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

/// `int8dec()`: decrement (the portable, non-aggregate branch).
pub fn int8dec(arg: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg, 1, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}

/// `int8inc_any()`: exact alias of [`int8inc`] (C body `return int8inc(fcinfo)`).
pub fn int8inc_any(arg: i64) -> PgResult<i64> {
    int8inc(arg)
}

/// `int8inc_float8_float8()`: exact alias of [`int8inc`].
pub fn int8inc_float8_float8(arg: i64) -> PgResult<i64> {
    int8inc(arg)
}

/// `int8dec_any()`: exact alias of [`int8dec`].
pub fn int8dec_any(arg: i64) -> PgResult<i64> {
    int8dec(arg)
}

/// `int8larger()`: the larger of two int8s.
pub fn int8larger(arg1: i64, arg2: i64) -> i64 {
    if arg1 > arg2 {
        arg1
    } else {
        arg2
    }
}

/// `int8smaller()`: the smaller of two int8s.
pub fn int8smaller(arg1: i64, arg2: i64) -> i64 {
    if arg1 < arg2 {
        arg1
    } else {
        arg2
    }
}

// int84 arithmetic: int64 op int32
pub fn int84pl(arg1: i64, arg2: i32) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int84mi(arg1: i64, arg2: i32) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int84mul(arg1: i64, arg2: i32) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
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

// int48 arithmetic: int32 op int64
pub fn int48pl(arg1: i32, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int48mi(arg1: i32, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int48mul(arg1: i32, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int48div(arg1: i32, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // No overflow is possible
    Ok(arg1 as i64 / arg2)
}

// int82 arithmetic: int64 op int16
pub fn int82pl(arg1: i64, arg2: i16) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int82mi(arg1: i64, arg2: i16) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int82mul(arg1: i64, arg2: i16) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1, arg2 as i64, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
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

// int28 arithmetic: int16 op int64
pub fn int28pl(arg1: i16, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_add_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int28mi(arg1: i16, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_sub_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int28mul(arg1: i16, arg2: i64) -> PgResult<i64> {
    let mut result = 0i64;
    if pg_mul_s64_overflow(arg1 as i64, arg2, &mut result) {
        return Err(bigint_out_of_range());
    }
    Ok(result)
}
pub fn int28div(arg1: i16, arg2: i64) -> PgResult<i64> {
    if arg2 == 0 {
        return Err(division_by_zero());
    }
    // No overflow is possible
    Ok(arg1 as i64 / arg2)
}

// ===========================================================================
// Binary (bitwise) arithmetic.
// ===========================================================================

pub fn int8and(arg1: i64, arg2: i64) -> i64 {
    arg1 & arg2
}
pub fn int8or(arg1: i64, arg2: i64) -> i64 {
    arg1 | arg2
}
pub fn int8xor(arg1: i64, arg2: i64) -> i64 {
    arg1 ^ arg2
}
pub fn int8not(arg1: i64) -> i64 {
    !arg1
}
pub fn int8shl(arg1: i64, arg2: i32) -> i64 {
    arg1 << arg2
}
pub fn int8shr(arg1: i64, arg2: i32) -> i64 {
    arg1 >> arg2
}

// ===========================================================================
// Conversion operators.
// ===========================================================================

/// `int48()`: int32 -> int64.
pub fn int48(arg: i32) -> i64 {
    arg as i64
}

/// `int84()`: int64 -> int32 (range-checked).
pub fn int84(arg: i64) -> PgResult<i32> {
    if arg < PG_INT32_MIN || arg > PG_INT32_MAX {
        return Err(
            PgError::error("integer out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        );
    }
    Ok(arg as i32)
}

/// `int28()`: int16 -> int64.
pub fn int28(arg: i16) -> i64 {
    arg as i64
}

/// `int82()`: int64 -> int16 (range-checked).
pub fn int82(arg: i64) -> PgResult<i16> {
    if arg < PG_INT16_MIN || arg > PG_INT16_MAX {
        return Err(PgError::error("smallint out of range")
            .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE));
    }
    Ok(arg as i16)
}

/// `i8tod()`: int64 -> float8.
pub fn i8tod(arg: i64) -> f64 {
    arg as f64
}

/// `dtoi8()`: float8 -> int64 (rounding + range check).
pub fn dtoi8(num: f64) -> PgResult<i64> {
    // Get rid of any fractional part; rint() passes NaN/Inf through unchanged.
    let num = num.round_ties_even();

    if num.is_nan() || !float8_fits_in_int64(num) {
        return Err(bigint_out_of_range());
    }

    Ok(num as i64)
}

/// `i8tof()`: int64 -> float4.
pub fn i8tof(arg: i64) -> f32 {
    arg as f32
}

/// `ftoi8()`: float4 -> int64 (rounding + range check).
pub fn ftoi8(num: f32) -> PgResult<i64> {
    let num = num.round_ties_even();

    if num.is_nan() || !float4_fits_in_int64(num) {
        return Err(bigint_out_of_range());
    }

    Ok(num as i64)
}

/// `i8tooid()`: int64 -> Oid (range-checked).
pub fn i8tooid(arg: i64) -> PgResult<u32> {
    if arg < 0 || arg > PG_UINT32_MAX {
        return Err(
            PgError::error("OID out of range").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
        );
    }
    Ok(arg as u32)
}

/// `oidtoi8()`: Oid -> int64.
pub fn oidtoi8(arg: u32) -> i64 {
    arg as i64
}

// ===========================================================================
// Set-returning generator (numeric series).
//
// The cross-call state and step iteration of generate_series(int8, int8 [,
// int8]) is driven by the executor's `FuncCallContext` / `SRF_*` frame, exactly
// as the `numeric` series generator is in
// `backend-utils-adt-numeric::series_srf`.  The portable first-call validation
// — `step == 0` rejected as ERRCODE_INVALID_PARAMETER_VALUE — and the
// overflow-stopping next-value computation are the only pure-int8 logic, ported
// here as helpers the SRF frame invokes.
// ===========================================================================

/// First-call validation for `generate_series_step_int8()`: the C body rejects
/// a zero step with `ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
/// errmsg("step size cannot equal zero")))`.
pub fn generate_series_int8_check_step(step: i64) -> PgResult<()> {
    if step == 0 {
        return Err(
            PgError::error("step size cannot equal zero").with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
        );
    }
    Ok(())
}

/// Per-call step of `generate_series_step_int8()`: given the saved `current`,
/// `finish` and `step`, returns `Some(next_current_or_none)` to emit `current`
/// and advance, or `None` to stop.  Mirrors the C loop body: when the next
/// value overflows, the step is zeroed so the current emission is the final
/// result (returned as `Some(None)`).
pub fn generate_series_int8_step(current: i64, finish: i64, step: i64) -> Option<Option<i64>> {
    if (step > 0 && current <= finish) || (step < 0 && current >= finish) {
        let mut next = 0i64;
        if pg_add_s64_overflow(current, step, &mut next) {
            // Final result: signal "emit current, then stop".
            Some(None)
        } else {
            Some(Some(next))
        }
    } else {
        None
    }
}
