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

use core::cmp::Ordering;

use mcx::{Mcx, PgVec};
use types_datum::Datum;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Arithmetic operator cores (on-disk byte images in/out).
// ---------------------------------------------------------------------------

pub fn numeric_add<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_add — numeric.c numeric_add")
}

pub fn numeric_sub<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_sub — numeric.c numeric_sub")
}

pub fn numeric_mul<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_mul — numeric.c numeric_mul")
}

pub fn numeric_div<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_div — numeric.c numeric_div")
}

pub fn numeric_div_trunc<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_div_trunc — numeric.c numeric_div_trunc")
}

pub fn numeric_mod<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_mod — numeric.c numeric_mod")
}

// ---------------------------------------------------------------------------
// Unary ops.
// ---------------------------------------------------------------------------

pub fn numeric_abs<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_abs — numeric.c numeric_abs")
}

pub fn numeric_uminus<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_uminus — numeric.c numeric_uminus")
}

pub fn numeric_uplus<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_uplus — numeric.c numeric_uplus")
}

pub fn numeric_inc<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_inc — numeric.c numeric_inc")
}

pub fn numeric_sign<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_sign — numeric.c numeric_sign")
}

// ---------------------------------------------------------------------------
// Comparison cores (pure; infallible).
// ---------------------------------------------------------------------------

pub fn numeric_cmp(a: &[u8], b: &[u8]) -> Ordering {
    let _ = (a, b);
    todo!("ops_sql::numeric_cmp — numeric.c numeric_cmp")
}

pub fn numeric_eq(a: &[u8], b: &[u8]) -> bool {
    let _ = (a, b);
    todo!("ops_sql::numeric_eq — numeric.c numeric_eq")
}

pub fn numeric_ne(a: &[u8], b: &[u8]) -> bool {
    let _ = (a, b);
    todo!("ops_sql::numeric_ne — numeric.c numeric_ne")
}

pub fn numeric_lt(a: &[u8], b: &[u8]) -> bool {
    let _ = (a, b);
    todo!("ops_sql::numeric_lt — numeric.c numeric_lt")
}

pub fn numeric_le(a: &[u8], b: &[u8]) -> bool {
    let _ = (a, b);
    todo!("ops_sql::numeric_le — numeric.c numeric_le")
}

pub fn numeric_gt(a: &[u8], b: &[u8]) -> bool {
    let _ = (a, b);
    todo!("ops_sql::numeric_gt — numeric.c numeric_gt")
}

pub fn numeric_ge(a: &[u8], b: &[u8]) -> bool {
    let _ = (a, b);
    todo!("ops_sql::numeric_ge — numeric.c numeric_ge")
}

// ---------------------------------------------------------------------------
// Round / trunc / ceil / floor + transcendental SQL wrappers.
// ---------------------------------------------------------------------------

pub fn numeric_round<'mcx>(mcx: Mcx<'mcx>, num: &[u8], scale: i32) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num, scale);
    todo!("ops_sql::numeric_round — numeric.c numeric_round")
}

pub fn numeric_trunc<'mcx>(mcx: Mcx<'mcx>, num: &[u8], scale: i32) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num, scale);
    todo!("ops_sql::numeric_trunc — numeric.c numeric_trunc")
}

pub fn numeric_ceil<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_ceil — numeric.c numeric_ceil")
}

pub fn numeric_floor<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_floor — numeric.c numeric_floor")
}

pub fn numeric_sqrt<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_sqrt — numeric.c numeric_sqrt")
}

pub fn numeric_exp<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_exp — numeric.c numeric_exp")
}

pub fn numeric_ln<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_ln — numeric.c numeric_ln")
}

pub fn numeric_log<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_log — numeric.c numeric_log")
}

pub fn numeric_power<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_power — numeric.c numeric_power")
}

// ---------------------------------------------------------------------------
// Special functions (numeric.c gcd_var/numeric_gcd/lcm/factorial/min/max/
// width_bucket_numeric).
// ---------------------------------------------------------------------------

pub fn numeric_gcd<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_gcd — numeric.c numeric_gcd")
}

pub fn numeric_lcm<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_lcm — numeric.c numeric_lcm")
}

pub fn numeric_factorial<'mcx>(mcx: Mcx<'mcx>, n: i64) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, n);
    todo!("ops_sql::numeric_factorial — numeric.c numeric_fac")
}

pub fn numeric_min<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_min — numeric.c numeric_smaller")
}

pub fn numeric_max<'mcx>(mcx: Mcx<'mcx>, a: &[u8], b: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, a, b);
    todo!("ops_sql::numeric_max — numeric.c numeric_larger")
}

pub fn width_bucket_numeric(
    operand: &[u8],
    bound1: &[u8],
    bound2: &[u8],
    count: &[u8],
) -> PgResult<i32> {
    let _ = (operand, bound1, bound2, count);
    todo!("ops_sql::width_bucket_numeric — numeric.c width_bucket_numeric")
}

// ---------------------------------------------------------------------------
// Scale / typmod / normalize helpers.
// ---------------------------------------------------------------------------

pub fn get_min_scale(num: &[u8]) -> i32 {
    let _ = num;
    todo!("ops_sql::get_min_scale — numeric.c get_min_scale")
}

pub fn numeric_trim_scale<'mcx>(mcx: Mcx<'mcx>, num: &[u8]) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, num);
    todo!("ops_sql::numeric_trim_scale — numeric.c numeric_trim_scale")
}

pub fn numeric_scale(num: &[u8]) -> PgResult<i32> {
    let _ = num;
    todo!("ops_sql::numeric_scale — numeric.c numeric_scale")
}

pub fn numerictypmodin(typmod_parts: &[i32]) -> PgResult<i32> {
    let _ = typmod_parts;
    todo!("ops_sql::numerictypmodin — numeric.c numerictypmodin")
}

pub fn numerictypmodout<'mcx>(mcx: Mcx<'mcx>, typmod: i32) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, typmod);
    todo!("ops_sql::numerictypmodout — numeric.c numerictypmodout")
}

/// `numeric_maximum_size(typmod)`: the maximum on-disk size of a `numeric` with
/// the given typmod, or -1 if indeterminate. Pure arithmetic; infallible.
pub fn numeric_maximum_size(typmod: i32) -> i32 {
    let _ = typmod;
    todo!("ops_sql::numeric_maximum_size — numeric.c numeric_maximum_size")
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
pub fn seam_numeric_subdiff(v1: Datum, v2: Datum) -> PgResult<f64> {
    let _ = (v1, v2);
    todo!("ops_sql::seam_numeric_subdiff — rangetypes.c:1703 numrange_subdiff")
}
