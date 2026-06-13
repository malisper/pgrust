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

use core::cmp::Ordering;

use mcx::Mcx;
use types_error::PgResult;
use types_numeric::var::NumericVar;
use types_numeric::NumericDigit;

// ---------------------------------------------------------------------------
// Preinitialized constants (numeric.c const_zero..const_one_point_one + the
// special const_nan/const_pinf/const_ninf). Allocated fresh in `mcx`.
// ---------------------------------------------------------------------------

pub fn const_zero(mcx: Mcx<'_>) -> NumericVar<'_> {
    let _ = mcx;
    todo!("kernel_var::const_zero — numeric.c const_zero")
}

pub fn const_one(mcx: Mcx<'_>) -> NumericVar<'_> {
    let _ = mcx;
    todo!("kernel_var::const_one — numeric.c const_one")
}

pub fn const_minus_one(mcx: Mcx<'_>) -> NumericVar<'_> {
    let _ = mcx;
    todo!("kernel_var::const_minus_one — numeric.c const_minus_one")
}

pub fn const_two(mcx: Mcx<'_>) -> NumericVar<'_> {
    let _ = mcx;
    todo!("kernel_var::const_two — numeric.c const_two")
}

pub fn const_ten(mcx: Mcx<'_>) -> NumericVar<'_> {
    let _ = mcx;
    todo!("kernel_var::const_ten — numeric.c const_ten")
}

pub fn const_zero_point_nine(mcx: Mcx<'_>) -> NumericVar<'_> {
    let _ = mcx;
    todo!("kernel_var::const_zero_point_nine — numeric.c const_zero_point_nine")
}

pub fn const_one_point_one(mcx: Mcx<'_>) -> NumericVar<'_> {
    let _ = mcx;
    todo!("kernel_var::const_one_point_one — numeric.c const_one_point_one")
}

// ---------------------------------------------------------------------------
// Lifecycle helpers (alloc_var/zero_var/set_var_from_var) — internal but
// shared across the kernel; sized in `mcx`.
// ---------------------------------------------------------------------------

/// `alloc_var(var, ndigits)`: (re)allocate the digit buffer to hold `ndigits`
/// logical digits, charged to `mcx`.
pub fn alloc_var<'mcx>(mcx: Mcx<'mcx>, ndigits: usize) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, ndigits);
    todo!("kernel_var::alloc_var — numeric.c alloc_var")
}

/// `set_var_from_var(value, dest)`: deep-copy `src` into a fresh var in `mcx`.
pub fn set_var_from_var<'mcx>(mcx: Mcx<'mcx>, src: &NumericVar<'_>) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, src);
    todo!("kernel_var::set_var_from_var — numeric.c set_var_from_var")
}

// ---------------------------------------------------------------------------
// Comparison (numeric.c cmp_var/cmp_var_common/cmp_abs/cmp_abs_common). Pure;
// no allocation.
// ---------------------------------------------------------------------------

pub fn cmp_var(var1: &NumericVar<'_>, var2: &NumericVar<'_>) -> Ordering {
    let _ = (var1, var2);
    todo!("kernel_var::cmp_var — numeric.c cmp_var")
}

pub fn cmp_var_common(
    var1digits: &[NumericDigit],
    var1weight: i32,
    var1sign: i32,
    var2digits: &[NumericDigit],
    var2weight: i32,
    var2sign: i32,
) -> Ordering {
    let _ = (var1digits, var1weight, var1sign, var2digits, var2weight, var2sign);
    todo!("kernel_var::cmp_var_common — numeric.c cmp_var_common")
}

pub fn cmp_abs(var1: &NumericVar<'_>, var2: &NumericVar<'_>) -> Ordering {
    let _ = (var1, var2);
    todo!("kernel_var::cmp_abs — numeric.c cmp_abs")
}

pub fn cmp_abs_common(
    var1digits: &[NumericDigit],
    var1weight: i32,
    var2digits: &[NumericDigit],
    var2weight: i32,
) -> Ordering {
    let _ = (var1digits, var1weight, var2digits, var2weight);
    todo!("kernel_var::cmp_abs_common — numeric.c cmp_abs_common")
}

// ---------------------------------------------------------------------------
// Add / subtract (numeric.c add_var/sub_var/add_abs/sub_abs).
// ---------------------------------------------------------------------------

pub fn add_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var1, var2);
    todo!("kernel_var::add_var — numeric.c add_var")
}

pub fn sub_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var1, var2);
    todo!("kernel_var::sub_var — numeric.c sub_var")
}

pub fn add_abs<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var1, var2);
    todo!("kernel_var::add_abs — numeric.c add_abs")
}

pub fn sub_abs<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var1, var2);
    todo!("kernel_var::sub_abs — numeric.c sub_abs")
}

// ---------------------------------------------------------------------------
// Round / truncate / strip in place (numeric.c round_var/trunc_var/strip_var).
// ---------------------------------------------------------------------------

/// `round_var(var, rscale)`: round in place to `rscale` decimal digits.
pub fn round_var(var: &mut NumericVar<'_>, rscale: i32) {
    let _ = (var, rscale);
    todo!("kernel_var::round_var — numeric.c round_var")
}

/// `trunc_var(var, rscale)`: truncate in place to `rscale` decimal digits.
pub fn trunc_var(var: &mut NumericVar<'_>, rscale: i32) {
    let _ = (var, rscale);
    todo!("kernel_var::trunc_var — numeric.c trunc_var")
}

/// `strip_var(var)`: strip leading/trailing zero digits in place.
pub fn strip_var(var: &mut NumericVar<'_>) {
    let _ = var;
    todo!("kernel_var::strip_var — numeric.c strip_var")
}

// ---------------------------------------------------------------------------
// Multiply (numeric.c mul_var).
// ---------------------------------------------------------------------------

pub fn mul_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var1, var2, rscale);
    todo!("kernel_var::mul_var — numeric.c mul_var")
}

// ---------------------------------------------------------------------------
// Divide / mod / floor / ceil (numeric.c div_var/div_var_int/mod_var/
// div_mod_var/floor_var/ceil_var/select_div_scale).
// ---------------------------------------------------------------------------

pub fn div_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
    rscale: i32,
    round: bool,
    exact: bool,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var1, var2, rscale, round, exact);
    todo!("kernel_var::div_var — numeric.c div_var")
}

pub fn select_div_scale(var1: &NumericVar<'_>, var2: &NumericVar<'_>) -> i32 {
    let _ = (var1, var2);
    todo!("kernel_var::select_div_scale — numeric.c select_div_scale")
}

pub fn mod_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var1, var2);
    todo!("kernel_var::mod_var — numeric.c mod_var")
}

pub fn div_mod_var<'mcx>(
    mcx: Mcx<'mcx>,
    var1: &NumericVar<'_>,
    var2: &NumericVar<'_>,
) -> PgResult<(NumericVar<'mcx>, NumericVar<'mcx>)> {
    let _ = (mcx, var1, var2);
    todo!("kernel_var::div_mod_var — numeric.c div_mod_var")
}

pub fn ceil_var<'mcx>(mcx: Mcx<'mcx>, var: &NumericVar<'_>) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var);
    todo!("kernel_var::ceil_var — numeric.c ceil_var")
}

pub fn floor_var<'mcx>(mcx: Mcx<'mcx>, var: &NumericVar<'_>) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, var);
    todo!("kernel_var::floor_var — numeric.c floor_var")
}
