//! Family: transcendental kernels (numeric.c `sqrt_var`/`exp_var`/`ln_var`/
//! `log_var`/`power_var`/`power_var_int`) + the small-int<->`NumericVar`
//! helpers (`int64_to_numericvar`/`numericvar_to_int64`/`estimate_ln_dweight`)
//! they build on.
//!
//! All allocate digit buffers and so take an explicit `Mcx<'mcx>` and return
//! [`PgResult`] where the C `ereport`s.

use mcx::Mcx;
use types_error::PgResult;
use types_numeric::var::NumericVar;

// ---------------------------------------------------------------------------
// Small-int <-> NumericVar helpers.
// ---------------------------------------------------------------------------

/// `int64_to_numericvar(val, var)`: build a `NumericVar` from an `i64`.
pub fn int64_to_numericvar<'mcx>(mcx: Mcx<'mcx>, val: i64) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, val);
    todo!("kernel_transcendental::int64_to_numericvar — numeric.c int64_to_numericvar")
}

/// `numericvar_to_int64(var)`: convert to `i64`, `Ok(None)` on the C `false`
/// (out of range / non-integral).
pub fn numericvar_to_int64(var: &NumericVar<'_>) -> PgResult<Option<i64>> {
    let _ = var;
    todo!("kernel_transcendental::numericvar_to_int64 — numeric.c numericvar_to_int64")
}

/// `estimate_ln_dweight(var)`: estimate the decimal weight of `ln(var)`.
pub fn estimate_ln_dweight(var: &NumericVar<'_>) -> PgResult<i32> {
    let _ = var;
    todo!("kernel_transcendental::estimate_ln_dweight — numeric.c estimate_ln_dweight")
}

// ---------------------------------------------------------------------------
// Transcendental kernels.
// ---------------------------------------------------------------------------

pub fn sqrt_var<'mcx>(
    mcx: Mcx<'mcx>,
    arg: &NumericVar<'_>,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, arg, rscale);
    todo!("kernel_transcendental::sqrt_var — numeric.c sqrt_var")
}

pub fn exp_var<'mcx>(
    mcx: Mcx<'mcx>,
    arg: &NumericVar<'_>,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, arg, rscale);
    todo!("kernel_transcendental::exp_var — numeric.c exp_var")
}

pub fn ln_var<'mcx>(mcx: Mcx<'mcx>, arg: &NumericVar<'_>, rscale: i32) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, arg, rscale);
    todo!("kernel_transcendental::ln_var — numeric.c ln_var")
}

pub fn log_var<'mcx>(
    mcx: Mcx<'mcx>,
    base: &NumericVar<'_>,
    num: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, base, num);
    todo!("kernel_transcendental::log_var — numeric.c log_var")
}

pub fn power_var<'mcx>(
    mcx: Mcx<'mcx>,
    base: &NumericVar<'_>,
    exp: &NumericVar<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, base, exp);
    todo!("kernel_transcendental::power_var — numeric.c power_var")
}

pub fn power_var_int<'mcx>(
    mcx: Mcx<'mcx>,
    base: &NumericVar<'_>,
    exp: i32,
    exp_dscale: i32,
    rscale: i32,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, base, exp, exp_dscale, rscale);
    todo!("kernel_transcendental::power_var_int — numeric.c power_var_int")
}
