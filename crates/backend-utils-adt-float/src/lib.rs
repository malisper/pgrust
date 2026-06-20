//! Port of `src/backend/utils/adt/float.c` — the built-in floating-point types
//! `float4` / `float8`.
//!
//! Error behavior matches C exactly (same message text and SQLSTATE):
//! `float8in_internal` / `float4in_internal` raise
//! `ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE` (22003) on `strtod`/`strtof` ERANGE; the
//! arithmetic cores raise the float overflow / underflow / zero-divide errors
//! from the shared `float_*_error` helpers.
//!
//! NaN/Inf handling follows `<utils/float.h>` verbatim (all NaNs compare equal
//! and sort after every non-NaN value). Shortest round-trip output uses
//! `common-ryu`, matching the default (`extra_float_digits > 0`) path; the
//! legacy `%.*g` rounding path is reproduced for `extra_float_digits <= 0`.
//!
//! The bare `Datum fn(FunctionCallInfo)` builtin-registry wiring is deferred
//! project-wide (the Datum-redesign lifetime gate); each function is exposed as
//! a typed core that the fmgr boundary will wrap one-to-one.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::manual_range_contains)]
#![allow(clippy::excessive_precision)]

use types_error::{
    PgError, PgResult, ERRCODE_DIVISION_BY_ZERO, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
};

pub mod aggregates;
pub mod fmgr_builtins;
pub mod funcs;
pub mod io;

pub use aggregates::{
    check_float8_array, float4_accum, float8_accum, float8_avg, float8_combine, float8_corr,
    float8_covar_pop, float8_covar_samp, float8_regr_accum, float8_regr_avgx, float8_regr_avgy,
    float8_regr_combine, float8_regr_intercept, float8_regr_r2, float8_regr_slope, float8_regr_sxx,
    float8_regr_sxy, float8_regr_syy, float8_stddev_pop, float8_stddev_samp, float8_var_pop,
    float8_var_samp,
};
pub use funcs::*;
pub use io::{
    float4in, float4in_internal, float4out, float4out_with, float4recv, float4send, float8in,
    float8in_internal, float8out, float8out_internal, float8out_internal_with, float8recv,
    float8send,
};

/// Install the seams this crate owns (`utils/float.h` inline arithmetic
/// primitives needed across a dependency cycle, e.g. by `cash.c`).
pub fn init_seams() {
    backend_utils_adt_float_seams::float8_mul::set(float8_mul);
    backend_utils_adt_float_seams::float8_div::set(float8_div);
    backend_utils_adt_float_seams::float8_pl::set(float8_pl);
    backend_utils_adt_float_seams::float8_mi::set(float8_mi);
    backend_utils_adt_float_seams::float8_eq::set(float8_eq);
    backend_utils_adt_float_seams::float8_lt::set(float8_lt);
    backend_utils_adt_float_seams::float8_gt::set(float8_gt);
    backend_utils_adt_float_seams::float8_min::set(float8_min);
    backend_utils_adt_float_seams::float8_max::set(float8_max);
    backend_utils_adt_float_seams::get_float8_infinity::set(get_float8_infinity);
    backend_utils_adt_float_seams::get_float8_nan::set(get_float8_nan);
    backend_utils_adt_float_seams::float_overflow_error::set(float_overflow_error);
    backend_utils_adt_float_seams::float_underflow_error::set(float_underflow_error);
    backend_utils_adt_float_seams::float8in_internal_endptr::set(float8in_internal_endptr_seam);
    backend_utils_adt_float_seams::float8out_internal::set(float8out_internal);

    // `extra_float_digits` GUC (float.c:40, `int extra_float_digits = 1`). C
    // exports the storage as a plain global that the GUC machinery writes
    // (`&extra_float_digits` in guc_tables.c, CLIENT_CONN_LOCALE) and that the
    // float-output functions read live. Install the accessors bridging this
    // crate's backing store into the `extra_float_digits` GUC var slot.
    backend_utils_misc_guc_tables::vars::extra_float_digits.install(
        backend_utils_misc_guc_tables::GucVarAccessors {
            get: get_extra_float_digits,
            set: set_extra_float_digits,
        },
    );

    // Register every `float.c` builtin into the fmgr-core builtin table
    // (C: `fmgr_builtins[]`), so by-OID dispatch resolves them.
    fmgr_builtins::register_float_builtins();
}

thread_local! {
    /// Backing store for the `extra_float_digits` GUC (float.c:40). Boot value
    /// is C's static initializer `int extra_float_digits = 1`; the GUC
    /// machinery overwrites it via the installed accessor on assignment.
    static EXTRA_FLOAT_DIGITS: core::cell::Cell<i32> = const { core::cell::Cell::new(1) };
}

/// Read the live `extra_float_digits` GUC value (C's `extra_float_digits`
/// global), as the float-output functions do.
pub fn get_extra_float_digits() -> i32 {
    EXTRA_FLOAT_DIGITS.with(|c| c.get())
}

/// Write the `extra_float_digits` backing store (the GUC assign path).
pub fn set_extra_float_digits(v: i32) {
    EXTRA_FLOAT_DIGITS.with(|c| c.set(v));
}

/// Adapter for the `float8in_internal_endptr` seam: the seam carries owned
/// `String`s and returns `(value, consumed)`, reporting the stopping point
/// (`endptr_p != NULL` mode), as `geo_ops.c`'s `single_decode` needs.
fn float8in_internal_endptr_seam(
    num: String,
    type_name: String,
    orig_string: String,
) -> PgResult<(f64, usize)> {
    let mut consumed = 0usize;
    // Geo I/O always wants a hard error here (no soft `pg_input_is_valid` path).
    let value =
        io::float8in_internal(&num, Some(&mut consumed), &type_name, &orig_string, None)?;
    Ok((value, consumed))
}

// ---------------------------------------------------------------------------
// Constants from <utils/float.h> and <float.h>.
// ---------------------------------------------------------------------------

/// `M_PI` (X/Open) used by `dpi()`.
pub const M_PI: f64 = core::f64::consts::PI;

/// `RADIANS_PER_DEGREE` from `<utils/float.h>`, i.e. PI / 180, given as the
/// exact decimal literal PostgreSQL uses (NOT recomputed, to match bit-for-bit).
pub const RADIANS_PER_DEGREE: f64 = 0.0174532925199432957692;

/// `FLT_DIG` from `<float.h>` (decimal digits a `float` can represent: 6).
pub const FLT_DIG: i32 = 6;
/// `DBL_DIG` from `<float.h>` (decimal digits a `double` can represent: 15).
pub const DBL_DIG: i32 = 15;

/// `extra_float_digits` GUC (float.c:40). Default 1: use shortest-decimal
/// output. When <= 0, the legacy `%.*g` rounding path is used. The fmgr layer
/// reads the live GUC; the cores accept the value as a parameter so they stay
/// pure and testable.
pub const DEFAULT_EXTRA_FLOAT_DIGITS: i32 = 1;

// ---------------------------------------------------------------------------
// Reasonably platform-independent infinity / NaN generators (float.h).
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Shared error reporters (float.c:85-107).
// ---------------------------------------------------------------------------

/// `float_overflow_error()` (float.c:85).
#[inline]
pub fn float_overflow_error() -> PgError {
    PgError::error("value out of range: overflow").with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `float_underflow_error()` (float.c:93).
#[inline]
pub fn float_underflow_error() -> PgError {
    PgError::error("value out of range: underflow")
        .with_sqlstate(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

/// `float_zero_divide_error()` (float.c:101).
#[inline]
pub fn float_zero_divide_error() -> PgError {
    PgError::error("division by zero").with_sqlstate(ERRCODE_DIVISION_BY_ZERO)
}

// ---------------------------------------------------------------------------
// is_infinite (float.c:117).
// ---------------------------------------------------------------------------

/// `is_infinite()` (float.c:117): -1 for -Inf, 1 for +Inf, 0 otherwise.
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

// ---------------------------------------------------------------------------
// FLOAT{4,8}_FITS_IN_INT{16,32,64} (c.h:1057-1068).
// ---------------------------------------------------------------------------

#[inline]
pub fn FLOAT4_FITS_IN_INT16(num: f32) -> bool {
    num >= (i16::MIN as f32) && num < -(i16::MIN as f32)
}

#[inline]
pub fn FLOAT4_FITS_IN_INT32(num: f32) -> bool {
    num >= (i32::MIN as f32) && num < -(i32::MIN as f32)
}

#[inline]
pub fn FLOAT4_FITS_IN_INT64(num: f32) -> bool {
    num >= (i64::MIN as f32) && num < -(i64::MIN as f32)
}

#[inline]
pub fn FLOAT8_FITS_IN_INT16(num: f64) -> bool {
    num >= (i16::MIN as f64) && num < -(i16::MIN as f64)
}

#[inline]
pub fn FLOAT8_FITS_IN_INT32(num: f64) -> bool {
    num >= (i32::MIN as f64) && num < -(i32::MIN as f64)
}

#[inline]
pub fn FLOAT8_FITS_IN_INT64(num: f64) -> bool {
    num >= (i64::MIN as f64) && num < -(i64::MIN as f64)
}

// ---------------------------------------------------------------------------
// Floating-point arithmetic with overflow/underflow reported as errors
// (float.h:160-271).
// ---------------------------------------------------------------------------

/// `float4_pl()` (float.h:160).
#[inline]
pub fn float4_pl(val1: f32, val2: f32) -> PgResult<f32> {
    let result = val1 + val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `float8_pl()` (float.h:172).
#[inline]
pub fn float8_pl(val1: f64, val2: f64) -> PgResult<f64> {
    let result = val1 + val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `float4_mi()` (float.h:184).
#[inline]
pub fn float4_mi(val1: f32, val2: f32) -> PgResult<f32> {
    let result = val1 - val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `float8_mi()` (float.h:196).
#[inline]
pub fn float8_mi(val1: f64, val2: f64) -> PgResult<f64> {
    let result = val1 - val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error());
    }
    Ok(result)
}

/// `float4_mul()` (float.h:208).
#[inline]
pub fn float4_mul(val1: f32, val2: f32) -> PgResult<f32> {
    let result = val1 * val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && val1 != 0.0 && val2 != 0.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `float8_mul()` (float.h:221).
#[inline]
pub fn float8_mul(val1: f64, val2: f64) -> PgResult<f64> {
    let result = val1 * val2;
    if result.is_infinite() && !val1.is_infinite() && !val2.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && val1 != 0.0 && val2 != 0.0 {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `float4_div()` (float.h:234).
#[inline]
pub fn float4_div(val1: f32, val2: f32) -> PgResult<f32> {
    if val2 == 0.0 && !val1.is_nan() {
        return Err(float_zero_divide_error());
    }
    let result = val1 / val2;
    if result.is_infinite() && !val1.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && val1 != 0.0 && !val2.is_infinite() {
        return Err(float_underflow_error());
    }
    Ok(result)
}

/// `float8_div()` (float.h:248).
#[inline]
pub fn float8_div(val1: f64, val2: f64) -> PgResult<f64> {
    if val2 == 0.0 && !val1.is_nan() {
        return Err(float_zero_divide_error());
    }
    let result = val1 / val2;
    if result.is_infinite() && !val1.is_infinite() {
        return Err(float_overflow_error());
    }
    if result == 0.0 && val1 != 0.0 && !val2.is_infinite() {
        return Err(float_underflow_error());
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// NaN-aware comparisons (float.h:280-379). All NaNs are equal and greater than
// every non-NaN value.
// ---------------------------------------------------------------------------

/// `float4_eq()` (float.h:280).
#[inline]
pub fn float4_eq(val1: f32, val2: f32) -> bool {
    if val1.is_nan() {
        val2.is_nan()
    } else {
        !val2.is_nan() && val1 == val2
    }
}

/// `float8_eq()` (float.h:286).
#[inline]
pub fn float8_eq(val1: f64, val2: f64) -> bool {
    if val1.is_nan() {
        val2.is_nan()
    } else {
        !val2.is_nan() && val1 == val2
    }
}

/// `float4_ne()` (float.h:292).
#[inline]
pub fn float4_ne(val1: f32, val2: f32) -> bool {
    if val1.is_nan() {
        !val2.is_nan()
    } else {
        val2.is_nan() || val1 != val2
    }
}

/// `float8_ne()` (float.h:298).
#[inline]
pub fn float8_ne(val1: f64, val2: f64) -> bool {
    if val1.is_nan() {
        !val2.is_nan()
    } else {
        val2.is_nan() || val1 != val2
    }
}

/// `float4_lt()` (float.h:304).
#[inline]
pub fn float4_lt(val1: f32, val2: f32) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}

/// `float8_lt()` (float.h:310).
#[inline]
pub fn float8_lt(val1: f64, val2: f64) -> bool {
    !val1.is_nan() && (val2.is_nan() || val1 < val2)
}

/// `float4_le()` (float.h:316).
#[inline]
pub fn float4_le(val1: f32, val2: f32) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}

/// `float8_le()` (float.h:322).
#[inline]
pub fn float8_le(val1: f64, val2: f64) -> bool {
    val2.is_nan() || (!val1.is_nan() && val1 <= val2)
}

/// `float4_gt()` (float.h:328).
#[inline]
pub fn float4_gt(val1: f32, val2: f32) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}

/// `float8_gt()` (float.h:334).
#[inline]
pub fn float8_gt(val1: f64, val2: f64) -> bool {
    !val2.is_nan() && (val1.is_nan() || val1 > val2)
}

/// `float4_ge()` (float.h:340).
#[inline]
pub fn float4_ge(val1: f32, val2: f32) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}

/// `float8_ge()` (float.h:346).
#[inline]
pub fn float8_ge(val1: f64, val2: f64) -> bool {
    val1.is_nan() || (!val2.is_nan() && val1 >= val2)
}

/// `float4_min()` (float.h:352).
#[inline]
pub fn float4_min(val1: f32, val2: f32) -> f32 {
    if float4_lt(val1, val2) {
        val1
    } else {
        val2
    }
}

/// `float8_min()` (float.h:358).
#[inline]
pub fn float8_min(val1: f64, val2: f64) -> f64 {
    if float8_lt(val1, val2) {
        val1
    } else {
        val2
    }
}

/// `float4_max()` (float.h:364).
#[inline]
pub fn float4_max(val1: f32, val2: f32) -> f32 {
    if float4_gt(val1, val2) {
        val1
    } else {
        val2
    }
}

/// `float8_max()` (float.h:370).
#[inline]
pub fn float8_max(val1: f64, val2: f64) -> f64 {
    if float8_gt(val1, val2) {
        val1
    } else {
        val2
    }
}

// ---------------------------------------------------------------------------
// FLOAT4 / FLOAT8 BASE OPERATIONS (float.c:587-712).
// ---------------------------------------------------------------------------

/// `float4abs()` core (float.c:590).
#[inline]
pub fn float4abs(arg1: f32) -> f32 {
    arg1.abs()
}

/// `float8abs()` core (float.c:656).
#[inline]
pub fn float8abs(arg1: f64) -> f64 {
    arg1.abs()
}

/// `float4um()` core (float.c:601): unary minus.
#[inline]
pub fn float4um(arg1: f32) -> f32 {
    -arg1
}

/// `float8um()` core (float.c:668): unary minus.
#[inline]
pub fn float8um(arg1: f64) -> f64 {
    -arg1
}

/// `float4up()` core (float.c:611): unary plus (identity).
#[inline]
pub fn float4up(arg: f32) -> f32 {
    arg
}

/// `float8up()` core (float.c:678): unary plus (identity).
#[inline]
pub fn float8up(arg: f64) -> f64 {
    arg
}

/// `float4larger()` core (float.c:619).
#[inline]
pub fn float4larger(arg1: f32, arg2: f32) -> f32 {
    if float4_gt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

/// `float4smaller()` core (float.c:633).
#[inline]
pub fn float4smaller(arg1: f32, arg2: f32) -> f32 {
    if float4_lt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

/// `float8larger()` core (float.c:686).
#[inline]
pub fn float8larger(arg1: f64, arg2: f64) -> f64 {
    if float8_gt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

/// `float8smaller()` core (float.c:700).
#[inline]
pub fn float8smaller(arg1: f64, arg2: f64) -> f64 {
    if float8_lt(arg1, arg2) {
        arg1
    } else {
        arg2
    }
}

// ---------------------------------------------------------------------------
// COMPARISON cores: *_cmp_internal (float.c:815, 909) and the btree cmp
// variants (float.c:880-1018), including the SortSupport fast comparators.
// ---------------------------------------------------------------------------

/// `float4_cmp_internal()` (float.c:815).
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

/// `float8_cmp_internal()` (float.c:909).
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

/// `btfloat4cmp()` core (float.c:880).
#[inline]
pub fn btfloat4cmp(arg1: f32, arg2: f32) -> i32 {
    float4_cmp_internal(arg1, arg2)
}

/// `btfloat8cmp()` core (float.c:974).
#[inline]
pub fn btfloat8cmp(arg1: f64, arg2: f64) -> i32 {
    float8_cmp_internal(arg1, arg2)
}

/// `btfloat48cmp()` core (float.c:1000): widen float4 to float8 and compare.
#[inline]
pub fn btfloat48cmp(arg1: f32, arg2: f64) -> i32 {
    float8_cmp_internal(arg1 as f64, arg2)
}

/// `btfloat84cmp()` core (float.c:1010): widen float4 to float8 and compare.
#[inline]
pub fn btfloat84cmp(arg1: f64, arg2: f32) -> i32 {
    float8_cmp_internal(arg1, arg2 as f64)
}

/// `btfloat4fastcmp()` (float.c:889): the SortSupport fast comparator installed
/// by `btfloat4sortsupport` (`ssup->comparator`). The Datum arguments unpack to
/// `float4` via `DatumGetFloat4`; the comparison is `float4_cmp_internal`.
#[inline]
pub fn btfloat4fastcmp(arg1: f32, arg2: f32) -> i32 {
    float4_cmp_internal(arg1, arg2)
}

/// `btfloat8fastcmp()` (float.c:983): the SortSupport fast comparator installed
/// by `btfloat8sortsupport`. Datums unpack to `float8`; compare with
/// `float8_cmp_internal`.
#[inline]
pub fn btfloat8fastcmp(arg1: f64, arg2: f64) -> i32 {
    float8_cmp_internal(arg1, arg2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nan_compares_after_everything() {
        let nan = f64::NAN;
        assert!(float8_eq(nan, nan));
        assert!(!float8_eq(nan, 1.0));
        assert!(float8_gt(nan, f64::INFINITY));
        assert!(!float8_lt(nan, 1.0));
        assert!(float8_ge(nan, nan));
        assert_eq!(float8_cmp_internal(nan, nan), 0);
        assert_eq!(float8_cmp_internal(nan, 1.0), 1);
        assert_eq!(float8_cmp_internal(1.0, nan), -1);
    }

    #[test]
    fn arithmetic_overflow_and_zero_divide() {
        let err = float8_pl(f64::MAX, f64::MAX).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        assert_eq!(err.message(), "value out of range: overflow");

        assert_eq!(float8_pl(f64::INFINITY, 1.0).unwrap(), f64::INFINITY);

        let err = float8_div(1.0, 0.0).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_DIVISION_BY_ZERO);
        assert_eq!(err.message(), "division by zero");

        assert!(float8_div(f64::NAN, 0.0).unwrap().is_nan());

        let err = float8_mul(f64::MIN_POSITIVE, f64::MIN_POSITIVE).unwrap_err();
        assert_eq!(err.message(), "value out of range: underflow");
    }

    #[test]
    fn fits_in_int_boundaries() {
        assert!(FLOAT8_FITS_IN_INT32(2147483647.0));
        assert!(!FLOAT8_FITS_IN_INT32(2147483648.0));
        assert!(FLOAT8_FITS_IN_INT32(-2147483648.0));
        assert!(!FLOAT8_FITS_IN_INT32(-2147483649.0));
        assert!(!FLOAT8_FITS_IN_INT32(f64::INFINITY));
    }

    #[test]
    fn is_infinite_signs() {
        assert_eq!(is_infinite(f64::INFINITY), 1);
        assert_eq!(is_infinite(f64::NEG_INFINITY), -1);
        assert_eq!(is_infinite(0.0), 0);
        assert_eq!(is_infinite(f64::NAN), 0);
    }
}
