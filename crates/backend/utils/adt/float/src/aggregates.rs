//! FLOAT AGGREGATE OPERATORS and SQL2003 BINARY AGGREGATES from float.c
//! (float.c:2878-3846): the accumulate / combine / final functions backing
//! `avg()`, the variance / stddev aggregates, and the linear-regression family
//! (`regr_*`, `covar_*`, `corr`). All carry the Youngs-Cramer numerically-stable
//! running-(co)variance kernel, ported faithfully.
//!
//! The transition value is a `float8[]` `ArrayType` (a varlena). C reads it with
//! `check_float8_array` (validate 1-D, non-null, FLOAT8OID, exactly `n` elems,
//! then treat `ARR_DATA_PTR` as a C array of `n` float8) and writes it back
//! either in place (inside an aggregate transition) or via
//! `construct_array_builtin` (a direct SQL call). This repo has no ambient
//! context: each transition / combine function takes the detoasted transition
//! array image (`&[u8]`) and a target `Mcx<'mcx>`, and ALWAYS returns a freshly
//! constructed array image (`PgVec<'mcx, u8>`). That is behavior-identical to C:
//! the `AggCheckCallContext` in-place leg is purely an allocation optimization
//! over the same final element values, which the executor reintroduces when it
//! owns the transition buffer. The final functions return `Option<f64>`
//! (`Ok(None)` is SQL NULL).

use ::arrayfuncs::construct::construct_array;
use ::arrayfuncs::foundation::{
    self, arr_dim, arr_elemtype, arr_hasnull, arr_ndim, fetch_att, FLOAT8OID,
};
use mcx::{Mcx, PgVec};
use ::datum::Datum;
use types_error::{PgError, PgResult};

use crate::{float8_pl, float_overflow_error, get_float8_nan};

/// FLOAT8 array element storage attributes (`pg_type`): 8-byte, pass-by-value,
/// `'d'` alignment — matching `construct_array_builtin`'s FLOAT8OID switch arm.
const FLOAT8_ELMLEN: i32 = 8;
const FLOAT8_ELMBYVAL: bool = foundation::FLOAT8PASSBYVAL;
const FLOAT8_ELMALIGN: u8 = b'd';

// ===========================================================================
// check_float8_array (float.c:2927).
// ===========================================================================

/// `check_float8_array(transarray, caller, n)` (float.c:2927): validate that
/// `transarray` is a 1-D, non-null, FLOAT8OID array of exactly `n` elements and
/// return its `n` values; otherwise raise
/// `elog(ERROR, "%s: expected %d-element float8 array", caller, n)`.
///
/// The C code does not call `deconstruct_array` (the data is just a C array of
/// N float8); we read each element directly with the same `fetch_att` walk a
/// float8 array uses (every element 8-byte, double-aligned, no NULL bitmap).
pub fn check_float8_array(transarray: &[u8], caller: &str, n: usize) -> PgResult<Vec<f64>> {
    // ARR_NDIM(transarray) != 1 || ARR_DIMS(transarray)[0] != n ||
    // ARR_HASNULL(transarray) || ARR_ELEMTYPE(transarray) != FLOAT8OID
    if arr_ndim(transarray) != 1
        || arr_dim(transarray, 0) != n as i32
        || arr_hasnull(transarray)
        || arr_elemtype(transarray) != FLOAT8OID
    {
        return Err(PgError::error(format!(
            "{caller}: expected {n}-element float8 array"
        )));
    }

    let mut out: Vec<f64> = Vec::with_capacity(n);
    let mut p = foundation::arr_data_ptr_off(transarray);
    for _ in 0..n {
        let d: Datum = fetch_att(transarray, p, FLOAT8_ELMBYVAL, FLOAT8_ELMLEN);
        out.push(d.as_f64());
        p = foundation::att_addlength_pointer(p, FLOAT8_ELMLEN, transarray, p);
        p = foundation::att_align_nominal(p, FLOAT8_ELMALIGN);
    }
    Ok(out)
}

/// Build the (always fresh) transition `float8[]` array image from `values`,
/// charged to `mcx`. Mirrors the shared C tail
/// `construct_array_builtin(transdatums, n, FLOAT8OID)`.
fn return_transarray<'mcx>(mcx: Mcx<'mcx>, values: &[f64]) -> PgResult<PgVec<'mcx, u8>> {
    let elems: Vec<Datum> = values.iter().map(|&v| Datum::from_f64(v)).collect();
    construct_array(
        mcx,
        &elems,
        FLOAT8OID,
        FLOAT8_ELMLEN,
        FLOAT8_ELMBYVAL,
        FLOAT8_ELMALIGN,
    )
}

// ===========================================================================
// float8_combine (float.c:2951): combine two 3-element transition states.
// ===========================================================================

/// `float8_combine(PG_FUNCTION_ARGS)` (float.c:2951).
pub fn float8_combine<'mcx>(
    mcx: Mcx<'mcx>,
    transarray1: &[u8],
    transarray2: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let transvalues1 = check_float8_array(transarray1, "float8_combine", 3)?;
    let transvalues2 = check_float8_array(transarray2, "float8_combine", 3)?;

    let (n1, sx1, sxx1) = (transvalues1[0], transvalues1[1], transvalues1[2]);
    let (n2, sx2, sxx2) = (transvalues2[0], transvalues2[1], transvalues2[2]);

    let (n, sx, sxx);
    if n1 == 0.0 {
        n = n2;
        sx = sx2;
        sxx = sxx2;
    } else if n2 == 0.0 {
        n = n1;
        sx = sx1;
        sxx = sxx1;
    } else {
        n = n1 + n2;
        sx = float8_pl(sx1, sx2)?;
        let tmp = sx1 / n1 - sx2 / n2;
        sxx = sxx1 + sxx2 + n1 * n2 * tmp * tmp / n;
        if sxx.is_infinite() && !sxx1.is_infinite() && !sxx2.is_infinite() {
            return Err(float_overflow_error());
        }
    }

    return_transarray(mcx, &[n, sx, sxx])
}

// ===========================================================================
// float8_accum / float4_accum (float.c:3043, 3124).
// ===========================================================================

/// Shared Youngs-Cramer single-value accumulation kernel for `float8_accum`
/// (float.c:3043) and `float4_accum` (float.c:3124).
fn accum_kernel<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    caller: &str,
    newval: f64,
) -> PgResult<PgVec<'mcx, u8>> {
    let transvalues = check_float8_array(transarray, caller, 3)?;
    let n0 = transvalues[0];
    let sx0 = transvalues[1];

    let mut n = transvalues[0];
    let mut sx = transvalues[1];
    let mut sxx = transvalues[2];

    n += 1.0;
    sx += newval;
    if n0 > 0.0 {
        let tmp = newval * n - sx;
        sxx += tmp * tmp / (n * n0);

        if sx.is_infinite() || sxx.is_infinite() {
            if !sx0.is_infinite() && !newval.is_infinite() {
                return Err(float_overflow_error());
            }
            sxx = get_float8_nan();
        }
    } else if newval.is_nan() || newval.is_infinite() {
        sxx = get_float8_nan();
    }

    return_transarray(mcx, &[n, sx, sxx])
}

/// `float8_accum(PG_FUNCTION_ARGS)` (float.c:3043).
pub fn float8_accum<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    newval: f64,
) -> PgResult<PgVec<'mcx, u8>> {
    accum_kernel(mcx, transarray, "float8_accum", newval)
}

/// `float4_accum(PG_FUNCTION_ARGS)` (float.c:3124): identical to `float8_accum`
/// but the input value is `float4`, widened to `float8`.
pub fn float4_accum<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    newval: f32,
) -> PgResult<PgVec<'mcx, u8>> {
    accum_kernel(mcx, transarray, "float4_accum", newval as f64)
}

// ===========================================================================
// Simple final functions (float.c:3207-3312).
// ===========================================================================

/// `float8_avg(PG_FUNCTION_ARGS)` (float.c:3207).
pub fn float8_avg(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_avg", 3)?;
    let n = transvalues[0];
    let sx = transvalues[1];

    if n == 0.0 {
        return Ok(None);
    }
    Ok(Some(sx / n))
}

/// `float8_var_pop(PG_FUNCTION_ARGS)` (float.c:3227).
pub fn float8_var_pop(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_var_pop", 3)?;
    let n = transvalues[0];
    let sxx = transvalues[2];

    if n == 0.0 {
        return Ok(None);
    }
    Ok(Some(sxx / n))
}

/// `float8_var_samp(PG_FUNCTION_ARGS)` (float.c:3249).
pub fn float8_var_samp(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_var_samp", 3)?;
    let n = transvalues[0];
    let sxx = transvalues[2];

    if n <= 1.0 {
        return Ok(None);
    }
    Ok(Some(sxx / (n - 1.0)))
}

/// `float8_stddev_pop(PG_FUNCTION_ARGS)` (float.c:3271).
pub fn float8_stddev_pop(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_stddev_pop", 3)?;
    let n = transvalues[0];
    let sxx = transvalues[2];

    if n == 0.0 {
        return Ok(None);
    }
    Ok(Some((sxx / n).sqrt()))
}

/// `float8_stddev_samp(PG_FUNCTION_ARGS)` (float.c:3293).
pub fn float8_stddev_samp(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_stddev_samp", 3)?;
    let n = transvalues[0];
    let sxx = transvalues[2];

    if n <= 1.0 {
        return Ok(None);
    }
    Ok(Some((sxx / (n - 1.0)).sqrt()))
}

// ===========================================================================
// float8_regr_accum (float.c:3336): accumulate one (Y, X) pair into a
// 6-element transition state. NOTE: Y is the FIRST argument.
// ===========================================================================

/// `float8_regr_accum(PG_FUNCTION_ARGS)` (float.c:3336). `newval_y` is argument
/// 1 and `newval_x` is argument 2.
pub fn float8_regr_accum<'mcx>(
    mcx: Mcx<'mcx>,
    transarray: &[u8],
    newval_y: f64,
    newval_x: f64,
) -> PgResult<PgVec<'mcx, u8>> {
    let transvalues = check_float8_array(transarray, "float8_regr_accum", 6)?;
    let n0 = transvalues[0];
    let sx0 = transvalues[1];
    let sy0 = transvalues[3];

    let mut n = transvalues[0];
    let mut sx = transvalues[1];
    let mut sxx = transvalues[2];
    let mut sy = transvalues[3];
    let mut syy = transvalues[4];
    let mut sxy = transvalues[5];

    n += 1.0;
    sx += newval_x;
    sy += newval_y;
    if n0 > 0.0 {
        let tmp_x = newval_x * n - sx;
        let tmp_y = newval_y * n - sy;
        let scale = 1.0 / (n * n0);
        sxx += tmp_x * tmp_x * scale;
        syy += tmp_y * tmp_y * scale;
        sxy += tmp_x * tmp_y * scale;

        if sx.is_infinite()
            || sxx.is_infinite()
            || sy.is_infinite()
            || syy.is_infinite()
            || sxy.is_infinite()
        {
            if ((sx.is_infinite() || sxx.is_infinite())
                && !sx0.is_infinite()
                && !newval_x.is_infinite())
                || ((sy.is_infinite() || syy.is_infinite())
                    && !sy0.is_infinite()
                    && !newval_y.is_infinite())
                || (sxy.is_infinite()
                    && !sx0.is_infinite()
                    && !newval_x.is_infinite()
                    && !sy0.is_infinite()
                    && !newval_y.is_infinite())
            {
                return Err(float_overflow_error());
            }

            if sxx.is_infinite() {
                sxx = get_float8_nan();
            }
            if syy.is_infinite() {
                syy = get_float8_nan();
            }
            if sxy.is_infinite() {
                sxy = get_float8_nan();
            }
        }
    } else {
        if newval_x.is_nan() || newval_x.is_infinite() {
            sxx = get_float8_nan();
            sxy = get_float8_nan();
        }
        if newval_y.is_nan() || newval_y.is_infinite() {
            syy = get_float8_nan();
            sxy = get_float8_nan();
        }
    }

    return_transarray(mcx, &[n, sx, sxx, sy, syy, sxy])
}

// ===========================================================================
// float8_regr_combine (float.c:3458): combine two 6-element transition states.
// ===========================================================================

/// `float8_regr_combine(PG_FUNCTION_ARGS)` (float.c:3458).
pub fn float8_regr_combine<'mcx>(
    mcx: Mcx<'mcx>,
    transarray1: &[u8],
    transarray2: &[u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let transvalues1 = check_float8_array(transarray1, "float8_regr_combine", 6)?;
    let transvalues2 = check_float8_array(transarray2, "float8_regr_combine", 6)?;

    let (n1, sx1, sxx1, sy1, syy1, sxy1) = (
        transvalues1[0],
        transvalues1[1],
        transvalues1[2],
        transvalues1[3],
        transvalues1[4],
        transvalues1[5],
    );
    let (n2, sx2, sxx2, sy2, syy2, sxy2) = (
        transvalues2[0],
        transvalues2[1],
        transvalues2[2],
        transvalues2[3],
        transvalues2[4],
        transvalues2[5],
    );

    let (n, sx, sxx, sy, syy, sxy);
    if n1 == 0.0 {
        n = n2;
        sx = sx2;
        sxx = sxx2;
        sy = sy2;
        syy = syy2;
        sxy = sxy2;
    } else if n2 == 0.0 {
        n = n1;
        sx = sx1;
        sxx = sxx1;
        sy = sy1;
        syy = syy1;
        sxy = sxy1;
    } else {
        n = n1 + n2;
        sx = float8_pl(sx1, sx2)?;
        let tmp1 = sx1 / n1 - sx2 / n2;
        sxx = sxx1 + sxx2 + n1 * n2 * tmp1 * tmp1 / n;
        if sxx.is_infinite() && !sxx1.is_infinite() && !sxx2.is_infinite() {
            return Err(float_overflow_error());
        }
        sy = float8_pl(sy1, sy2)?;
        let tmp2 = sy1 / n1 - sy2 / n2;
        syy = syy1 + syy2 + n1 * n2 * tmp2 * tmp2 / n;
        if syy.is_infinite() && !syy1.is_infinite() && !syy2.is_infinite() {
            return Err(float_overflow_error());
        }
        sxy = sxy1 + sxy2 + n1 * n2 * tmp1 * tmp2 / n;
        if sxy.is_infinite() && !sxy1.is_infinite() && !sxy2.is_infinite() {
            return Err(float_overflow_error());
        }
    }

    return_transarray(mcx, &[n, sx, sxx, sy, syy, sxy])
}

// ===========================================================================
// Regression / covariance / correlation final functions (float.c:3590-3846).
// ===========================================================================

/// `float8_regr_sxx(PG_FUNCTION_ARGS)` (float.c:3590).
pub fn float8_regr_sxx(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_sxx", 6)?;
    let n = transvalues[0];
    let sxx = transvalues[2];

    if n < 1.0 {
        return Ok(None);
    }
    Ok(Some(sxx))
}

/// `float8_regr_syy(PG_FUNCTION_ARGS)` (float.c:3611).
pub fn float8_regr_syy(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_syy", 6)?;
    let n = transvalues[0];
    let syy = transvalues[4];

    if n < 1.0 {
        return Ok(None);
    }
    Ok(Some(syy))
}

/// `float8_regr_sxy(PG_FUNCTION_ARGS)` (float.c:3632).
pub fn float8_regr_sxy(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_sxy", 6)?;
    let n = transvalues[0];
    let sxy = transvalues[5];

    if n < 1.0 {
        return Ok(None);
    }
    // A negative result is valid here.
    Ok(Some(sxy))
}

/// `float8_regr_avgx(PG_FUNCTION_ARGS)` (float.c:3653).
pub fn float8_regr_avgx(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_avgx", 6)?;
    let n = transvalues[0];
    let sx = transvalues[1];

    if n < 1.0 {
        return Ok(None);
    }
    Ok(Some(sx / n))
}

/// `float8_regr_avgy(PG_FUNCTION_ARGS)` (float.c:3672).
pub fn float8_regr_avgy(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_avgy", 6)?;
    let n = transvalues[0];
    let sy = transvalues[3];

    if n < 1.0 {
        return Ok(None);
    }
    Ok(Some(sy / n))
}

/// `float8_covar_pop(PG_FUNCTION_ARGS)` (float.c:3691).
pub fn float8_covar_pop(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_covar_pop", 6)?;
    let n = transvalues[0];
    let sxy = transvalues[5];

    if n < 1.0 {
        return Ok(None);
    }
    Ok(Some(sxy / n))
}

/// `float8_covar_samp(PG_FUNCTION_ARGS)` (float.c:3710).
pub fn float8_covar_samp(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_covar_samp", 6)?;
    let n = transvalues[0];
    let sxy = transvalues[5];

    if n < 2.0 {
        return Ok(None);
    }
    Ok(Some(sxy / (n - 1.0)))
}

/// `float8_corr(PG_FUNCTION_ARGS)` (float.c:3729).
pub fn float8_corr(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_corr", 6)?;
    let n = transvalues[0];
    let sxx = transvalues[2];
    let syy = transvalues[4];
    let sxy = transvalues[5];

    if n < 1.0 {
        return Ok(None);
    }
    // Per spec, return NULL for horizontal and vertical lines.
    if sxx == 0.0 || syy == 0.0 {
        return Ok(None);
    }
    Ok(Some(sxy / (sxx * syy).sqrt()))
}

/// `float8_regr_r2(PG_FUNCTION_ARGS)` (float.c:3758).
pub fn float8_regr_r2(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_r2", 6)?;
    let n = transvalues[0];
    let sxx = transvalues[2];
    let syy = transvalues[4];
    let sxy = transvalues[5];

    if n < 1.0 {
        return Ok(None);
    }
    // Per spec, return NULL for a vertical line.
    if sxx == 0.0 {
        return Ok(None);
    }
    // Per spec, return 1.0 for a horizontal line.
    if syy == 0.0 {
        return Ok(Some(1.0));
    }
    Ok(Some((sxy * sxy) / (sxx * syy)))
}

/// `float8_regr_slope(PG_FUNCTION_ARGS)` (float.c:3791).
pub fn float8_regr_slope(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_slope", 6)?;
    let n = transvalues[0];
    let sxx = transvalues[2];
    let sxy = transvalues[5];

    if n < 1.0 {
        return Ok(None);
    }
    // Per spec, return NULL for a vertical line.
    if sxx == 0.0 {
        return Ok(None);
    }
    Ok(Some(sxy / sxx))
}

/// `float8_regr_intercept(PG_FUNCTION_ARGS)` (float.c:3818).
pub fn float8_regr_intercept(transarray: &[u8]) -> PgResult<Option<f64>> {
    let transvalues = check_float8_array(transarray, "float8_regr_intercept", 6)?;
    let n = transvalues[0];
    let sx = transvalues[1];
    let sxx = transvalues[2];
    let sy = transvalues[3];
    let sxy = transvalues[5];

    if n < 1.0 {
        return Ok(None);
    }
    // Per spec, return NULL for a vertical line.
    if sxx == 0.0 {
        return Ok(None);
    }
    Ok(Some((sy - sx * sxy / sxx) / n))
}
