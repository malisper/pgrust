//! Youngs-Cramer aggregate transition/final cores. The C transition value is
//! a float8[] varlena; kernels here are pure over [f64; N] and the image
//! helpers read/build the exact construct_array_builtin(FLOAT8OID) layout, so
//! the executor's agg frame owns all allocation (in-place C leg included).

use ::types_core::FLOAT8OID;
use ::types_error::{PgError, PgResult};

use crate::{float8_pl, float_overflow_error, get_float8_nan};

// 1-D no-nulls array header: vl_len_, ndim, dataoffset, elemtype, dims[1],
// lbound[1] — 24 bytes, already MAXALIGNed for the float8 data that follows.
pub const FLOAT8_ARRAY_HDRSZ: usize = 24;

pub const fn float8_transarray_size(n: usize) -> usize {
    FLOAT8_ARRAY_HDRSZ + 8 * n
}

pub fn check_float8_array<const N: usize>(image: &[u8], caller: &str) -> PgResult<[f64; N]> {
    let word = |off: usize| -> i32 {
        i32::from_ne_bytes(image[off..off + 4].try_into().expect("bounded read"))
    };
    if image.len() < FLOAT8_ARRAY_HDRSZ
        || word(4) != 1
        || word(16) != N as i32
        || word(8) != 0
        || word(12) != FLOAT8OID as i32
        || image.len() < float8_transarray_size(N)
    {
        return Err(bad_transarray(caller, N));
    }
    let mut out = [0.0f64; N];
    for (i, v) in out.iter_mut().enumerate() {
        let off = FLOAT8_ARRAY_HDRSZ + 8 * i;
        *v = f64::from_ne_bytes(image[off..off + 8].try_into().expect("bounded read"));
    }
    Ok(out)
}

#[track_caller]
#[cold]
#[inline(never)]
fn bad_transarray(caller: &str, n: usize) -> Box<PgError> {
    // C: elog(ERROR, "%s: expected %d-element float8 array", caller, n).
    Box::new(PgError::error(format!(
        "{caller}: expected {n}-element float8 array"
    )))
}

pub fn write_float8_transarray(values: &[f64], out: &mut [u8]) -> usize {
    let size = float8_transarray_size(values.len());
    debug_assert!(out.len() >= size);
    let w = |out: &mut [u8], off: usize, v: i32| {
        out[off..off + 4].copy_from_slice(&v.to_ne_bytes());
    };
    w(out, 0, (size as i32) << 2); // SET_VARSIZE, 4-byte header
    w(out, 4, 1);
    w(out, 8, 0);
    w(out, 12, FLOAT8OID as i32);
    w(out, 16, values.len() as i32);
    w(out, 20, 1);
    for (i, v) in values.iter().enumerate() {
        let off = FLOAT8_ARRAY_HDRSZ + 8 * i;
        out[off..off + 8].copy_from_slice(&v.to_ne_bytes());
    }
    size
}

pub fn float8_combine(t1: [f64; 3], t2: [f64; 3]) -> PgResult<[f64; 3]> {
    let [n1, sx1, sxx1] = t1;
    let [n2, sx2, sxx2] = t2;

    if n1 == 0.0 {
        return Ok(t2);
    }
    if n2 == 0.0 {
        return Ok(t1);
    }
    let n = n1 + n2;
    let sx = float8_pl(sx1, sx2)?;
    let tmp = sx1 / n1 - sx2 / n2;
    let sxx = sxx1 + sxx2 + n1 * n2 * tmp * tmp / n;
    if sxx.is_infinite() && !sxx1.is_infinite() && !sxx2.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok([n, sx, sxx])
}

fn accum_kernel(trans: [f64; 3], newval: f64) -> PgResult<[f64; 3]> {
    let [n0, sx0, sxx0] = trans;

    let n = n0 + 1.0;
    let sx = sx0 + newval;
    let mut sxx = sxx0;
    if n0 > 0.0 {
        // fp-contract parity with the compiled C (see float8_regr_accum).
        let tmp = f64::mul_add(newval, n, -sx);
        sxx += tmp * tmp / (n * n0);

        if sx.is_infinite() || sxx.is_infinite() {
            if !sx0.is_infinite() && !newval.is_infinite() {
                return Err(float_overflow_error().into());
            }
            sxx = get_float8_nan();
        }
    } else if newval.is_nan() || newval.is_infinite() {
        sxx = get_float8_nan();
    }

    Ok([n, sx, sxx])
}

pub fn float8_accum(trans: [f64; 3], newval: f64) -> PgResult<[f64; 3]> {
    accum_kernel(trans, newval)
}

pub fn float4_accum(trans: [f64; 3], newval: f32) -> PgResult<[f64; 3]> {
    accum_kernel(trans, newval as f64)
}

pub fn float8_avg(trans: [f64; 3]) -> Option<f64> {
    let [n, sx, _] = trans;
    if n == 0.0 {
        return None;
    }
    Some(sx / n)
}

pub fn float8_var_pop(trans: [f64; 3]) -> Option<f64> {
    let [n, _, sxx] = trans;
    if n == 0.0 {
        return None;
    }
    Some(sxx / n)
}

pub fn float8_var_samp(trans: [f64; 3]) -> Option<f64> {
    let [n, _, sxx] = trans;
    if n <= 1.0 {
        return None;
    }
    Some(sxx / (n - 1.0))
}

pub fn float8_stddev_pop(trans: [f64; 3]) -> Option<f64> {
    let [n, _, sxx] = trans;
    if n == 0.0 {
        return None;
    }
    Some((sxx / n).sqrt())
}

pub fn float8_stddev_samp(trans: [f64; 3]) -> Option<f64> {
    let [n, _, sxx] = trans;
    if n <= 1.0 {
        return None;
    }
    Some((sxx / (n - 1.0)).sqrt())
}

// float8_regr_accum: newval_y is argument 1 and newval_x is argument 2.
pub fn float8_regr_accum(trans: [f64; 6], newval_y: f64, newval_x: f64) -> PgResult<[f64; 6]> {
    let [n0, sx0, sxx0, sy0, syy0, sxy0] = trans;

    let n = n0 + 1.0;
    let sx = sx0 + newval_x;
    let sy = sy0 + newval_y;
    let mut sxx = sxx0;
    let mut syy = syy0;
    let mut sxy = sxy0;

    if n0 > 0.0 {
        // mul_add mirrors clang/gcc default fp-contract on `S += t*t*scale`
        // (compiled C emits FMA; without it the last bit diverges from PG).
        let tmp_x = f64::mul_add(newval_x, n, -sx);
        let tmp_y = f64::mul_add(newval_y, n, -sy);
        let scale = 1.0 / (n * n0);
        sxx = f64::mul_add(tmp_x * tmp_x, scale, sxx);
        syy = f64::mul_add(tmp_y * tmp_y, scale, syy);
        sxy = f64::mul_add(tmp_x * tmp_y, scale, sxy);

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
                return Err(float_overflow_error().into());
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

    Ok([n, sx, sxx, sy, syy, sxy])
}

pub fn float8_regr_combine(t1: [f64; 6], t2: [f64; 6]) -> PgResult<[f64; 6]> {
    let [n1, sx1, sxx1, sy1, syy1, sxy1] = t1;
    let [n2, sx2, sxx2, sy2, syy2, sxy2] = t2;

    if n1 == 0.0 {
        return Ok(t2);
    }
    if n2 == 0.0 {
        return Ok(t1);
    }
    let n = n1 + n2;
    let sx = float8_pl(sx1, sx2)?;
    let tmp1 = sx1 / n1 - sx2 / n2;
    let sxx = sxx1 + sxx2 + n1 * n2 * tmp1 * tmp1 / n;
    if sxx.is_infinite() && !sxx1.is_infinite() && !sxx2.is_infinite() {
        return Err(float_overflow_error().into());
    }
    let sy = float8_pl(sy1, sy2)?;
    let tmp2 = sy1 / n1 - sy2 / n2;
    let syy = syy1 + syy2 + n1 * n2 * tmp2 * tmp2 / n;
    if syy.is_infinite() && !syy1.is_infinite() && !syy2.is_infinite() {
        return Err(float_overflow_error().into());
    }
    let sxy = sxy1 + sxy2 + n1 * n2 * tmp1 * tmp2 / n;
    if sxy.is_infinite() && !sxy1.is_infinite() && !sxy2.is_infinite() {
        return Err(float_overflow_error().into());
    }
    Ok([n, sx, sxx, sy, syy, sxy])
}

pub fn float8_regr_sxx(trans: [f64; 6]) -> Option<f64> {
    let [n, _, sxx, ..] = trans;
    if n < 1.0 {
        return None;
    }
    Some(sxx)
}

pub fn float8_regr_syy(trans: [f64; 6]) -> Option<f64> {
    let [n, _, _, _, syy, _] = trans;
    if n < 1.0 {
        return None;
    }
    Some(syy)
}

pub fn float8_regr_sxy(trans: [f64; 6]) -> Option<f64> {
    let [n, _, _, _, _, sxy] = trans;
    if n < 1.0 {
        return None;
    }
    Some(sxy)
}

pub fn float8_regr_avgx(trans: [f64; 6]) -> Option<f64> {
    let [n, sx, ..] = trans;
    if n < 1.0 {
        return None;
    }
    Some(sx / n)
}

pub fn float8_regr_avgy(trans: [f64; 6]) -> Option<f64> {
    let [n, _, _, sy, _, _] = trans;
    if n < 1.0 {
        return None;
    }
    Some(sy / n)
}

pub fn float8_covar_pop(trans: [f64; 6]) -> Option<f64> {
    let [n, _, _, _, _, sxy] = trans;
    if n < 1.0 {
        return None;
    }
    Some(sxy / n)
}

pub fn float8_covar_samp(trans: [f64; 6]) -> Option<f64> {
    let [n, _, _, _, _, sxy] = trans;
    if n < 2.0 {
        return None;
    }
    Some(sxy / (n - 1.0))
}

pub fn float8_corr(trans: [f64; 6]) -> Option<f64> {
    let [n, _, sxx, _, syy, sxy] = trans;
    if n < 1.0 || sxx == 0.0 || syy == 0.0 {
        return None;
    }
    Some(sxy / (sxx * syy).sqrt())
}

pub fn float8_regr_r2(trans: [f64; 6]) -> Option<f64> {
    let [n, _, sxx, _, syy, sxy] = trans;
    if n < 1.0 || sxx == 0.0 {
        return None;
    }
    if syy == 0.0 {
        return Some(1.0);
    }
    Some((sxy * sxy) / (sxx * syy))
}

pub fn float8_regr_slope(trans: [f64; 6]) -> Option<f64> {
    let [n, _, sxx, _, _, sxy] = trans;
    if n < 1.0 || sxx == 0.0 {
        return None;
    }
    Some(sxy / sxx)
}

pub fn float8_regr_intercept(trans: [f64; 6]) -> Option<f64> {
    let [n, sx, sxx, sy, _, sxy] = trans;
    if n < 1.0 || sxx == 0.0 {
        return None;
    }
    Some((sy - sx * sxy / sxx) / n)
}
