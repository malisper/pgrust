//! fmgr wrappers (`fc_*`) + registry for numeric.c's fmgr-callable rows.
//! Numeric results follow the result-mcx convention with the pool-backed
//! NumericImage as call scratch (notes/fc-result-convention.md); numeric_out
//! keeps the retained-cstring-scratch precedent. recv/send ride the
//! binary-wire fmgr frame (types_fmgr::wire). Still deferred: hash (see
//! ops.rs); sortsupport lives in sortsupport.rs.

use ::datum::Datum;
use ::mcx::Allocator;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::var::NumericImage;
use crate::Num;

// int8 is pass-by-value in this build (USE_FLOAT8_BYVAL), so C's
// modify-in-place AggCheckCallContext branch is compiled out.
#[inline]
pub fn int4_sum(oldsum: Option<i64>, newval: Option<i32>) -> Option<i64> {
    match (oldsum, newval) {
        (None, None) => None,
        (None, Some(v)) => Some(v as i64),
        (Some(s), None) => Some(s),
        // C (numeric.c int4_sum/int2_sum): `newval = oldsum + (int64) ...`
        // with no overflow check; PostgreSQL builds with -fwrapv, so the
        // int64 accumulation WRAPS (C 18.3: int4_sum(INT64_MAX, 1) →
        // -9223372036854775808).
        (Some(s), Some(v)) => Some(s.wrapping_add(v as i64)),
    }
}

#[inline]
pub fn int2_sum(oldsum: Option<i64>, newval: Option<i16>) -> Option<i64> {
    int4_sum(oldsum, newval.map(i32::from))
}

macro_rules! fc_sum {
    ($($fc:ident: $core:ident($get:ident);)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            let oldsum = (!a.isnull).then(|| a.value.as_i64());
            let newval = (!b.isnull).then(|| b.value.$get());
            match $core(oldsum, newval) {
                Some(v) => Ok(Datum::from_i64(v)),
                None => {
                    fcinfo.isnull = true;
                    Ok(Datum::null())
                }
            }
        }
    )*};
}

fc_sum! {
    fc_int4_sum: int4_sum(as_i32);
    fc_int2_sum: int2_sum(as_i16);
}

// int8_sum (numeric.c): numeric transition state — no in-place branch, the
// state is variable size.
pub fn fc_int8_sum(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = *fcinfo.args_n::<2>();
    if a.isnull {
        if b.isnull {
            return Ok(fcinfo.return_null());
        }
        return img_result(fcinfo, &crate::ops::int64_to_numeric(b.value.as_i64()));
    }
    if b.isnull {
        return Ok(a.value);
    }
    // SAFETY: null-checked numeric arg of a non-strict fn.
    let oldsum = unsafe { num_arg(fcinfo, 0)? };
    let add = crate::ops::int64_to_numeric(b.value.as_i64());
    let img = crate::ops::numeric_add_common(oldsum, add.num())?;
    img_result(fcinfo, &img)
}

/// C's PG_GETARG_NUMERIC: a 1B-short image (tuple-packed numerics land here
/// with misaligned digits) expands into the frame's result mcx, mirroring
/// DatumGetNumeric's detoast into CurrentMemoryContext.
/// # Safety
/// Arg `i` is a non-null numeric varlena (strict fn).
#[inline]
pub unsafe fn num_arg(fcinfo: &Fcinfo, i: usize) -> PgResult<Num<'_>> {
    // SAFETY: forwarded caller contract.
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    let payload = if v.is_short() {
        v.data_expanded(fcinfo.result_mcx())?
    } else {
        v.data()
    };
    Ok(Num::from_payload(payload))
}

fn img_result(fcinfo: &Fcinfo, img: &NumericImage) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), img.as_bytes())
}

pub fn fc_numeric_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of numeric_in is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let had_esc = esc.is_some();
    match crate::io::numeric_in(&s, typmod, esc)? {
        Some(img) => img_result(fcinfo, &img),
        // Soft error already saved; the value is C's garbage datum.
        None if had_esc => Ok(Datum::null()),
        None => panic!("numeric_in: soft-error escape without an escontext"),
    }
}

// C pallocs the cstring per row; the resolved FmgrInfo owns retained scratch
// (rule 7) and the result datum aliases it until the next call.
struct OutBuf(Vec<u8>);

pub fn fc_numeric_out(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let Some(flinfo) = flinfo else {
        panic!("numeric_out: cstring result needs a resolved FmgrInfo's scratch")
    };
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    let buf = &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0;
    buf.clear();
    crate::io::numeric_out_into(num, buf);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_numeric_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(2);
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let img = crate::numeric_recv(buf, typmod)?;
    img_result(fcinfo, &img)
}

pub fn fc_numeric_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::numeric_send(mcx, num)?))
}

pub fn fc_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    let img = crate::numeric_apply_typmod(num, fcinfo.arg_i32(1))?;
    img_result(fcinfo, &img)
}

macro_rules! fc_num_binop {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null numeric varlenas (strict fn).
            let (a, b) = unsafe { (num_arg(fcinfo, 0)?, num_arg(fcinfo, 1)?) };
            let img = crate::$core(a, b)?;
            img_result(fcinfo, &img)
        }
    )*};
}

fc_num_binop! {
    fc_numeric_add: numeric_add_common;
    fc_numeric_sub: numeric_sub_common;
    fc_numeric_mul: numeric_mul_common;
    fc_numeric_div: numeric_div_common;
    fc_numeric_div_trunc: numeric_div_trunc_common;
    fc_numeric_mod: numeric_mod_common;
    fc_numeric_gcd: numeric_gcd_common;
    fc_numeric_lcm: numeric_lcm_common;
    fc_numeric_log: numeric_log;
    fc_numeric_power: numeric_power;
}

macro_rules! fc_num_cmp {
    ($($fc:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null numeric varlenas (strict fn).
            let (a, b) = unsafe { (num_arg(fcinfo, 0)?, num_arg(fcinfo, 1)?) };
            Ok(Datum::$conv(crate::$core(a, b)))
        }
    )*};
}

fc_num_cmp! {
    fc_numeric_eq: numeric_eq -> from_bool;
    fc_numeric_ne: numeric_ne -> from_bool;
    fc_numeric_lt: numeric_lt -> from_bool;
    fc_numeric_le: numeric_le -> from_bool;
    fc_numeric_gt: numeric_gt -> from_bool;
    fc_numeric_ge: numeric_ge -> from_bool;
    fc_numeric_cmp: cmp_numerics -> from_i32;
}

macro_rules! fc_num_unary {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
            let num = unsafe { num_arg(fcinfo, 0) }?;
            let img = crate::$core(num);
            img_result(fcinfo, &img)
        }
    )*};
}

fc_num_unary! {
    fc_numeric_abs: numeric_abs;
    fc_numeric_uminus: numeric_uminus;
    fc_numeric_uplus: numeric_uplus;
}

macro_rules! fc_num_unary_res {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
            let num = unsafe { num_arg(fcinfo, 0) }?;
            let img = crate::$core(num)?;
            img_result(fcinfo, &img)
        }
    )*};
}

fc_num_unary_res! {
    fc_numeric_sqrt: numeric_sqrt;
    fc_numeric_exp: numeric_exp;
    fc_numeric_ln: numeric_ln;
}

pub fn fc_numeric_fac(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = crate::numeric_fac(fcinfo.arg_i64(0))?;
    img_result(fcinfo, &img)
}

pub fn fc_width_bucket_numeric(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args 0-2 are non-null numeric varlenas (strict fn).
    let (operand, b1, b2) =
        unsafe { (num_arg(fcinfo, 0)?, num_arg(fcinfo, 1)?, num_arg(fcinfo, 2)?) };
    let count = fcinfo.arg_i32(3);
    Ok(Datum::from_i32(crate::width_bucket_numeric(
        operand, b1, b2, count,
    )?))
}

// C returns one of the input pointers — so do smaller/larger. The tie must
// keep num2 as C (numeric.c numeric_smaller/larger) does: equal numerics can
// have different images (1.0 vs 1.00 dscale), so the winner's identity is
// value-visible through numeric_out (proofs divergence #9).
pub fn fc_numeric_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null numeric varlenas (strict fn).
    let (a, b) = unsafe { (num_arg(fcinfo, 0)?, num_arg(fcinfo, 1)?) };
    Ok(if crate::cmp_numerics(a, b) > 0 {
        fcinfo.arg(0)
    } else {
        fcinfo.arg(1)
    })
}

pub fn fc_numeric_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null numeric varlenas (strict fn).
    let (a, b) = unsafe { (num_arg(fcinfo, 0)?, num_arg(fcinfo, 1)?) };
    Ok(if crate::cmp_numerics(a, b) < 0 {
        fcinfo.arg(0)
    } else {
        fcinfo.arg(1)
    })
}

pub fn fc_int2_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    img_result(fcinfo, &crate::int2_numeric(fcinfo.arg_i16(0)))
}

pub fn fc_int4_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    img_result(fcinfo, &crate::int4_numeric(fcinfo.arg_i32(0)))
}

pub fn fc_int8_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    img_result(fcinfo, &crate::int8_numeric(fcinfo.arg_i64(0)))
}

pub fn fc_float4_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = crate::float4_numeric(fcinfo.arg_f32(0))?;
    img_result(fcinfo, &img)
}

pub fn fc_float8_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = crate::float8_numeric(fcinfo.arg_f64(0))?;
    img_result(fcinfo, &img)
}

pub fn fc_numeric_int4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    Ok(Datum::from_i32(crate::numeric_int4(num)?))
}

pub fn fc_numeric_int8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    Ok(Datum::from_i64(crate::numeric_int8(num)?))
}

pub fn fc_numeric_float4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    Ok(Datum::from_f32(crate::numeric_float4(num)?))
}

pub fn fc_numeric_float8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    Ok(Datum::from_f64(crate::numeric_float8(num)?))
}

// C PolyNumAggState under HAVE_INT128 = Int128AggState; INTERNAL agg states
// are allocated by the transfn in the aggcontext bump arena (fcinfo->context/
// AggCheckCallContext). The arena is wholesale-reset, so states stay
// drop-free (const-asserted per-type in agg_state_arg).
#[cold]
#[inline(never)]
fn non_aggregate_context() -> Box<::types_error::PgError> {
    Box::new(::types_error::PgError::error(
        "aggregate function called in non-aggregate context",
    ))
}

// makeNumericAggState/makePolyNumAggState (numeric.c): allocate the INTERNAL
// state in the agg context; also hands the context back for the digit
// buffers (C's state->agg_context).
fn agg_state_arg<'a, T>(
    fcinfo: &Fcinfo,
    arg0: ::datum::NullableDatum,
    init: impl FnOnce() -> T,
) -> PgResult<(*mut T, ::mcx::Mcx<'a>)> {
    const { assert!(!core::mem::needs_drop::<T>()) }
    // SAFETY: context, if set, is the evaltrans build's AggStateNode, live
    // across every call through this frame.
    let Some(agg_mcx) = (unsafe { fcinfo.agg_context() }) else {
        return Err(non_aggregate_context());
    };
    if !arg0.isnull {
        return Ok((arg0.value.as_usize() as *mut T, agg_mcx));
    }
    let layout = core::alloc::Layout::new::<T>();
    let raw = ::mcx::Allocator::allocate(&agg_mcx, layout)
        .map_err(|_| agg_mcx.oom(layout.size()))?;
    let p = raw.cast::<T>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(init()) };
    Ok((p, agg_mcx))
}

pub fn fc_int8_avg_accum(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use crate::aggregates::Int128AggState;
    let [a, b] = *fcinfo.args_n::<2>();
    let (state, _) = agg_state_arg(fcinfo, a, || Int128AggState::new(false))?;
    if !b.isnull {
        // SAFETY: a non-null arg0 is the aggcontext-lived state this transfn
        // chain returned; no other reference is live during the call.
        unsafe { crate::aggregates::do_int128_accum(&mut *state, b.value.as_i64() as i128) };
    }
    Ok(Datum::from_usize(state as usize))
}

// int2_accum/int4_accum (numeric.c), HAVE_INT128 arm: PolyNumAggState with
// sumX2.
macro_rules! fc_poly_accum {
    ($($fc:ident: $get:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            use crate::aggregates::Int128AggState;
            let [a, b] = *fcinfo.args_n::<2>();
            let (state, _) = agg_state_arg(fcinfo, a, || Int128AggState::new(true))?;
            if !b.isnull {
                // SAFETY: as fc_int8_avg_accum.
                unsafe {
                    crate::aggregates::do_int128_accum(&mut *state, b.value.$get() as i128)
                };
            }
            Ok(Datum::from_usize(state as usize))
        }
    )*};
}

fc_poly_accum! {
    fc_int2_accum: as_i16;
    fc_int4_accum: as_i32;
}

#[cold]
#[inline(never)]
fn inv_null_state(name: &str) -> Box<::types_error::PgError> {
    Box::new(::types_error::PgError::error(format!("{name} called with NULL state")))
}

// int2/int4_accum_inv + int8_avg_accum_inv (numeric.c), HAVE_INT128 arm.
macro_rules! fc_int128_inv {
    ($($fc:ident: $get:ident, $name:literal;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            use crate::aggregates::Int128AggState;
            let [a, b] = *fcinfo.args_n::<2>();
            if a.isnull {
                return Err(inv_null_state($name));
            }
            let state = a.value.as_usize() as *mut Int128AggState;
            if !b.isnull {
                // SAFETY: a non-null arg0 is the aggcontext-lived state the
                // forward transfn returned; no other reference is live.
                unsafe { crate::aggregates::do_int128_discard(&mut *state, b.value.$get() as i128) };
            }
            Ok(Datum::from_usize(state as usize))
        }
    )*};
}

fc_int128_inv! {
    fc_int2_accum_inv: as_i16, "int2_accum_inv";
    fc_int4_accum_inv: as_i32, "int4_accum_inv";
    fc_int8_avg_accum_inv: as_i64, "int8_avg_accum_inv";
}

// int8_accum_inv (numeric.c): the NumericAggState lane (int8's sumX2 can
// exceed int128); a false discard is a hard error — dscale is always zero.
pub fn fc_int8_accum_inv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use crate::aggregates::NumericAggState;
    let [a, b] = *fcinfo.args_n::<2>();
    if a.isnull {
        return Err(inv_null_state("int8_accum_inv"));
    }
    let state = a.value.as_usize() as *mut NumericAggState;
    if !b.isnull {
        let img = crate::ops::int64_to_numeric(b.value.as_i64());
        // SAFETY: a non-null arg0 is the aggcontext-lived state the forward
        // transfn returned; no other reference is live during the call.
        let ok = unsafe {
            let agg_mcx = fcinfo.agg_context().ok_or_else(non_aggregate_context)?;
            crate::aggregates::do_numeric_discard(&mut *state, agg_mcx, img.num())?
        };
        if !ok {
            panic!("do_numeric_discard failed unexpectedly");
        }
    }
    Ok(Datum::from_usize(state as usize))
}

// numeric_accum_inv (numeric.c): a false discard returns SQL NULL — the
// window machinery's restart signal.
pub fn fc_numeric_accum_inv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use crate::aggregates::NumericAggState;
    let [a, b] = *fcinfo.args_n::<2>();
    if a.isnull {
        return Err(inv_null_state("numeric_accum_inv"));
    }
    let state = a.value.as_usize() as *mut NumericAggState;
    if !b.isnull {
        // SAFETY: state as fc_int8_accum_inv; arg1 read guarded by isnull.
        let ok = unsafe {
            let num = num_arg(fcinfo, 1)?;
            let agg_mcx = fcinfo.agg_context().ok_or_else(non_aggregate_context)?;
            crate::aggregates::do_numeric_discard(&mut *state, agg_mcx, num)?
        };
        if !ok {
            fcinfo.return_null();
            return Ok(Datum::from_usize(0));
        }
    }
    Ok(Datum::from_usize(state as usize))
}

// numeric_accum/numeric_avg_accum (numeric.c).
fn numeric_accum_common(fcinfo: &mut Fcinfo, calc_sum_x2: bool) -> PgResult<Datum> {
    use crate::aggregates::NumericAggState;
    let [a, b] = *fcinfo.args_n::<2>();
    let (state, agg_mcx) = agg_state_arg(fcinfo, a, || NumericAggState::new(calc_sum_x2))?;
    if !b.isnull {
        // SAFETY: arg1 read guarded by the isnull check; state as
        // fc_int8_avg_accum.
        unsafe {
            let num = num_arg(fcinfo, 1)?;
            crate::aggregates::do_numeric_accum(&mut *state, agg_mcx, num)?;
        }
    }
    Ok(Datum::from_usize(state as usize))
}

pub fn fc_numeric_accum(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    numeric_accum_common(fcinfo, true)
}

pub fn fc_numeric_avg_accum(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    numeric_accum_common(fcinfo, false)
}

pub fn fc_int8_accum(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use crate::aggregates::NumericAggState;
    let [a, b] = *fcinfo.args_n::<2>();
    let (state, agg_mcx) = agg_state_arg(fcinfo, a, || NumericAggState::new(true))?;
    if !b.isnull {
        // SAFETY: as fc_int8_avg_accum.
        unsafe {
            crate::aggregates::do_numeric_accum_int64(&mut *state, agg_mcx, b.value.as_i64())?
        };
    }
    Ok(Datum::from_usize(state as usize))
}

// numeric_combine/numeric_avg_combine (numeric.c): NULL state2 passes state1
// through (possibly NULL); NULL state1 copies state2 into the agg context.
fn numeric_combine_common(fcinfo: &mut Fcinfo, with_sum_x2: bool) -> PgResult<Datum> {
    use crate::aggregates::NumericAggState;
    let [a, b] = *fcinfo.args_n::<2>();
    // SAFETY: context, if set, is the live AggStateNode (agg_state_arg note).
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context());
    }
    if b.isnull {
        if a.isnull {
            fcinfo.return_null();
        }
        return Ok(a.value);
    }
    // SAFETY: non-null state args are live states from this combine lane's
    // transfn/deserialfn (arg1 lives in the per-tuple mcx); distinct
    // allocations, sole references during the call.
    let state2 = unsafe { &mut *(b.value.as_usize() as *mut NumericAggState) };
    let (state1, agg_mcx) = agg_state_arg(fcinfo, a, || NumericAggState::new(with_sum_x2))?;
    // SAFETY: as state2.
    unsafe {
        if a.isnull {
            crate::aggregates::numeric_agg_copy(&mut *state1, state2, agg_mcx, with_sum_x2)?;
        } else {
            crate::aggregates::numeric_agg_combine(&mut *state1, state2, agg_mcx, with_sum_x2)?;
        }
    }
    Ok(Datum::from_usize(state1 as usize))
}

pub fn fc_numeric_combine(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    numeric_combine_common(fcinfo, true)
}

pub fn fc_numeric_avg_combine(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    numeric_combine_common(fcinfo, false)
}

// numeric_poly_combine/int8_avg_combine (numeric.c), HAVE_INT128 arm.
fn poly_combine_common(fcinfo: &mut Fcinfo, with_sum_x2: bool) -> PgResult<Datum> {
    use crate::aggregates::Int128AggState;
    let [a, b] = *fcinfo.args_n::<2>();
    // SAFETY: as numeric_combine_common.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context());
    }
    if b.isnull {
        if a.isnull {
            fcinfo.return_null();
        }
        return Ok(a.value);
    }
    // SAFETY: as numeric_combine_common; state2 read-only here.
    let state2 = unsafe { &*(b.value.as_usize() as *const Int128AggState) };
    let (state1, _) = agg_state_arg(fcinfo, a, || Int128AggState::new(with_sum_x2))?;
    // SAFETY: as numeric_combine_common.
    unsafe {
        if a.isnull {
            (*state1).n = state2.n;
            (*state1).sum_x = state2.sum_x;
            if with_sum_x2 {
                (*state1).sum_x2 = state2.sum_x2;
            }
        } else if state2.n > 0 {
            (*state1).n += state2.n;
            (*state1).sum_x += state2.sum_x;
            if with_sum_x2 {
                (*state1).sum_x2 += state2.sum_x2;
            }
        }
    }
    Ok(Datum::from_usize(state1 as usize))
}

pub fn fc_numeric_poly_combine(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    poly_combine_common(fcinfo, true)
}

pub fn fc_int8_avg_combine(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    poly_combine_common(fcinfo, false)
}

// numeric_serialize/numeric_avg_serialize (numeric.c).
fn numeric_serialize_common(fcinfo: &mut Fcinfo, with_sum_x2: bool) -> PgResult<Datum> {
    use crate::aggregates::NumericAggState;
    // SAFETY: as numeric_combine_common.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context());
    }
    let a = fcinfo.args_n::<1>()[0];
    // SAFETY: strict fn — arg 0 is the aggcontext-lived state (transfn
    // contract); finalize's lazy carry is the only mutation.
    let state = unsafe { &mut *(a.value.as_usize() as *mut NumericAggState) };
    let mut buf = ::pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    crate::aggregates::numeric_agg_state_serialize(state, with_sum_x2, &mut buf)?;
    Ok(varlena_result(::pqformat::pq_endtypsend(buf)))
}

pub fn fc_numeric_serialize(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    numeric_serialize_common(fcinfo, true)
}

pub fn fc_numeric_avg_serialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    numeric_serialize_common(fcinfo, false)
}

// C reads the bytea in place (initReadOnlyStringInfo over VARDATA_ANY); the
// copy into a cursor buffer is observationally identical.
fn deserialize_buf<'a>(fcinfo: &'a Fcinfo) -> PgResult<::stringinfo::StringInfo<'a>> {
    // SAFETY: strict fn — arg 0 is a non-null bytea varlena.
    let sstate = unsafe { fcinfo.arg_varlena_packed(0)? };
    let data = sstate.data();
    let mut buf =
        ::stringinfo::StringInfo::with_capacity_in(fcinfo.result_mcx(), data.len() + 1)?;
    buf.append_bytes(data)?;
    Ok(buf)
}

// numeric_deserialize/numeric_avg_deserialize (numeric.c): the state lands in
// CurrentMemoryContext (the per-tuple result mcx here); the combine step
// copies it into the agg context.
fn numeric_deserialize_common(fcinfo: &mut Fcinfo, with_sum_x2: bool) -> PgResult<Datum> {
    use crate::aggregates::NumericAggState;
    // SAFETY: as numeric_combine_common.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context());
    }
    let mcx = fcinfo.result_mcx();
    let mut buf = deserialize_buf(fcinfo)?;
    let state = crate::aggregates::numeric_agg_state_deserialize(&mut buf, mcx, with_sum_x2)?;
    let layout = core::alloc::Layout::new::<NumericAggState>();
    let raw = ::mcx::Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(layout.size()))?;
    let p = raw.cast::<NumericAggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(state) };
    Ok(Datum::from_usize(p as usize))
}

pub fn fc_numeric_deserialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    numeric_deserialize_common(fcinfo, true)
}

pub fn fc_numeric_avg_deserialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    numeric_deserialize_common(fcinfo, false)
}

// numeric_poly_serialize/int8_avg_serialize (numeric.c), HAVE_INT128 arm.
fn poly_serialize_common(fcinfo: &mut Fcinfo, with_sum_x2: bool) -> PgResult<Datum> {
    use crate::aggregates::Int128AggState;
    // SAFETY: as numeric_combine_common.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context());
    }
    let a = fcinfo.args_n::<1>()[0];
    // SAFETY: strict fn — arg 0 is the aggcontext-lived state; read-only.
    let state = unsafe { &*(a.value.as_usize() as *const Int128AggState) };
    let mut buf = ::pqformat::pq_begintypsend(fcinfo.result_mcx())?;
    crate::aggregates::int128_agg_state_serialize(state, with_sum_x2, &mut buf)?;
    Ok(varlena_result(::pqformat::pq_endtypsend(buf)))
}

pub fn fc_numeric_poly_serialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    poly_serialize_common(fcinfo, true)
}

pub fn fc_int8_avg_serialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    poly_serialize_common(fcinfo, false)
}

// numeric_poly_deserialize/int8_avg_deserialize (numeric.c), HAVE_INT128 arm.
fn poly_deserialize_common(fcinfo: &mut Fcinfo, with_sum_x2: bool) -> PgResult<Datum> {
    use crate::aggregates::Int128AggState;
    // SAFETY: as numeric_combine_common.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context());
    }
    let mut buf = deserialize_buf(fcinfo)?;
    let state = crate::aggregates::int128_agg_state_deserialize(&mut buf, with_sum_x2)?;
    let mcx = fcinfo.result_mcx();
    let layout = core::alloc::Layout::new::<Int128AggState>();
    let raw = ::mcx::Allocator::allocate(&mcx, layout).map_err(|_| mcx.oom(layout.size()))?;
    let p = raw.cast::<Int128AggState>().as_ptr();
    // SAFETY: fresh allocation of the exact layout.
    unsafe { p.write(state) };
    Ok(Datum::from_usize(p as usize))
}

pub fn fc_numeric_poly_deserialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    poly_deserialize_common(fcinfo, true)
}

pub fn fc_int8_avg_deserialize(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    poly_deserialize_common(fcinfo, false)
}

macro_rules! fc_poly_final {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = fcinfo.args_n::<1>()[0];
            // SAFETY: a non-null arg0 is the aggcontext-lived Int128AggState
            // (transfn contract); read-only here.
            let state = (!a.isnull)
                .then(|| unsafe { &*(a.value.as_usize() as *const crate::aggregates::Int128AggState) });
            match crate::aggregates::$core(state)? {
                Some(img) => img_result(fcinfo, &img),
                None => {
                    fcinfo.isnull = true;
                    Ok(Datum::null())
                }
            }
        }
    )*};
}

fc_poly_final! {
    fc_numeric_poly_sum: numeric_poly_sum;
    fc_numeric_poly_avg: numeric_poly_avg;
}

// numeric_sum/numeric_avg/stddev-family finals over NumericAggState (finalize
// carries lazily; idempotent, so shared transstates re-finalize safely).
macro_rules! fc_num_final {
    ($($fc:ident: $core:expr;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = fcinfo.args_n::<1>()[0];
            // SAFETY: a non-null arg0 is the aggcontext-lived NumericAggState
            // (transfn contract); sole reference during the call.
            let state = (!a.isnull).then(|| unsafe {
                &mut *(a.value.as_usize() as *mut crate::aggregates::NumericAggState)
            });
            #[allow(clippy::redundant_closure_call)]
            match ($core)(state)? {
                Some(img) => img_result(fcinfo, &img),
                None => {
                    fcinfo.isnull = true;
                    Ok(Datum::null())
                }
            }
        }
    )*};
}

fc_num_final! {
    fc_numeric_sum: crate::aggregates::numeric_sum;
    fc_numeric_avg: crate::aggregates::numeric_avg;
    fc_numeric_var_samp: |s| crate::aggregates::numeric_stddev_internal(s, true, true);
    fc_numeric_stddev_samp: |s| crate::aggregates::numeric_stddev_internal(s, false, true);
    fc_numeric_var_pop: |s| crate::aggregates::numeric_stddev_internal(s, true, false);
    fc_numeric_stddev_pop: |s| crate::aggregates::numeric_stddev_internal(s, false, false);
}

macro_rules! fc_poly_stddev_final {
    ($($fc:ident: ($variance:expr, $sample:expr);)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = fcinfo.args_n::<1>()[0];
            // SAFETY: as fc_poly_final.
            let state = (!a.isnull).then(|| unsafe {
                &*(a.value.as_usize() as *const crate::aggregates::Int128AggState)
            });
            match crate::aggregates::numeric_poly_stddev_internal(state, $variance, $sample)? {
                Some(img) => img_result(fcinfo, &img),
                None => {
                    fcinfo.isnull = true;
                    Ok(Datum::null())
                }
            }
        }
    )*};
}

fc_poly_stddev_final! {
    fc_numeric_poly_var_samp: (true, true);
    fc_numeric_poly_stddev_samp: (false, true);
    fc_numeric_poly_var_pop: (true, false);
    fc_numeric_poly_stddev_pop: (false, false);
}

// avg(int2)/avg(int4) transition lane (numeric.c Int8TransTypeData): the
// transtype is a 2-element int8 array mutated in place under an agg context
// (the by-ref trans step sees the same pointer come back — no copy).
const ARR_OVERHEAD_NONULLS_1: usize = 24;
const INT8_TRANSARRAY_SIZE: usize = ARR_OVERHEAD_NONULLS_1 + 16;

#[cold]
#[inline(never)]
fn bad_int8_transarray() -> Box<::types_error::PgError> {
    Box::new(::types_error::PgError::error("expected 2-element int8 array"))
}

// PG_GETARG_ARRAYTYPE_P + the hasnull/size validation shared by the
// avg-transition family; returns ARR_DATA_PTR as the count/sum pair.
//
// # Safety
// Arg 0 is a non-null _int8 array datum (strict fn), 8-aligned and writable
// when reached through the agg transvalue lane.
unsafe fn int8_transarray(fcinfo: &Fcinfo, copy: bool) -> PgResult<*mut i64> {
    // SAFETY: forwarded caller contract.
    unsafe { int8_transarray_at(fcinfo, 0, copy) }
}

// # Safety
// As int8_transarray, for arg `i`.
unsafe fn int8_transarray_at(fcinfo: &Fcinfo, i: usize, copy: bool) -> PgResult<*mut i64> {
    // SAFETY: forwarded caller contract.
    let arr = unsafe {
        let p = fcinfo.arg_ptr(i);
        if ::types_tuple::varatt::varatt_is_1b(p) && !::types_tuple::varatt::varatt_is_1b_e(p) {
            // C pg_detoast_datum: a tuple-packed short image (e.g. a partial
            // result off the parallel tuple queue) expands into a fresh 4B-U
            // copy in CurrentMemoryContext (the result mcx here).
            let payload_len = ::types_tuple::varatt::varsize_1b(p) - 1;
            let mcx = fcinfo.result_mcx();
            let layout = core::alloc::Layout::from_size_align(payload_len + 4, 8)
                .expect("transarray layout");
            let raw: core::ptr::NonNull<u8> = ::mcx::Allocator::allocate(&mcx, layout)
                .map_err(|_| mcx.oom(layout.size()))?
                .cast();
            let dst = raw.as_ptr();
            dst.cast::<u32>()
                .write(::types_tuple::varatt::set_varsize_4b_word((payload_len + 4) as u32));
            core::ptr::copy_nonoverlapping(p.add(1), dst.add(4), payload_len);
            dst
        } else if !::types_tuple::varatt::varatt_is_4b_u(p) {
            panic!("int8 transarray: toasted array datum (detoast unported)");
        } else if copy {
            let img = core::slice::from_raw_parts(p, ::types_tuple::varatt::varsize_4b(p));
            byref_result(fcinfo.result_mcx(), img)?.as_usize() as *mut u8
        } else {
            p.cast_mut()
        }
    };
    debug_assert!(arr as usize % 8 == 0, "transarray must be MAXALIGNed");
    // SAFETY: 4B-U image at least header-readable; size validated before the
    // data pointer is used.
    unsafe {
        let size = ::types_tuple::varatt::varsize_4b(arr);
        let hasnull = arr.add(8).cast::<i32>().read() != 0;
        if hasnull || size != INT8_TRANSARRAY_SIZE {
            return Err(bad_int8_transarray());
        }
        Ok(arr.add(ARR_OVERHEAD_NONULLS_1).cast::<i64>())
    }
}

macro_rules! fc_int_avg_accum {
    ($($fc:ident: $get:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let newval = fcinfo.args_n::<2>()[1].value.$get() as i64;
            // C copies the array unless invoked as an aggregate (in-place cheat).
            // SAFETY: strict fn — arg 0 is a non-null _int8 array; the agg
            // lane hands the aggcontext-lived, MAXALIGNed transvalue.
            let in_agg = unsafe { fcinfo.agg_context() }.is_some();
            // SAFETY: as above.
            let td = unsafe { int8_transarray(fcinfo, !in_agg)? };
            // SAFETY: validated 2-slot int8 payload behind td; the array
            // image starts ARR_OVERHEAD_NONULLS_1 bytes before it.
            unsafe {
                *td += 1;
                *td.add(1) += newval;
                Ok(Datum::from_usize(td.cast::<u8>().sub(ARR_OVERHEAD_NONULLS_1) as usize))
            }
        }
    )*};
}

fc_int_avg_accum! {
    fc_int2_avg_accum: as_i16;
    fc_int4_avg_accum: as_i32;
}

// int2/int4_avg_accum_inv (numeric.c): the moving-aggregate inverse over the
// int8[2] {count,sum} transarray.
macro_rules! fc_int_avg_accum_inv {
    ($($fc:ident: $get:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let newval = fcinfo.args_n::<2>()[1].value.$get() as i64;
            // SAFETY: strict fn — arg 0 is a non-null _int8 array; the agg
            // lane hands the aggcontext-lived, MAXALIGNed transvalue.
            let in_agg = unsafe { fcinfo.agg_context() }.is_some();
            // SAFETY: as above.
            let td = unsafe { int8_transarray(fcinfo, !in_agg)? };
            // SAFETY: validated 2-slot int8 payload behind td.
            unsafe {
                *td -= 1;
                *td.add(1) -= newval;
                Ok(Datum::from_usize(td.cast::<u8>().sub(ARR_OVERHEAD_NONULLS_1) as usize))
            }
        }
    )*};
}

fc_int_avg_accum_inv! {
    fc_int2_avg_accum_inv: as_i16;
    fc_int4_avg_accum_inv: as_i32;
}

// int4_avg_combine (numeric.c:6832): in-place combine over the int8[2]
// {count,sum} transarray; shared by avg/sum(int2) and avg/sum(int4).
pub fn fc_int4_avg_combine(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: as numeric_combine_common.
    if unsafe { fcinfo.agg_context() }.is_none() {
        return Err(non_aggregate_context());
    }
    // SAFETY: strict fn — both args are non-null _int8 array transvalues;
    // the agg lane hands aggcontext-lived, MAXALIGNed images.
    unsafe {
        let td1 = int8_transarray_at(fcinfo, 0, false)?;
        let td2 = int8_transarray_at(fcinfo, 1, false)?;
        *td1 += *td2;
        *td1.add(1) += *td2.add(1);
        Ok(Datum::from_usize(td1.cast::<u8>().sub(ARR_OVERHEAD_NONULLS_1) as usize))
    }
}

pub fn fc_int8_avg(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null _int8 array transvalue.
    let td = unsafe { int8_transarray(fcinfo, false)? };
    // SAFETY: validated 2-slot int8 payload.
    let (count, sum) = unsafe { (*td, *td.add(1)) };
    if count == 0 {
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }
    let img = crate::ops::int64_avg_div(sum, count)?;
    img_result(fcinfo, &img)
}


pub fn fc_numerictypmodin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 of numerictypmodin is a non-null cstring[] datum
    // (strict fn); typenameTypeMod builds it flat (never toasted).
    let arr = unsafe { fcinfo.arg_varlena_raw(0) };
    let tl = ::arrayfuncs::array_get_integer_typmods(mcx, arr)?;
    Ok(Datum::from_i32(crate::ops::numerictypmodin_core(&tl)?))
}

pub fn fc_numeric_round(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    let img = crate::numeric_round_common(num, fcinfo.arg_i32(1))?;
    img_result(fcinfo, &img)
}

pub fn fc_numeric_trunc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    let img = crate::numeric_trunc_common(num, fcinfo.arg_i32(1))?;
    img_result(fcinfo, &img)
}

pub fn fc_numeric_int2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    Ok(Datum::from_i16(crate::numeric_int2(num)?))
}

pub fn fc_numeric_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    if num.is_special() {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i32(num.dscale()))
}

pub fn fc_numeric_min_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    if num.is_special() {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_i32(crate::numeric_min_scale(num)))
}

pub fn fc_numeric_trim_scale(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    let img = crate::numeric_trim_scale(num)?;
    img_result(fcinfo, &img)
}

fc_num_unary_res! {
    fc_numeric_ceil: numeric_ceil;
    fc_numeric_floor: numeric_floor;
    fc_numeric_sign: numeric_sign;
    fc_numeric_inc: numeric_inc;
}

// numeric_support (numeric.c): SupportRequestSimplify only — an unconstrained
// destination typmod, or same-scale non-decreasing precision, becomes a
// RelabelType (no rewrite).
pub fn fc_numeric_support(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use ::types_nodes::{supportnodes::SupportRequestSimplify, NodeTag};
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *const NodeTag;
    // SAFETY: prosupport contract — arg points at a live tag-first node.
    if unsafe { *p } != NodeTag::T_SupportRequestSimplify {
        return Ok(Datum::from_usize(0));
    }
    // SAFETY: tag checked; the planner owns the request node for the call.
    let req = unsafe { &*(a.value.as_usize() as *const SupportRequestSimplify) };
    let fexpr = req
        .fcall
        .and_then(|n| n.as_func_expr())
        .unwrap_or_else(|| panic!("numeric_support: SupportRequestSimplify without a FuncExpr"));
    assert!(fexpr.args.len() >= 2);
    let Some(c) = fexpr.args.nth(1).as_const() else {
        return Ok(Datum::from_usize(0));
    };
    if c.constisnull {
        return Ok(Datum::from_usize(0));
    }
    let source = fexpr.args.nth(0);
    let old_typmod = nodes_core::expr_typmod(source);
    let new_typmod = c.constvalue.as_i32();
    let old_scale = crate::ops::numeric_typmod_scale(old_typmod);
    let new_scale = crate::ops::numeric_typmod_scale(new_typmod);
    let old_precision = crate::ops::numeric_typmod_precision(old_typmod);
    let new_precision = crate::ops::numeric_typmod_precision(new_typmod);
    if !crate::ops::is_valid_numeric_typmod(new_typmod)
        || (crate::ops::is_valid_numeric_typmod(old_typmod)
            && new_scale == old_scale
            && new_precision >= old_precision)
    {
        let mcx = req.mcx.expect("numeric_support: request carries an mcx");
        let ret = nodes_core::relabel_to_typmod(mcx, source, new_typmod)?;
        return Ok(Datum::from_usize(ret.as_raw().as_ptr() as usize));
    }
    Ok(Datum::from_usize(0))
}

pub fn fc_hash_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    Ok(Datum::from_u32(crate::hash_numeric(num)))
}

pub fn fc_hash_numeric_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) }?;
    let seed = fcinfo.arg_i64(1) as u64;
    Ok(Datum::from_u64(crate::hash_numeric_extended(num, seed)))
}

pub fn fc_in_range_numeric_numeric(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args 0-2 are non-null numeric varlenas (strict fn).
    let (val, base, offset) =
        unsafe { (num_arg(fcinfo, 0)?, num_arg(fcinfo, 1)?, num_arg(fcinfo, 2)?) };
    let sub = fcinfo.arg_bool(3);
    let less = fcinfo.arg_bool(4);
    Ok(Datum::from_bool(crate::in_range_numeric_numeric(
        val, base, offset, sub, less,
    )?))
}

// OIDs 3260 2-arg / 3259 3-arg share the C body (PG_NARGS demuxes) over the
// funcapi ValuePerCall frame.
pub fn fc_generate_series_numeric(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("generate_series_numeric: NULL flinfo");
    if !flinfo.has_fn_extra() {
        // SAFETY: catalog args are non-null numeric varlenas (strict fn).
        let (start, stop) = unsafe { (num_arg(fcinfo, 0)?, num_arg(fcinfo, 1)?) };
        let step = if fcinfo.nargs() == 3 {
            // SAFETY: as above.
            Some(unsafe { num_arg(fcinfo, 2) }?)
        } else {
            None
        };
        let state = crate::series::GenerateSeriesNumeric::new(start, stop, step)?;
        let fctx = ::funcapi_srf::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(std::boxed::Box::new(state));
    }
    let next = ::funcapi_srf::per_MultiFuncCall(flinfo)
        .user_fctx
        .as_mut()
        .expect("generate_series_numeric: user_fctx set at first call")
        .downcast_mut::<crate::series::GenerateSeriesNumeric>()
        .expect("generate_series_numeric: user_fctx is GenerateSeriesNumeric")
        .next()?;
    match next {
        Some(img) => {
            let d = img_result(fcinfo, &img)?;
            Ok(::funcapi_srf::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(::funcapi_srf::srf_return_done(flinfo, fcinfo)),
    }
}

// C's DatumGetNumeric for planner Consts: 1B-short images realign into the
// frame's result mcx.
unsafe fn num_from_const_datum<'a>(fcinfo: &'a Fcinfo, d: Datum) -> PgResult<Num<'a>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — a live in-line varlena const datum.
    unsafe {
        let b0 = *p;
        if b0 & 0x01 == 0x01 {
            assert!(b0 != 0x01, "numeric const: external toast datum");
            let total = ((b0 >> 1) & 0x7F) as usize;
            let src = core::slice::from_raw_parts(p.add(1), total - 1);
            let mcx = fcinfo.result_mcx();
            let layout =
                core::alloc::Layout::from_size_align(src.len(), 8).expect("numeric const layout");
            let dst: core::ptr::NonNull<u8> =
                mcx.allocate(layout).map_err(|_| mcx.oom(layout.size()))?.cast();
            core::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_ptr(), src.len());
            Ok(Num::from_payload(core::slice::from_raw_parts(dst.as_ptr(), src.len())))
        } else {
            assert!(b0 & 0x03 == 0, "numeric const: compressed datum");
            Ok(Num::from_payload(::datum::VarlenaRef::from_ptr(p).data()))
        }
    }
}

// SupportRequestRows over all-Const args; anything else returns NULL so
// callers fall back (C estimates planner-folded exprs — Param estimation
// unported, same divergence as generate_series_int4_support).
pub fn fc_generate_series_numeric_support(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let p = a.value.as_usize() as *mut ();
    // SAFETY: prosupport contract — the internal arg points at a live
    // tag-first support-request node exclusively owned by this call.
    let Some(req) = (unsafe { ::types_nodes::supportnodes::support_request_rows_mut(p) }) else {
        return Ok(Datum::from_usize(0));
    };
    let args = match req.node.and_then(|n| n.as_func_expr()) {
        Some(fe) => &fe.args,
        None => return Ok(Datum::from_usize(0)),
    };
    let mut vals = [Datum::null(); 3];
    let nargs = args.len();
    for (i, arg) in args.iter().enumerate() {
        match arg.as_const() {
            Some(c) if c.constisnull => {
                req.rows = 0.0;
                return Ok(Datum::from_usize(p as usize));
            }
            Some(c) => vals[i] = c.constvalue,
            None => return Ok(Datum::from_usize(0)),
        }
    }
    // SAFETY: non-null numeric Const datums per the constisnull check above.
    let (start, stop) = unsafe {
        (
            num_from_const_datum(fcinfo, vals[0])?,
            num_from_const_datum(fcinfo, vals[1])?,
        )
    };
    let step = if nargs >= 3 {
        // SAFETY: as above.
        Some(unsafe { num_from_const_datum(fcinfo, vals[2]) }?)
    } else {
        None
    };
    match crate::series::generate_series_numeric_rows(start, stop, step)? {
        Some(rows) => {
            req.rows = rows;
            Ok(Datum::from_usize(p as usize))
        }
        None => Ok(Datum::from_usize(0)),
    }
}

const fn b(foid: ::types_core::Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
}

const fn srf(foid: ::types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

// pg_proc.dat rows, OID-ascending (1840/1841 proisstrict 'f', rest 't').
pub const NUMERIC_BUILTINS: &[FmgrBuiltin] = &[
    b(1376, "factorial", 1, true, fc_numeric_fac),
    b(1701, "numeric_in", 3, true, fc_numeric_in),
    b(1702, "numeric_out", 1, true, fc_numeric_out),
    b(1703, "numeric", 2, true, fc_numeric),
    b(1704, "numeric_abs", 1, true, fc_numeric_abs),
    b(1718, "numeric_eq", 2, true, fc_numeric_eq),
    b(1719, "numeric_ne", 2, true, fc_numeric_ne),
    b(1720, "numeric_gt", 2, true, fc_numeric_gt),
    b(1721, "numeric_ge", 2, true, fc_numeric_ge),
    b(1722, "numeric_lt", 2, true, fc_numeric_lt),
    b(1723, "numeric_le", 2, true, fc_numeric_le),
    b(1724, "numeric_add", 2, true, fc_numeric_add),
    b(1725, "numeric_sub", 2, true, fc_numeric_sub),
    b(1726, "numeric_mul", 2, true, fc_numeric_mul),
    b(1727, "numeric_div", 2, true, fc_numeric_div),
    b(1728, "mod", 2, true, fc_numeric_mod),
    b(1729, "numeric_mod", 2, true, fc_numeric_mod),
    b(1730, "sqrt", 1, true, fc_numeric_sqrt),
    b(1731, "numeric_sqrt", 1, true, fc_numeric_sqrt),
    b(1732, "exp", 1, true, fc_numeric_exp),
    b(1733, "numeric_exp", 1, true, fc_numeric_exp),
    b(1734, "ln", 1, true, fc_numeric_ln),
    b(1735, "numeric_ln", 1, true, fc_numeric_ln),
    b(1736, "log", 2, true, fc_numeric_log),
    b(1737, "numeric_log", 2, true, fc_numeric_log),
    b(1738, "pow", 2, true, fc_numeric_power),
    b(1739, "numeric_power", 2, true, fc_numeric_power),
    b(1740, "int4_numeric", 1, true, fc_int4_numeric),
    b(1742, "float4_numeric", 1, true, fc_float4_numeric),
    b(1743, "float8_numeric", 1, true, fc_float8_numeric),
    b(1744, "numeric_int4", 1, true, fc_numeric_int4),
    b(1745, "numeric_float4", 1, true, fc_numeric_float4),
    b(1746, "numeric_float8", 1, true, fc_numeric_float8),
    b(1779, "numeric_int8", 1, true, fc_numeric_int8),
    b(1766, "numeric_smaller", 2, true, fc_numeric_smaller),
    b(1767, "numeric_larger", 2, true, fc_numeric_larger),
    b(1769, "numeric_cmp", 2, true, fc_numeric_cmp),
    b(1771, "numeric_uminus", 1, true, fc_numeric_uminus),
    b(1781, "int8_numeric", 1, true, fc_int8_numeric),
    b(1782, "int2_numeric", 1, true, fc_int2_numeric),
    b(1833, "numeric_accum", 2, false, fc_numeric_accum),
    b(1834, "int2_accum", 2, false, fc_int2_accum),
    b(1835, "int4_accum", 2, false, fc_int4_accum),
    b(1836, "int8_accum", 2, false, fc_int8_accum),
    b(1837, "numeric_avg", 1, false, fc_numeric_avg),
    b(1838, "numeric_var_samp", 1, false, fc_numeric_var_samp),
    b(1839, "numeric_stddev_samp", 1, false, fc_numeric_stddev_samp),
    b(1840, "int2_sum", 2, false, fc_int2_sum),
    b(1841, "int4_sum", 2, false, fc_int4_sum),
    b(1842, "int8_sum", 2, false, fc_int8_sum),
    b(1962, "int2_avg_accum", 2, true, fc_int2_avg_accum),
    b(1963, "int4_avg_accum", 2, true, fc_int4_avg_accum),
    b(1964, "int8_avg", 1, true, fc_int8_avg),
    b(1915, "numeric_uplus", 1, true, fc_numeric_uplus),
    b(1707, "numeric_round", 2, true, fc_numeric_round),
    b(1709, "numeric_trunc", 2, true, fc_numeric_trunc),
    b(1783, "numeric_int2", 1, true, fc_numeric_int2),
    b(1973, "numeric_div_trunc", 2, true, fc_numeric_div_trunc),
    b(3281, "numeric_scale", 1, true, fc_numeric_scale),
    b(5042, "numeric_min_scale", 1, true, fc_numeric_min_scale),
    b(5043, "numeric_trim_scale", 1, true, fc_numeric_trim_scale),
    b(1980, "numeric_div_trunc", 2, true, fc_numeric_div_trunc),
    b(2169, "power", 2, true, fc_numeric_power),
    b(2170, "width_bucket", 4, true, fc_width_bucket_numeric),
    b(2460, "numeric_recv", 3, true, fc_numeric_recv),
    b(2461, "numeric_send", 1, true, fc_numeric_send),
    b(2514, "numeric_var_pop", 1, false, fc_numeric_var_pop),
    b(2596, "numeric_stddev_pop", 1, false, fc_numeric_stddev_pop),
    b(2746, "int8_avg_accum", 2, false, fc_int8_avg_accum),
    b(3548, "numeric_accum_inv", 2, false, fc_numeric_accum_inv),
    b(3567, "int2_accum_inv", 2, false, fc_int2_accum_inv),
    b(3568, "int4_accum_inv", 2, false, fc_int4_accum_inv),
    b(3569, "int8_accum_inv", 2, false, fc_int8_accum_inv),
    b(3387, "int8_avg_accum_inv", 2, false, fc_int8_avg_accum_inv),
    b(3570, "int2_avg_accum_inv", 2, true, fc_int2_avg_accum_inv),
    b(3571, "int4_avg_accum_inv", 2, true, fc_int4_avg_accum_inv),
    b(2858, "numeric_avg_accum", 2, false, fc_numeric_avg_accum),
    b(3341, "numeric_combine", 2, false, fc_numeric_combine),
    b(3337, "numeric_avg_combine", 2, false, fc_numeric_avg_combine),
    b(2740, "numeric_avg_serialize", 1, true, fc_numeric_avg_serialize),
    b(2741, "numeric_avg_deserialize", 2, true, fc_numeric_avg_deserialize),
    b(3335, "numeric_serialize", 1, true, fc_numeric_serialize),
    b(3336, "numeric_deserialize", 2, true, fc_numeric_deserialize),
    b(3338, "numeric_poly_combine", 2, false, fc_numeric_poly_combine),
    b(3339, "numeric_poly_serialize", 1, true, fc_numeric_poly_serialize),
    b(3340, "numeric_poly_deserialize", 2, true, fc_numeric_poly_deserialize),
    b(2785, "int8_avg_combine", 2, false, fc_int8_avg_combine),
    b(3324, "int4_avg_combine", 2, true, fc_int4_avg_combine),
    b(2786, "int8_avg_serialize", 1, true, fc_int8_avg_serialize),
    b(2787, "int8_avg_deserialize", 2, true, fc_int8_avg_deserialize),
    b(2917, "numerictypmodin", 1, true, fc_numerictypmodin),
    b(3178, "numeric_sum", 1, false, fc_numeric_sum),
    b(3388, "numeric_poly_sum", 1, false, fc_numeric_poly_sum),
    b(3389, "numeric_poly_avg", 1, false, fc_numeric_poly_avg),
    b(3390, "numeric_poly_var_pop", 1, false, fc_numeric_poly_var_pop),
    b(3391, "numeric_poly_var_samp", 1, false, fc_numeric_poly_var_samp),
    b(3392, "numeric_poly_stddev_pop", 1, false, fc_numeric_poly_stddev_pop),
    b(3393, "numeric_poly_stddev_samp", 1, false, fc_numeric_poly_stddev_samp),
    b(5048, "gcd", 2, true, fc_numeric_gcd),
    b(5049, "lcm", 2, true, fc_numeric_lcm),
    b(1705, "abs", 1, true, fc_numeric_abs),
    b(1706, "sign", 1, true, fc_numeric_sign),
    b(1711, "ceil", 1, true, fc_numeric_ceil),
    b(1712, "floor", 1, true, fc_numeric_floor),
    b(1764, "numeric_inc", 1, true, fc_numeric_inc),
    b(2167, "ceiling", 1, true, fc_numeric_ceil),
    b(3157, "numeric_support", 1, true, fc_numeric_support),
    b(432, "hash_numeric", 1, true, fc_hash_numeric),
    b(780, "hash_numeric_extended", 2, true, fc_hash_numeric_extended),
    srf(3259, "generate_series", 3, fc_generate_series_numeric),
    srf(3260, "generate_series", 2, fc_generate_series_numeric),
    b(4141, "in_range_numeric_numeric", 5, true, fc_in_range_numeric_numeric),
    b(6357, "generate_series_numeric_support", 1, true, fc_generate_series_numeric_support),
];
