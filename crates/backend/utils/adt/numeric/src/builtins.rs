//! fmgr wrappers (`fc_*`) + registry for numeric.c's fmgr-callable rows.
//! Numeric results follow the result-mcx convention with the pool-backed
//! NumericImage as call scratch (notes/fc-result-convention.md); numeric_out
//! keeps the retained-cstring-scratch precedent. Still deferred: recv/send
//! (pqformat frame), sortsupport/hash (see ops.rs).

use ::datum::Datum;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
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
        (Some(s), Some(v)) => Some(s + v as i64),
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

/// # Safety
/// Arg `i` is a non-null numeric varlena (strict fn); 1B-short images panic
/// in `Num::digits` (C's DatumGetNumeric detoast-expand is a caller concern).
#[inline]
unsafe fn num_arg(fcinfo: &Fcinfo, i: usize) -> Num<'_> {
    // SAFETY: forwarded caller contract.
    Num::from_payload(unsafe { fcinfo.arg_varlena_packed(i) }.data())
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
    let num = unsafe { num_arg(fcinfo, 0) };
    if !flinfo.has_fn_extra() {
        flinfo.set_fn_extra(OutBuf(Vec::new()));
    }
    let buf = &mut flinfo.fn_extra_mut::<OutBuf>().unwrap().0;
    buf.clear();
    crate::io::numeric_out_into(num, buf);
    buf.push(0);
    Ok(Datum::from_usize(buf.as_ptr() as usize))
}

pub fn fc_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null numeric varlena (strict fn).
    let num = unsafe { num_arg(fcinfo, 0) };
    let img = crate::numeric_apply_typmod(num, fcinfo.arg_i32(1))?;
    img_result(fcinfo, &img)
}

macro_rules! fc_num_binop {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null numeric varlenas (strict fn).
            let (a, b) = unsafe { (num_arg(fcinfo, 0), num_arg(fcinfo, 1)) };
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
            let (a, b) = unsafe { (num_arg(fcinfo, 0), num_arg(fcinfo, 1)) };
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
            let num = unsafe { num_arg(fcinfo, 0) };
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
            let num = unsafe { num_arg(fcinfo, 0) };
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
        unsafe { (num_arg(fcinfo, 0), num_arg(fcinfo, 1), num_arg(fcinfo, 2)) };
    let count = fcinfo.arg_i32(3);
    Ok(Datum::from_i32(crate::width_bucket_numeric(
        operand, b1, b2, count,
    )?))
}

// C returns one of the input pointers — so do smaller/larger.
pub fn fc_numeric_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null numeric varlenas (strict fn).
    let (a, b) = unsafe { (num_arg(fcinfo, 0), num_arg(fcinfo, 1)) };
    Ok(if crate::cmp_numerics(a, b) >= 0 {
        fcinfo.arg(0)
    } else {
        fcinfo.arg(1)
    })
}

pub fn fc_numeric_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null numeric varlenas (strict fn).
    let (a, b) = unsafe { (num_arg(fcinfo, 0), num_arg(fcinfo, 1)) };
    Ok(if crate::cmp_numerics(a, b) <= 0 {
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

const fn b(foid: ::types_core::Oid, name: &'static str, nargs: i16, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict, retset: false, func }
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
    b(1766, "numeric_smaller", 2, true, fc_numeric_smaller),
    b(1767, "numeric_larger", 2, true, fc_numeric_larger),
    b(1769, "numeric_cmp", 2, true, fc_numeric_cmp),
    b(1771, "numeric_uminus", 1, true, fc_numeric_uminus),
    b(1781, "int8_numeric", 1, true, fc_int8_numeric),
    b(1782, "int2_numeric", 1, true, fc_int2_numeric),
    b(1840, "int2_sum", 2, false, fc_int2_sum),
    b(1841, "int4_sum", 2, false, fc_int4_sum),
    b(1915, "numeric_uplus", 1, true, fc_numeric_uplus),
    b(1980, "numeric_div_trunc", 2, true, fc_numeric_div_trunc),
    b(2169, "power", 2, true, fc_numeric_power),
    b(2170, "width_bucket", 4, true, fc_width_bucket_numeric),
    b(5048, "gcd", 2, true, fc_numeric_gcd),
    b(5049, "lcm", 2, true, fc_numeric_lcm),
];
