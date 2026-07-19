//! fmgr wrappers (`fc_*`) + `CASH_BUILTINS` for fmgr-core. cash_recv/cash_send
//! (2492/2493) ride the binary-wire fmgr frame (types_fmgr::wire), cash_words
//! (935) the text-result convention (the varlena textin precedent); their
//! value cores live in the crate root.

use std::borrow::Cow;

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

fn in_arg<'a>(fcinfo: &'a Fcinfo) -> Cow<'a, str> {
    // SAFETY: catalog arg 0 of cash_in is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    String::from_utf8_lossy(s.to_bytes())
}

pub fn fc_cash_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let s = in_arg(fcinfo);
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(crate::cash_in(&s, esc)?))
}

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the int.c out-function precedent). The Datum aliases it until the next
// out call on this thread.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; crate::CASH_OUT_BUFLEN + 1]> =
        const { core::cell::UnsafeCell::new([0; crate::CASH_OUT_BUFLEN + 1]) };
}

pub fn fc_cash_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let value = a.value.as_i64();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let body: &mut [u8; crate::CASH_OUT_BUFLEN] =
            (&mut buf[..crate::CASH_OUT_BUFLEN]).try_into().unwrap();
        let len = crate::cash_out_into(value, body)?;
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc_cash2 {
    ($($fc:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::$conv(crate::$core(a.value.as_i64(), b.value.as_i64())))
        }
    )*};
}

fc_cash2! {
    fc_cash_eq: cash_eq -> from_bool;
    fc_cash_ne: cash_ne -> from_bool;
    fc_cash_lt: cash_lt -> from_bool;
    fc_cash_le: cash_le -> from_bool;
    fc_cash_gt: cash_gt -> from_bool;
    fc_cash_ge: cash_ge -> from_bool;
    fc_cash_cmp: cash_cmp -> from_i32;
    fc_cashlarger: cashlarger -> from_i64;
    fc_cashsmaller: cashsmaller -> from_i64;
}

macro_rules! fc_cash2_fallible {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_i64(crate::$core(a.value.as_i64(), b.value.as_i64())?))
        }
    )*};
}

fc_cash2_fallible! {
    fc_cash_pl: cash_pl;
    fc_cash_mi: cash_mi;
    fc_cash_mul_int8: cash_mul_int64;
    fc_cash_div_int8: cash_div_int64;
}

pub fn fc_cash_div_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_f64(crate::cash_div_cash(
        a.value.as_i64(),
        b.value.as_i64(),
    )?))
}

pub fn fc_cash_mul_flt8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_float8(
        a.value.as_i64(),
        b.value.as_f64(),
    )?))
}

pub fn fc_flt8_mul_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_float8(
        b.value.as_i64(),
        a.value.as_f64(),
    )?))
}

pub fn fc_cash_div_flt8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_div_float8(
        a.value.as_i64(),
        b.value.as_f64(),
    )?))
}

pub fn fc_cash_mul_flt4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_float8(
        a.value.as_i64(),
        b.value.as_f32() as f64,
    )?))
}

pub fn fc_flt4_mul_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_float8(
        b.value.as_i64(),
        a.value.as_f32() as f64,
    )?))
}

pub fn fc_cash_div_flt4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_div_float8(
        a.value.as_i64(),
        b.value.as_f32() as f64,
    )?))
}

pub fn fc_cash_mul_int4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_int64(
        a.value.as_i64(),
        b.value.as_i32() as i64,
    )?))
}

pub fn fc_int4_mul_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_int64(
        b.value.as_i64(),
        a.value.as_i32() as i64,
    )?))
}

pub fn fc_cash_div_int4(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_div_int64(
        a.value.as_i64(),
        b.value.as_i32() as i64,
    )?))
}

pub fn fc_cash_mul_int2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_int64(
        a.value.as_i64(),
        b.value.as_i16() as i64,
    )?))
}

pub fn fc_int2_mul_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_int64(
        b.value.as_i64(),
        a.value.as_i16() as i64,
    )?))
}

pub fn fc_cash_div_int2(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_div_int64(
        a.value.as_i64(),
        b.value.as_i16() as i64,
    )?))
}

pub fn fc_int8_mul_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    Ok(Datum::from_i64(crate::cash_mul_int64(
        b.value.as_i64(),
        a.value.as_i64(),
    )?))
}

pub fn fc_int4_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_i64(crate::int4_cash(a.value.as_i32())?))
}

pub fn fc_int8_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_i64(crate::int8_cash(a.value.as_i64())?))
}

pub fn fc_cash_words(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let text = crate::cash_words(fcinfo.result_mcx(), a.value.as_i64())?;
    Ok(varlena_result(text))
}

pub fn fc_cash_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let img = crate::cash_numeric(a.value.as_i64())?;
    byref_result(fcinfo.result_mcx(), img.as_bytes())
}

pub fn fc_numeric_cash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of numeric_cash is a non-null numeric varlena (strict fn).
    let num = unsafe { adt_numeric::builtins::num_arg(fcinfo, 0) }?;
    Ok(Datum::from_i64(crate::numeric_cash(num)?))
}

pub fn fc_cash_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg 0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(crate::cash_recv(buf)?))
}

pub fn fc_cash_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::cash_send(mcx, a.value.as_i64())?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const CASH_BUILTINS: &[FmgrBuiltin] = &[
    b(377, "cash_cmp", 2, fc_cash_cmp),
    b(846, "cash_mul_flt4", 2, fc_cash_mul_flt4),
    b(847, "cash_div_flt4", 2, fc_cash_div_flt4),
    b(848, "flt4_mul_cash", 2, fc_flt4_mul_cash),
    b(862, "int4_mul_cash", 2, fc_int4_mul_cash),
    b(863, "int2_mul_cash", 2, fc_int2_mul_cash),
    b(864, "cash_mul_int4", 2, fc_cash_mul_int4),
    b(865, "cash_div_int4", 2, fc_cash_div_int4),
    b(866, "cash_mul_int2", 2, fc_cash_mul_int2),
    b(867, "cash_div_int2", 2, fc_cash_div_int2),
    b(886, "cash_in", 1, fc_cash_in),
    b(887, "cash_out", 1, fc_cash_out),
    b(888, "cash_eq", 2, fc_cash_eq),
    b(889, "cash_ne", 2, fc_cash_ne),
    b(890, "cash_lt", 2, fc_cash_lt),
    b(891, "cash_le", 2, fc_cash_le),
    b(892, "cash_gt", 2, fc_cash_gt),
    b(893, "cash_ge", 2, fc_cash_ge),
    b(894, "cash_pl", 2, fc_cash_pl),
    b(895, "cash_mi", 2, fc_cash_mi),
    b(896, "cash_mul_flt8", 2, fc_cash_mul_flt8),
    b(897, "cash_div_flt8", 2, fc_cash_div_flt8),
    b(898, "cashlarger", 2, fc_cashlarger),
    b(899, "cashsmaller", 2, fc_cashsmaller),
    b(919, "flt8_mul_cash", 2, fc_flt8_mul_cash),
    b(935, "cash_words", 1, fc_cash_words),
    b(2492, "cash_recv", 1, fc_cash_recv),
    b(2493, "cash_send", 1, fc_cash_send),
    b(3344, "cash_mul_int8", 2, fc_cash_mul_int8),
    b(3345, "cash_div_int8", 2, fc_cash_div_int8),
    b(3399, "int8_mul_cash", 2, fc_int8_mul_cash),
    b(3811, "int4_cash", 1, fc_int4_cash),
    b(3812, "int8_cash", 1, fc_int8_cash),
    b(3822, "cash_div_cash", 2, fc_cash_div_cash),
    b(3823, "cash_numeric", 1, fc_cash_numeric),
    b(3824, "numeric_cash", 1, fc_numeric_cash),
];
