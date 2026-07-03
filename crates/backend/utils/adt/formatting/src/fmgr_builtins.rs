//! fmgr wrappers (`fc_*`) + `FORMATTING_BUILTINS`. to_char/to_number/to_date/
//! to_timestamp on the result-mcx convention. All strict (pg_proc default).

use ::datum::Datum;
use ::numeric::Num;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::dch_entry;
use crate::num_entry;

pub fn fc_numeric_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null numeric/text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
    let mcx = fcinfo.result_mcx();
    let n = Num::from_payload(val.data());
    Ok(varlena_result(num_entry::numeric_to_char(mcx, n, fmt.data())?))
}

pub fn fc_int4_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1) };
    let v = fcinfo.arg_i32(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::int4_to_char(mcx, v, fmt.data())?))
}

pub fn fc_int8_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1) };
    let v = fcinfo.arg_i64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::int8_to_char(mcx, v, fmt.data())?))
}

pub fn fc_float4_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1) };
    let v = fcinfo.arg_f32(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::float4_to_char(mcx, v, fmt.data())?))
}

pub fn fc_float8_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1) };
    let v = fcinfo.arg_f64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::float8_to_char(mcx, v, fmt.data())?))
}

pub fn fc_numeric_to_number(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
    let mcx = fcinfo.result_mcx();
    // C returns NULL for empty/oversized fmt (PG_RETURN_NULL).
    match num_entry::numeric_to_number(mcx, val.data(), fmt.data())? {
        Some(img) => byref_result(mcx, img.as_bytes()),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_timestamp_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1) };
    let ts = fcinfo.arg_i64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(dch_entry::timestamp_to_char(mcx, ts, fmt.data())?))
}

pub fn fc_timestamptz_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1) };
    let ts = fcinfo.arg_i64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(dch_entry::timestamptz_to_char(mcx, ts, fmt.data())?))
}

pub fn fc_to_timestamp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_i64(dch_entry::to_timestamp(mcx, val.data(), fmt.data())?))
}

pub fn fc_to_date(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0), fcinfo.arg_varlena_packed(1)) };
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_i32(dch_entry::to_date(mcx, val.data(), fmt.data())?))
}

pub fn fc_interval_to_char(_f: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    panic!("interval to_char (interval_to_char) not ported");
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

pub const FORMATTING_BUILTINS: &[FmgrBuiltin] = &[
    b(1768, "interval_to_char", 2, fc_interval_to_char),
    b(1770, "timestamptz_to_char", 2, fc_timestamptz_to_char),
    b(1772, "numeric_to_char", 2, fc_numeric_to_char),
    b(1773, "int4_to_char", 2, fc_int4_to_char),
    b(1774, "int8_to_char", 2, fc_int8_to_char),
    b(1775, "float4_to_char", 2, fc_float4_to_char),
    b(1776, "float8_to_char", 2, fc_float8_to_char),
    b(1777, "numeric_to_number", 2, fc_numeric_to_number),
    b(1778, "to_timestamp", 2, fc_to_timestamp),
    b(1780, "to_date", 2, fc_to_date),
    b(2049, "timestamp_to_char", 2, fc_timestamp_to_char),
];
