//! fmgr wrappers (`fc_*`) + `FORMATTING_BUILTINS`. to_char/to_number/to_date/
//! to_timestamp on the result-mcx convention. All strict (pg_proc default).

use ::adt_timestamp::TIMESTAMP_NOT_FINITE;
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
use crate::tables::NUM_MAX_ITEM_SIZ;

pub fn fc_numeric_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null numeric/text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    // C's PG_GETARG_NUMERIC detoast: 1B-short images realign into the result mcx.
    let n = Num::from_payload(if val.is_short() {
        val.data_expanded(mcx)?
    } else {
        val.data()
    });
    Ok(varlena_result(num_entry::numeric_to_char(mcx, n, fmt.data())?))
}

pub fn fc_int4_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let v = fcinfo.arg_i32(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::int4_to_char(mcx, v, fmt.data())?))
}

pub fn fc_int8_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let v = fcinfo.arg_i64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::int8_to_char(mcx, v, fmt.data())?))
}

pub fn fc_float4_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let v = fcinfo.arg_f32(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::float4_to_char(mcx, v, fmt.data())?))
}

pub fn fc_float8_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let v = fcinfo.arg_f64(0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(num_entry::float8_to_char(mcx, v, fmt.data())?))
}

pub fn fc_numeric_to_number(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    // C: len <= 0 || len >= (INT_MAX)/NUM_MAX_ITEM_SIZ -> NULL.
    let len = fmt.data().len();
    if len == 0 || len >= (i32::MAX as usize) / NUM_MAX_ITEM_SIZ {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    let img = num_entry::numeric_to_number(mcx, val.data(), fmt.data())?;
    byref_result(mcx, img.as_bytes())
}

pub fn fc_timestamp_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let ts = fcinfo.arg_i64(0);
    if fmt.data().is_empty() || TIMESTAMP_NOT_FINITE(ts) {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(dch_entry::timestamp_to_char(mcx, fcinfo.fncollation, ts, fmt.data())?))
}

pub fn fc_timestamptz_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null text varlena.
    let fmt = unsafe { fcinfo.arg_varlena_packed(1)? };
    let ts = fcinfo.arg_i64(0);
    if fmt.data().is_empty() || TIMESTAMP_NOT_FINITE(ts) {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(dch_entry::timestamptz_to_char(mcx, fcinfo.fncollation, ts, fmt.data())?))
}

pub fn fc_to_timestamp(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_i64(dch_entry::to_timestamp(mcx, fcinfo.fncollation, val.data(), fmt.data())?))
}

pub fn fc_to_date(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — args 0/1 are non-null text varlenas.
    let (val, fmt) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_i32(dch_entry::to_date(mcx, fcinfo.fncollation, val.data(), fmt.data())?))
}

pub fn fc_interval_to_char(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null interval (typlen 16, typalign
    // d), arg 1 a non-null text varlena; both live for the call.
    let (it, fmt) = unsafe {
        let p = fcinfo.arg_ptr(0);
        (
            ::adt_datetime::Interval {
                time: (p as *const i64).read_unaligned(),
                day: (p.add(8) as *const i32).read_unaligned(),
                month: (p.add(12) as *const i32).read_unaligned(),
            },
            fcinfo.arg_varlena_packed(1)?,
        )
    };
    if fmt.data().is_empty() || it.not_finite() {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(dch_entry::interval_to_char(mcx, fcinfo.fncollation, &it, fmt.data())?))
}

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs: 2,
        strict: true,
        retset: false,
        func,
    }
}

pub const FORMATTING_BUILTINS: &[FmgrBuiltin] = &[
    b(1768, "interval_to_char", fc_interval_to_char),
    b(1770, "timestamptz_to_char", fc_timestamptz_to_char),
    b(1772, "numeric_to_char", fc_numeric_to_char),
    b(1773, "int4_to_char", fc_int4_to_char),
    b(1774, "int8_to_char", fc_int8_to_char),
    b(1775, "float4_to_char", fc_float4_to_char),
    b(1776, "float8_to_char", fc_float8_to_char),
    b(1777, "numeric_to_number", fc_numeric_to_number),
    b(1778, "to_timestamp", fc_to_timestamp),
    b(1780, "to_date", fc_to_date),
    b(2049, "timestamp_to_char", fc_timestamp_to_char),
];
