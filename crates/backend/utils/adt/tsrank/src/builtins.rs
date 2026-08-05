use ::adt_tsvector_core::builtins::{arg_tsquery, arg_tsvector};
use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx};
use ::types_core::Oid;
use ::types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED,
};
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

use crate::rank::{calc_rank, DEFAULT_WEIGHTS, DEF_NORM_METHOD, NUM_WEIGHTS};
use crate::rank_cd::calc_rank_cd;

// getWeights over a float4[] arg rebuilt as a full 4B-header image.
fn arg_weights(mcx: Mcx<'_>, fcinfo: &Fcinfo, i: usize) -> PgResult<[f32; NUM_WEIGHTS]> {
    // SAFETY: catalog arg is a non-null live float4[] varlena.
    let pv = unsafe { fcinfo.arg_varlena_packed(i) }?;
    let payload = if pv.is_short() { pv.data_expanded(mcx)? } else { pv.data() };
    let mut full = vec_with_capacity_in(mcx, payload.len() + 4)?;
    full.extend_from_slice(&[0u8; 4]);
    ::mcx::vec_append_bytes(&mut full, payload)?;
    let img = full.leak();

    let ndim = i32::from_ne_bytes(img[4..8].try_into().unwrap());
    if ndim != 1 {
        return Err(PgError::error("array of weight must be one-dimensional")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .into());
    }
    let nitems = i32::from_ne_bytes(img[16..20].try_into().unwrap());
    if nitems < NUM_WEIGHTS as i32 {
        return Err(PgError::error("array of weight is too short")
            .with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .into());
    }
    if ::arrayfuncs::array_contains_nulls(img) {
        return Err(PgError::error("array of weight must not contain nulls")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED)
            .into());
    }
    let dataoffset = i32::from_ne_bytes(img[8..12].try_into().unwrap()) as usize;
    let data_at = if dataoffset != 0 {
        dataoffset
    } else {
        // ARR_OVERHEAD_NONULLS(1) = MAXALIGN(16 + 8)
        24
    };
    let mut ws = [0f32; NUM_WEIGHTS];
    for (k, w) in ws.iter_mut().enumerate() {
        let off = data_at + k * 4;
        let v = f32::from_ne_bytes(img[off..off + 4].try_into().unwrap());
        *w = if v >= 0.0 { v } else { DEFAULT_WEIGHTS[k] };
        if *w > 1.0 {
            return Err(PgError::error("weight out of range")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .into());
        }
    }
    Ok(ws)
}

pub fn fc_ts_rank_wttf(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let w = arg_weights(mcx, fcinfo, 0)?;
    let txt = arg_tsvector(fcinfo, 1)?;
    let query = arg_tsquery(fcinfo, 2)?;
    let method = fcinfo.arg_i32(3);
    Ok(Datum::from_f32(calc_rank(mcx, &w, txt, query, method)?))
}

pub fn fc_ts_rank_wtt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let w = arg_weights(mcx, fcinfo, 0)?;
    let txt = arg_tsvector(fcinfo, 1)?;
    let query = arg_tsquery(fcinfo, 2)?;
    Ok(Datum::from_f32(calc_rank(mcx, &w, txt, query, DEF_NORM_METHOD)?))
}

pub fn fc_ts_rank_ttf(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let txt = arg_tsvector(fcinfo, 0)?;
    let query = arg_tsquery(fcinfo, 1)?;
    let method = fcinfo.arg_i32(2);
    Ok(Datum::from_f32(calc_rank(mcx, &DEFAULT_WEIGHTS, txt, query, method)?))
}

pub fn fc_ts_rank_tt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let txt = arg_tsvector(fcinfo, 0)?;
    let query = arg_tsquery(fcinfo, 1)?;
    Ok(Datum::from_f32(calc_rank(mcx, &DEFAULT_WEIGHTS, txt, query, DEF_NORM_METHOD)?))
}

pub fn fc_ts_rankcd_wttf(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let w = arg_weights(mcx, fcinfo, 0)?;
    let txt = arg_tsvector(fcinfo, 1)?;
    let query = arg_tsquery(fcinfo, 2)?;
    let method = fcinfo.arg_i32(3);
    Ok(Datum::from_f32(calc_rank_cd(mcx, &w, txt, query, method)?))
}

pub fn fc_ts_rankcd_wtt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let w = arg_weights(mcx, fcinfo, 0)?;
    let txt = arg_tsvector(fcinfo, 1)?;
    let query = arg_tsquery(fcinfo, 2)?;
    Ok(Datum::from_f32(calc_rank_cd(mcx, &w, txt, query, DEF_NORM_METHOD)?))
}

pub fn fc_ts_rankcd_ttf(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let txt = arg_tsvector(fcinfo, 0)?;
    let query = arg_tsquery(fcinfo, 1)?;
    let method = fcinfo.arg_i32(2);
    Ok(Datum::from_f32(calc_rank_cd(mcx, &DEFAULT_WEIGHTS, txt, query, method)?))
}

pub fn fc_ts_rankcd_tt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: the armed result mcx outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let txt = arg_tsvector(fcinfo, 0)?;
    let query = arg_tsquery(fcinfo, 1)?;
    Ok(Datum::from_f32(calc_rank_cd(mcx, &DEFAULT_WEIGHTS, txt, query, DEF_NORM_METHOD)?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const TSRANK_BUILTINS: &[FmgrBuiltin] = &[
    b(3703, "ts_rank_wttf", 4, fc_ts_rank_wttf),
    b(3704, "ts_rank_wtt", 3, fc_ts_rank_wtt),
    b(3705, "ts_rank_ttf", 3, fc_ts_rank_ttf),
    b(3706, "ts_rank_tt", 2, fc_ts_rank_tt),
    b(3707, "ts_rankcd_wttf", 4, fc_ts_rankcd_wttf),
    b(3708, "ts_rankcd_wtt", 3, fc_ts_rankcd_wtt),
    b(3709, "ts_rankcd_ttf", 3, fc_ts_rankcd_ttf),
    b(3710, "ts_rankcd_tt", 2, fc_ts_rankcd_tt),
];
