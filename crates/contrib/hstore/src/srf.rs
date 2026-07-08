//! skeys / svals / each (hstore_op.c) — C runs these value-per-call; here
//! they materialize through funcapi::InitMaterializedSRF (both FunctionScan
//! and ProjectSet allow SFRM_Materialize).

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::arg_hstore;

pub fn fc_hstore_skeys(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_skeys: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: catalog arg is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for i in 0..hs.count() {
        let d = varlena_result(varlena::cstring_to_text(mcx, hs.key(i))?);
        srf.putvalues(&[d], &[false])?;
    }
    Ok(srf.finish(fcinfo))
}

pub fn fc_hstore_svals(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_svals: resolved FmgrInfo required");
    // SAFETY: as fc_hstore_skeys.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: catalog arg is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for i in 0..hs.count() {
        if hs.val_isnull(i) {
            srf.putvalues(&[Datum::null()], &[true])?;
        } else {
            let d = varlena_result(varlena::cstring_to_text(mcx, hs.val(i))?);
            srf.putvalues(&[d], &[false])?;
        }
    }
    Ok(srf.finish(fcinfo))
}

pub fn fc_hstore_each(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_each: resolved FmgrInfo required");
    // SAFETY: as fc_hstore_skeys.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    // SAFETY: catalog arg is non-null (strict fn).
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for i in 0..hs.count() {
        let key = varlena_result(varlena::cstring_to_text(mcx, hs.key(i))?);
        if hs.val_isnull(i) {
            srf.putvalues(&[key, Datum::null()], &[false, true])?;
        } else {
            let val = varlena_result(varlena::cstring_to_text(mcx, hs.val(i))?);
            srf.putvalues(&[key, val], &[false, false])?;
        }
    }
    Ok(srf.finish(fcinfo))
}
