//! skeys / svals / each (hstore_op.c) — C runs these value-per-call; here
//! they materialize through funcapi::InitMaterializedSRF (both FunctionScan
//! and ProjectSet allow SFRM_Materialize).

use datum::Datum;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::arg_hstore;

fn owned_pairs(fcinfo: &Fcinfo) -> PgResult<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
    // SAFETY: catalog arg is non-null (strict fn). Owned copies release the
    // fcinfo borrow before InitMaterializedSRF takes it mutably.
    let hs = unsafe { arg_hstore(fcinfo, 0)? };
    Ok((0..hs.count())
        .map(|i| {
            (
                hs.key(i).to_vec(),
                (!hs.val_isnull(i)).then(|| hs.val(i).to_vec()),
            )
        })
        .collect())
}

pub fn fc_hstore_skeys(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_skeys: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let pairs = owned_pairs(fcinfo)?;
    // Scalar SETOF: the stored tupdesc comes from the caller's expectedDesc
    // (plain-flags InitMaterializedSRF requires a row type).
    let mut srf =
        funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, funcapi::MAT_SRF_USE_EXPECTED_DESC)?;
    for (key, _) in &pairs {
        let d = varlena_result(varlena::cstring_to_text(mcx, key)?);
        srf.putvalues(&[d], &[false])?;
    }
    Ok(srf.finish(fcinfo))
}

pub fn fc_hstore_svals(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_svals: resolved FmgrInfo required");
    // SAFETY: as fc_hstore_skeys.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let pairs = owned_pairs(fcinfo)?;
    let mut srf =
        funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, funcapi::MAT_SRF_USE_EXPECTED_DESC)?;
    for (_, val) in &pairs {
        match val {
            Some(v) => {
                let d = varlena_result(varlena::cstring_to_text(mcx, v)?);
                srf.putvalues(&[d], &[false])?;
            }
            None => srf.putvalues(&[Datum::null()], &[true])?,
        }
    }
    Ok(srf.finish(fcinfo))
}

pub fn fc_hstore_each(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("hstore_each: resolved FmgrInfo required");
    // SAFETY: as fc_hstore_skeys.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let pairs = owned_pairs(fcinfo)?;
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    for (k, val) in &pairs {
        let key = varlena_result(varlena::cstring_to_text(mcx, k)?);
        match val {
            Some(v) => {
                let vd = varlena_result(varlena::cstring_to_text(mcx, v)?);
                srf.putvalues(&[key, vd], &[false, false])?;
            }
            None => srf.putvalues(&[key, Datum::null()], &[false, true])?,
        }
    }
    Ok(srf.finish(fcinfo))
}
