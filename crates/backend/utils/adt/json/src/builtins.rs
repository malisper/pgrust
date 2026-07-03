//! fmgr wrappers (`fc_*`) + the `JSON_BUILTINS` table for fmgr-core.
//! json_in/out/recv/send only; the rest of json.c is on separate lanes and
//! stays loud through fmgr-core's unported-OID panic.

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

pub fn fc_json_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of json_in is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let had_esc = esc.is_some();
    match crate::json_in(mcx, s, esc)? {
        Some(v) => Ok(varlena_result(v)),
        None if had_esc => Ok(Datum::null()),
        None => panic!("json_in: soft-error escape without an escontext"),
    }
}

pub fn fc_json_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(cstring_result(crate::json_out(mcx, payload)?))
}

pub fn fc_json_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of json_recv is a live &mut StringInfo (internal ABI).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::json_recv(mcx, buf)?))
}

pub fn fc_json_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::json_send(mcx, payload)?))
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

// pg_proc.dat: json_in/out/recv/send, all proisstrict, none retset.
pub const JSON_BUILTINS: &[FmgrBuiltin] = &[
    b(321, "json_in", 1, fc_json_in),
    b(322, "json_out", 1, fc_json_out),
    b(323, "json_recv", 1, fc_json_recv),
    b(324, "json_send", 1, fc_json_send),
];
