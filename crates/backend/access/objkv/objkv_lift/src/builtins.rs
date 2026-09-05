//! `LANGUAGE internal` entry points. Reserved-range oids, no C counterpart;
//! created on demand:
//!
//! ```sql
//! CREATE FUNCTION pgrust_objkv_lift()        RETURNS text AS 'pgrust_objkv_lift'        LANGUAGE internal;
//! CREATE FUNCTION pgrust_objkv_lift_verify() RETURNS text AS 'pgrust_objkv_lift_verify' LANGUAGE internal;
//! CREATE FUNCTION pgrust_objkv_lift_finish() RETURNS text AS 'pgrust_objkv_lift_finish' LANGUAGE internal;
//! ```
use ::datum::Datum;
use ::types_error::PgResult;
use ::types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

pub const OBJKV_LIFT_FOID: ::types_core::Oid = 9010;
pub const OBJKV_LIFT_VERIFY_FOID: ::types_core::Oid = 9011;
pub const OBJKV_LIFT_FINISH_FOID: ::types_core::Oid = 9012;

fn run(
    fcinfo: &mut Fcinfo,
    f: fn(::mcx::Mcx<'_>) -> PgResult<String>,
) -> PgResult<Datum> {
    // SAFETY: the caller arms the result context before the call, as the
    // other reserved-range builtins document.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let text = ::varlena::cstring_to_text(mcx, f(mcx)?.as_bytes())?;
    Ok(varlena_result(text))
}

fn fc_lift(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    run(fcinfo, crate::lift)
}
fn fc_verify(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    run(fcinfo, crate::verify)
}
fn fc_finish(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    run(fcinfo, crate::finish)
}

pub static LIFT_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin { foid: OBJKV_LIFT_FOID, name: "pgrust_objkv_lift", nargs: 0, strict: false, retset: false, func: fc_lift },
    FmgrBuiltin { foid: OBJKV_LIFT_VERIFY_FOID, name: "pgrust_objkv_lift_verify", nargs: 0, strict: false, retset: false, func: fc_verify },
    FmgrBuiltin { foid: OBJKV_LIFT_FINISH_FOID, name: "pgrust_objkv_lift_finish", nargs: 0, strict: false, retset: false, func: fc_finish },
];
