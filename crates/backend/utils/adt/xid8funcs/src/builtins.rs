//! fmgr wrappers (`fc_*`) + `XID8FUNCS_BUILTINS` for fmgr-core. The txid_*
//! rows (int8/txid_snapshot) and the xid8/pg_snapshot rows share prosrc, as in
//! C's fmgr_builtins[].

use ::datum::{Datum, Varlena};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

use crate::SnapView;

fn arg_snap<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<SnapView<'a>> {
    // SAFETY: catalog arg i of these strict fns is a non-null pg_snapshot
    // varlena, live for the call.
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(SnapView::new(v.data()))
}

pub fn fc_pg_snapshot_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of the in-function is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let mcx = fcinfo.result_mcx();
    match crate::parse_snapshot(mcx, &s, esc)? {
        Some(v) => Ok(varlena_result(v)),
        None => Ok(varlena_result(crate::snapshot_image(mcx, 1, 1, &[])?)),
    }
}

pub fn fc_pg_snapshot_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let snap = arg_snap(fcinfo, 0)?;
    let mut out = crate::snapshot_out_bytes(fcinfo.result_mcx(), &snap)?;
    out.push(0);
    Ok(cstring_result(out))
}

pub fn fc_pg_snapshot_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(varlena_result(crate::snapshot_recv(fcinfo.result_mcx(), buf)?))
}

pub fn fc_pg_snapshot_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let snap = arg_snap(fcinfo, 0)?;
    let mcx = fcinfo.result_mcx();
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, snap.nxip())?;
    ::pqformat::pq_sendint64(&mut buf, snap.xmin())?;
    ::pqformat::pq_sendint64(&mut buf, snap.xmax())?;
    for i in 0..snap.nxip() as usize {
        ::pqformat::pq_sendint64(&mut buf, snap.xip(i))?;
    }
    Ok(varlena_result(::pqformat::pq_endtypsend(buf)))
}

pub fn fc_pg_current_xact_id(
    _flinfo: Option<&mut FmgrInfo>,
    _fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(Datum::from_u64(crate::pg_current_xact_id()?))
}

pub fn fc_pg_current_xact_id_if_assigned(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    match crate::pg_current_xact_id_if_assigned() {
        Some(fxid) => Ok(Datum::from_u64(fxid)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_current_snapshot(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    Ok(varlena_result(crate::pg_current_snapshot(fcinfo.result_mcx())?))
}

pub fn fc_pg_visible_in_snapshot(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let value = fcinfo.arg(0).as_u64();
    let snap = arg_snap(fcinfo, 1)?;
    Ok(Datum::from_bool(crate::is_visible_fxid(value, &snap)))
}

pub fn fc_pg_snapshot_xmin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u64(arg_snap(fcinfo, 0)?.xmin()))
}

pub fn fc_pg_snapshot_xmax(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u64(arg_snap(fcinfo, 0)?.xmax()))
}

struct SnapXips {
    xips: Vec<u64>,
}

pub fn fc_pg_snapshot_xip(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_snapshot_xip: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let snap = arg_snap(fcinfo, 0)?;
        let xips: Vec<u64> = (0..snap.nxip() as usize).map(|i| snap.xip(i)).collect();
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(SnapXips { xips }));
    }
    let fctx = ::funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let next = fctx
        .user_fctx
        .as_ref()
        .expect("pg_snapshot_xip: user_fctx set at first call")
        .downcast_ref::<SnapXips>()
        .expect("pg_snapshot_xip: user_fctx is SnapXips")
        .xips
        .get(idx)
        .copied();
    match next {
        Some(v) => Ok(::funcapi::srf_return_next(flinfo, fcinfo, Datum::from_u64(v))),
        None => Ok(::funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub fn fc_pg_xact_status(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let fxid = fcinfo.arg(0).as_u64();
    match crate::pg_xact_status(fxid)? {
        Some(status) => {
            let mcx = fcinfo.result_mcx();
            let mut image = ::mcx::vec_with_capacity_in(mcx, 4 + status.len())?;
            ::mcx::vec_append_bytes(&mut image, &[0u8; 4])?;
            ::mcx::vec_append_bytes(&mut image, status.as_bytes())?;
            Ok(varlena_result(Varlena::from_image(image)))
        }
        None => Ok(fcinfo.return_null()),
    }
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

// pg_export_snapshot (snapmgr.c): hosted with the snapshot builtins (the
// crate already owns the snapmgr dep).
pub fn fc_pg_export_snapshot(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let name = snapmgr::ExportSnapshot(&snapmgr::GetActiveSnapshot())?;
    let mcx = fcinfo.result_mcx();
    let mut image = ::mcx::vec_with_capacity_in(mcx, 4 + name.len())?;
    ::mcx::vec_append_bytes(&mut image, &[0u8; 4])?;
    ::mcx::vec_append_bytes(&mut image, name.as_bytes())?;
    Ok(varlena_result(Varlena::from_image(image)))
}

// pg_proc.dat rows over xid8funcs.c prosrcs: the txid_* legacy aliases
// (2939-2948, 3348, 3360) and the xid8/pg_snapshot rows (5055-5066).
pub const XID8FUNCS_BUILTINS: &[FmgrBuiltin] = &[
    b(2939, "pg_snapshot_in", 1, fc_pg_snapshot_in),
    b(2940, "pg_snapshot_out", 1, fc_pg_snapshot_out),
    b(2941, "pg_snapshot_recv", 1, fc_pg_snapshot_recv),
    b(2942, "pg_snapshot_send", 1, fc_pg_snapshot_send),
    b(2943, "pg_current_xact_id", 0, fc_pg_current_xact_id),
    b(2944, "pg_current_snapshot", 0, fc_pg_current_snapshot),
    b(2945, "pg_snapshot_xmin", 1, fc_pg_snapshot_xmin),
    b(2946, "pg_snapshot_xmax", 1, fc_pg_snapshot_xmax),
    srf(2947, "pg_snapshot_xip", 1, fc_pg_snapshot_xip),
    b(2948, "pg_visible_in_snapshot", 2, fc_pg_visible_in_snapshot),
    b(3348, "pg_current_xact_id_if_assigned", 0, fc_pg_current_xact_id_if_assigned),
    b(3360, "pg_xact_status", 1, fc_pg_xact_status),
    b(3809, "pg_export_snapshot", 0, fc_pg_export_snapshot),
    b(5055, "pg_snapshot_in", 1, fc_pg_snapshot_in),
    b(5056, "pg_snapshot_out", 1, fc_pg_snapshot_out),
    b(5057, "pg_snapshot_recv", 1, fc_pg_snapshot_recv),
    b(5058, "pg_snapshot_send", 1, fc_pg_snapshot_send),
    b(5059, "pg_current_xact_id", 0, fc_pg_current_xact_id),
    b(5060, "pg_current_xact_id_if_assigned", 0, fc_pg_current_xact_id_if_assigned),
    b(5061, "pg_current_snapshot", 0, fc_pg_current_snapshot),
    b(5062, "pg_snapshot_xmin", 1, fc_pg_snapshot_xmin),
    b(5063, "pg_snapshot_xmax", 1, fc_pg_snapshot_xmax),
    srf(5064, "pg_snapshot_xip", 1, fc_pg_snapshot_xip),
    b(5065, "pg_visible_in_snapshot", 2, fc_pg_visible_in_snapshot),
    b(5066, "pg_xact_status", 1, fc_pg_xact_status),
];
