//! pg_get_multixact_members (multixact.c) — hosted apart from the multixact
//! crate because fmgr_core's dep tree reaches multixact (adt_scalar).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use datum::Datum;
use types_core::{RECORDOID, TEXTOID, XIDOID};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::{byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use multixact::{mxstatus_to_string, FirstMultiXactId, GetMultiXactIdMembers};

pub fn register_builtins() {
    fmgr_core::register_late_builtins(MULTIXACTFUNCS_BUILTINS);
}

static MULTIXACTFUNCS_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 3819,
    name: "pg_get_multixact_members",
    nargs: 1,
    strict: true,
    retset: true,
    func: fc_pg_get_multixact_members,
}];

struct MemberRows {
    tuples: Vec<Vec<u8>>,
}

fn collect_rows(fcinfo: &Fcinfo, mxid: u32) -> PgResult<MemberRows> {
    let mcx = fcinfo.result_mcx();
    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 2)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("xid"), XIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 2, Some("mode"), TEXTOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // C: BuildTupleFromCStrings over get_call_result_type's blessed tupdesc.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut tuples: Vec<Vec<u8>> = Vec::new();
    let mut form_err: Option<Box<PgError>> = None;
    GetMultiXactIdMembers(mxid, false, false, &mut |members| {
        for m in members {
            let mode = match varlena::cstring_to_text(mcx, mxstatus_to_string(m.status).as_bytes())
            {
                Ok(t) => t,
                Err(e) => {
                    form_err.get_or_insert(e);
                    return;
                }
            };
            let values =
                [Datum::from_u32(m.xid), Datum::from_usize(mode.as_bytes().as_ptr() as usize)];
            match heaptuple::heap_form_tuple(mcx, &desc, &values, &[false; 2]) {
                Ok(tuple) => tuples.push(tuple.image().to_vec()),
                Err(e) => {
                    form_err.get_or_insert(e);
                    return;
                }
            }
        }
    })?;
    if let Some(e) = form_err {
        return Err(e);
    }
    Ok(MemberRows { tuples })
}

pub fn fc_pg_get_multixact_members(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_multixact_members: NULL flinfo");
    let mxid = fcinfo.arg(0).as_u32();
    if mxid < FirstMultiXactId {
        return Err(Box::new(
            PgError::error(format!("invalid MultiXactId: {mxid}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    if !flinfo.has_fn_extra() {
        let rows = collect_rows(fcinfo, mxid)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_get_multixact_members: rows set at first call")
        .downcast_ref::<MemberRows>()
        .expect("pg_get_multixact_members: user_fctx is MemberRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use types_fmgr::LocalFcinfo;

    #[test]
    fn rows_match_canonical() {
        fmgr_core::assert_rows_match_canonical(MULTIXACTFUNCS_BUILTINS);
    }

    // C: mxid < FirstMultiXactId => ERRCODE_INVALID_PARAMETER_VALUE
    // "invalid MultiXactId: %u".
    #[test]
    fn invalid_multixactid_is_22023() {
        let mut fci = LocalFcinfo::<1>::new(0);
        fci.set_arg(0, Datum::from_u32(0));
        let mut flinfo = types_fmgr::FmgrInfo::unresolved();
        let err = fc_pg_get_multixact_members(Some(&mut flinfo), &mut fci).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    }
}
