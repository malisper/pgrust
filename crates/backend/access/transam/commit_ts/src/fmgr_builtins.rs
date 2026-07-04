use datum::Datum;
use types_core::TransactionIdIsNormal;
use types_error::{PgError, PgResult};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::{GetLatestCommitTsData, TransactionIdGetCommitTsData};

pub(crate) fn register_builtins() {
    fmgr_core::register_late_builtins(COMMIT_TS_BUILTINS);
}

static COMMIT_TS_BUILTINS: &[FmgrBuiltin] = &[
    FmgrBuiltin {
        foid: 3581,
        name: "pg_xact_commit_timestamp",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_xact_commit_timestamp,
    },
    FmgrBuiltin {
        foid: 3583,
        name: "pg_last_committed_xact",
        nargs: 0,
        strict: true,
        retset: false,
        func: fc_pg_last_committed_xact,
    },
    FmgrBuiltin {
        foid: 6168,
        name: "pg_xact_commit_timestamp_origin",
        nargs: 1,
        strict: true,
        retset: false,
        func: fc_pg_xact_commit_timestamp_origin,
    },
];

fn composite_result(
    flinfo: &FmgrInfo,
    fcinfo: &mut Fcinfo,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result has tupdesc");
    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, values, isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

pub fn fc_pg_xact_commit_timestamp(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let xid = fcinfo.arg(0).as_u32();
    match TransactionIdGetCommitTsData(xid)? {
        Some((ts, _nodeid)) => Ok(Datum::from_i64(ts)),
        None => {
            fcinfo.isnull = true;
            Ok(Datum::from_usize(0))
        }
    }
}

pub fn fc_pg_last_committed_xact(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_last_committed_xact: NULL flinfo");
    let (xid, ts, nodeid) = GetLatestCommitTsData()?;

    if !TransactionIdIsNormal(xid) {
        composite_result(flinfo, fcinfo, &[Datum::from_usize(0); 3], &[true; 3])
    } else {
        let values = [
            Datum::from_u32(xid),
            Datum::from_i64(ts),
            Datum::from_u32(nodeid as u32),
        ];
        composite_result(flinfo, fcinfo, &values, &[false; 3])
    }
}

pub fn fc_pg_xact_commit_timestamp_origin(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_xact_commit_timestamp_origin: NULL flinfo");
    let xid = fcinfo.arg(0).as_u32();

    match TransactionIdGetCommitTsData(xid)? {
        Some((ts, nodeid)) => {
            let values = [Datum::from_i64(ts), Datum::from_u32(nodeid as u32)];
            composite_result(flinfo, fcinfo, &values, &[false; 2])
        }
        None => composite_result(flinfo, fcinfo, &[Datum::from_usize(0); 2], &[true; 2]),
    }
}
