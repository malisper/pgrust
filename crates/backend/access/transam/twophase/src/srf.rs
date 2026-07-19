use datum::Datum;
use types_core::{OIDOID, RECORDOID, TEXTOID, TIMESTAMPTZOID, XIDOID};
use types_error::PgResult;
use types_fmgr::{byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

use crate::finish::prepared_xact_rows;

pub(crate) fn register_builtins() {
    fmgr_core::register_late_builtins(TWOPHASE_BUILTINS);
}

static TWOPHASE_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 1065,
    name: "pg_prepared_xact",
    nargs: 0,
    strict: true,
    retset: true,
    func: fc_pg_prepared_xact,
}];

struct PreparedRows {
    tuples: Vec<Vec<u8>>,
}

fn collect_rows(fcinfo: &Fcinfo) -> PgResult<PreparedRows> {
    let mcx = fcinfo.result_mcx();
    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 5)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("transaction"), XIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 2, Some("gid"), TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 3, Some("prepared"), TIMESTAMPTZOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 4, Some("ownerid"), OIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 5, Some("dbid"), OIDOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // C: BlessTupleDesc — register the anonymous record typmod so the
    // function scan (and record_out) can resolve the returned datums.
    // Dormant in regress (max_prepared_transactions=0 keeps the view empty);
    // load-bearing on a promoted standby with file-restored prepared xacts
    // (009_twophase #12: 'record type has not been registered').
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let rows = prepared_xact_rows();
    let mut tuples = Vec::with_capacity(rows.len());
    for row in &rows {
        let gid = varlena_result(varlena::cstring_to_text(mcx, row.gid.as_bytes())?);
        let values = [
            Datum::from_u32(row.transaction),
            gid,
            Datum::from_i64(row.prepared),
            Datum::from_u32(row.ownerid),
            Datum::from_u32(row.dbid),
        ];
        let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &[false; 5])?;
        tuples.push(tuple.image().to_vec());
    }
    Ok(PreparedRows { tuples })
}

pub fn fc_pg_prepared_xact(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_prepared_xact: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let rows = collect_rows(fcinfo)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_prepared_xact: rows set at first call")
        .downcast_ref::<PreparedRows>()
        .expect("pg_prepared_xact: user_fctx is PreparedRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}
