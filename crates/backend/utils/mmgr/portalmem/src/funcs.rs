use ::datum::Datum;
use ::types_error::PgResult;
use ::types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

pub fn fc_pg_cursor(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_cursor: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts, 6);

    for row in crate::pg_cursor_rows(mcx)?.iter() {
        let nulls = [false; 6];
        let values = [
            varlena_result(varlena::cstring_to_text(mcx, row.name.as_bytes())?),
            varlena_result(varlena::cstring_to_text(mcx, row.statement.as_bytes())?),
            Datum::from_bool(row.is_holdable),
            Datum::from_bool(row.is_binary),
            Datum::from_bool(row.is_scrollable),
            Datum::from_i64(row.creation_time),
        ];
        srf.putvalues(&values, &nulls)?;
    }
    Ok(srf.finish(fcinfo))
}

pub const PORTALMEM_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 2511,
    name: "pg_cursor",
    nargs: 0,
    strict: true,
    retset: true,
    func: fc_pg_cursor,
}];
