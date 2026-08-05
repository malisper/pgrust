//! aio_funcs.c. C's pg_get_aios walks pgaio_ctl->io_handles, the
//! shared-memory table of PgAioHandles across backends. The uring transport
//! keeps per-ring in-flight counts with no handle table, so the view has no
//! rows to project; matching C at rest (no IO in flight at query time).

use types_error::PgResult;

const PG_GET_AIOS_COLS: usize = 15;

pub fn fc_pg_get_aios(
    flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> PgResult<datum::Datum> {
    let flinfo = flinfo.expect("pg_get_aios: resolved FmgrInfo required");
    // SAFETY: executor arms es_query_cxt pre-call; it outlives this frame.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    debug_assert_eq!(srf.tupdesc.natts as usize, PG_GET_AIOS_COLS);
    Ok(srf.finish(fcinfo))
}

pub const AIO_FUNCS_BUILTINS: &[types_fmgr::FmgrBuiltin] = &[types_fmgr::FmgrBuiltin {
    foid: 6399,
    name: "pg_get_aios",
    nargs: 0,
    strict: true,
    retset: true,
    func: fc_pg_get_aios,
}];
