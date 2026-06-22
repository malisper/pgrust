//! `pg_cursor()` (OID 2511) registered as an executor-frame materialize-mode
//! set-returning function — the SRF backing the `pg_cursors` system view, which
//! lists every open cursor (portal) of the current session.
//!
//! The scan-and-project core (one `hash_seq_search` walk of the portal table
//! forming the 6-column rows) is ported in
//! [`backend_utils_mmgr_portalmem::pg_cursor`] (its rightful `portalmem.c`
//! owner). Here it is registered in the executor-frame SRF table via
//! [`register_srf`]; it bypasses the by-OID builtin registry whose tag-only
//! `resultinfo` cannot carry the live `ReturnSetInfo` (the WONTFIX dual-home,
//! same as [`crate::pg_event_trigger_dropped_objects`]).

use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `pg_cursor()` (OID 2511).
const PG_CURSOR: Oid = 2511;

/// Register `pg_cursor` in the executor-frame SRF table.
pub(crate) fn register_pg_cursor() {
    register_srf(PG_CURSOR, pg_cursor);
}

/// `pg_cursor(PG_FUNCTION_ARGS)` (portalmem.c) over the executor frame. The
/// whole protocol (`InitMaterializedSRF`, the per-portal 6-column projection,
/// the tuplestore append, returning `(Datum) 0`) lives in the owner crate.
fn pg_cursor<'mcx>(fcinfo: &mut FunctionCallInfoBaseData<'mcx>) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_cursor: fn_mcxt set by ExecMakeTableFunctionResult");
    backend_utils_mmgr_portalmem::pg_cursor(mcx, fcinfo)
}
