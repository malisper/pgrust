//! `pg_get_wait_events()` (OID 6318) registered as an executor-frame
//! materialize-mode set-returning function — the SRF backing the
//! `pg_wait_events` view.
//!
//! `wait_event_funcs.c`'s `pg_get_wait_events` is a materialize-mode SRF emitting
//! one `(type text, name text, description text)` row per wait event: every
//! built-in event from the generated `waitEventData[]` table followed by the
//! registered Extension and InjectionPoint custom events. The enumeration core
//! (the static `waitEventData[]` build + the custom-event walk) is ported in
//! [`waitevent::pg_get_wait_events`], which hands back a
//! `Vec<WaitEventRow>`.
//!
//! Here that core is driven over the executor frame in materialize mode: the row
//! set is fixed and known up front, so the whole tuplestore is filled once,
//! emitting the identical rows the C `tuplestore_putvalues` loop produces.
//! Registered from [`register_pg_get_wait_events`] (called by `init_seams`); it
//! bypasses the by-OID builtin registry whose tag-only `resultinfo` cannot carry
//! the live `ReturnSetInfo` (the WONTFIX dual-home).

use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use ::nodes::funcapi::MAT_SRF_USE_EXPECTED_DESC;
use types_tuple::heaptuple::Datum;

use ::funcapi::srf_support::{materialized_srf_putvalues, InitMaterializedSRF};

use crate::register_srf;

/// `pg_get_wait_events()` (OID 6318).
const PG_GET_WAIT_EVENTS: Oid = 6318;

/// Register `pg_get_wait_events` in the executor-frame SRF table.
pub(crate) fn register_pg_get_wait_events() {
    register_srf(PG_GET_WAIT_EVENTS, pg_get_wait_events);
}

/// `CStringGetTextDatum(s)` over the call's per-query context.
fn text_datum<'mcx>(mcx: Mcx<'mcx>, s: &str) -> PgResult<Datum<'mcx>> {
    varlena_seams::cstring_to_text_v::call(mcx, s)
}

/// `pg_get_wait_events(PG_FUNCTION_ARGS)` (wait_event_funcs.c) over the executor
/// frame.
fn pg_get_wait_events<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx: Mcx<'mcx> = fcinfo
        .fn_mcxt
        .expect("pg_get_wait_events: fn_mcxt set by ExecMakeTableFunctionResult");

    // The wait-event enumeration core (static table + custom events).
    let rows = waitevent::pg_get_wait_events()?;

    // C: InitMaterializedSRF(fcinfo, 0). The owned model takes the executor's
    // already-resolved `(text, text, text)` descriptor via
    // MAT_SRF_USE_EXPECTED_DESC.
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;

    let rsinfo = fcinfo
        .resultinfo
        .as_mut()
        .expect("pg_get_wait_events: InitMaterializedSRF establishes fcinfo->resultinfo");

    for row in &rows {
        let values = [
            text_datum(mcx, &row.type_)?,
            text_datum(mcx, &row.name)?,
            text_datum(mcx, &row.description)?,
        ];
        let nulls = [false, false, false];
        materialized_srf_putvalues(rsinfo, &values, &nulls)?;
    }

    // C: SRF_RETURN_DONE — the whole set is in the materialize tuplestore.
    fcinfo.isnull = true;
    Ok(Datum::null())
}
