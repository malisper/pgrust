//! `pg_event_trigger_dropped_objects()` (OID 3566) registered as an
//! executor-frame materialize-mode set-returning function — the `sql_drop`
//! event-trigger function that lists every object the firing command dropped.
//!
//! The snapshot-and-project core (reading
//! `currentEventTriggerState->SQLDropList` and forming the 12-column rows) is
//! ported in [`event_trigger::pg_event_trigger_dropped_objects`]
//! (its rightful `event_trigger.c` owner). Here it is registered in the
//! executor-frame SRF table via [`register_srf`]; it bypasses the by-OID builtin
//! registry whose tag-only `resultinfo` cannot carry the live `ReturnSetInfo`
//! (the WONTFIX dual-home, same as [`crate::pg_lock_status`]).

use types_core::Oid;
use types_error::PgResult;
use ::nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::heaptuple::Datum;

use crate::register_srf;

/// `pg_event_trigger_dropped_objects()` (OID 3566).
const PG_EVENT_TRIGGER_DROPPED_OBJECTS: Oid = 3566;

/// Register `pg_event_trigger_dropped_objects` in the executor-frame SRF table.
pub(crate) fn register_pg_event_trigger_dropped_objects() {
    register_srf(
        PG_EVENT_TRIGGER_DROPPED_OBJECTS,
        pg_event_trigger_dropped_objects,
    );
}

/// `pg_event_trigger_dropped_objects(PG_FUNCTION_ARGS)` (event_trigger.c) over
/// the executor frame. The whole protocol (out-of-context guard,
/// `InitMaterializedSRF`, the per-`SQLDropObject` 12-column projection, the
/// tuplestore append, returning `(Datum) 0`) lives in the owner crate.
fn pg_event_trigger_dropped_objects<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_event_trigger_dropped_objects: fn_mcxt set by ExecMakeTableFunctionResult");
    event_trigger::pg_event_trigger_dropped_objects(mcx, fcinfo)
}
