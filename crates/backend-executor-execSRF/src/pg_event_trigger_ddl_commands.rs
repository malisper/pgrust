//! `pg_event_trigger_ddl_commands()` (OID 4568) registered as an
//! executor-frame materialize-mode set-returning function — the
//! `ddl_command_end` event-trigger function that lists every DDL command the
//! firing command ran.
//!
//! The snapshot-and-project core (reading
//! `currentEventTriggerState->commandList` and forming the 9-column rows) is
//! ported in [`backend_commands_event_trigger::pg_event_trigger_ddl_commands`]
//! (its rightful `event_trigger.c` owner). Here it is registered in the
//! executor-frame SRF table via [`register_srf`]; it bypasses the by-OID builtin
//! registry whose tag-only `resultinfo` cannot carry the live `ReturnSetInfo`
//! (the WONTFIX dual-home, same as [`crate::pg_event_trigger_dropped_objects`]).

use types_core::Oid;
use types_error::PgResult;
use types_nodes::fmgr::FunctionCallInfoBaseData;
use types_tuple::backend_access_common_heaptuple::Datum;

use crate::register_srf;

/// `pg_event_trigger_ddl_commands()` (OID 4568).
const PG_EVENT_TRIGGER_DDL_COMMANDS: Oid = 4568;

/// Register `pg_event_trigger_ddl_commands` in the executor-frame SRF table.
pub(crate) fn register_pg_event_trigger_ddl_commands() {
    register_srf(PG_EVENT_TRIGGER_DDL_COMMANDS, pg_event_trigger_ddl_commands);
}

/// `pg_event_trigger_ddl_commands(PG_FUNCTION_ARGS)` (event_trigger.c) over the
/// executor frame. The whole protocol (out-of-context guard,
/// `InitMaterializedSRF`, the per-`CollectedCommand` 9-column projection, the
/// tuplestore append, returning `(Datum) 0`) lives in the owner crate.
fn pg_event_trigger_ddl_commands<'mcx>(
    fcinfo: &mut FunctionCallInfoBaseData<'mcx>,
) -> PgResult<Datum<'mcx>> {
    let mcx = fcinfo
        .fn_mcxt
        .expect("pg_event_trigger_ddl_commands: fn_mcxt set by ExecMakeTableFunctionResult");
    backend_commands_event_trigger::pg_event_trigger_ddl_commands(mcx, fcinfo)
}
