//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the scalar
//! SQL-callable functions in `event_trigger.c` whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! [`register_event_trigger_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them.
//! OIDs / nargs / strict / retset are transcribed exactly from `pg_proc.dat`.

use ::datum::Datum;
use ::types_error::PgResult;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

/// `pg_event_trigger_table_rewrite_oid` (event_trigger.c) — C:
/// `PG_RETURN_OID(currentEventTriggerState->table_rewrite_oid)`. Takes no
/// arguments; out-of-context guard lives in the owner body.
fn fc_pg_event_trigger_table_rewrite_oid(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let oid = crate::pg_event_trigger_table_rewrite_oid()?;
    Ok(Datum::from_oid(oid))
}

/// `pg_event_trigger_table_rewrite_reason` (event_trigger.c) — C:
/// `PG_RETURN_INT32(currentEventTriggerState->table_rewrite_reason)`. Takes no
/// arguments; out-of-context guard lives in the owner body.
fn fc_pg_event_trigger_table_rewrite_reason(
    _fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let reason = crate::pg_event_trigger_table_rewrite_reason()?;
    Ok(Datum::from_i32(reason))
}

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    func: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    (
        BuiltinFunction {
            foid,
            name: name.to_string(),
            nargs,
            strict,
            retset,
            func: None,
        },
        func,
    )
}

/// Register the scalar `event_trigger.c` builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/retset are from
/// `pg_proc.dat`; `strict` follows `Gen_fmgrtab.pl`'s `proisstrict // "t"`
/// default (true when `pg_proc.dat` omits `proisstrict`, as both rows do) — both
/// take 0 args and are not set-returning.
pub fn register_event_trigger_builtins() {
    fmgr_core::register_builtins_native([
        builtin(
            4566,
            "pg_event_trigger_table_rewrite_oid",
            0,
            true,
            false,
            fc_pg_event_trigger_table_rewrite_oid,
        ),
        builtin(
            4567,
            "pg_event_trigger_table_rewrite_reason",
            0,
            true,
            false,
            fc_pg_event_trigger_table_rewrite_reason,
        ),
    ]);
}
