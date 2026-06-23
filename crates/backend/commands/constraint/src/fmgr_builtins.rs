//! The fmgr builtin layer for `unique_key_recheck` (`constraint.c`).
//!
//! `unique_key_recheck` is the AFTER ROW trigger function (pg_proc OID 1250,
//! `F_UNIQUE_KEY_RECHECK`) that performs the deferred uniqueness / exclusion
//! check.  C invokes it through `fmgr` with the `TriggerData` riding on
//! `fcinfo->context`; the body returns `PointerGetDatum(NULL)`.
//!
//! Without this registration the deferrable-unique / exclusion triggers that
//! `index_constraints` installs (their `tgfoid` is `F_UNIQUE_KEY_RECHECK`) would
//! dispatch to a null `fn_addr`, so a deferred unique/exclusion check queued at
//! INSERT/UPDATE could never fire at end-of-statement / commit / `SET
//! CONSTRAINTS`.
//!
//! Like `ri_triggers.c`'s procs, the per-call `TriggerData` rides the trigger
//! manager's thread-local side-channel (installed by `exec_call_trigger_func`);
//! `called_as_trigger` is read through the trigger seam, and the value core
//! reads the rest through its trigger seams.  OID / nargs / strict / retset are
//! transcribed from `pg_proc.dat` (`pronargs 0`, strict by default, not retset),
//! matching the `fmgr_builtins[]` canonical row.

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};
use ::types_ri_triggers::TriggerDataRef;

use trigger_seams as trigger;

/// `F_UNIQUE_KEY_RECHECK` (`fmgroids.h`) — pg_proc OID of `unique_key_recheck`.
const F_UNIQUE_KEY_RECHECK: u32 = 1250;

/// The current-`TriggerData` marker handle (the firing path mints the same
/// value; the trigger-data accessors read the thread-local, not the handle).
const CURRENT_TRIGGER: TriggerDataRef = TriggerDataRef(1);

/// fmgr native adapter for `unique_key_recheck`: build the scratch parent
/// context (C's `CurrentMemoryContext` at the trigger call), read
/// `CALLED_AS_TRIGGER(fcinfo)` through the trigger seam, forward to the value
/// core, and return `PointerGetDatum(NULL)` (a null `Datum`).
fn fc_unique_key_recheck(_fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let parent = mcx::MemoryContext::new("unique_key_recheck fmgr scratch");
    let called_as_trigger = trigger::called_as_trigger::call(CURRENT_TRIGGER);
    crate::unique_key_recheck(&parent, called_as_trigger, CURRENT_TRIGGER)
}

/// Register `unique_key_recheck` (C: its `fmgr_builtins[]` row). Called from
/// this crate's [`crate::init_seams`].
pub fn register_constraint_builtins() {
    fmgr_core::register_builtins_native([(
        BuiltinFunction {
            foid: F_UNIQUE_KEY_RECHECK,
            name: "unique_key_recheck".to_string(),
            // pg_proc.dat: proargtypes '' (nargs 0), no explicit proisstrict
            // (defaults 't' => strict), no proretset (false). Must match the
            // `fmgr_builtins[]` canonical row so the completeness guard does not
            // flag a metadata mismatch.
            nargs: 0,
            strict: true,
            retset: false,
            func: None,
        },
        fc_unique_key_recheck as PgFnNative,
    )]);
}
