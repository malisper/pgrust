//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions of `trigger.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_trigger_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs / nargs
//! / strict / retset are transcribed exactly from `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Result writers.
// ---------------------------------------------------------------------------

/// `PG_RETURN_INT32(v)`: the result word for an `int4` return.
#[inline]
fn ret_int32(v: i32) -> Datum {
    Datum::from_i32(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_trigger_depth()` (trigger.c:6719). No arguments; returns the current
/// trigger recursion depth as `int4`.
fn fc_pg_trigger_depth(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_int32(crate::firing::pg_trigger_depth()))
}

// ---------------------------------------------------------------------------
// Registration.
// ---------------------------------------------------------------------------

fn builtin(
    foid: u32,
    name: &str,
    nargs: i16,
    strict: bool,
    retset: bool,
    native: PgFnNative,
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
        native,
    )
}

/// Register every scalar `trigger.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset transcribed
/// from `pg_proc.dat` (pg_trigger_depth: nargs 0, not strict, not retset).
pub fn register_trigger_builtins() {
    backend_utils_fmgr_core::register_builtins_native([builtin(
        3163,
        "pg_trigger_depth",
        0,
        true,
        false,
        fc_pg_trigger_depth,
    )]);
}
