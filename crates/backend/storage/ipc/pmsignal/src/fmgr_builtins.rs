//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! backend-signaling functions of `signalfuncs.c`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate::signalfuncs`], and
//! writes back the result word. [`register_pmsignal_builtins`] registers every
//! row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat` (all four rows take the defaults
//! `proisstrict => 't'`, `proretset => 'f'`; only their `proargtypes` differ).
//!
//! All four cores return `PgResult<bool>`; an `Err` is raised through the one
//! dispatch point every builtin crosses (`invoke_pgfunction`'s `catch_unwind`).

use datum::Datum;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("signalfunc: missing arg").value.as_i32()
}

/// `PG_GETARG_INT64(i)` → `DatumGetInt64`: arg `i`'s word as a signed 64-bit
/// integer. `int8` is pass-by-value on the 64-bit fmgr boundary.
#[inline]
fn arg_int64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("signalfunc: missing arg").value.as_i64()
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// C: `Datum pg_cancel_backend(PG_FUNCTION_ARGS)` — `PG_GETARG_INT32(0)` is the
/// target pid; result is `bool`.
fn fc_pg_cancel_backend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let pid = arg_int32(fcinfo, 0);
    Ok(ret_bool(crate::pg_cancel_backend(pid)?))
}

/// C: `Datum pg_terminate_backend(PG_FUNCTION_ARGS)` — `PG_GETARG_INT32(0)` is
/// the target pid, `PG_GETARG_INT64(1)` the timeout; result is `bool`.
fn fc_pg_terminate_backend(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let pid = arg_int32(fcinfo, 0);
    let timeout = arg_int64(fcinfo, 1);
    Ok(ret_bool(crate::pg_terminate_backend(pid, timeout)?))
}

/// C: `Datum pg_reload_conf(PG_FUNCTION_ARGS)` — no arguments; result is `bool`.
fn fc_pg_reload_conf(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::pg_reload_conf()?))
}

/// C: `Datum pg_rotate_logfile(PG_FUNCTION_ARGS)` — no arguments; result is
/// `bool`.
fn fc_pg_rotate_logfile(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::pg_rotate_logfile()?))
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

/// Register every `signalfuncs.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat` (all default `proisstrict => 't'`,
/// `proretset => 'f'`).
pub fn register_pmsignal_builtins() {
    fmgr_core::register_builtins_native([
        // proargtypes 'int4'      -> nargs 1
        builtin(2171, "pg_cancel_backend", 1, true, false, fc_pg_cancel_backend),
        // proargtypes 'int4 int8' -> nargs 2
        builtin(
            2096,
            "pg_terminate_backend",
            2,
            true,
            false,
            fc_pg_terminate_backend,
        ),
        // proargtypes ''          -> nargs 0
        builtin(2621, "pg_reload_conf", 0, true, false, fc_pg_reload_conf),
        builtin(2622, "pg_rotate_logfile", 0, true, false, fc_pg_rotate_logfile),
    ]);
}
