//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `mcxtfuncs.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_mcxtfuncs_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs
//! / nargs / strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! `pg_get_backend_memory_contexts` is NOT registered here: it is a
//! set-returning function whose `ReturnSetInfo` / `InitMaterializedSRF`
//! protocol is part of the project-wide fmgr/Datum-layer deferral (its fmgr
//! entry point is a loud panic; the SRF result lane is not expressible at this
//! boundary).

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_INT32(i)` → `DatumGetInt32`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo
        .arg(i)
        .expect("mcxtfuncs fn: missing arg")
        .value
        .as_i32()
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_log_backend_memory_contexts(pid int4) -> bool`.
///
/// C: `int pid = PG_GETARG_INT32(0); ...; PG_RETURN_BOOL(result)`. The core
/// emits the WARNING/`PG_RETURN_BOOL(false)` paths internally (returning
/// `Ok(false)`); only a hard `ereport(ERROR)` surfaces as `Err`, which is
/// raised through the fmgr dispatch point.
fn fc_pg_log_backend_memory_contexts(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let pid = arg_int32(fcinfo, 0);
    match crate::pg_log_backend_memory_contexts(pid) {
        Ok(b) => ret_bool(b),
        Err(e) => raise(e),
    }
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
    func: fn(&mut FunctionCallInfoBaseData) -> Datum,
) -> BuiltinFunction {
    BuiltinFunction {
        foid,
        name: name.to_string(),
        nargs,
        strict,
        retset,
        func: Some(func),
    }
}

/// Register every expressible `mcxtfuncs.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from the generated `fmgrtab.c` (oid 4543: int4 → bool;
/// inherits `proisstrict BKI_DEFAULT(t)` so `strict = true`; no `proretset` so
/// `retset = false`).
pub fn register_mcxtfuncs_builtins() {
    backend_utils_fmgr_core::register_builtins([builtin(
        4543,
        "pg_log_backend_memory_contexts",
        1,
        true,
        false,
        fc_pg_log_backend_memory_contexts,
    )]);
}
