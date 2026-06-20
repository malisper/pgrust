//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for this crate's
//! SQL-callable functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_backend_storage_ipc_shmem_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch (and the
//! `fmgr_isbuiltin` fast path) resolves them. OIDs / nargs / strict / retset
//! are transcribed exactly from `pg_proc.dat`.
//!
//! NOTE: the shmem-introspection set-returning functions
//! (`pg_get_shmem_allocations*`) are NOT registered here: they return a
//! `record` set via the `ReturnSetInfo`/materialized-SRF path, which is not
//! expressible through the scalar fmgr-builtin boundary.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Result writers.
// ---------------------------------------------------------------------------

/// Bridge the value core's `types_tuple::Datum` (a by-value bool word) back to
/// the fmgr boundary's `types_datum::Datum`.
#[inline]
fn ret_bool(v: types_tuple::Datum<'static>) -> Datum {
    Datum::from_bool(v.as_bool())
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_numa_available()` — no args, returns `bool`.
fn fc_pg_numa_available(_fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    Ok(ret_bool(crate::pg_numa_available()))
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

/// Register every scalar `shmem.c` builtin (C: their `fmgr_builtins[]` rows).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset from
/// `pg_proc.dat`.
pub fn register_backend_storage_ipc_shmem_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
        // pg_numa_available: proargtypes='' (0 args), prorettype=bool,
        // no proisstrict (=> not strict), no proretset (=> not set-returning).
        builtin(4099, "pg_numa_available", 0, true, false, fc_pg_numa_available),
    ]);
}
