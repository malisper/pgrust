//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions defined in `jit.c`.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_jit_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs / nargs
//! / strict / retset are transcribed exactly from `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

/// A scratch context for cores that take an `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("jit fmgr scratch")
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

/// `pg_jit_available()` (OID 315): no args, returns `bool`.
fn fc_pg_jit_available(_fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    match crate::pg_jit_available(m.mcx()) {
        Ok(d) => d,
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

/// Register every `jit.c` builtin (C: their `fmgr_builtins[]` rows). Called
/// from this crate's `init_seams()`. OIDs/nargs/strict/retset transcribed from
/// `pg_proc.dat`: `pg_jit_available` is `proargtypes => ''` (nargs 0),
/// `provolatile => 'v'`, no `proisstrict` (not strict), `prorettype => 'bool'`
/// scalar (not retset).
pub fn register_jit_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(315, "pg_jit_available", 0, true, false, fc_pg_jit_available),
    ]);
}
