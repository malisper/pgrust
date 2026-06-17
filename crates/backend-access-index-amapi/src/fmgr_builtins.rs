//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `amapi.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_amapi_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs / nargs
//! / strict / retset are transcribed exactly from `pg_proc.dat`.

use types_core::Oid;
use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo.arg(i).expect("amapi fn: missing arg").value.as_oid()
}

#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("amapi fmgr scratch")
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

/// `amvalidate(PG_FUNCTION_ARGS)` (amapi.c): `Oid opclassoid = PG_GETARG_OID(0);`
/// `... PG_RETURN_BOOL(result);`.
fn fc_amvalidate(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let opclassoid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match crate::amvalidate(m.mcx(), opclassoid) {
        Ok(result) => ret_bool(result),
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

/// Register every `amapi.c` SQL-callable builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs / nargs / strict /
/// retset from `pg_proc.dat`.
pub fn register_amapi_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // amvalidate(oid) -> bool. pg_proc.dat oid 338: proargtypes 'oid'
        // (nargs 1), no proisstrict (strict false), no proretset (retset false).
        builtin(338, "amvalidate", 1, false, false, fc_amvalidate),
    ]);
}
