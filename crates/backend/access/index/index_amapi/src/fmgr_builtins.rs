//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `amapi.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word.
//! [`register_amapi_builtins`] registers every row into the fmgr-core builtin
//! table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them. OIDs / nargs
//! / strict / retset are transcribed exactly from `pg_proc.dat`.

use ::types_core::Oid;
use ::datum::Datum;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

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

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `amvalidate(PG_FUNCTION_ARGS)` (amapi.c): `Oid opclassoid = PG_GETARG_OID(0);`
/// `... PG_RETURN_BOOL(result);`.
fn fc_amvalidate(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let opclassoid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    let result = crate::amvalidate(m.mcx(), opclassoid)?;
    Ok(ret_bool(result))
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

/// Register every `amapi.c` SQL-callable builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs / nargs / strict /
/// retset from `pg_proc.dat`.
pub fn register_amapi_builtins() {
    fmgr_core::register_builtins_native([
        // amvalidate(oid) -> bool. pg_proc.dat oid 338: proargtypes 'oid'
        // (nargs 1), no proisstrict (strict false), no proretset (retset false).
        builtin(338, "amvalidate", 1, true, false, fc_amvalidate),
    ]);
}
