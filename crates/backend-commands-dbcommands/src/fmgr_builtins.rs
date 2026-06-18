//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `dbcommands.c` whose argument/result types are expressible at
//! the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_dbcommands_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("dbcommands fn: missing arg")
        .value
        .as_oid()
}

/// Set a `text` result on the by-ref lane (the boundary owns the `VARHDRSZ`
/// framing; we hand over the header-less payload bytes), mirroring
/// `PG_RETURN_TEXT_P(cstring_to_text(...))`.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(s.into_bytes()));
    Datum::from_usize(0)
}

/// `PG_RETURN_NULL()`: mark the result NULL and return a dummy word.
#[inline]
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("dbcommands fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_database_collation_actual_version(oid) -> text` (dbcommands.c:2776). The
/// core returns `Option<String>`: `Some` → `PG_RETURN_TEXT_P`, `None` →
/// `PG_RETURN_NULL` (the database has no recorded collation version).
fn fc_pg_database_collation_actual_version(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let dbid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match crate::pg_database_collation_actual_version(m.mcx(), dbid) {
        Ok(Some(version)) => ret_text(fcinfo, version),
        Ok(None) => ret_null(fcinfo),
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

/// Register every SQL-callable `dbcommands.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`.
pub fn register_dbcommands_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // proargtypes => 'oid' (nargs 1); proisstrict default 't'; not retset.
        builtin(
            6249,
            "pg_database_collation_actual_version",
            1,
            true,
            false,
            fc_pg_database_collation_actual_version,
        ),
    ]);
}
