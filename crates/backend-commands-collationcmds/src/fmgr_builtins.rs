//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `collationcmds.c` whose argument/result types are expressible
//! at the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_collationcmds_builtins`] registers every
//! row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OIDs / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use types_core::Oid;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_OID(i)` → `DatumGetObjectId`: the low 32 bits of arg `i`'s word.
/// `regnamespace` is binary-identical to `oid`, so the namespace arg reads the
/// same way.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Oid {
    fcinfo
        .arg(i)
        .expect("collationcmds fn: missing arg")
        .value
        .as_oid()
}

/// `PG_RETURN_INT32`: a 4-byte by-value result word.
#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
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
    mcx::MemoryContext::new("collationcmds fmgr scratch")
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

/// `pg_import_system_collations(regnamespace) -> int4` (collationcmds.c:835).
/// The core returns the count of collations created; `PG_RETURN_INT32`.
fn fc_pg_import_system_collations(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let nspid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match crate::pg_import_system_collations(m.mcx(), nspid) {
        Ok(ncreated) => ret_i32(ncreated),
        Err(e) => raise(e),
    }
}

/// `pg_collation_actual_version(oid) -> text` (collationcmds.c:506). The core
/// returns `Option<String>`: `Some` → `PG_RETURN_TEXT_P`, `None` →
/// `PG_RETURN_NULL` (the collation has no recorded actual version).
fn fc_pg_collation_actual_version(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let collid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match crate::pg_collation_actual_version(m.mcx(), collid) {
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

/// Register every SQL-callable `collationcmds.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`
/// (both default `proisstrict => 't'`, default `proretset => 'f'`, nargs 1).
pub fn register_collationcmds_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // proargtypes => 'regnamespace' (nargs 1); prorettype => 'int4'.
        builtin(
            3445,
            "pg_import_system_collations",
            1,
            true,
            false,
            fc_pg_import_system_collations,
        ),
        // proargtypes => 'oid' (nargs 1); prorettype => 'text'.
        builtin(
            3448,
            "pg_collation_actual_version",
            1,
            true,
            false,
            fc_pg_collation_actual_version,
        ),
    ]);
}
