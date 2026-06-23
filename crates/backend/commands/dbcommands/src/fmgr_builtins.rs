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

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::types_core::Oid;

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
    // `cstring_to_text`: build a header-ful `text` image (4-byte length word).
    let payload = s.into_bytes();
    let total = payload.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
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

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_database_collation_actual_version(oid) -> text` (dbcommands.c:2776). The
/// core returns `Option<String>`: `Some` → `PG_RETURN_TEXT_P`, `None` →
/// `PG_RETURN_NULL` (the database has no recorded collation version).
fn fc_pg_database_collation_actual_version(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let dbid = arg_oid(fcinfo, 0);
    let m = scratch_mcx();
    match crate::pg_database_collation_actual_version(m.mcx(), dbid)? {
        Some(version) => Ok(ret_text(fcinfo, version)),
        None => Ok(ret_null(fcinfo)),
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

/// Register every SQL-callable `dbcommands.c` builtin (C: their
/// `fmgr_builtins[]` rows). Called from this crate's `init_seams()`.
/// OIDs/nargs/strict/retset transcribed exactly from `pg_proc.dat`.
pub fn register_dbcommands_builtins() {
    fmgr_core::register_builtins_native([
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
