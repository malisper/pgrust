//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions in `snapmgr.c` whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_snapmgr_builtins`] registers every row into
//! the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Result writers.
// ---------------------------------------------------------------------------

/// Set a `text` (`PG_RETURN_TEXT_P(cstring_to_text(...))`) result on the by-ref
/// lane and return the dummy word. The boundary's varlena lane carries the
/// header-stripped text content bytes (the inverse of `as_varlena` on a `text`
/// argument).
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(s.into_bytes()));
    Datum::from_usize(0)
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

/// `pg_export_snapshot` (snapmgr.c:1289) — C:
/// `PG_RETURN_TEXT_P(cstring_to_text(ExportSnapshot(GetActiveSnapshot())))`.
/// Takes no arguments; returns the export token as `text`.
fn fc_pg_export_snapshot(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    match crate::pg_export_snapshot() {
        Ok(token) => ret_text(fcinfo, token),
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

/// Register every expressible `snapmgr.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat`.
pub fn register_snapmgr_builtins() {
    backend_utils_fmgr_core::register_builtins([
        // pg_export_snapshot: proargtypes => '' (0 args), prorettype => 'text';
        // no proisstrict (=> not strict), no proretset (=> not set-returning).
        builtin(3809, "pg_export_snapshot", 0, true, false, fc_pg_export_snapshot),
    ]);
}
