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

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Result writers.
// ---------------------------------------------------------------------------

/// Set a `text` (`PG_RETURN_TEXT_P(cstring_to_text(...))`) result on the by-ref
/// lane and return the dummy word. The boundary's varlena lane carries the
/// header-stripped text content bytes (the inverse of `as_varlena` on a `text`
/// argument).
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

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `pg_export_snapshot` (snapmgr.c:1289) — C:
/// `PG_RETURN_TEXT_P(cstring_to_text(ExportSnapshot(GetActiveSnapshot())))`.
/// Takes no arguments; returns the export token as `text`.
fn fc_pg_export_snapshot(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let token = crate::pg_export_snapshot()?;
    Ok(ret_text(fcinfo, token))
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
    func: PgFnNative,
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
        func,
    )
}

/// Register every expressible `snapmgr.c` builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat`.
pub fn register_snapmgr_builtins() {
    fmgr_core::register_builtins_native([
        // pg_export_snapshot: proargtypes => '' (0 args), prorettype => 'text';
        // no proisstrict (=> not strict), no proretset (=> not set-returning).
        builtin(3809, "pg_export_snapshot", 0, true, false, fc_pg_export_snapshot),
    ]);
}
