//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! functions implemented in `miscinit.c` whose argument/result types are
//! expressible at the current fmgr boundary.
//!
//! Currently that is just `system_user()` (SQL `SYSTEM_USER`): zero arguments,
//! a `text` result. Each entry is a `fc_<name>` adapter that reads its arguments
//! off the fmgr call frame, calls the matching value core, and writes back the
//! result word / by-reference payload. [`register_miscinit_builtins`] registers
//! every row into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID
//! dispatch resolves them. OID / nargs / strict / retset are transcribed
//! exactly from `pg_proc.dat`.

use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Result writers.
// ---------------------------------------------------------------------------

/// Set a `text` result on the by-ref lane and return the dummy word. The
/// boundary carries `text` as the bare payload bytes (the 4-byte varlena length
/// header is not stored), mirroring the established adt convention
/// (`CStringGetTextDatum`, with the header stripped at the boundary).
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

/// `Datum system_user(PG_FUNCTION_ARGS)` (`miscinit.c:948`) — SQL `SYSTEM_USER`.
///
/// C: `if (!MyClientConnectionInfo.authn_id) PG_RETURN_NULL();` then
/// `PG_RETURN_DATUM(CStringGetTextDatum(GetSystemUser()))`. The value core
/// returns `Option<String>` — `None` is C's NULL return, `Some(s)` is the
/// `auth_method:authn_id` text.
fn fc_system_user(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    match crate::system_user() {
        Some(s) => Ok(ret_text(fcinfo, s)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
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

/// Register every `miscinit.c` SQL-callable builtin (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OID / nargs / strict / retset
/// transcribed exactly from `pg_proc.dat`.
pub fn register_miscinit_builtins() {
    fmgr_core::register_builtins_native([
        // 6311  system_user()  -> text   (nargs 0, strict, not retset)
        builtin(6311, "system_user", 0, true, false, fc_system_user),
    ]);
}
