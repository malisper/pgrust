//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! entry point of `format_type.c` whose argument/result types are expressible at
//! the current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core, and writes back the result word /
//! by-reference payload. [`register_format_type_builtins`] registers every row
//! into the fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch
//! resolves them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! `format_type` is `proisstrict => 'f'` (NOT strict): the C body explicitly
//! tests `PG_ARGISNULL(0)` / `PG_ARGISNULL(1)` and routes a NULL `oid` to
//! `PG_RETURN_NULL()` and a NULL `typmod` to the "no typmod available" path.
//! The adapter therefore reads each arg's `isnull` and passes `Option<Oid>` /
//! `Option<i32>` into [`crate::format_type`], which already encodes that exact
//! NULL logic, and writes the `Option<text>` result back (a `None` result is
//! `PG_RETURN_NULL()`).

use fmgr::boundary::RefPayload;
use fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use types_core::Oid;
use datum::Datum;
use types_error::PgResult;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_ARGISNULL(i) ? None : Some(PG_GETARG_OID(i))` — arg `i`'s OID, or `None`
/// for a SQL NULL (this function is not strict).
#[inline]
fn arg_oid_opt(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Option<Oid> {
    let a = fcinfo.arg(i).expect("format_type fn: missing arg");
    if a.isnull {
        None
    } else {
        Some(a.value.as_oid())
    }
}

/// `PG_ARGISNULL(i) ? None : Some(PG_GETARG_INT32(i))` — arg `i`'s `int4`, or
/// `None` for a SQL NULL.
#[inline]
fn arg_int32_opt(fcinfo: &FunctionCallInfoBaseData, i: usize) -> Option<i32> {
    let a = fcinfo.arg(i).expect("format_type fn: missing arg");
    if a.isnull {
        None
    } else {
        Some(a.value.as_i32())
    }
}

/// Write a `text` result on the by-ref lane (C: `PG_RETURN_TEXT_P` /
/// `cstring_to_text`), or set the result NULL (C: `PG_RETURN_NULL()`). The
/// boundary re-wraps the payload bytes with the varlena header. Returns the
/// result word (a dummy `0`).
#[inline]
fn ret_text_opt(fcinfo: &mut FunctionCallInfoBaseData, s: Option<String>) -> Datum {
    match s {
        Some(s) => {
            // cstring_to_text: prepend the 4-byte varlena header (header-ful).
            let payload = s.into_bytes();
            let mut img = Vec::with_capacity(datum::varlena::VARHDRSZ + payload.len());
            img.extend_from_slice(&datum::varlena::set_varsize_4b(
                datum::varlena::VARHDRSZ + payload.len(),
            ));
            img.extend_from_slice(&payload);
            fcinfo.set_ref_result(RefPayload::Varlena(img));
        }
        None => fcinfo.set_result_null(true),
    }
    Datum::from_usize(0)
}

/// A scratch context for the `format_type` core, which allocates its
/// `PgString` result through `Mcx`. The result string is copied into an owned
/// `String` before the context is dropped on return.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("format_type fmgr scratch")
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `format_type(oid, int4)` (OID 1081) — not strict.
fn fc_format_type(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let type_oid = arg_oid_opt(fcinfo, 0);
    let typemod = arg_int32_opt(fcinfo, 1);
    let m = scratch_mcx();
    let result: Option<String> = match crate::format_type(m.mcx(), type_oid, typemod)? {
        Some(name) => Some(String::from(name.as_str())),
        None => None,
    };
    Ok(ret_text_opt(fcinfo, result))
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

/// Register the `format_type.c` fmgr builtin this crate owns (C: its
/// `fmgr_builtins[]` row). Called from this crate's `init_seams()`. OID / nargs
/// / strict / retset transcribed exactly from `pg_proc.dat`: `format_type` is
/// `proisstrict => 'f'` (not strict), 2 args (`oid int4`), no `proretset`.
pub fn register_format_type_builtins() {
    fmgr_core::register_builtins_native([builtin(
        1081,
        "format_type",
        2,
        false,
        false,
        fc_format_type,
    )]);
}
