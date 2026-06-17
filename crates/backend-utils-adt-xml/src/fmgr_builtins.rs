//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the `xml.c`
//! SQL-callable functions whose argument/result types are expressible at the
//! current fmgr boundary.
//!
//! Each entry is a `fc_<name>` adapter that reads its arguments off the fmgr
//! call frame, calls the matching value core in [`crate`], and writes back the
//! result word. [`register_xml_builtins`] registers every row into the
//! fmgr-core builtin table (C: `fmgr_builtins[]`), so by-OID dispatch resolves
//! them. OIDs / nargs / strict / retset are transcribed exactly from
//! `pg_proc.dat`.
//!
//! Only the `xml_is_well_formed*` family is registered here: each takes a single
//! `text` argument (read as its detoasted `VARDATA_ANY` payload bytes, exactly
//! the `&[u8]` the value core consumes) and returns a by-value `bool`. The
//! remaining `xml.c` builtins are not registered here because their argument or
//! result types (the `xml` varlena type, `xmltype` constructors, XMLTABLE
//! table-function plumbing, etc.) are out of scope for this lane.

use types_datum::Datum;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text` arg's detoasted `VARDATA_ANY` payload bytes on the by-ref lane
/// (C: `VARDATA_ANY(data)` / `VARSIZE_ANY_EXHDR(data)`).
#[inline]
fn arg_text_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("xml fn: text arg missing from by-ref lane")
}

/// `PG_RETURN_BOOL(b)`: the boolean result word.
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    let chars = types_error::unpack_sqlstate(err.sqlstate());
    let code = core::str::from_utf8(&chars).unwrap_or("XX000");
    std::panic::panic_any(format!("PGRUST-SQLSTATE:{code}:{}", err.message()));
}

/// Unwrap a `PgResult`, re-raising its error through `raise`.
#[inline]
fn ok<T>(r: types_error::PgResult<T>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => raise(e),
    }
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `xml_is_well_formed(text)` (OID 3051).
fn fc_xml_is_well_formed(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_text_bytes(fcinfo, 0);
    ret_bool(ok(crate::xml_is_well_formed(data)))
}

/// `xml_is_well_formed_document(text)` (OID 3052).
fn fc_xml_is_well_formed_document(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_text_bytes(fcinfo, 0);
    ret_bool(ok(crate::xml_is_well_formed_document(data)))
}

/// `xml_is_well_formed_content(text)` (OID 3053).
fn fc_xml_is_well_formed_content(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_text_bytes(fcinfo, 0);
    ret_bool(ok(crate::xml_is_well_formed_content(data)))
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

/// Register the `xml_is_well_formed*` builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat` (each: 1 arg, `proisstrict => 't'`,
/// not retset).
pub fn register_xml_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(3051, "xml_is_well_formed", 1, true, false, fc_xml_is_well_formed),
        builtin(
            3052,
            "xml_is_well_formed_document",
            1,
            true,
            false,
            fc_xml_is_well_formed_document,
        ),
        builtin(
            3053,
            "xml_is_well_formed_content",
            1,
            true,
            false,
            fc_xml_is_well_formed_content,
        ),
    ]);
}
