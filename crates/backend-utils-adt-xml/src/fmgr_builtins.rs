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

/// A `cstring` arg's owned text (C: `PG_GETARG_CSTRING`).
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("xml fn: cstring arg missing from by-ref lane")
}

/// `PG_RETURN_XML_P(x)` / `PG_RETURN_TEXT_P` — a by-ref varlena result word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(types_fmgr::RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// `PG_RETURN_CSTRING(s)` — a by-ref cstring result word.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(types_fmgr::RefPayload::Cstring(
        String::from_utf8_lossy(&s).into_owned(),
    ));
    Datum::from_usize(0)
}

/// `xml_in(cstring)` (OID 2893) — parse a `cstring` to the `xml` type.
fn fc_xml_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let out = ok(crate::xml_in(s));
    ret_varlena(fcinfo, out)
}

/// `xml_out(xml)` (OID 2894) — render an `xml` value to its `cstring` image.
fn fc_xml_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let x = arg_text_bytes(fcinfo, 0);
    let out = ok(crate::xml_out(x));
    ret_cstring(fcinfo, out)
}

/// `xmlcomment(text)` (OID 2895) — `<!--text-->`.
fn fc_xmlcomment(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let arg = arg_text_bytes(fcinfo, 0);
    let out = ok(crate::xmlcomment(arg));
    ret_varlena(fcinfo, out)
}

/// `texttoxml(text)` (OID 2896, `xml(text)`) — the `text::xml` cast.
fn fc_texttoxml(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_text_bytes(fcinfo, 0);
    let out = ok(crate::texttoxml(data));
    ret_varlena(fcinfo, out)
}

/// `xmltotext(xml)` — the `xml::text` cast (binary-compatible passthrough).
fn fc_xmltotext(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let data = arg_text_bytes(fcinfo, 0);
    let out = ok(crate::xmltotext(data));
    ret_varlena(fcinfo, out)
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
        // Type I/O + casts: the `xml` type's input/output functions and the
        // text<->xml casts, so xml values parse, print, and round-trip.
        builtin(2893, "xml_in", 1, true, false, fc_xml_in),
        builtin(2894, "xml_out", 1, true, false, fc_xml_out),
        builtin(2895, "xmlcomment", 1, true, false, fc_xmlcomment),
        builtin(2896, "texttoxml", 1, true, false, fc_texttoxml),
        builtin(2922, "xmltotext", 1, true, false, fc_xmltotext),
    ]);
}
