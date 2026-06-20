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
use types_error::PgResult;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A `text` arg's detoasted `VARDATA_ANY` payload bytes on the by-ref lane
/// (C: `VARDATA_ANY(data)` / `VARSIZE_ANY_EXHDR(data)`).
#[inline]
fn arg_text_bytes<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("xml fn: text arg missing from by-ref lane");
    // VARDATA_ANY: skip the 4-byte varlena header on the header-ful image.
    &image[types_datum::varlena::VARHDRSZ..]
}

/// `PG_RETURN_BOOL(b)`: the boolean result word.
#[inline]
fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters.
// ---------------------------------------------------------------------------

/// `xml_is_well_formed(text)` (OID 3051).
fn fc_xml_is_well_formed(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let data = arg_text_bytes(fcinfo, 0);
    Ok(ret_bool(crate::xml_is_well_formed(data)?))
}

/// `xml_is_well_formed_document(text)` (OID 3052).
fn fc_xml_is_well_formed_document(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let data = arg_text_bytes(fcinfo, 0);
    Ok(ret_bool(crate::xml_is_well_formed_document(data)?))
}

/// `xml_is_well_formed_content(text)` (OID 3053).
fn fc_xml_is_well_formed_content(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let data = arg_text_bytes(fcinfo, 0);
    Ok(ret_bool(crate::xml_is_well_formed_content(data)?))
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
    // cstring_to_text: prepend the 4-byte varlena header (header-ful image).
    let mut img = Vec::with_capacity(types_datum::varlena::VARHDRSZ + bytes.len());
    img.extend_from_slice(&types_datum::varlena::set_varsize_4b(
        types_datum::varlena::VARHDRSZ + bytes.len(),
    ));
    img.extend_from_slice(&bytes);
    fcinfo.set_ref_result(types_fmgr::RefPayload::Varlena(img));
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
///
/// C: `escontext = (Node *) fcinfo->context` — thread the soft `ErrorSaveContext`
/// off the fmgr frame into `xml_in`, so a malformed-XML parse `ereturn`s INTO the
/// escontext (the soft path `pg_input_is_valid` / `pg_input_error_info` rely on)
/// and returns NULL, instead of throwing a hard error. With no soft sink the
/// escontext is `None` and the parse error is thrown as `Err`, as before.
fn fc_xml_in(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // `arg_cstring` borrows `fcinfo` immutably while `escontext_mut()` needs
    // `&mut`; copy the input to an owned string first.
    let s = arg_cstring(fcinfo, 0).to_string();
    match crate::xml_in(&s, fcinfo.escontext_mut())? {
        Some(out) => Ok(ret_varlena(fcinfo, out)),
        None => {
            // C: `if (doc == NULL) PG_RETURN_NULL();`
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

/// `xml_out(xml)` (OID 2894) — render an `xml` value to its `cstring` image.
fn fc_xml_out(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let x = arg_text_bytes(fcinfo, 0);
    let out = crate::xml_out(x)?;
    Ok(ret_cstring(fcinfo, out))
}

/// `xmlcomment(text)` (OID 2895) — `<!--text-->`.
fn fc_xmlcomment(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let arg = arg_text_bytes(fcinfo, 0);
    let out = crate::xmlcomment(arg)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `texttoxml(text)` (OID 2896, `xml(text)`) — the `text::xml` cast.
fn fc_texttoxml(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let data = arg_text_bytes(fcinfo, 0);
    let out = crate::texttoxml(data)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `xmltotext(xml)` — the `xml::text` cast (binary-compatible passthrough).
fn fc_xmltotext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let data = arg_text_bytes(fcinfo, 0);
    let out = crate::xmltotext(data)?;
    Ok(ret_varlena(fcinfo, out))
}

// ---------------------------------------------------------------------------
// Additional argument readers for the multi-arg xml export functions.
// ---------------------------------------------------------------------------

/// `PG_GETARG_BOOL(i)`: the boolean arg word.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("xml fn: missing bool arg").value.as_bool()
}

/// `PG_GETARG_OID(i)` (regclass): the OID arg word.
#[inline]
fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> u32 {
    fcinfo.arg(i).expect("xml fn: missing oid arg").value.as_u32()
}

/// `PG_GETARG_INT32(i)`.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("xml fn: missing int4 arg").value.as_i32()
}

/// A `text`/`refcursor` arg as a UTF-8 `&str` (its detoasted payload bytes).
#[inline]
fn arg_text_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    core::str::from_utf8(arg_text_bytes(fcinfo, i)).expect("xml fn: text arg not valid UTF-8")
}

/// A `name` arg as a UTF-8 `&str`. A `name` crosses the by-ref lane as its raw
/// NUL-padded `NAMEDATALEN` buffer (no varlena header); NUL-trim then decode.
#[inline]
fn arg_name_str<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    let payload = fcinfo
        .ref_arg(i)
        .expect("xml fn: name arg missing from by-ref lane");
    let bytes: &[u8] = match payload {
        types_fmgr::RefPayload::Varlena(b) => b.as_slice(),
        types_fmgr::RefPayload::Cstring(s) => s.as_bytes(),
        other => panic!("xml fn: name arg has unexpected by-ref payload {other:?}"),
    };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    core::str::from_utf8(&bytes[..end]).expect("xml fn: name arg not valid UTF-8")
}

/// An optional `xml` arg (for the non-strict `xmlconcat2`): `None` when the arg
/// is SQL NULL, else its detoasted payload bytes.
#[inline]
fn arg_xml_opt<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> Option<&'a [u8]> {
    if fcinfo.args.get(i).map(|a| a.isnull).unwrap_or(true) {
        None
    } else {
        Some(arg_text_bytes(fcinfo, i))
    }
}

/// `PG_RETURN_BYTEA_P(b)` — a header-ful `bytea` varlena result word.
#[inline]
fn ret_bytea(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    ret_varlena(fcinfo, bytes)
}

/// `PG_RETURN_BOOL(b)`.
#[inline]
fn ret_bool_w(v: bool) -> Datum {
    ret_bool(v)
}

// ---------------------------------------------------------------------------
// fc_ adapters — predicates / send / concat / text.
// ---------------------------------------------------------------------------

/// `xmlexists(text, xml) -> bool` (OID 2614).
fn fc_xmlexists(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let xpath = arg_text_bytes(fcinfo, 0);
    let data = arg_text_bytes(fcinfo, 1);
    Ok(ret_bool_w(crate::xmlexists(xpath, data)?))
}

/// `xmlvalidate(xml, text) -> bool` (OID 2897). The core is the removed-feature
/// stub: it ignores both args and always errors.
fn fc_xmlvalidate(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let _ = fcinfo;
    Ok(ret_bool_w(crate::xmlvalidate(
        Datum::from_usize(0),
        Datum::from_usize(0),
    )?))
}

/// `xml_send(xml) -> bytea` (OID 2899).
fn fc_xml_send(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let x = arg_text_bytes(fcinfo, 0);
    let out = crate::xml_send(x)?;
    Ok(ret_bytea(fcinfo, out))
}

/// `xmlconcat2(xml, xml) -> xml` (OID 2900, NOT strict).
fn fc_xmlconcat2(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_xml_opt(fcinfo, 0);
    let b = arg_xml_opt(fcinfo, 1);
    match crate::xmlconcat2(a, b)? {
        Some(out) => Ok(ret_varlena(fcinfo, out)),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

/// `xmltext(text) -> xml` (OID 3813).
fn fc_xmltext(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let arg = arg_text_bytes(fcinfo, 0);
    let out = crate::xmltext(arg)?;
    Ok(ret_varlena(fcinfo, out))
}

// ---------------------------------------------------------------------------
// fc_ adapters — table / query / cursor / schema / database export.
// ---------------------------------------------------------------------------

/// `table_to_xml(regclass, bool, bool, text) -> xml` (OID 2923).
fn fc_table_to_xml(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::table_to_xml(relid, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `query_to_xml(text, bool, bool, text) -> xml` (OID 2924).
fn fc_query_to_xml(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let query = arg_text_str(fcinfo, 0).to_owned();
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::query_to_xml(&query, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `cursor_to_xml(refcursor, int4, bool, bool, text) -> xml` (OID 2925).
fn fc_cursor_to_xml(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let name = arg_text_str(fcinfo, 0).to_owned();
    let count = arg_int32(fcinfo, 1);
    let nulls = arg_bool(fcinfo, 2);
    let tableforest = arg_bool(fcinfo, 3);
    let targetns = arg_text_str(fcinfo, 4).to_owned();
    let out = crate::cursor_to_xml(&name, count, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `table_to_xmlschema(regclass, bool, bool, text) -> xml` (OID 2926).
fn fc_table_to_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::table_to_xmlschema(relid, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `query_to_xmlschema(text, bool, bool, text) -> xml` (OID 2927).
fn fc_query_to_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let query = arg_text_str(fcinfo, 0).to_owned();
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::query_to_xmlschema(&query, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `cursor_to_xmlschema(refcursor, bool, bool, text) -> xml` (OID 2928).
fn fc_cursor_to_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let name = arg_text_str(fcinfo, 0).to_owned();
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::cursor_to_xmlschema(&name, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `table_to_xml_and_xmlschema(regclass, bool, bool, text) -> xml` (OID 2929).
fn fc_table_to_xml_and_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let relid = arg_oid(fcinfo, 0);
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::table_to_xml_and_xmlschema(relid, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `query_to_xml_and_xmlschema(text, bool, bool, text) -> xml` (OID 2930).
fn fc_query_to_xml_and_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let query = arg_text_str(fcinfo, 0).to_owned();
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::query_to_xml_and_xmlschema(&query, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `schema_to_xml(name, bool, bool, text) -> xml` (OID 2933).
fn fc_schema_to_xml(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let name = arg_name_str(fcinfo, 0).to_owned();
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::schema_to_xml(&name, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `schema_to_xmlschema(name, bool, bool, text) -> xml` (OID 2934).
fn fc_schema_to_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let name = arg_name_str(fcinfo, 0).to_owned();
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::schema_to_xmlschema(&name, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `schema_to_xml_and_xmlschema(name, bool, bool, text) -> xml` (OID 2935).
fn fc_schema_to_xml_and_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let name = arg_name_str(fcinfo, 0).to_owned();
    let nulls = arg_bool(fcinfo, 1);
    let tableforest = arg_bool(fcinfo, 2);
    let targetns = arg_text_str(fcinfo, 3).to_owned();
    let out = crate::schema_to_xml_and_xmlschema(&name, nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `database_to_xml(bool, bool, text) -> xml` (OID 2936).
fn fc_database_to_xml(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let nulls = arg_bool(fcinfo, 0);
    let tableforest = arg_bool(fcinfo, 1);
    let targetns = arg_text_str(fcinfo, 2).to_owned();
    let out = crate::database_to_xml(nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `database_to_xmlschema(bool, bool, text) -> xml` (OID 2937).
fn fc_database_to_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let nulls = arg_bool(fcinfo, 0);
    let tableforest = arg_bool(fcinfo, 1);
    let targetns = arg_text_str(fcinfo, 2).to_owned();
    let out = crate::database_to_xmlschema(nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
}

/// `database_to_xml_and_xmlschema(bool, bool, text) -> xml` (OID 2938).
fn fc_database_to_xml_and_xmlschema(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let nulls = arg_bool(fcinfo, 0);
    let tableforest = arg_bool(fcinfo, 1);
    let targetns = arg_text_str(fcinfo, 2).to_owned();
    let out = crate::database_to_xml_and_xmlschema(nulls, tableforest, &targetns)?;
    Ok(ret_varlena(fcinfo, out))
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

/// Register the `xml_is_well_formed*` builtins (C: their `fmgr_builtins[]`
/// rows). Called from this crate's `init_seams()`. OIDs/nargs/strict/retset
/// transcribed exactly from `pg_proc.dat` (each: 1 arg, `proisstrict => 't'`,
/// not retset).
pub fn register_xml_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
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
        // xml predicates / constructors / send.
        builtin(2614, "xmlexists", 2, true, false, fc_xmlexists),
        builtin(2897, "xmlvalidate", 2, true, false, fc_xmlvalidate),
        builtin(2899, "xml_send", 1, true, false, fc_xml_send),
        builtin(2900, "xmlconcat2", 2, false, false, fc_xmlconcat2),
        builtin(3813, "xmltext", 1, true, false, fc_xmltext),
        // table/query/cursor -> xml [and] xmlschema.
        builtin(2923, "table_to_xml", 4, true, false, fc_table_to_xml),
        builtin(2924, "query_to_xml", 4, true, false, fc_query_to_xml),
        builtin(2925, "cursor_to_xml", 5, true, false, fc_cursor_to_xml),
        builtin(2926, "table_to_xmlschema", 4, true, false, fc_table_to_xmlschema),
        builtin(2927, "query_to_xmlschema", 4, true, false, fc_query_to_xmlschema),
        builtin(2928, "cursor_to_xmlschema", 4, true, false, fc_cursor_to_xmlschema),
        builtin(
            2929,
            "table_to_xml_and_xmlschema",
            4,
            true,
            false,
            fc_table_to_xml_and_xmlschema,
        ),
        builtin(
            2930,
            "query_to_xml_and_xmlschema",
            4,
            true,
            false,
            fc_query_to_xml_and_xmlschema,
        ),
        // schema/database -> xml [and] xmlschema.
        builtin(2933, "schema_to_xml", 4, true, false, fc_schema_to_xml),
        builtin(2934, "schema_to_xmlschema", 4, true, false, fc_schema_to_xmlschema),
        builtin(
            2935,
            "schema_to_xml_and_xmlschema",
            4,
            true,
            false,
            fc_schema_to_xml_and_xmlschema,
        ),
        builtin(2936, "database_to_xml", 3, true, false, fc_database_to_xml),
        builtin(2937, "database_to_xmlschema", 3, true, false, fc_database_to_xmlschema),
        builtin(
            2938,
            "database_to_xml_and_xmlschema",
            3,
            true,
            false,
            fc_database_to_xml_and_xmlschema,
        ),
    ]);
}
