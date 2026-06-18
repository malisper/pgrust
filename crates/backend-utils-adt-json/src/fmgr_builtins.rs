//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `json.c` functions whose argument/result types are expressible at the
//! current fmgr boundary: the `json` type's I/O quartet (`json_in`/`json_out`/
//! `json_recv`/`json_send`) and `json_typeof`.
//!
//! `json` is a pass-by-reference varlena whose internal representation is the
//! same as `text` (the validated UTF-8 bytes verbatim). Its values cross the
//! fmgr boundary on the by-reference side channel exactly like `text`: an arg
//! arrives header-stripped (`VARDATA_ANY`, via `fcinfo.ref_arg(i)
//! .as_varlena()`), and a by-reference result is the payload bytes set via
//! `fcinfo.set_ref_result(RefPayload::Varlena(..))`. The bare by-value word is
//! the null/dummy word.
//!
//! Each entry is a `fc_<name>` adapter that reads its args off the fmgr call
//! frame, calls the matching value core, and writes the result. OIDs / nargs /
//! strict / retset are transcribed exactly from `pg_proc.dat`.
//!
//! The K5-gated variadic-`"any"` constructors (`json_build_object` /
//! `json_build_array`) and the SRF / aggregate entries are NOT registered here.
//! The `to_json` / `row_to_json` / `array_to_json` / `json_object` family IS
//! registered: each reads its value arg off the fmgr frame (by-value scalar
//! word, by-reference varlena, or composite record), resolves the input type's
//! OID via `get_fn_expr_argtype` where the C uses it (the polymorphic
//! `anyelement` / `anyarray` / `record` argument), and drives the type-output
//! dispatch through the value core.

use types_datum::Datum;
use types_fmgr::boundary::RefPayload;
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData};

use backend_utils_fmgr_core as fmgr_core;
/// The unified value type the cores consume (`types_tuple::Datum`).
use types_tuple::Datum as ValDatum;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` payload bytes (`VARDATA_ANY`): the lane carries
/// `json`/`text` args header-stripped.
#[inline]
fn arg_text_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("json fn: by-ref `json`/`text` arg missing from by-ref lane")
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
#[inline]
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("json fn: cstring arg missing from by-ref lane")
}

/// `get_fn_expr_argtype(fcinfo->flinfo, i)`: the actual type OID of a
/// polymorphic argument resolved from the calling expression tree (the
/// `anyelement` / `anyarray` / `record` resolution path).
#[inline]
fn fn_expr_argtype(fcinfo: &FunctionCallInfoBaseData, i: i32) -> types_core::Oid {
    fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), i)
}

/// Normalize a `RefPayload::Varlena` lane image into the FULL varlena image
/// (header present) the json value cores' `Datum::ByRef` convention requires.
///
/// The fmgr by-reference lane is not uniform: a *scalar* varlena type
/// (`text`/`varchar`/numeric/...) crosses header-STRIPPED (`VARDATA_ANY`
/// payload — what `PG_GETARG_TEXT_PP` consumers like `upper` read directly),
/// while an *array*/composite varlena crosses as its full on-disk image (what
/// `deconstruct_array`'s `detoast_attr` consumes). The json cores
/// (`text_to_cstring_v`, `detoast_attr`) uniformly expect the full image, so
/// re-attach a 4-byte varlena header to a stripped scalar payload; an image
/// whose 4-byte header already accounts for its full byte length is passed
/// through unchanged.
fn varlena_full_image<'mcx>(mcx: mcx::Mcx<'mcx>, b: &[u8]) -> types_error::PgResult<ValDatum<'mcx>> {
    // VARATT_IS_4B_U + VARSIZE == len ⇒ already a full uncompressed varlena
    // image (an array / composite / numeric on-disk value). The 4-byte header's
    // low two bits are 0 for the uncompressed 4-byte form, and the high 30 bits
    // are the total size including the header.
    let already_full = b.len() >= 4 && (b[0] & 0x03) == 0 && {
        let hdr = u32::from_ne_bytes([b[0], b[1], b[2], b[3]]);
        ((hdr >> 2) & 0x3FFF_FFFF) as usize == b.len()
    };
    if already_full {
        return Ok(ValDatum::ByRef(mcx::slice_in(mcx, b)?));
    }
    // SET_VARSIZE(image, VARHDRSZ + len): native-order `(total) << 2`.
    let total = 4 + b.len();
    let mut image = mcx::vec_with_capacity_in::<u8>(mcx, total)?;
    let header = ((total as u32) << 2).to_ne_bytes();
    image.extend_from_slice(&header);
    image.extend_from_slice(b);
    Ok(ValDatum::ByRef(image))
}

/// Materialize argument `i` as the unified `types_tuple::Datum` the json value
/// cores consume: a by-value scalar word, a by-reference varlena (`ByRef`, the
/// FULL image — see [`varlena_full_image`]), a `cstring`, or a composite record
/// (`Composite`). Scratch copies live in `mcx`. The arg must be non-NULL (every
/// entry here is `proisstrict`).
fn arg_value<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<ValDatum<'mcx>> {
    // A by-reference arg rides the by-ref lane; a by-value arg is the bare word.
    Ok(match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => varlena_full_image(mcx, b)?,
        Some(RefPayload::Cstring(s)) => ValDatum::Cstring(s.clone()),
        Some(RefPayload::Composite(image)) => ValDatum::Composite(
            types_tuple::FormedTuple::from_datum_image(mcx, image)?,
        ),
        Some(RefPayload::Expanded(eo)) => {
            // C: a `VARATT_IS_EXPANDED` value; the cores flatten through
            // `clone_in` when they need the byte image.
            ValDatum::ByRef(mcx::slice_in(mcx, &types_datum::flatten_expanded(eo.as_ref()))?)
        }
        // No output-family entry takes an `internal` argument.
        Some(RefPayload::Internal(_)) => {
            panic!("json output fn: unexpected `internal` argument on the by-ref lane")
        }
        None => ValDatum::ByVal(
            fcinfo
                .arg(i)
                .expect("json fn: missing by-value arg")
                .value
                .as_usize(),
        ),
    })
}

/// Set a `json`/`text`/`bytea` (by-reference) result on the by-ref lane and
/// return the dummy by-value word.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Set a `cstring` (`_out`) result on the by-ref lane.
#[inline]
fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// bytes are copied onto the by-ref lane before it drops.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("json fmgr scratch")
}

/// Raise a builtin's `ereport(ERROR)` through the one dispatch point every
/// builtin crosses (`invoke_pgfunction`'s `catch_unwind`).
fn raise(err: types_error::PgError) -> ! {
    std::panic::panic_any(err);
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
// I/O adapters (json.c).
// ---------------------------------------------------------------------------

/// `json_in(cstring) -> json` (oid 321). The validated text bytes become the
/// `json` value's content; cross back on the by-ref lane header-stripped.
fn fc_json_in(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let s = arg_cstring(fcinfo, 0);
    let m = scratch_mcx();
    // A swallowed soft error would yield None via an errsave path that does not
    // run here (hard errors raise above), so the Null arm is unreachable in the
    // ERROR-context dispatch; produce empty content bytes for completeness.
    let bytes = ok(crate::json_in(m.mcx(), s.as_bytes()))
        .map(|image| image.as_slice().to_vec())
        .unwrap_or_default();
    ret_varlena(fcinfo, bytes)
}

/// `json_out(json) -> cstring` (oid 322): a `json` value is its own text.
fn fc_json_out(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let json = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::json_out(m.mcx(), json));
    ret_cstring(fcinfo, String::from_utf8_lossy(bytes.as_slice()).into_owned())
}

/// `json_recv(internal) -> json` (oid 323): read + validate the message bytes.
fn fc_json_recv(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let buf = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let image = ok(crate::json_recv(m.mcx(), buf));
    ret_varlena(fcinfo, image.as_slice().to_vec())
}

/// `json_send(json) -> bytea` (oid 324): the text bytes framed by the wire
/// layer; the body is the value's content bytes.
fn fc_json_send(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let json = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = ok(crate::json_send(m.mcx(), json));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// `json_typeof(json) -> text` (oid 3968).
fn fc_json_typeof(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let json = arg_text_payload(fcinfo, 0);
    let typ = ok(crate::json_typeof(json));
    ret_varlena(fcinfo, typ.as_bytes().to_vec())
}

// ---------------------------------------------------------------------------
// Output-family adapters (to_json / array_to_json / row_to_json / json_object).
// ---------------------------------------------------------------------------

/// `PG_GETARG_BOOL(i)`: a by-value boolean (the `pretty` flag).
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("json fn: missing bool arg").value.as_bool()
}

/// `to_json(anyelement) -> json` (oid 3176). `val_type =
/// get_fn_expr_argtype(flinfo, 0)` resolves the polymorphic input type, then
/// the value is classified + rendered by the core.
fn fc_to_json(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let val_type = fn_expr_argtype(fcinfo, 0);
    let m = scratch_mcx();
    let val = ok(arg_value(m.mcx(), fcinfo, 0));
    let bytes = ok(crate::to_json(m.mcx(), &val, val_type));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// `array_to_json(anyarray) -> json` (oid 3153). The element type is read from
/// the array image by the core (`deconstruct_array`); no `flinfo` lookup needed.
fn fc_array_to_json(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let array = ok(arg_value(m.mcx(), fcinfo, 0));
    let bytes = ok(crate::array_to_json(m.mcx(), &array));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// `array_to_json(anyarray, bool) -> json` (oid 3154): optional pretty-printing.
fn fc_array_to_json_pretty(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let use_line_feeds = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let array = ok(arg_value(m.mcx(), fcinfo, 0));
    let bytes = ok(crate::array_to_json_pretty(m.mcx(), &array, use_line_feeds));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// `row_to_json(record) -> json` (oid 3155).
fn fc_row_to_json(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let row = ok(arg_value(m.mcx(), fcinfo, 0));
    let bytes = ok(crate::row_to_json(m.mcx(), &row));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// `row_to_json(record, bool) -> json` (oid 3156): optional pretty-printing.
fn fc_row_to_json_pretty(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let use_line_feeds = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let row = ok(arg_value(m.mcx(), fcinfo, 0));
    let bytes = ok(crate::row_to_json_pretty(m.mcx(), &row, use_line_feeds));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// Deconstruct a `text[]` argument into `(ndim, dims, payload-or-null)` via the
/// jsonfuncs `deconstruct_text_array` seam (C: `deconstruct_array(..., TEXTOID,
/// -1, false, 'i', ...)`).
fn text_array_arg<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<(i32, Vec<i32>, Vec<Option<Vec<u8>>>)> {
    let arr = arg_value(mcx, fcinfo, i)?;
    backend_utils_adt_jsonfuncs_seams::deconstruct_text_array::call(mcx, &arr)
}

/// Split a deconstructed text array into the `(datums, nulls)` shape the
/// `json_object` cores consume: a null element contributes an empty payload
/// placeholder (never read) and a `true` null flag.
fn split_payloads(elems: &[Option<Vec<u8>>]) -> (Vec<&[u8]>, Vec<bool>) {
    let mut datums: Vec<&[u8]> = Vec::with_capacity(elems.len());
    let mut nulls: Vec<bool> = Vec::with_capacity(elems.len());
    for e in elems {
        match e {
            Some(b) => {
                datums.push(b.as_slice());
                nulls.push(false);
            }
            None => {
                datums.push(&[]);
                nulls.push(true);
            }
        }
    }
    (datums, nulls)
}

/// `json_object(text[]) -> json` (oid 3202): a one- or two-dimensional text
/// array of alternating key/value pairs.
fn fc_json_object(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let (ndim, dims, elems) = ok(text_array_arg(m.mcx(), fcinfo, 0));
    let (datums, nulls) = split_payloads(&elems);
    let bytes = ok(crate::json_object(m.mcx(), ndim, &dims, &datums, &nulls));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
}

/// `json_object(text[], text[]) -> json` (oid 3203): separate key and value
/// text arrays.
fn fc_json_object_two_arg(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    let m = scratch_mcx();
    let (nkdims, _kdims, kelems) = ok(text_array_arg(m.mcx(), fcinfo, 0));
    let (nvdims, _vdims, velems) = ok(text_array_arg(m.mcx(), fcinfo, 1));
    let (key_datums, key_nulls) = split_payloads(&kelems);
    let (val_datums, val_nulls) = split_payloads(&velems);
    let bytes = ok(crate::json_object_two_arg(
        m.mcx(),
        nkdims,
        nvdims,
        &key_datums,
        &key_nulls,
        &val_datums,
        &val_nulls,
    ));
    ret_varlena(fcinfo, bytes.as_slice().to_vec())
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

/// Register the expressible scalar `json.c` builtins. Called from this crate's
/// `init_seams()`. OIDs/nargs/strict/retset transcribed from `pg_proc.dat`.
pub fn register_json_builtins() {
    backend_utils_fmgr_core::register_builtins([
        builtin(321, "json_in", 1, true, false, fc_json_in),
        builtin(322, "json_out", 1, true, false, fc_json_out),
        builtin(323, "json_recv", 1, true, false, fc_json_recv),
        builtin(324, "json_send", 1, true, false, fc_json_send),
        builtin(3968, "json_typeof", 1, true, false, fc_json_typeof),
        // The output family: resolved-arg-type + arbitrary-type output dispatch.
        builtin(3176, "to_json", 1, true, false, fc_to_json),
        builtin(3153, "array_to_json", 1, true, false, fc_array_to_json),
        builtin(3154, "array_to_json_pretty", 2, true, false, fc_array_to_json_pretty),
        builtin(3155, "row_to_json", 1, true, false, fc_row_to_json),
        builtin(3156, "row_to_json_pretty", 2, true, false, fc_row_to_json_pretty),
        builtin(3202, "json_object", 1, true, false, fc_json_object),
        builtin(3203, "json_object_two_arg", 2, true, false, fc_json_object_two_arg),
    ]);
}
