//! The fmgr builtin layer (`Datum fn(PG_FUNCTION_ARGS)`) for the SQL-callable
//! `jsonfuncs.c` scalar/operator functions whose argument/result types are
//! expressible at the current fmgr boundary: the `->`/`->>`/`#>`/`#>>` field /
//! element / path getters, the `||`/`-`/`#-` set operators (`jsonb_concat`,
//! `jsonb_delete[_idx,_array,_path]`), `jsonb_set` / `jsonb_insert`,
//! `json[b]_strip_nulls`, `jsonb_pretty`, `json[b]_typeof`, and
//! `json[b]_array_length`.
//!
//! [`register_jsonfuncs_builtins`] registers every row into the fmgr-core
//! builtin table (C: `fmgr_builtins[]`). OIDs / nargs / strict / retset are
//! transcribed exactly from `pg_proc.dat`.
//!
//! # Boundary conventions
//!
//! * **`jsonb`** is a pass-by-reference varlena. A `jsonb` arg arrives on the
//!   by-reference side channel as its COMPLETE varlena byte image (with
//!   `VARHDRSZ` header); the getfield / setops cores slice `&jb[VARHDRSZ..]`
//!   themselves, so the wrappers pass the full image. A `jsonb` result is the
//!   full varlena image set via `RefPayload::Varlena`.
//! * **`json`** is text-shaped: its values cross the lane header-stripped
//!   (`VARDATA_ANY`), exactly like `text`. The `json` cores consume / produce
//!   the bare content bytes.
//! * **`text`** (key / `_text` result) crosses header-stripped.
//! * **`text[]`** (path / keys) arrives as its detoasted array varlena bytes;
//!   the wrapper deconstructs it into `(ndim, elems)` through arrayfuncs'
//!   `deconstruct_text_array_with_ndim_bytes` (the fmgr/array boundary that the
//!   `*_path` cores expect already done) and reproduces the C
//!   `array_contains_nulls` null-element handling where relevant.
//!
//! The variadic-`"any"` constructors (`json[b]_build_*`, `json[b]_object`) +
//! the `to_json` / `to_jsonb` family (need resolved arg type Oid + arbitrary
//! output dispatch) and the set-returning functions (`json[b]_each[_text]`,
//! `json[b]_object_keys`, `json[b]_array_elements[_text]`) are NOT registered
//! here.

use ::datum::Datum;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use ::arrayfuncs::construct::deconstruct_text_array_with_ndim_bytes;

/// `VARHDRSZ` — the uncompressed 4-byte varlena length-word size.
const VARHDRSZ: usize = 4;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// A by-reference varlena arg's FULL byte image (with `VARHDRSZ` header), read
/// off the by-reference lane verbatim. Under the header-ful convention `jsonb`
/// and array (`text[]`) values both cross as their complete on-disk image.
#[inline]
fn arg_varlena_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("jsonfuncs fn: by-ref varlena arg missing from by-ref lane")
}

/// A `jsonb` arg's FULL varlena byte image (with `VARHDRSZ` header). The
/// getfield / setops cores slice `&jb[VARHDRSZ..]` themselves.
#[inline]
fn arg_jsonb_image<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    arg_varlena_image(fcinfo, i)
}

/// A `json`/`text` arg's payload bytes (`VARDATA_ANY`): under the header-ful
/// convention the lane carries the full varlena image, so strip its leading
/// `VARHDRSZ` header to recover the payload the cores consume.
#[inline]
fn arg_text_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = arg_varlena_image(fcinfo, i);
    // `VARDATA_ANY`: skip ONE header byte for a short (1-byte, low-bit-set) header,
    // else `VARHDRSZ`. A small stored value arrives short-headed once
    // `SHORT_VARLENA_PACKING` is on; a fixed 4-byte strip would drop three payload
    // bytes. No-op while the flag is off (every stored value is 4-byte).
    match image.first() {
        Some(&h) if h != 0x01 && (h & 0x01) == 0x01 => &image[1..],
        Some(_) if image.len() >= VARHDRSZ => &image[VARHDRSZ..],
        _ => &[],
    }
}

/// `PG_GETARG_INT32(i)`.
#[inline]
fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("jsonfuncs fn: missing int4 arg").value.as_i32()
}

/// `PG_GETARG_BOOL(i)`.
#[inline]
fn arg_bool(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).expect("jsonfuncs fn: missing bool arg").value.as_bool()
}

/// Deconstruct a `text[]` arg (detoasted array varlena bytes on the lane) into
/// `(ARR_NDIM, elements)`, each element `Option<Vec<u8>>` (`None` == SQL NULL),
/// the form the `*_path` / `*_array` cores consume.
fn arg_text_array(
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<(i32, Vec<Option<Vec<u8>>>)> {
    let bytes = arg_varlena_image(fcinfo, i);
    let (ndim, elems) = deconstruct_text_array_with_ndim_bytes(bytes)?;
    let path = elems
        .into_iter()
        .map(|e| if e.is_null { None } else { Some(e.value) })
        .collect();
    Ok((ndim, path))
}

/// Set a `jsonb` (by-reference) result on the by-ref lane and return the dummy
/// by-value word. The bytes are the full jsonb varlena image (header-ful);
/// carried verbatim.
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

/// Set an optional `jsonb` by-reference result; `None` produces a SQL NULL. The
/// bytes are the full jsonb varlena image (header-ful); carried verbatim.
#[inline]
fn ret_opt_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Option<Vec<u8>>) -> Datum {
    match bytes {
        Some(b) => {
            fcinfo.set_ref_result(RefPayload::Varlena(b));
            Datum::from_usize(0)
        }
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

/// Frame a bare `text`/`json` content payload as a full 4-byte-header varlena
/// image (`SET_VARSIZE(image, VARHDRSZ + len)`, native-order `(total) << 2`) —
/// the header-ful form the lane now carries for every varlena value.
fn frame_text(payload: &[u8]) -> Vec<u8> {
    let total = VARHDRSZ + payload.len();
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    image.extend_from_slice(payload);
    image
}

/// Set a `text`/`json` (by-reference) result on the by-ref lane. The cores
/// return the bare content payload, so frame it header-ful.
#[inline]
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(frame_text(&payload)));
    Datum::from_usize(0)
}

/// Set an optional `text`/`json` by-reference result; `None` produces a SQL
/// NULL. The cores return the bare content payload, so frame it header-ful.
#[inline]
fn ret_opt_text(fcinfo: &mut FunctionCallInfoBaseData, payload: Option<Vec<u8>>) -> Datum {
    match payload {
        Some(b) => {
            fcinfo.set_ref_result(RefPayload::Varlena(frame_text(&b)));
            Datum::from_usize(0)
        }
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    }
}

#[inline]
fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// A scratch context for cores that allocate their result through `Mcx`. The
/// bytes are copied onto the by-ref lane before it drops.
fn scratch_mcx() -> mcx::MemoryContext {
    mcx::MemoryContext::new("jsonfuncs fmgr scratch")
}

#[inline]
fn opt_bytes(v: Option<mcx::PgVec<'_, u8>>) -> Option<Vec<u8>> {
    v.map(|b| b.as_slice().to_vec())
}

// ---------------------------------------------------------------------------
// jsonb field / element / path getters (-> ->> #> #>>).
// ---------------------------------------------------------------------------

/// `jsonb_object_field(jsonb, text) -> jsonb` (oid 3478): `jb -> key`.
fn fc_jsonb_object_field(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let key = arg_text_payload(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::jsonb_object_field(m.mcx(), &jb, &key)?);
    Ok(ret_opt_varlena(fcinfo, r))
}

/// `jsonb_object_field_text(jsonb, text) -> text` (oid 3214): `jb ->> key`.
fn fc_jsonb_object_field_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let key = arg_text_payload(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::jsonb_object_field_text(m.mcx(), &jb, &key)?);
    Ok(ret_opt_text(fcinfo, r))
}

/// `jsonb_array_element(jsonb, int4) -> jsonb` (oid 3215): `jb -> n`.
fn fc_jsonb_array_element(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let n = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::jsonb_array_element(m.mcx(), &jb, n)?);
    Ok(ret_opt_varlena(fcinfo, r))
}

/// `jsonb_array_element_text(jsonb, int4) -> text` (oid 3216): `jb ->> n`.
fn fc_jsonb_array_element_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let n = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::jsonb_array_element_text(m.mcx(), &jb, n)?);
    Ok(ret_opt_text(fcinfo, r))
}

/// `jsonb_extract_path(jsonb, variadic text[]) -> jsonb` (oid 3217): `jb #> path`.
fn fc_jsonb_extract_path(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let (_ndim, path) = arg_text_array(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::jsonb_extract_path(m.mcx(), &jb, &path)?);
    Ok(ret_opt_varlena(fcinfo, r))
}

/// `jsonb_extract_path_text(jsonb, variadic text[]) -> text` (oid 3940): `jb #>> path`.
fn fc_jsonb_extract_path_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let (_ndim, path) = arg_text_array(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::jsonb_extract_path_text(m.mcx(), &jb, &path)?);
    Ok(ret_opt_text(fcinfo, r))
}

// ---------------------------------------------------------------------------
// json (text) field / element / path getters.
// ---------------------------------------------------------------------------

/// `json_object_field(json, text) -> json` (oid 3947).
fn fc_json_object_field(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    let fname = arg_text_payload(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::json_object_field(m.mcx(), &json, &fname)?);
    Ok(ret_opt_text(fcinfo, r))
}

/// `json_object_field_text(json, text) -> text` (oid 3948).
fn fc_json_object_field_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    let fname = arg_text_payload(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::json_object_field_text(m.mcx(), &json, &fname)?);
    Ok(ret_opt_text(fcinfo, r))
}

/// `json_array_element(json, int4) -> json` (oid 3949).
fn fc_json_array_element(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    let n = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::json_array_element(m.mcx(), &json, n)?);
    Ok(ret_opt_text(fcinfo, r))
}

/// `json_array_element_text(json, int4) -> text` (oid 3950).
fn fc_json_array_element_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    let n = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::json_array_element_text(m.mcx(), &json, n)?);
    Ok(ret_opt_text(fcinfo, r))
}

/// `json_extract_path(json, variadic text[]) -> json` (oid 3951).
fn fc_json_extract_path(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    let (_ndim, path) = arg_text_array(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::json_extract_path(m.mcx(), &json, &path)?);
    Ok(ret_opt_text(fcinfo, r))
}

/// `json_extract_path_text(json, variadic text[]) -> text` (oid 3953).
fn fc_json_extract_path_text(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    let (_ndim, path) = arg_text_array(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = opt_bytes(crate::getfield::json_extract_path_text(m.mcx(), &json, &path)?);
    Ok(ret_opt_text(fcinfo, r))
}

// ---------------------------------------------------------------------------
// jsonb set / delete / concat operators.
// ---------------------------------------------------------------------------

/// `jsonb_concat(jsonb, jsonb) -> jsonb` (oid 3301): the `||` operator.
fn fc_jsonb_concat(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb1 = arg_jsonb_image(fcinfo, 0).to_vec();
    let jb2 = arg_jsonb_image(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let r = crate::setops::jsonb_concat(m.mcx(), &jb1, &jb2)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `jsonb_delete(jsonb, text) -> jsonb` (oid 3302): `jb - key`.
fn fc_jsonb_delete(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let key = arg_text_payload(fcinfo, 1).to_vec();
    let m = scratch_mcx();
    let r = crate::setops::jsonb_delete(m.mcx(), &jb, &key)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `jsonb_delete_idx(jsonb, int4) -> jsonb` (oid 3303): `jb - n`.
fn fc_jsonb_delete_idx(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let idx = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let r = crate::setops::jsonb_delete_idx(m.mcx(), &jb, idx)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `jsonb_delete_array(jsonb, text[]) -> jsonb` (oid 3343): `jb - keys[]`.
fn fc_jsonb_delete_array(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let (ndim, keys) = arg_text_array(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = crate::setops::jsonb_delete_array(m.mcx(), &jb, &keys, ndim)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `jsonb_delete_path(jsonb, text[]) -> jsonb` (oid 3304): the `#-` operator.
fn fc_jsonb_delete_path(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let (ndim, path) = arg_text_array(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = crate::setops::jsonb_delete_path(m.mcx(), &jb, &path, ndim)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `jsonb_set(jsonb, text[], jsonb, bool) -> jsonb` (oid 3305).
fn fc_jsonb_set(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let (ndim, path) = arg_text_array(fcinfo, 1)?;
    let newjb = arg_jsonb_image(fcinfo, 2).to_vec();
    let create = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let r = crate::setops::jsonb_set(m.mcx(), &jb, &path, ndim, &newjb, create)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `jsonb_set_lax(jsonb, text[], jsonb, boolean, text) -> jsonb` (oid 5054).
///
/// `proisstrict => 'f'`: any of the 5 args may be SQL NULL. C maps
/// `PG_ARGISNULL(0|1|3)` -> `PG_RETURN_NULL()`, `PG_ARGISNULL(4)` -> an error,
/// and `PG_ARGISNULL(2)` (the new value) triggers the lax `null_value_treatment`
/// dispatch. `json_null` is the on-disk `jsonb` image for the literal `null`
/// (C's `DirectFunctionCall1(jsonb_in, "null")`), built here as the serialized
/// `jbvNull` scalar.
fn fc_jsonb_set_lax(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // PG_ARGISNULL(i): a by-ref arg is NULL when its lane payload is absent and
    // the nullable-datum flag is set; a by-value arg (bool) reads `isnull`.
    let isnull = |i: usize| fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true);

    let arg0: Option<&[u8]> =
        if isnull(0) { None } else { fcinfo.ref_arg(0).and_then(|p| p.as_varlena()) };

    // The `text[]` path arg is deconstructed (with ndim) only when present.
    let path: Option<(Vec<Option<Vec<u8>>>, i32)> = if isnull(1) {
        None
    } else {
        let bytes = arg_varlena_image(fcinfo, 1);
        let (ndim, elems) = deconstruct_text_array_with_ndim_bytes(bytes)?;
        let p: Vec<Option<Vec<u8>>> = elems
            .into_iter()
            .map(|e| if e.is_null { None } else { Some(e.value) })
            .collect();
        Some((p, ndim))
    };
    let path_ref: Option<(&[Option<Vec<u8>>], i32)> =
        path.as_ref().map(|(p, n)| (p.as_slice(), *n));

    let newjsonb: Option<&[u8]> =
        if isnull(2) { None } else { fcinfo.ref_arg(2).and_then(|p| p.as_varlena()) };

    let create: Option<bool> = if isnull(3) { None } else { Some(arg_bool(fcinfo, 3)) };

    let handle_null: Option<Vec<u8>> = if isnull(4) {
        None
    } else {
        Some(arg_text_payload(fcinfo, 4).to_vec())
    };

    let m = scratch_mcx();
    // Re-home the by-ref args (they borrow fcinfo) into the scratch arena so they
    // satisfy the `'mcx`-lifetimed jsonb_set_lax signature (zero-copy read path).
    let arg0: Option<&[u8]> = match arg0 {
        Some(b) => Some(::mcx::slice_borrow_in(m.mcx(), b)?),
        None => None,
    };
    let newjsonb: Option<&[u8]> = match newjsonb {
        Some(b) => Some(::mcx::slice_borrow_in(m.mcx(), b)?),
        None => None,
    };
    // json_null = JsonbValueToJsonb(jbvNull) — the on-disk image for `null`.
    let json_null = jsonb_util::JsonbValueToJsonb(
        m.mcx(),
        &jsonb_util::JsonbValue::null(),
    )?
    .as_slice()
    .to_vec();
    let json_null = ::mcx::slice_borrow_in(m.mcx(), &json_null)?;

    let r = crate::setops::jsonb_set_lax(
        m.mcx(),
        arg0,
        path_ref,
        newjsonb,
        create,
        handle_null.as_deref(),
        json_null,
    )?;
    Ok(ret_opt_varlena(fcinfo, r.map(|v| v.as_slice().to_vec())))
}

/// `jsonb_insert(jsonb, text[], jsonb, bool) -> jsonb` (oid 3579).
fn fc_jsonb_insert(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let (ndim, path) = arg_text_array(fcinfo, 1)?;
    let newjb = arg_jsonb_image(fcinfo, 2).to_vec();
    let after = arg_bool(fcinfo, 3);
    let m = scratch_mcx();
    let r = crate::setops::jsonb_insert(m.mcx(), &jb, &path, ndim, &newjb, after)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `jsonb_pretty(jsonb) -> text` (oid 3306).
fn fc_jsonb_pretty(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let r = crate::setops::jsonb_pretty(m.mcx(), &jb)?;
    Ok(ret_text(fcinfo, r.as_slice().to_vec()))
}

// ---------------------------------------------------------------------------
// strip_nulls.
// ---------------------------------------------------------------------------

/// `jsonb_strip_nulls(jsonb, bool) -> jsonb` (oid 3262).
fn fc_jsonb_strip_nulls(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let strip_in_arrays = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let jb = ::mcx::slice_borrow_in(m.mcx(), &jb)?;
    let r = crate::strip::jsonb_strip_nulls(m.mcx(), jb, strip_in_arrays)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `json_strip_nulls(json, bool) -> json` (oid 3261).
fn fc_json_strip_nulls(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    let strip_in_arrays = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let r = crate::strip::json_strip_nulls(m.mcx(), &json, strip_in_arrays)?;
    Ok(ret_text(fcinfo, r.as_slice().to_vec()))
}

// ---------------------------------------------------------------------------
// typeof / array_length.
// ---------------------------------------------------------------------------

/// `jsonb_typeof(jsonb) -> text` (oid 3210).
fn fc_jsonb_typeof(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    let m = scratch_mcx();
    let typ = adt_jsonb::jsonb_typeof(m.mcx(), ::mcx::slice_borrow_in(m.mcx(), &jb)?)?;
    Ok(ret_text(fcinfo, typ.as_bytes().to_vec()))
}

/// `jsonb_array_length(jsonb) -> int4` (oid 3207).
fn fc_jsonb_array_length(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let jb = arg_jsonb_image(fcinfo, 0).to_vec();
    Ok(ret_i32(crate::length::jsonb_array_length(&jb)?))
}

/// `json_array_length(json) -> int4` (oid 3956).
fn fc_json_array_length(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0).to_vec();
    Ok(ret_i32(crate::length::json_array_length(&json)?))
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
            name: alloc::string::String::from(name),
            nargs,
            strict,
            retset,
            func: None,
        },
        native,
    )
}

/// Register the expressible scalar/operator `jsonfuncs.c` builtins as
/// **Result-native** (the panic→Result migration; see
/// `docs/proposals/panic-to-result-migration.md`). Called from this crate's
/// `init_seams()`. OIDs/nargs/strict/retset transcribed from `pg_proc.dat`
/// (all `proisstrict => 't'` default; none `proretset`).
pub fn register_jsonfuncs_builtins() {
    fmgr_core::register_builtins_native([
        // jsonb getters.
        builtin(3478, "jsonb_object_field", 2, true, false, fc_jsonb_object_field),
        builtin(3214, "jsonb_object_field_text", 2, true, false, fc_jsonb_object_field_text),
        builtin(3215, "jsonb_array_element", 2, true, false, fc_jsonb_array_element),
        builtin(3216, "jsonb_array_element_text", 2, true, false, fc_jsonb_array_element_text),
        builtin(3217, "jsonb_extract_path", 2, true, false, fc_jsonb_extract_path),
        builtin(3940, "jsonb_extract_path_text", 2, true, false, fc_jsonb_extract_path_text),
        // json getters.
        builtin(3947, "json_object_field", 2, true, false, fc_json_object_field),
        builtin(3948, "json_object_field_text", 2, true, false, fc_json_object_field_text),
        builtin(3949, "json_array_element", 2, true, false, fc_json_array_element),
        builtin(3950, "json_array_element_text", 2, true, false, fc_json_array_element_text),
        builtin(3951, "json_extract_path", 2, true, false, fc_json_extract_path),
        builtin(3953, "json_extract_path_text", 2, true, false, fc_json_extract_path_text),
        // jsonb set / delete / concat.
        builtin(3301, "jsonb_concat", 2, true, false, fc_jsonb_concat),
        builtin(3302, "jsonb_delete", 2, true, false, fc_jsonb_delete),
        builtin(3303, "jsonb_delete_idx", 2, true, false, fc_jsonb_delete_idx),
        builtin(3343, "jsonb_delete_array", 2, true, false, fc_jsonb_delete_array),
        builtin(3304, "jsonb_delete_path", 2, true, false, fc_jsonb_delete_path),
        builtin(3305, "jsonb_set", 4, true, false, fc_jsonb_set),
        // jsonb_set_lax: proisstrict 'f' (handles SQL-NULL new value).
        builtin(5054, "jsonb_set_lax", 5, false, false, fc_jsonb_set_lax),
        builtin(3579, "jsonb_insert", 4, true, false, fc_jsonb_insert),
        builtin(3306, "jsonb_pretty", 1, true, false, fc_jsonb_pretty),
        // strip_nulls.
        builtin(3262, "jsonb_strip_nulls", 2, true, false, fc_jsonb_strip_nulls),
        builtin(3261, "json_strip_nulls", 2, true, false, fc_json_strip_nulls),
        // typeof / array_length.
        builtin(3210, "jsonb_typeof", 1, true, false, fc_jsonb_typeof),
        builtin(3207, "jsonb_array_length", 1, true, false, fc_jsonb_array_length),
        builtin(3956, "json_array_length", 1, true, false, fc_json_array_length),
    ]);

    // The non-set json/jsonb record functions (`json[b]_populate_record`,
    // `json[b]_to_record`, `jsonb_populate_record_valid`; `proretset => 'f'`).
    // C lists them in `fmgr_builtins[]` like any other internal function — so
    // `fmgr_info` must resolve them (by OID via `fmgr_isbuiltin`, by name via
    // `fmgr_internal_function`), or a scalar `SELECT json_populate_record(...)`
    // fails name resolution before evaluation. Their RESULT is the executor-frame
    // record protocol, which the by-OID builtin dispatch's tag-only `resultinfo`
    // ABI frame cannot carry (#327 dual-fcinfo-home), so the scalar
    // `EEOP_FUNCEXPR` interpreter step routes these OIDs to execSRF's
    // `invoke_scalar_record_function` instead of the builtin call address — these
    // rows therefore register METADATA ONLY (`func: None`, no native body; the
    // builtin address is never invoked). OIDs/nargs/strict transcribed from
    // `pg_proc.dat` (`json[b]_populate_record` + `_valid` are `proisstrict => 'f'`;
    // `*_to_record` default strict).
    fmgr_core::register_builtins([
        BuiltinFunction {
            foid: 3960,
            name: alloc::string::String::from("json_populate_record"),
            nargs: 3,
            strict: false,
            retset: false,
            func: None,
        },
        BuiltinFunction {
            foid: 3209,
            name: alloc::string::String::from("jsonb_populate_record"),
            nargs: 2,
            strict: false,
            retset: false,
            func: None,
        },
        BuiltinFunction {
            foid: 6338,
            name: alloc::string::String::from("jsonb_populate_record_valid"),
            nargs: 2,
            strict: false,
            retset: false,
            func: None,
        },
        BuiltinFunction {
            foid: 3204,
            name: alloc::string::String::from("json_to_record"),
            nargs: 1,
            strict: true,
            retset: false,
            func: None,
        },
        BuiltinFunction {
            foid: 3490,
            name: alloc::string::String::from("jsonb_to_record"),
            nargs: 1,
            strict: true,
            retset: false,
            func: None,
        },
    ]);
}
