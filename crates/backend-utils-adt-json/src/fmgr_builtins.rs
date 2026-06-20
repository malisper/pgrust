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
use types_fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use backend_utils_fmgr_core as fmgr_core;
/// The unified value type the cores consume (`types_tuple::Datum`).
use types_tuple::Datum as ValDatum;

// ---------------------------------------------------------------------------
// Argument readers / result writers.
// ---------------------------------------------------------------------------

/// `PG_GETARG_TEXT_PP(i)` payload bytes (`VARDATA_ANY`): under the header-ful
/// convention the lane carries the full `json`/`text` varlena image, so strip
/// its leading `VARHDRSZ` header to recover the payload the cores consume.
#[inline]
fn arg_text_payload<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    let image = fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("json fn: by-ref `json`/`text` arg missing from by-ref lane");
    &image[VARHDRSZ..]
}

/// `VARHDRSZ` — the uncompressed 4-byte varlena length-word size.
const VARHDRSZ: usize = 4;

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

/// Materialize argument `i` as the unified `types_tuple::Datum` the json value
/// cores consume: a by-value scalar word, a by-reference varlena (`ByRef`, the
/// FULL header-ful image, carried verbatim under the header-ful convention), a
/// `cstring`, or a composite record (`Composite`). Scratch copies live in `mcx`.
/// The arg must be non-NULL (every entry here is `proisstrict`).
fn arg_value<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> types_error::PgResult<ValDatum<'mcx>> {
    // A by-reference arg rides the by-ref lane; a by-value arg is the bare word.
    Ok(match fcinfo.ref_arg(i) {
        Some(RefPayload::Varlena(b)) => ValDatum::ByRef(mcx::slice_in(mcx, b)?),
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
/// return the dummy by-value word. Under the header-ful convention the cores
/// return the bare payload, so frame it as a full 4-byte-header varlena image
/// (`SET_VARSIZE(image, VARHDRSZ + len)`, native-order `(total) << 2`).
#[inline]
fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, payload: Vec<u8>) -> Datum {
    let total = VARHDRSZ + payload.len();
    let mut image = Vec::with_capacity(total);
    image.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    image.extend_from_slice(&payload);
    fcinfo.set_ref_result(RefPayload::Varlena(image));
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

// ---------------------------------------------------------------------------
// I/O adapters (json.c).
// ---------------------------------------------------------------------------

/// `json_in(cstring) -> json` (oid 321). The validated text bytes become the
/// `json` value's content; cross back on the by-ref lane header-stripped.
fn fc_json_in(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    // C: `json_in` forwards `fcinfo->context` (the soft `ErrorSaveContext`) to
    // `json_errsave_error`. Copy the cstring to an owned buffer first so the
    // immutable `fcinfo` borrow is released before taking the `&mut` escontext
    // borrow.
    let s = arg_cstring(fcinfo, 0).as_bytes().to_vec();
    let m = scratch_mcx();
    let escontext = fcinfo.escontext_mut();
    // Copy out of the scratch arena before it drops (the result borrows `m`).
    let bytes = crate::json_in(m.mcx(), &s, escontext)?.map(|image| image.as_slice().to_vec());
    Ok(match bytes {
        // Validated text becomes the `json` value's content bytes.
        Some(b) => ret_varlena(fcinfo, b),
        // Soft parse failure (`ereturn(escontext, (Datum) 0, ...)`): SQL NULL.
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}

/// `json_out(json) -> cstring` (oid 322): a `json` value is its own text.
fn fc_json_out(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::json_out(m.mcx(), json)?;
    Ok(ret_cstring(fcinfo, String::from_utf8_lossy(bytes.as_slice()).into_owned()))
}

/// `json_recv(internal) -> json` (oid 323): read + validate the message bytes.
fn fc_json_recv(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let buf = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let image = crate::json_recv(m.mcx(), buf)?;
    Ok(ret_varlena(fcinfo, image.as_slice().to_vec()))
}

/// `json_send(json) -> bytea` (oid 324): the text bytes framed by the wire
/// layer; the body is the value's content bytes.
fn fc_json_send(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0);
    let m = scratch_mcx();
    let bytes = crate::json_send(m.mcx(), json)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

/// `json_typeof(json) -> text` (oid 3968).
fn fc_json_typeof(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let json = arg_text_payload(fcinfo, 0);
    let typ = crate::json_typeof(json)?;
    Ok(ret_varlena(fcinfo, typ.as_bytes().to_vec()))
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
fn fc_to_json(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let val_type = fn_expr_argtype(fcinfo, 0);
    let m = scratch_mcx();
    let val = arg_value(m.mcx(), fcinfo, 0)?;
    let bytes = crate::to_json(m.mcx(), &val, val_type)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

/// `array_to_json(anyarray) -> json` (oid 3153). The element type is read from
/// the array image by the core (`deconstruct_array`); no `flinfo` lookup needed.
fn fc_array_to_json(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let array = arg_value(m.mcx(), fcinfo, 0)?;
    let bytes = crate::array_to_json(m.mcx(), &array)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

/// `array_to_json(anyarray, bool) -> json` (oid 3154): optional pretty-printing.
fn fc_array_to_json_pretty(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let use_line_feeds = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let array = arg_value(m.mcx(), fcinfo, 0)?;
    let bytes = crate::array_to_json_pretty(m.mcx(), &array, use_line_feeds)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

/// `row_to_json(record) -> json` (oid 3155).
fn fc_row_to_json(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let row = arg_value(m.mcx(), fcinfo, 0)?;
    let bytes = crate::row_to_json(m.mcx(), &row)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

/// `row_to_json(record, bool) -> json` (oid 3156): optional pretty-printing.
fn fc_row_to_json_pretty(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let use_line_feeds = arg_bool(fcinfo, 1);
    let m = scratch_mcx();
    let row = arg_value(m.mcx(), fcinfo, 0)?;
    let bytes = crate::row_to_json_pretty(m.mcx(), &row, use_line_feeds)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
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
fn fc_json_object(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let (ndim, dims, elems) = text_array_arg(m.mcx(), fcinfo, 0)?;
    let (datums, nulls) = split_payloads(&elems);
    let bytes = crate::json_object(m.mcx(), ndim, &dims, &datums, &nulls)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

/// `json_object(text[], text[]) -> json` (oid 3203): separate key and value
/// text arrays.
fn fc_json_object_two_arg(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let (nkdims, _kdims, kelems) = text_array_arg(m.mcx(), fcinfo, 0)?;
    let (nvdims, _vdims, velems) = text_array_arg(m.mcx(), fcinfo, 1)?;
    let (key_datums, key_nulls) = split_payloads(&kelems);
    let (val_datums, val_nulls) = split_payloads(&velems);
    let bytes = crate::json_object_two_arg(
        m.mcx(),
        nkdims,
        nvdims,
        &key_datums,
        &key_nulls,
        &val_datums,
        &val_nulls,
    )?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

// ---------------------------------------------------------------------------
// VARIADIC-"any" constructors (json.c): json_build_object / json_build_array.
//
// C entry point:
//     nargs = extract_variadic_args(fcinfo, 0, true, &args, &types, &nulls);
//     if (nargs < 0) PG_RETURN_NULL();
//     PG_RETURN_DATUM(json_build_*_worker(nargs, args, nulls, types, ...));
// `extract_variadic_args(fcinfo, variadic_start=0, convert_unknown=true)`
// (funcapi.c) is reproduced by [`extract_variadic_args`], mirroring the proven
// jsonb.c form (the variadic-array deconstruct seam is jsonb-owned; the
// extraction logic is identical).
// ---------------------------------------------------------------------------

/// `UNKNOWNOID` / `TEXTOID` (pg_type.dat): the unknown→text coercion of
/// `extract_variadic_args(..., convert_unknown=true)`.
const UNKNOWNOID: types_core::Oid = 705;
const TEXTOID_VARIADIC: types_core::Oid = 25;

/// The extracted variadic argument vectors (`Datum *args`, `Oid *types`,
/// `bool *nulls`), or `None` for the C `nargs < 0` `PG_RETURN_NULL()` case
/// (`VARIADIC NULL`). The `Datum`s are canonical `types_tuple::Datum`s living in
/// the supplied scratch `mcx`.
struct VariadicArgs<'mcx> {
    args: Vec<ValDatum<'mcx>>,
    types: Vec<types_core::Oid>,
    nulls: Vec<bool>,
}

/// `extract_variadic_args(fcinfo, variadic_start, convert_unknown=true, ...)`
/// (funcapi.c). `None` is the C `return -1` (`VARIADIC NULL`).
fn extract_variadic_args<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    fcinfo: &FunctionCallInfoBaseData,
    variadic_start: usize,
) -> types_error::PgResult<Option<VariadicArgs<'mcx>>> {
    let variadic = fmgr_core::get_fn_expr_variadic(fcinfo.flinfo.as_deref());

    if variadic {
        // Assert(PG_NARGS() == variadic_start + 1);
        // if (PG_ARGISNULL(variadic_start)) return -1;
        if fcinfo.arg(variadic_start).map(|d| d.isnull).unwrap_or(true) {
            return Ok(None);
        }
        // array_in = PG_GETARG_ARRAYTYPE_P(variadic_start); element_type =
        // ARR_ELEMTYPE(array_in); deconstruct_array(...) — all element types
        // are element_type.
        let array_image = arg_value(mcx, fcinfo, variadic_start)?;
        let array_bytes = match array_image {
            ValDatum::ByRef(b) => b.as_slice().to_vec(),
            _ => panic!("json variadic: VARIADIC array arg not on the by-ref lane"),
        };
        let (element_type, elems): (types_core::Oid, Vec<(ValDatum<'mcx>, bool)>) =
            backend_utils_adt_jsonb_seams::extract_variadic_array::call(mcx, &array_bytes)?;
        let n = elems.len();
        let mut args = Vec::with_capacity(n);
        let mut nulls = Vec::with_capacity(n);
        let mut types = Vec::with_capacity(n);
        for (d, isnull) in elems {
            args.push(d);
            nulls.push(isnull);
            types.push(element_type);
        }
        Ok(Some(VariadicArgs { args, types, nulls }))
    } else {
        // nargs = PG_NARGS() - variadic_start;
        let nargs = (fcinfo.nargs as usize).saturating_sub(variadic_start);
        let mut args = Vec::with_capacity(nargs);
        let mut nulls = Vec::with_capacity(nargs);
        let mut types = Vec::with_capacity(nargs);

        for i in 0..nargs {
            let idx = i + variadic_start;
            let is_null = fcinfo.arg(idx).map(|d| d.isnull).unwrap_or(false);
            let mut typ = fn_expr_argtype(fcinfo, idx as i32);

            // Turn an `unknown`-type constant (a cstring on the by-ref lane)
            // into text — the only `unknown` arg the json builders can see is
            // such a literal.
            let value: ValDatum<'mcx> = if typ == UNKNOWNOID {
                if is_null {
                    typ = TEXTOID_VARIADIC;
                    ValDatum::null()
                } else if let Some(s) = fcinfo.ref_arg(idx).and_then(|p| p.as_cstring()) {
                    typ = TEXTOID_VARIADIC;
                    // args_res[i] = CStringGetTextDatum(PG_GETARG_POINTER(i));
                    backend_utils_adt_varlena_seams::cstring_to_text_v::call(mcx, s)?
                } else {
                    ValDatum::null()
                }
            } else if is_null {
                ValDatum::null()
            } else {
                // No conversion needed, just take the datum as given.
                arg_value(mcx, fcinfo, idx)?
            };

            // if (!OidIsValid(types_res[i]) || (convert_unknown && types_res[i]
            // == UNKNOWNOID)) ereport(ERROR, ...).
            if typ == 0 || typ == UNKNOWNOID {
                return Err(PgError::error(alloc::format!(
                    "could not determine data type for argument {}",
                    i + 1
                ))
                .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE));
            }

            args.push(value);
            nulls.push(is_null);
            types.push(typ);
        }

        Ok(Some(VariadicArgs { args, types, nulls }))
    }
}

/// `json_build_object(PG_FUNCTION_ARGS)` (json.c) — oid 3200.
fn fc_json_build_object(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();
    let extracted = extract_variadic_args(mcx, fcinfo, 0)?;
    let out = match extracted {
        Some(v) => crate::json_build_object(mcx, Some((&v.args, &v.nulls, &v.types)))?,
        None => None,
    };
    match out {
        Some(bytes) => Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec())),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

/// `json_build_object_noargs(PG_FUNCTION_ARGS)` (json.c) — oid 3201.
fn fc_json_build_object_noargs(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let bytes = crate::json_build_object_noargs(m.mcx())?.as_slice().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
}

/// `json_build_array(PG_FUNCTION_ARGS)` (json.c) — oid 3198.
fn fc_json_build_array(fcinfo: &mut FunctionCallInfoBaseData) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let mcx = m.mcx();
    let extracted = extract_variadic_args(mcx, fcinfo, 0)?;
    let out = match extracted {
        Some(v) => crate::json_build_array(mcx, Some((&v.args, &v.nulls, &v.types)))?,
        None => None,
    };
    match out {
        Some(bytes) => Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec())),
        None => {
            fcinfo.set_result_null(true);
            Ok(Datum::from_usize(0))
        }
    }
}

/// `json_build_array_noargs(PG_FUNCTION_ARGS)` (json.c) — oid 3199.
fn fc_json_build_array_noargs(
    fcinfo: &mut FunctionCallInfoBaseData,
) -> types_error::PgResult<Datum> {
    let m = scratch_mcx();
    let bytes = crate::json_build_array_noargs(m.mcx())?.as_slice().to_vec();
    Ok(ret_varlena(fcinfo, bytes))
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

/// Register the expressible scalar `json.c` builtins as **Result-native** (the
/// panic→Result migration; see `docs/proposals/panic-to-result-migration.md`).
/// Called from this crate's `init_seams()`. OIDs/nargs/strict/retset transcribed
/// from `pg_proc.dat`.
pub fn register_json_builtins() {
    backend_utils_fmgr_core::register_builtins_native([
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
