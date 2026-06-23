//! fmgr registration for the polymorphic varlena-array I/O functions
//! (`array_in`/`array_out`/`array_recv`/`array_send`). These are the entry
//! points the fmgr registry dispatches by OID: the input function `array_in`
//! (oid 750) is what `getTypeInputInfo` resolves for every `_T` array type, so
//! e.g. nodeAgg's `GetAggInitVal` reaches it to materialize an aggregate's
//! `agginitval` text (`{0,0}`) into a transition array.
//!
//! The element-type I/O is resolved inside the ported bodies (`io::array_in`
//! etc.) through `get_array_element_io_data` + the fmgr owner's
//! `input_function_call_safe` / `array_output_function_call` seams. Here we
//! only marshal the array value across the fmgr boundary: a `cstring`/array
//! arg on the by-reference side channel, the by-value `typioparam`/`typmod`
//! words, and the array/cstring result back on the by-reference lane.

use ::mcx::MemoryContext;
use ::datum::Datum;
use ::types_error::PgResult;
use ::fmgr::boundary::RefPayload;
use ::fmgr::{BuiltinFunction, FunctionCallInfoBaseData, PgFnNative};

use detoast_seams as detoast_seam;

fn scratch_mcx() -> MemoryContext {
    MemoryContext::new("arrayfuncs fmgr scratch")
}

/// `PG_GETARG_CSTRING(i)`: the input cstring on the by-ref lane.
fn arg_cstring<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a str {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_cstring())
        .expect("array fn: cstring arg missing from by-ref lane")
}

/// `PG_GETARG_*ARRAYTYPE_P(i)` / `PG_GETARG_BYTEA_PP(i)`: the by-ref varlena
/// (array image / binary message buffer) on the by-ref lane.
fn arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> &'a [u8] {
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .expect("array fn: by-ref varlena arg missing from by-ref lane")
}

/// `PG_GETARG_*ARRAYTYPE_P(i)` (array.h): the array argument **detoasted**
/// (`DatumGetArrayTypeP` == `pg_detoast_datum`). A stored `anyarray` column
/// (e.g. pg_statistic `stavalues`) can be inline-compressed (`VARATT_IS_4B_C`)
/// or stored external when it exceeds the toast threshold; the raw by-ref bytes
/// then carry a compressed/external header, not a plain `ArrayType`. Reading
/// `ARR_NDIM`/`ARR_DIMS` off that header yields garbage (and `array_length`
/// reads back NULL). Every C array built-in resolves its array arg through
/// `PG_GETARG_*ARRAYTYPE_P`, which detoasts first; mirror that here. Detoast is
/// a verbatim copy for an already-plain value, so this is faithful for all
/// callers (`pg_detoast_datum` is a no-op on a non-extended datum).
fn arg_array_detoast(fcinfo: &FunctionCallInfoBaseData, i: usize) -> PgResult<Vec<u8>> {
    let raw = arg_varlena(fcinfo, i);
    let m = scratch_mcx();
    let detoasted = detoast_seam::detoast_attr::call(m.mcx(), raw)?;
    Ok(detoasted.as_slice().to_vec())
}

fn arg_oid(fcinfo: &FunctionCallInfoBaseData, i: usize) -> ::types_core::Oid {
    fcinfo.arg(i).expect("array fn: missing arg").value.as_oid()
}

fn arg_int32(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i32 {
    fcinfo.arg(i).expect("array fn: missing arg").value.as_i32()
}

fn ret_varlena(fcinfo: &mut FunctionCallInfoBaseData, bytes: Vec<u8>) -> Datum {
    fcinfo.set_ref_result(RefPayload::Varlena(bytes));
    Datum::from_usize(0)
}

fn ret_cstring(fcinfo: &mut FunctionCallInfoBaseData, s: String) -> Datum {
    fcinfo.set_ref_result(RefPayload::Cstring(s));
    Datum::from_usize(0)
}

/// `array_in(cstring, oid, int4) -> anyarray` (oid 750). arg0 is the input
/// text, arg1 the element type (`typioparam`), arg2 the typmod.
fn fc_array_in(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let string = arg_cstring(fcinfo, 0).to_string();
    let element_type = arg_oid(fcinfo, 1);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    // Forward the soft ErrorSaveContext installed on the frame by
    // InputFunctionCallSafe so a malformed literal or bad element value
    // `ereturn`s into the sink (returning `Ok(None)`) instead of throwing past
    // `invoke?`.
    let parsed = crate::io::array_in(
        m.mcx(),
        &string,
        element_type,
        typmod,
        fcinfo.escontext_mut(),
    )?;
    Ok(match parsed {
        Some(image) => ret_varlena(fcinfo, image.as_slice().to_vec()),
        // Soft-error path: escontext recorded the failure; return a SQL NULL
        // placeholder the caller discards after `soft_error_occurred()`.
        None => {
            fcinfo.set_result_null(true);
            Datum::from_usize(0)
        }
    })
}

/// `array_out(anyarray) -> cstring` (oid 751).
fn fc_array_out(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let bytes = crate::io::array_out(m.mcx(), &array)?;
    // PG_RETURN_CSTRING produces a NUL-terminated cstring; strip the terminator
    // for the cstring lane.
    let raw = bytes.as_slice();
    let body = raw.strip_suffix(&[0u8]).unwrap_or(raw);
    Ok(ret_cstring(fcinfo, String::from_utf8_lossy(body).into_owned()))
}

/// `array_recv(internal, oid, int4) -> anyarray` (oid 2400). arg0 is the binary
/// message buffer (StringInfo), arg1 the element type, arg2 the typmod.
fn fc_array_recv(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // arg0 is the binary message StringInfo (C: PG_GETARG_POINTER), not a
    // toastable varlena — read it verbatim, do NOT detoast.
    let buf = arg_varlena(fcinfo, 0).to_vec();
    let spec_element_type = arg_oid(fcinfo, 1);
    let typmod = arg_int32(fcinfo, 2);
    let m = scratch_mcx();
    let image = crate::io::array_recv(m.mcx(), &buf, spec_element_type, typmod)?;
    Ok(ret_varlena(fcinfo, image.as_slice().to_vec()))
}

/// `array_send(anyarray) -> bytea` (oid 2401).
fn fc_array_send(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let bytes = crate::io::array_send(m.mcx(), &array)?;
    Ok(ret_varlena(fcinfo, bytes.as_slice().to_vec()))
}

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

/// Register the polymorphic array I/O builtins (C: their `fmgr_builtins[]`
/// rows) as **Result-native** (panic→Result migration). Called from this
/// crate's `init_seams()`.
pub fn register_arrayfuncs_builtins() {
    fmgr_core::register_builtins_native([
        builtin(750, "array_in", 3, true, false, fc_array_in),
        builtin(751, "array_out", 1, true, false, fc_array_out),
        builtin(2400, "array_recv", 3, true, false, fc_array_recv),
        builtin(2401, "array_send", 1, true, false, fc_array_send),
    ]);
}

// ===========================================================================
// SQL-facing array builtins (the bodies in `array_userfuncs`, `element_slice`,
// `ops`, and `sql`). Each `fc_` wrapper marshals fmgr's by-value word /
// by-reference lane onto the ported body's value-typed signature and back.
// ===========================================================================

use ::array::ArrayElementDatum;
use ::types_core::Oid;

/// `PG_GETARG_ANY_ARRAY_P(i)` when the arg may be NULL: the by-ref varlena image
/// on the by-ref lane, or `None` for a SQL-NULL.
fn opt_arg_varlena<'a>(fcinfo: &'a FunctionCallInfoBaseData, i: usize) -> Option<&'a [u8]> {
    if fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true) {
        return None;
    }
    fcinfo
        .ref_arg(i)
        .and_then(|p| p.as_varlena())
        .or(Some(&[]))
}

/// `PG_GETARG_ANY_ARRAY_P(i)` when the arg may be NULL, **detoasted**
/// (`DatumGetArrayTypeP`). Mirrors [`arg_array_detoast`] for the nullable lane:
/// a stored array column can be inline-compressed/external, so the raw by-ref
/// bytes must be detoasted before any `ARR_*` header read. `None` for SQL-NULL.
fn opt_arg_array_detoast(
    fcinfo: &FunctionCallInfoBaseData,
    i: usize,
) -> PgResult<Option<Vec<u8>>> {
    let raw = match opt_arg_varlena(fcinfo, i) {
        Some(r) => r,
        None => return Ok(None),
    };
    let m = scratch_mcx();
    let detoasted = detoast_seam::detoast_attr::call(m.mcx(), raw)?;
    Ok(Some(detoasted.as_slice().to_vec()))
}

/// `PG_ARGISNULL(i)`.
fn arg_isnull(fcinfo: &FunctionCallInfoBaseData, i: usize) -> bool {
    fcinfo.arg(i).map(|d| d.isnull).unwrap_or(true)
}

/// `fcinfo->fncollation`.
fn collation(fcinfo: &FunctionCallInfoBaseData) -> Oid {
    fcinfo.fncollation
}

/// Materialize argument `i` as an `ArrayElementDatum` for `element_type`. A
/// pass-by-value element rides the by-value word; a by-reference element rides
/// its on-disk bytes on the by-ref lane (mirroring C's bare `Datum` that is
/// either the value or a pointer into stored bytes).
fn arg_element<'a>(
    fcinfo: &'a FunctionCallInfoBaseData,
    i: usize,
    element_type: Oid,
) -> PgResult<ArrayElementDatum<'a>> {
    let s = lsyscache_seams::get_typlenbyvalalign::call(element_type)?;
    Ok(if s.typbyval {
        ArrayElementDatum::ByValue(fcinfo.arg(i).map(|d| d.value).unwrap_or(Datum::from_usize(0)))
    } else {
        ArrayElementDatum::ByRef(
            fcinfo
                .ref_arg(i)
                .and_then(|p| {
                    p.as_varlena()
                        .or_else(|| p.as_composite())
                        .or_else(|| p.as_cstring().map(|c| c.as_bytes()))
                })
                .expect("array element fn: by-ref element missing from by-ref lane"),
        )
    })
}

fn ret_bool(v: bool) -> Datum {
    Datum::from_bool(v)
}

fn ret_i32(v: i32) -> Datum {
    Datum::from_i32(v)
}

/// `PG_RETURN_UINT32`.
fn ret_u32(v: u32) -> Datum {
    Datum::from_u32(v)
}

/// `PG_RETURN_UINT64`.
fn ret_u64(v: u64) -> Datum {
    Datum::from_u64(v)
}

/// `PG_GETARG_INT64(i)`: the by-value 64-bit word.
fn arg_i64(fcinfo: &FunctionCallInfoBaseData, i: usize) -> i64 {
    fcinfo.arg(i).expect("array fn: missing arg").value.as_i64()
}

/// `PG_RETURN_NULL()`.
fn ret_null(fcinfo: &mut FunctionCallInfoBaseData) -> Datum {
    fcinfo.set_result_null(true);
    Datum::from_usize(0)
}

/// Return an `Option<i32>` result, mapping `None` to SQL-NULL.
fn ret_opt_i32(fcinfo: &mut FunctionCallInfoBaseData, v: Option<i32>) -> Datum {
    match v {
        Some(n) => Datum::from_i32(n),
        None => ret_null(fcinfo),
    }
}

/// Return an array (by-ref varlena image) or SQL-NULL.
fn ret_opt_array(fcinfo: &mut FunctionCallInfoBaseData, image: Option<Vec<u8>>) -> Datum {
    match image {
        Some(img) => {
            fcinfo.set_ref_result(RefPayload::Varlena(img));
            Datum::from_usize(0)
        }
        None => ret_null(fcinfo),
    }
}

/// `cstring_to_text(buf)`: wrap a header-less UTF-8 payload as a `text` varlena.
fn ret_text(fcinfo: &mut FunctionCallInfoBaseData, payload: &[u8]) -> Datum {
    let total = payload.len() + 4;
    let mut img = Vec::with_capacity(total);
    img.extend_from_slice(&((total as u32) << 2).to_ne_bytes());
    img.extend_from_slice(payload);
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Datum::from_usize(0)
}

// --- comparison / containment (strict, anyarray anyarray -> bool) -----------

fn fc_array_eq(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::array_eq(&a, &b, collation(fcinfo))?))
}

fn fc_array_ne(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::array_ne(&a, &b, collation(fcinfo))?))
}

fn fc_array_lt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::array_lt(&a, &b, collation(fcinfo))?))
}

fn fc_array_gt(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::array_gt(&a, &b, collation(fcinfo))?))
}

fn fc_array_le(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::array_le(&a, &b, collation(fcinfo))?))
}

fn fc_array_ge(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::array_ge(&a, &b, collation(fcinfo))?))
}

fn fc_arraycontains(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::arraycontains(&a, &b, collation(fcinfo))?))
}

fn fc_arraycontained(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::arraycontained(&a, &b, collation(fcinfo))?))
}

fn fc_arrayoverlap(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_bool(crate::ops::arrayoverlap(&a, &b, collation(fcinfo))?))
}

// --- btarraycmp / hash (strict, anyarray -> int4 / int4 / int8) -------------

/// `btarraycmp(anyarray, anyarray) -> int4` (oid 382): `array_cmp(fcinfo)`.
fn fc_btarraycmp(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    Ok(ret_i32(crate::ops::btarraycmp(&a, &b, collation(fcinfo))?))
}

/// `hash_array(anyarray) -> int4` (oid 626): `PG_RETURN_UINT32(result)`.
fn fc_hash_array(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    Ok(ret_u32(crate::ops::hash_array(&a, collation(fcinfo))?))
}

/// `hash_array_extended(anyarray, int8) -> int8` (oid 782). arg1 is the seed
/// (`PG_GETARG_INT64(1)` cast to `uint64`); `PG_RETURN_UINT64(result)`.
fn fc_hash_array_extended(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let seed = arg_i64(fcinfo, 1) as u64;
    Ok(ret_u64(crate::ops::hash_array_extended(&a, collation(fcinfo), seed)?))
}

// --- dims / bounds (strict, anyarray [int4] -> int4 / text) -----------------

fn fc_array_ndims(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    Ok(ret_opt_i32(fcinfo, crate::element_slice::array_ndims(&a)))
}

fn fc_array_dims(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let payload: Option<Vec<u8>> =
        crate::element_slice::array_dims(m.mcx(), &a)?.map(|v| v.as_slice().to_vec());
    Ok(match payload {
        Some(v) => ret_text(fcinfo, &v),
        None => ret_null(fcinfo),
    })
}

fn fc_array_lower(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let reqdim = arg_int32(fcinfo, 1);
    Ok(ret_opt_i32(fcinfo, crate::element_slice::array_lower(&a, reqdim)))
}

fn fc_array_upper(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let reqdim = arg_int32(fcinfo, 1);
    Ok(ret_opt_i32(fcinfo, crate::element_slice::array_upper(&a, reqdim)))
}

fn fc_array_length(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let reqdim = arg_int32(fcinfo, 1);
    Ok(ret_opt_i32(fcinfo, crate::element_slice::array_length(&a, reqdim)))
}

fn fc_array_cardinality(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    Ok(ret_i32(crate::element_slice::array_cardinality(&a)))
}

// --- array_larger / array_smaller (strict, anyarray anyarray -> anyarray) ---

fn fc_array_larger(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = crate::sql::array_larger(m.mcx(), &a, &b, collation(fcinfo))?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

fn fc_array_smaller(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let a = arg_array_detoast(fcinfo, 0)?;
    let b = arg_array_detoast(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = crate::sql::array_smaller(m.mcx(), &a, &b, collation(fcinfo))?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

// --- array_cat / append / prepend (non-strict, anyarray -> anyarray) --------

fn fc_array_cat(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let v1 = opt_arg_array_detoast(fcinfo, 0)?;
    let v2 = opt_arg_array_detoast(fcinfo, 1)?;
    let m = scratch_mcx();
    let r = crate::array_userfuncs::array_cat(
        m.mcx(),
        v1.as_deref(),
        v2.as_deref(),
    )?;
    Ok(ret_opt_array(fcinfo, r.map(|v| v.as_slice().to_vec())))
}

/// Resolve the element type, length/byval/align triple, and a bare-word `Datum`
/// for the new element of an `array_append`/`array_prepend` call. The new
/// element is `fcinfo` arg `elem_argno`; the array (arg `array_argno`) supplies
/// the element type via its header when non-NULL, else via the function's
/// resolved argument type (`get_fn_expr_argtype`). For a by-reference element
/// the returned `Datum` word is a pointer into the element bytes, which are kept
/// alive by the returned `held` buffer (mirroring C's bare `Datum` pointing into
/// stored bytes). Returns `(element_type, elmlen, elmbyval, elmalign,
/// data_value, isnull, held)`.
#[allow(clippy::type_complexity)]
fn resolve_push_element(
    fcinfo: &FunctionCallInfoBaseData,
    array_argno: usize,
    elem_argno: usize,
    array: Option<&[u8]>,
) -> PgResult<(Oid, i32, bool, u8, Datum, bool, Option<Vec<u8>>)> {
    // Element type: from the (non-NULL) array header, else the function's
    // resolved array argument type's element type.
    let element_type = match array {
        Some(a) => crate::foundation::arr_elemtype(a),
        None => {
            let arr_type =
                fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), array_argno as i32);
            lsyscache_seams::get_element_type::call(arr_type)?
                .expect("array_push: function array argument is not an array type")
        }
    };
    let s = lsyscache_seams::get_typlenbyvalalign::call(element_type)?;
    let isnull = arg_isnull(fcinfo, elem_argno);
    if isnull {
        return Ok((element_type, s.typlen as i32, s.typbyval, s.typalign as u8, Datum::from_usize(0), true, None));
    }
    if s.typbyval {
        let word = fcinfo.arg(elem_argno).map(|d| d.value).unwrap_or(Datum::from_usize(0));
        Ok((element_type, s.typlen as i32, s.typbyval, s.typalign as u8, word, false, None))
    } else {
        // By-reference element: copy its bytes into a held buffer and pass a
        // pointer to them as the data_value word (C: PG_GETARG_DATUM is a
        // pointer into the stored element).
        let bytes = fcinfo
            .ref_arg(elem_argno)
            .and_then(|p| {
                p.as_varlena()
                    .or_else(|| p.as_composite())
                    .or_else(|| p.as_cstring().map(|c| c.as_bytes()))
            })
            .expect("array_push: by-ref element missing from by-ref lane")
            .to_vec();
        let ptr = bytes.as_ptr() as usize;
        Ok((
            element_type,
            s.typlen as i32,
            s.typbyval,
            s.typalign as u8,
            Datum::from_usize(ptr),
            false,
            Some(bytes),
        ))
    }
}

/// `array_append(anyarray, anyelement) -> anyarray` (oid 378). Non-strict.
fn fc_array_append(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array_vec = opt_arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let (element_type, elmlen, elmbyval, elmalign, data_value, isnull, _held) =
        resolve_push_element(fcinfo, 0, 1, array_vec.as_deref())?;

    // fetch_array_arg_replace_nulls: a NULL array becomes an empty array.
    let array: Vec<u8> = match &array_vec {
        Some(a) => a.clone(),
        None => crate::construct::construct_empty_array(m.mcx(), element_type)?
            .as_slice()
            .to_vec(),
    };

    let ndims = crate::foundation::arr_ndim(&array);
    // index of added elem is at lb[0] + (dimv[0] - 1) + 1 == lb[0] + dimv[0].
    let indx: i32 = if ndims == 1 {
        let lb0 = crate::foundation::arr_lbound(&array, 0);
        let dim0 = crate::foundation::arr_dim(&array, 0);
        match lb0.checked_add(dim0) {
            Some(v) => v,
            None => return Err(integer_out_of_range()),
        }
    } else if ndims == 0 {
        1
    } else {
        return Err(empty_or_one_dim_err());
    };

    let result = crate::element_slice::array_set_element(
        m.mcx(),
        &array,
        1,
        &[indx],
        data_value,
        isnull,
        -1,
        elmlen,
        elmbyval,
        elmalign,
    )?;
    Ok(ret_varlena(fcinfo, result.as_slice().to_vec()))
}

/// `array_prepend(anyelement, anyarray) -> anyarray` (oid 379). Non-strict.
fn fc_array_prepend(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array_vec = opt_arg_array_detoast(fcinfo, 1)?;
    let m = scratch_mcx();
    let (element_type, elmlen, elmbyval, elmalign, data_value, isnull, _held) =
        resolve_push_element(fcinfo, 1, 0, array_vec.as_deref())?;

    let array: Vec<u8> = match &array_vec {
        Some(a) => a.clone(),
        None => crate::construct::construct_empty_array(m.mcx(), element_type)?
            .as_slice()
            .to_vec(),
    };

    let ndims = crate::foundation::arr_ndim(&array);
    let (indx, lb0): (i32, i32) = if ndims == 1 {
        let lb = crate::foundation::arr_lbound(&array, 0);
        match lb.checked_sub(1) {
            Some(v) => (v, lb),
            None => return Err(integer_out_of_range()),
        }
    } else if ndims == 0 {
        (1, 1)
    } else {
        return Err(empty_or_one_dim_err());
    };

    let mut result = crate::element_slice::array_set_element(
        m.mcx(),
        &array,
        1,
        &[indx],
        data_value,
        isnull,
        -1,
        elmlen,
        elmbyval,
        elmalign,
    )?;
    // Readjust result's lower bound to match the input's, as expected for
    // prepend (C: eah->lbound[0] = lb0).
    if crate::foundation::arr_ndim(&result) == 1 {
        crate::foundation::write_lbounds(&mut result, 1, &[lb0]);
    }
    Ok(ret_varlena(fcinfo, result.as_slice().to_vec()))
}

fn integer_out_of_range() -> ::types_error::PgError {
    ::types_error::PgError::error("integer out of range")
        .with_sqlstate(::types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
}

fn empty_or_one_dim_err() -> ::types_error::PgError {
    ::types_error::PgError::error("argument must be empty or one-dimensional array")
        .with_sqlstate(::types_error::ERRCODE_DATA_EXCEPTION)
}

// --- array_position / array_positions (non-strict) --------------------------

fn fc_array_position(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = opt_arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let searched = match (&array, arg_isnull(fcinfo, 1)) {
        (Some(a), false) => Some(arg_element(fcinfo, 1, crate::foundation::arr_elemtype(a))?),
        _ => None,
    };
    let r = crate::array_userfuncs::array_position(
        m.mcx(),
        array.as_deref(),
        searched,
        collation(fcinfo),
    )?;
    Ok(ret_opt_i32(fcinfo, r))
}

fn fc_array_position_start(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = opt_arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let searched = match (&array, arg_isnull(fcinfo, 1)) {
        (Some(a), false) => Some(arg_element(fcinfo, 1, crate::foundation::arr_elemtype(a))?),
        _ => None,
    };
    let start = if arg_isnull(fcinfo, 2) {
        None
    } else {
        Some(arg_int32(fcinfo, 2))
    };
    let r = crate::array_userfuncs::array_position_start(
        m.mcx(),
        array.as_deref(),
        searched,
        start,
        collation(fcinfo),
    )?;
    Ok(ret_opt_i32(fcinfo, r))
}

fn fc_array_positions(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = opt_arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let searched = match (&array, arg_isnull(fcinfo, 1)) {
        (Some(a), false) => Some(arg_element(fcinfo, 1, crate::foundation::arr_elemtype(a))?),
        _ => None,
    };
    let r = crate::array_userfuncs::array_positions(
        m.mcx(),
        array.as_deref(),
        searched,
        collation(fcinfo),
    )?;
    Ok(ret_opt_array(fcinfo, r.map(|v| v.as_slice().to_vec())))
}

// --- array_remove / array_replace (non-strict) ------------------------------

fn fc_array_remove(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // C array_remove uses PG_GETARG_ARRAYTYPE_P, which detoasts: a stored array
    // can be inline-compressed/external, so read it through the detoasting lane
    // before any ARR_* header read (matches array_position(s)/array_dims; raw
    // opt_arg_varlena would read a garbage header on a toasted input).
    let array = match opt_arg_array_detoast(fcinfo, 0)? {
        None => return Ok(ret_null(fcinfo)),
        Some(a) => a,
    };
    let element_type = crate::foundation::arr_elemtype(&array);
    let search_isnull = arg_isnull(fcinfo, 1);
    let search = if search_isnull {
        ArrayElementDatum::ByValue(Datum::from_usize(0))
    } else {
        arg_element(fcinfo, 1, element_type)?
    };
    let m = scratch_mcx();
    let r = crate::sql::array_remove(
        m.mcx(),
        &array,
        search,
        search_isnull,
        collation(fcinfo),
    )?;
    Ok(ret_opt_array(fcinfo, Some(r.as_slice().to_vec())))
}

fn fc_array_replace(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    // C array_replace uses PG_GETARG_ARRAYTYPE_P, which detoasts (see
    // fc_array_remove): detoast before reading the ARR_* header so a toasted /
    // inline-compressed array input is not read as a garbage header.
    let array = match opt_arg_array_detoast(fcinfo, 0)? {
        None => return Ok(ret_null(fcinfo)),
        Some(a) => a,
    };
    let element_type = crate::foundation::arr_elemtype(&array);
    let search_isnull = arg_isnull(fcinfo, 1);
    let search = if search_isnull {
        ArrayElementDatum::ByValue(Datum::from_usize(0))
    } else {
        arg_element(fcinfo, 1, element_type)?
    };
    let replace_isnull = arg_isnull(fcinfo, 2);
    let replace = if replace_isnull {
        ArrayElementDatum::ByValue(Datum::from_usize(0))
    } else {
        arg_element(fcinfo, 2, element_type)?
    };
    let m = scratch_mcx();
    let r = crate::sql::array_replace(
        m.mcx(),
        &array,
        search,
        search_isnull,
        replace,
        replace_isnull,
        collation(fcinfo),
    )?;
    Ok(ret_opt_array(fcinfo, Some(r.as_slice().to_vec())))
}

// --- trim_array / width_bucket_array (strict, anyarray ... -> ...) ----------

fn fc_trim_array(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let n = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let img = crate::sql::trim_array(m.mcx(), &array, n)?.as_slice().to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

fn fc_width_bucket_array(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let thresholds = arg_array_detoast(fcinfo, 1)?;
    let element_type = crate::foundation::arr_elemtype(&thresholds);
    let operand = arg_element(fcinfo, 0, element_type)?;
    Ok(ret_i32(crate::sql::width_bucket_array(
        operand,
        &thresholds,
        collation(fcinfo),
    )?))
}

// --- array_shuffle / array_sample / array_reverse (strict) ------------------

fn fc_array_shuffle(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let img = crate::array_userfuncs::array_shuffle(m.mcx(), &array)?.as_slice().to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

fn fc_array_sample(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let n = arg_int32(fcinfo, 1);
    let m = scratch_mcx();
    let img = crate::array_userfuncs::array_sample(m.mcx(), &array, n)?.as_slice().to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

fn fc_array_reverse(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let img = crate::array_userfuncs::array_reverse(m.mcx(), &array)?.as_slice().to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

// --- array_sort family (strict, anyarray [bool [bool]] -> anyarray) ---------

fn fc_array_sort(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let m = scratch_mcx();
    let arr = ::mcx::slice_in(m.mcx(), &array)?;
    let img = crate::array_userfuncs::array_sort(m.mcx(), &arr, collation(fcinfo))?
        .as_slice()
        .to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

fn fc_array_sort_order(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let descending = fcinfo.arg(1).map(|d| d.value.as_bool()).unwrap_or(false);
    let m = scratch_mcx();
    let arr = ::mcx::slice_in(m.mcx(), &array)?;
    let img = crate::array_userfuncs::array_sort_order(
        m.mcx(),
        &arr,
        descending,
        collation(fcinfo),
    )?
    .as_slice()
    .to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

fn fc_array_sort_order_nulls_first(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    let array = arg_array_detoast(fcinfo, 0)?;
    let descending = fcinfo.arg(1).map(|d| d.value.as_bool()).unwrap_or(false);
    let nulls_first = fcinfo.arg(2).map(|d| d.value.as_bool()).unwrap_or(false);
    let m = scratch_mcx();
    let arr = ::mcx::slice_in(m.mcx(), &array)?;
    let img = crate::array_userfuncs::array_sort_order_nulls_first(
        m.mcx(),
        &arr,
        descending,
        nulls_first,
        collation(fcinfo),
    )?
    .as_slice()
    .to_vec();
    fcinfo.set_ref_result(RefPayload::Varlena(img));
    Ok(Datum::from_usize(0))
}

// --- array_fill / array_fill_with_lower_bounds (non-strict) -----------------

/// Deconstruct an `int4[]` dimension/low-bound array argument into its contained
/// `int` values, mirroring the `array_fill_internal` (arrayfuncs.c) preamble:
/// reject a multi-dimensional array (`ARR_NDIM(dims) > 1`) and a NULL-containing
/// array (`array_contains_nulls(dims)`), then read `ARR_DATA_PTR` as `int *` of
/// length `(ARR_NDIM > 0) ? ARR_DIMS[0] : 0`. The returned Vec is that `int *`
/// span; for a 0-D array it is empty (C `ndims == 0`).
fn deconstruct_int4_array(array: &[u8]) -> PgResult<Vec<i32>> {
    if crate::foundation::arr_ndim(array) > 1 {
        return Err(::types_error::PgError::error("wrong number of array subscripts")
            .with_sqlstate(::types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .with_detail("Dimension array must be one dimensional."));
    }
    if crate::construct::array_contains_nulls(array) {
        return Err(::types_error::PgError::error("dimension values cannot be null")
            .with_sqlstate(::types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }
    // ndims = number of int values in the (1-D) array's data area.
    let count = if crate::foundation::arr_ndim(array) > 0 {
        crate::foundation::arr_dim(array, 0)
    } else {
        0
    };
    let base = crate::foundation::arr_data_offset(array);
    let mut out = Vec::with_capacity(count.max(0) as usize);
    for k in 0..count.max(0) as usize {
        let off = base + k * 4;
        out.push(i32::from_ne_bytes([
            array[off],
            array[off + 1],
            array[off + 2],
            array[off + 3],
        ]));
    }
    Ok(out)
}

/// Resolve the `value` (anyelement, arg 0) of an `array_fill` call into a bare
/// `Datum` word (mirroring C's `value = PG_GETARG_DATUM(0)`), plus the resolved
/// input element type from `get_fn_expr_argtype(flinfo, 0)`. A by-reference
/// element rides its on-disk bytes on the by-ref lane; the returned word then
/// points into the held buffer (kept alive by the returned Vec), matching how
/// `array_fill_internal` later reads/detoasts the value pointer.
#[allow(clippy::type_complexity)]
fn resolve_fill_value(
    fcinfo: &FunctionCallInfoBaseData,
) -> PgResult<(Oid, Datum, bool, Option<Vec<u8>>)> {
    let elmtype =
        fmgr_core::get_fn_expr_argtype(fcinfo.flinfo.as_deref(), 0);
    if !::types_core::OidIsValid(elmtype) {
        return Err(::types_error::PgError::error(
            "could not determine data type of input",
        ));
    }
    if arg_isnull(fcinfo, 0) {
        return Ok((elmtype, Datum::from_usize(0), true, None));
    }
    let s = lsyscache_seams::get_typlenbyvalalign::call(elmtype)?;
    if s.typbyval {
        let word = fcinfo
            .arg(0)
            .map(|d| d.value)
            .unwrap_or(Datum::from_usize(0));
        Ok((elmtype, word, false, None))
    } else {
        let bytes = fcinfo
            .ref_arg(0)
            .and_then(|p| {
                p.as_varlena()
                    .or_else(|| p.as_composite())
                    .or_else(|| p.as_cstring().map(|c| c.as_bytes()))
            })
            .expect("array_fill: by-ref element missing from by-ref lane")
            .to_vec();
        let ptr = bytes.as_ptr() as usize;
        Ok((elmtype, Datum::from_usize(ptr), false, Some(bytes)))
    }
}

/// `array_fill(anyelement, int4[]) -> anyarray` (oid 1193). Non-strict.
fn fc_array_fill(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if arg_isnull(fcinfo, 1) {
        return Err(::types_error::PgError::error(
            "dimension array or low bound array cannot be null",
        )
        .with_sqlstate(::types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }
    let dims_img = arg_array_detoast(fcinfo, 1)?;
    let dims = deconstruct_int4_array(&dims_img)?;
    let (elmtype, value, isnull, _held) = resolve_fill_value(fcinfo)?;
    let m = scratch_mcx();
    let r = crate::sql::array_fill(m.mcx(), value, isnull, elmtype, &dims, &[])?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// `array_fill(anyelement, int4[], int4[]) -> anyarray` (oid 1286). Non-strict.
fn fc_array_fill_with_lower_bounds(fcinfo: &mut FunctionCallInfoBaseData) -> PgResult<Datum> {
    if arg_isnull(fcinfo, 1) || arg_isnull(fcinfo, 2) {
        return Err(::types_error::PgError::error(
            "dimension array or low bound array cannot be null",
        )
        .with_sqlstate(::types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED));
    }
    let dims_img = arg_array_detoast(fcinfo, 1)?;
    let lbs_img = arg_array_detoast(fcinfo, 2)?;
    let dims = deconstruct_int4_array(&dims_img)?;
    let lbs = deconstruct_int4_array(&lbs_img)?;
    // C `array_fill_internal` size-checks the explicit low-bound array against
    // `ndims` (`ndims != ARR_DIMS(lbs)[0]`) — including an explicit empty `{}`
    // low-bound array against a non-empty dims array. The shared `array_fill`
    // body treats an empty `lbs` slice as "apply default low bounds" (the no-lbs
    // path), so an explicit empty `{}` must be range-checked here before the body
    // could silently default it.
    if lbs.len() != dims.len() {
        return Err(::types_error::PgError::error("wrong number of array subscripts")
            .with_sqlstate(::types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR)
            .with_detail("Low bound array has different size than dimensions array."));
    }
    let (elmtype, value, isnull, _held) = resolve_fill_value(fcinfo)?;
    let m = scratch_mcx();
    let r = crate::sql::array_fill(m.mcx(), value, isnull, elmtype, &dims, &lbs)?;
    Ok(ret_varlena(fcinfo, r.as_slice().to_vec()))
}

/// A strict (`proisstrict => 't'`) builtin row.
fn sbuiltin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    builtin(foid, name, nargs, true, false, native)
}

/// A non-strict (`proisstrict => 'f'`) builtin row.
fn nbuiltin(
    foid: u32,
    name: &str,
    nargs: i16,
    native: PgFnNative,
) -> (BuiltinFunction, PgFnNative) {
    builtin(foid, name, nargs, false, false, native)
}

/// Register the SQL-facing array builtins whose bodies are ported, as
/// **Result-native** (panic→Result migration).
pub fn register_arrayfuncs_sql_builtins() {
    fmgr_core::register_builtins_native([
        // comparison / containment
        sbuiltin(744, "array_eq", 2, fc_array_eq),
        sbuiltin(390, "array_ne", 2, fc_array_ne),
        sbuiltin(391, "array_lt", 2, fc_array_lt),
        sbuiltin(392, "array_gt", 2, fc_array_gt),
        sbuiltin(393, "array_le", 2, fc_array_le),
        sbuiltin(396, "array_ge", 2, fc_array_ge),
        sbuiltin(515, "array_larger", 2, fc_array_larger),
        sbuiltin(516, "array_smaller", 2, fc_array_smaller),
        sbuiltin(2748, "arraycontains", 2, fc_arraycontains),
        sbuiltin(2749, "arraycontained", 2, fc_arraycontained),
        sbuiltin(2747, "arrayoverlap", 2, fc_arrayoverlap),
        // btarraycmp / hash
        sbuiltin(382, "btarraycmp", 2, fc_btarraycmp),
        sbuiltin(626, "hash_array", 1, fc_hash_array),
        sbuiltin(782, "hash_array_extended", 2, fc_hash_array_extended),
        // dims / bounds
        sbuiltin(748, "array_ndims", 1, fc_array_ndims),
        sbuiltin(747, "array_dims", 1, fc_array_dims),
        sbuiltin(2091, "array_lower", 2, fc_array_lower),
        sbuiltin(2092, "array_upper", 2, fc_array_upper),
        sbuiltin(2176, "array_length", 2, fc_array_length),
        sbuiltin(3179, "array_cardinality", 1, fc_array_cardinality),
        // trim / width_bucket / shuffle / sample / reverse
        sbuiltin(6172, "trim_array", 2, fc_trim_array),
        sbuiltin(3218, "width_bucket_array", 2, fc_width_bucket_array),
        sbuiltin(6215, "array_shuffle", 1, fc_array_shuffle),
        sbuiltin(6216, "array_sample", 2, fc_array_sample),
        sbuiltin(6381, "array_reverse", 1, fc_array_reverse),
        sbuiltin(6388, "array_sort", 1, fc_array_sort),
        sbuiltin(6389, "array_sort_order", 2, fc_array_sort_order),
        sbuiltin(6390, "array_sort_order_nulls_first", 3, fc_array_sort_order_nulls_first),
        // append / prepend / cat / position / positions / remove / replace
        nbuiltin(378, "array_append", 2, fc_array_append),
        nbuiltin(379, "array_prepend", 2, fc_array_prepend),
        nbuiltin(383, "array_cat", 2, fc_array_cat),
        nbuiltin(3277, "array_position", 2, fc_array_position),
        nbuiltin(3278, "array_position_start", 3, fc_array_position_start),
        nbuiltin(3279, "array_positions", 2, fc_array_positions),
        nbuiltin(3167, "array_remove", 2, fc_array_remove),
        nbuiltin(3168, "array_replace", 3, fc_array_replace),
        // fill (non-strict: a NULL value yields an all-NULL array)
        nbuiltin(1193, "array_fill", 2, fc_array_fill),
        nbuiltin(1286, "array_fill_with_lower_bounds", 3, fc_array_fill_with_lower_bounds),
    ]);
}
