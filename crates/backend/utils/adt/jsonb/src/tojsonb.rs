//! jsonb.c to_jsonb slice: json_categorize_type + datum_to_jsonb_internal
//! over the type categories, incl. array/composite recursion, plus
//! JsonEncodeDateTime (json.c) for the datetime categories.

extern crate alloc;

use crate::build::convert_to_jsonb;
use crate::container::*;
use crate::iter::{JsonbIterator, WjbToken};
use crate::mutate::JsonbPush;
use datum::Datum;
use mcx::{Mcx, PgVec};
use stack_depth::check_stack_depth;
use types_core::catalog::{
    FirstNormalObjectId, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, BOOLOID, DATEOID, FLOAT4OID,
    FLOAT8OID, INT2OID, INT4OID, INT8OID, JSONBOID, JSONOID, NUMERICOID, RECORDARRAYOID,
    TIMESTAMPOID, TIMESTAMPTZOID,
};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use types_fmgr::FmgrInfo;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum JsonTypeCategory {
    Null,
    Bool,
    Numeric,
    Date,
    Timestamp,
    Timestamptz,
    Json,
    Jsonb,
    Array,
    Composite,
    Cast,
    Other,
}

/// A categorized value type with its output function resolved once
/// (C re-resolves via OidOutputFunctionCall per datum).
pub struct ValCategory {
    pub category: JsonTypeCategory,
    pub outfunc: Option<FmgrInfo>,
}

#[track_caller]
#[cold]
#[inline(never)]
fn invalid_param(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

#[cold]
#[inline(never)]
pub fn no_input_type() -> Box<PgError> {
    invalid_param("could not determine input data type")
}

fn resolve_output(typoid: Oid) -> PgResult<Option<FmgrInfo>> {
    let (outfunc, _isvarlena) = lsyscache::getTypeOutputInfo(typoid)?;
    Ok(Some(fmgr_seams::fmgr_info::call(outfunc)?))
}

/// C: json_categorize_type (jsonfuncs.c:5999) with is_jsonb=true.
pub fn json_categorize_type(typoid: Oid) -> PgResult<ValCategory> {
    let typoid = lsyscache::getBaseType(typoid)?;
    let (category, outfunc) = match typoid {
        BOOLOID => (JsonTypeCategory::Bool, None),
        INT2OID | INT4OID | INT8OID | FLOAT4OID | FLOAT8OID | NUMERICOID => {
            (JsonTypeCategory::Numeric, resolve_output(typoid)?)
        }
        DATEOID => (JsonTypeCategory::Date, None),
        TIMESTAMPOID => (JsonTypeCategory::Timestamp, None),
        TIMESTAMPTZOID => (JsonTypeCategory::Timestamptz, None),
        JSONOID => (JsonTypeCategory::Json, None),
        JSONBOID => (JsonTypeCategory::Jsonb, None),
        _ => {
            if lsyscache::get_element_type(typoid)? != InvalidOid
                || typoid == ANYARRAYOID
                || typoid == ANYCOMPATIBLEARRAYOID
                || typoid == RECORDARRAYOID
            {
                (JsonTypeCategory::Array, None)
            } else if lsyscache::type_is_rowtype(typoid)? {
                (JsonTypeCategory::Composite, None)
            } else if typoid >= FirstNormalObjectId {
                let castfunc = fmgr_seams::find_json_cast_func::call(typoid)?;
                if castfunc != InvalidOid {
                    (
                        JsonTypeCategory::Cast,
                        Some(fmgr_seams::fmgr_info::call(castfunc)?),
                    )
                } else {
                    (JsonTypeCategory::Other, resolve_output(typoid)?)
                }
            } else {
                (JsonTypeCategory::Other, resolve_output(typoid)?)
            }
        }
    };
    Ok(ValCategory { category, outfunc })
}

// C: OidOutputFunctionCall — the cstring result is copied into the arena
// (flinfo-scratch results are reused by the next call).
fn output_call<'mcx>(
    mcx: Mcx<'mcx>,
    outfunc: &mut Option<FmgrInfo>,
    val: Datum,
) -> PgResult<&'mcx [u8]> {
    let flinfo = outfunc.as_mut().expect("category with output function");
    let d = types_fmgr::function_call1_coll_in(flinfo, InvalidOid, mcx, val)?;
    // SAFETY: output functions return a NUL-terminated cstring datum.
    let bytes = unsafe { core::ffi::CStr::from_ptr((d.as_usize() as *const u8).cast()) }.to_bytes();
    Ok(mcx::slice_in(mcx, bytes)?.leak())
}

// A detoasted varlena image (4B header) for a by-ref datum.
fn detoast_datum<'mcx>(mcx: Mcx<'mcx>, val: Datum) -> PgResult<&'mcx [u8]> {
    let p = val.as_usize() as *const u8;
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    Ok(detoast_seams::detoast_attr::call(mcx, raw)?.leak())
}

fn varlena_payload(image: &[u8]) -> &[u8] {
    if image[0] & 0x01 == 0x01 {
        &image[1..(image[0] >> 1) as usize]
    } else {
        &image[4..(u32::from_ne_bytes(image[..4].try_into().unwrap()) >> 2) as usize]
    }
}

pub use adt_json::tojson::json_encode_datetime_in as json_encode_datetime;

// C: the "Now insert jb into result" tail of datum_to_jsonb_internal.
fn insert_item<'mcx>(
    ps: &mut JsonbPush<'mcx>,
    item: JsonbItem<'mcx>,
    key_scalar: bool,
) -> PgResult<()> {
    if ps.depth() == 0 {
        ps.push(
            WjbToken::BeginArray,
            JsonbItem::Array {
                n_elems: 1,
                raw_scalar: true,
            },
        )?;
        ps.push(WjbToken::Elem, item)?;
        ps.push_token(WjbToken::EndArray)
    } else if ps.in_array() {
        ps.push(WjbToken::Elem, item)
    } else {
        ps.push(
            if key_scalar {
                WjbToken::Key
            } else {
                WjbToken::Value
            },
            item,
        )
    }
}

/// C: datum_to_jsonb_internal.
pub fn datum_to_jsonb_internal<'mcx>(
    mcx: Mcx<'mcx>,
    ps: &mut JsonbPush<'mcx>,
    val: Datum,
    is_null: bool,
    cat: &mut ValCategory,
    key_scalar: bool,
) -> PgResult<()> {
    use JsonTypeCategory::*;
    check_stack_depth()?;

    if is_null {
        debug_assert!(!key_scalar);
        return insert_item(ps, JsonbItem::Null, false);
    }
    if key_scalar && matches!(cat.category, Array | Composite | Json | Jsonb | Cast) {
        return Err(invalid_param(
            "key value must be scalar, not array, composite, or json",
        ));
    }

    let item: JsonbItem<'mcx> = match cat.category {
        Array => return array_to_jsonb_internal(mcx, ps, val),
        Composite => return composite_to_jsonb(mcx, ps, val),
        Bool => {
            if key_scalar {
                JsonbItem::String(if val.as_bool() { b"true" } else { b"false" })
            } else {
                JsonbItem::Bool(val.as_bool())
            }
        }
        Numeric => {
            let outputstr = output_call(mcx, &mut cat.outfunc, val)?;
            if key_scalar {
                JsonbItem::String(outputstr)
            } else if outputstr.contains(&b'N') || outputstr.contains(&b'n') {
                // C: invalid numeric output (NaN/Infinity) renders as string.
                JsonbItem::String(outputstr)
            } else {
                let s = core::str::from_utf8(outputstr).expect("numeric output is ASCII");
                let img = adt_numeric::numeric_in(s, -1, None)?
                    .expect("numeric_in of numeric output never soft-fails");
                JsonbItem::Numeric(mcx::slice_in(mcx, img.as_bytes())?.leak())
            }
        }
        Date => JsonbItem::String(json_encode_datetime(mcx, val, DATEOID)?),
        Timestamp => JsonbItem::String(json_encode_datetime(mcx, val, TIMESTAMPOID)?),
        Timestamptz => JsonbItem::String(json_encode_datetime(mcx, val, TIMESTAMPTZOID)?),
        Json => {
            let image = detoast_datum(mcx, val)?;
            return crate::io::parse_json_into(mcx, ps, varlena_payload(image));
        }
        Cast => {
            // outfunc is the cast function: json text parsed into the builder
            // (jsonb.c JSONTYPE_CAST falls into the JSONTYPE_JSON arm).
            let flinfo = cat.outfunc.as_mut().expect("cast function");
            let d = types_fmgr::function_call1_coll_in(flinfo, InvalidOid, mcx, val)?;
            let image = detoast_datum(mcx, d)?;
            return crate::io::parse_json_into(mcx, ps, varlena_payload(image));
        }
        Jsonb => {
            let image = detoast_datum(mcx, val)?;
            let payload = jsonb_image_payload(image);
            if container_is_scalar(payload) {
                let v = get_ith_value(payload, 0).expect("raw-scalar container");
                // scalar_jsonb: falls through to the common insert tail.
                v
            } else {
                let mut it = JsonbIterator::init(mcx, payload)?;
                loop {
                    let (tok, v) = it.next(false);
                    if tok == WjbToken::Done {
                        break;
                    }
                    ps.push(tok, v)?;
                }
                return Ok(());
            }
        }
        Null | Other => {
            let outputstr = output_call(mcx, &mut cat.outfunc, val)?;
            crate::io::check_string_len_hard(outputstr.len())?;
            JsonbItem::String(outputstr)
        }
    };
    insert_item(ps, item, key_scalar)
}

// The root JsonbContainer payload of a detoasted 4B-header jsonb image,
// re-aligned when the header offset leaves embedded numerics misaligned.
fn jsonb_image_payload(image: &[u8]) -> &[u8] {
    debug_assert!(image[0] & 0x01 == 0);
    &image[4..]
}

/// C: array_to_jsonb_internal + array_dim_to_jsonb.
fn array_to_jsonb_internal<'mcx>(
    mcx: Mcx<'mcx>,
    ps: &mut JsonbPush<'mcx>,
    val: Datum,
) -> PgResult<()> {
    let flat = detoast_datum(mcx, val)?;
    let element_type = arrayfuncs::arr_elemtype(flat);
    let (ndim, dims, _lbs) = arrayfuncs::read_dims_lbounds(flat);
    let nitems: i64 = dims[..ndim.max(0) as usize]
        .iter()
        .map(|&d| d as i64)
        .product::<i64>()
        * if ndim > 0 { 1 } else { 0 };

    if nitems <= 0 {
        ps.push_token(WjbToken::BeginArray)?;
        return ps.push_token(WjbToken::EndArray);
    }

    let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(element_type)?;
    let mut tcat = json_categorize_type(element_type)?;
    let (elems, nulls) =
        arrayfuncs::deconstruct_array(mcx, flat, elmlen as i32, elmbyval, elmalign as u8, true)?;

    let mut count = 0usize;
    array_dim_to_jsonb(
        mcx,
        ps,
        0,
        ndim as usize,
        &dims[..ndim as usize],
        &elems,
        &nulls,
        &mut count,
        &mut tcat,
    )
}

#[allow(clippy::too_many_arguments)]
fn array_dim_to_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    ps: &mut JsonbPush<'mcx>,
    dim: usize,
    ndims: usize,
    dims: &[i32],
    vals: &[Datum],
    nulls: &[bool],
    valcount: &mut usize,
    tcat: &mut ValCategory,
) -> PgResult<()> {
    debug_assert!(dim < ndims);
    ps.push_token(WjbToken::BeginArray)?;
    for _ in 0..dims[dim] {
        if dim + 1 == ndims {
            datum_to_jsonb_internal(mcx, ps, vals[*valcount], nulls[*valcount], tcat, false)?;
            *valcount += 1;
        } else {
            array_dim_to_jsonb(mcx, ps, dim + 1, ndims, dims, vals, nulls, valcount, tcat)?;
        }
    }
    ps.push_token(WjbToken::EndArray)
}

/// C: composite_to_jsonb.
fn composite_to_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    ps: &mut JsonbPush<'mcx>,
    val: Datum,
) -> PgResult<()> {
    let image = detoast_datum(mcx, val)?;
    // SAFETY: a detoasted composite datum is a HeapTupleHeader image; the
    // arena copy is 8-aligned and readable for its datum length.
    let header = unsafe { &*(image.as_ptr() as *const types_tuple::htup::HeapTupleHeaderData) };
    let tup_type = header.type_id();
    let tup_typmod = header.typmod();
    let tupdesc = typcache_seams::lookup_rowtype_tupdesc_copy::call(mcx, tup_type, tup_typmod)?;
    // SAFETY: image is the live 8-aligned tuple image for its datum length.
    let tuple = unsafe {
        types_tuple::htup::HeapTupleData::from_raw_parts(
            image.as_ptr(),
            header.datum_length(),
            Default::default(),
            InvalidOid,
        )
    };
    let natts = tupdesc.natts as usize;
    let mut values: PgVec<'mcx, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    let mut isnull: PgVec<'mcx, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    isnull.resize(natts, true);
    types_tuple::getattr::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut isnull);

    ps.push_token(WjbToken::BeginObject)?;
    for i in 0..natts {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        let attname: &'mcx [u8] = mcx::slice_in(mcx, att.attname.name_str())?.leak();
        ps.push(WjbToken::Key, JsonbItem::String(attname))?;
        if isnull[i] {
            insert_item(ps, JsonbItem::Null, false)?;
        } else {
            let mut tcat = json_categorize_type(att.atttypid)?;
            datum_to_jsonb_internal(mcx, ps, values[i], false, &mut tcat, false)?;
        }
    }
    ps.push_token(WjbToken::EndObject)
}

/// C: add_jsonb.
pub fn add_jsonb<'mcx>(
    mcx: Mcx<'mcx>,
    ps: &mut JsonbPush<'mcx>,
    val: Datum,
    is_null: bool,
    val_type: Oid,
    key_scalar: bool,
) -> PgResult<()> {
    if val_type == InvalidOid {
        return Err(no_input_type());
    }
    let mut cat = if is_null {
        ValCategory {
            category: JsonTypeCategory::Null,
            outfunc: None,
        }
    } else {
        json_categorize_type(val_type)?
    };
    datum_to_jsonb_internal(mcx, ps, val, is_null, &mut cat, key_scalar)
}

#[track_caller]
#[cold]
#[inline(never)]
fn odd_argument_list() -> Box<PgError> {
    Box::new(
        PgError::error("argument list must have even number of elements")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint(
                "The arguments of jsonb_build_object() must consist of alternating keys and \
                 values.",
            ),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn null_key(argno: usize) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!("argument {argno}: key must not be null"))
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

/// C: jsonb_build_object_worker.
pub fn jsonb_build_object_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    if args.len() % 2 != 0 {
        return Err(odd_argument_list());
    }
    let mut ps = JsonbPush::new(mcx)?;
    ps.push_object_start(unique_keys, absent_on_null)?;
    let mut i = 0;
    while i < args.len() {
        if nulls[i] {
            return Err(null_key(i + 1));
        }
        // Skipped keys still enter the frame when unique_keys — the
        // uniqueify skip_nulls pass drops them (jsonb.c:1155-1161).
        if absent_on_null && nulls[i + 1] && !unique_keys {
            i += 2;
            continue;
        }
        add_jsonb(mcx, &mut ps, args[i], false, types[i], true)?;
        add_jsonb(mcx, &mut ps, args[i + 1], nulls[i + 1], types[i + 1], false)?;
        i += 2;
    }
    ps.push_token(WjbToken::EndObject)?;
    convert_to_jsonb(mcx, &ps.finish())
}

/// C: jsonb_build_array_worker.
pub fn jsonb_build_array_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut ps = JsonbPush::new(mcx)?;
    ps.push_token(WjbToken::BeginArray)?;
    for i in 0..args.len() {
        if absent_on_null && nulls[i] {
            continue;
        }
        add_jsonb(mcx, &mut ps, args[i], nulls[i], types[i], false)?;
    }
    ps.push_token(WjbToken::EndArray)?;
    convert_to_jsonb(mcx, &ps.finish())
}

/// C: to_jsonb_is_immutable (jsonb.c).
pub fn to_jsonb_is_immutable(typoid: Oid) -> PgResult<bool> {
    let cat = json_categorize_type(typoid)?;
    Ok(match cat.category {
        JsonTypeCategory::Null
        | JsonTypeCategory::Bool
        | JsonTypeCategory::Json
        | JsonTypeCategory::Jsonb => true,
        JsonTypeCategory::Date
        | JsonTypeCategory::Timestamp
        | JsonTypeCategory::Timestamptz
        | JsonTypeCategory::Array
        | JsonTypeCategory::Composite => false,
        // 'i' = PROVOLATILE_IMMUTABLE.
        _ => {
            let oid = cat.outfunc.as_ref().map_or(InvalidOid, |f| f.fn_oid);
            lsyscache::func_volatile(oid)? == b'i' as i8
        }
    })
}

/// datum_to_jsonb over a compile-resolved category carrier
/// (execExprInterp.c ExecEvalJsonConstructor JSCTOR_JSON_SCALAR).
pub fn datum_to_jsonb_cat<'mcx>(
    mcx: Mcx<'mcx>,
    val: Datum,
    cat: &mut ValCategory,
) -> PgResult<PgVec<'mcx, u8>> {
    let mut ps = JsonbPush::new(mcx)?;
    datum_to_jsonb_internal(mcx, &mut ps, val, false, cat, false)?;
    convert_to_jsonb(mcx, &ps.finish())
}

/// C: to_jsonb + datum_to_jsonb.
pub fn to_jsonb<'mcx>(mcx: Mcx<'mcx>, val: Datum, val_type: Oid) -> PgResult<PgVec<'mcx, u8>> {
    if val_type == InvalidOid {
        return Err(no_input_type());
    }
    let mut cat = json_categorize_type(val_type)?;
    let mut ps = JsonbPush::new(mcx)?;
    datum_to_jsonb_internal(mcx, &mut ps, val, false, &mut cat, false)?;
    convert_to_jsonb(mcx, &ps.finish())
}

// Text-datum payload from a deconstructed text[] element.
fn obj_elem_payload<'mcx>(d: Datum) -> &'mcx [u8] {
    // SAFETY: non-null text element datums point into the flat array image,
    // which lives in the 'mcx arena the array was detoasted into.
    let pv = unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
    pv.data()
}

#[cold]
fn obj_subscript_error(msg: &'static str) -> alloc::boxed::Box<types_error::PgError> {
    alloc::boxed::Box::new(
        types_error::PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

#[cold]
fn obj_null_key() -> alloc::boxed::Box<types_error::PgError> {
    alloc::boxed::Box::new(
        types_error::PgError::error("null value not allowed for object key")
            .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

fn jsonb_object_finish<'mcx>(
    mcx: Mcx<'mcx>,
    pairs: &[(Datum, bool)],
) -> PgResult<PgVec<'mcx, u8>> {
    let mut ps = crate::mutate::JsonbPush::new(mcx)?;
    ps.push_token(WjbToken::BeginObject)?;
    for chunk in pairs.chunks_exact(2) {
        let (kd, knull) = chunk[0];
        if knull {
            return Err(obj_null_key());
        }
        ps.push(WjbToken::Key, JsonbItem::String(obj_elem_payload(kd)))?;
        let (vd, vnull) = chunk[1];
        if vnull {
            ps.push(WjbToken::Value, JsonbItem::Null)?;
        } else {
            ps.push(WjbToken::Value, JsonbItem::String(obj_elem_payload(vd)))?;
        }
    }
    ps.push_token(WjbToken::EndObject)?;
    crate::build::convert_to_jsonb(mcx, &ps.finish())
}

/// C: jsonb_object (text[] key/value pairs).
pub fn jsonb_object<'mcx>(mcx: Mcx<'mcx>, array: &'mcx [u8]) -> PgResult<PgVec<'mcx, u8>> {
    let ndims = arrayfuncs::arr_ndim(array);
    let dims = arrayfuncs::read_dims_lbounds(array).1;
    match ndims {
        0 => return jsonb_object_finish(mcx, &[]),
        1 => {
            if dims[0] % 2 != 0 {
                return Err(obj_subscript_error("array must have even number of elements"));
            }
        }
        2 => {
            if dims[1] != 2 {
                return Err(obj_subscript_error("array must have two columns"));
            }
        }
        _ => return Err(obj_subscript_error("wrong number of array subscripts")),
    }
    let (elems, nulls) =
        arrayfuncs::deconstruct_array_builtin(mcx, array, types_core::TEXTOID, true)?;
    let pairs: alloc::vec::Vec<(Datum, bool)> =
        elems.iter().copied().zip(nulls.iter().copied()).collect();
    jsonb_object_finish(mcx, &pairs[..(pairs.len() / 2) * 2])
}

/// C: jsonb_object_two_arg (text[] keys, text[] values).
pub fn jsonb_object_two_arg<'mcx>(
    mcx: Mcx<'mcx>,
    key_array: &'mcx [u8],
    val_array: &'mcx [u8],
) -> PgResult<PgVec<'mcx, u8>> {
    let nkdims = arrayfuncs::arr_ndim(key_array);
    let nvdims = arrayfuncs::arr_ndim(val_array);
    if nkdims > 1 || nkdims != nvdims {
        return Err(obj_subscript_error("wrong number of array subscripts"));
    }
    if nkdims == 0 {
        return jsonb_object_finish(mcx, &[]);
    }
    let (key_elems, key_nulls) =
        arrayfuncs::deconstruct_array_builtin(mcx, key_array, types_core::TEXTOID, true)?;
    let (val_elems, val_nulls) =
        arrayfuncs::deconstruct_array_builtin(mcx, val_array, types_core::TEXTOID, true)?;
    if key_elems.len() != val_elems.len() {
        return Err(obj_subscript_error("mismatched array dimensions"));
    }
    let mut pairs: alloc::vec::Vec<(Datum, bool)> =
        alloc::vec::Vec::with_capacity(key_elems.len() * 2);
    for i in 0..key_elems.len() {
        pairs.push((key_elems[i], key_nulls[i]));
        pairs.push((val_elems[i], val_nulls[i]));
    }
    jsonb_object_finish(mcx, &pairs)
}
