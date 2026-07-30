//! json.c value-rendering half: json_categorize_type (is_jsonb=false),
//! datum_to_json_internal with array/composite recursion, JsonEncodeDateTime,
//! the build_object/build_array workers and json_object(text[]).

extern crate alloc;

use crate::escape_json;
use adt_datetime::consts::{pg_tm, MAXDATELEN, POSTGRES_EPOCH_JDATE, USE_XSD_DATES};
use datum::Datum;
use mcx::{Mcx, PgVec};
use stack_depth::check_stack_depth;
use stringinfo::StringInfo;
use types_core::catalog::{
    FirstNormalObjectId, ANYARRAYOID, ANYCOMPATIBLEARRAYOID, BOOLOID, DATEOID, FLOAT4OID,
    FLOAT8OID, INT2OID, INT4OID, INT8OID, JSONBOID, JSONOID, NUMERICOID, RECORDARRAYOID, TEXTOID,
    TIMESTAMPOID, TIMESTAMPTZOID,
};
use types_core::{InvalidOid, Oid};
use types_error::{
    PgError, PgResult, ERRCODE_ARRAY_SUBSCRIPT_ERROR, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NULL_VALUE_NOT_ALLOWED,
};
use types_fmgr::FmgrInfo;

const F_TEXTOUT: Oid = 47;
const F_BPCHAROUT: Oid = 1045;
const F_VARCHAROUT: Oid = 1047;

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
pub struct TypeCat {
    pub category: JsonTypeCategory,
    pub outfuncoid: Oid,
    outfunc: Option<FmgrInfo>,
}

impl TypeCat {
    pub fn null() -> TypeCat {
        TypeCat {
            category: JsonTypeCategory::Null,
            outfuncoid: InvalidOid,
            outfunc: None,
        }
    }
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

#[track_caller]
#[cold]
#[inline(never)]
fn null_object_key() -> Box<PgError> {
    Box::new(
        PgError::error("null value not allowed for object key")
            .with_sqlstate(ERRCODE_NULL_VALUE_NOT_ALLOWED),
    )
}

fn resolve_output(typoid: Oid) -> PgResult<(Oid, Option<FmgrInfo>)> {
    let (outfunc, _isvarlena) = lsyscache::getTypeOutputInfo(typoid)?;
    Ok((outfunc, Some(fmgr_seams::fmgr_info::call(outfunc)?)))
}

/// C: json_categorize_type (jsonfuncs.c:5999) with is_jsonb=false.
pub fn json_categorize_type(typoid: Oid) -> PgResult<TypeCat> {
    let typoid = lsyscache::getBaseType(typoid)?;
    let (category, (outfuncoid, outfunc)) = match typoid {
        BOOLOID => (JsonTypeCategory::Bool, (InvalidOid, None)),
        INT2OID | INT4OID | INT8OID | FLOAT4OID | FLOAT8OID | NUMERICOID => {
            (JsonTypeCategory::Numeric, resolve_output(typoid)?)
        }
        DATEOID => (JsonTypeCategory::Date, (InvalidOid, None)),
        TIMESTAMPOID => (JsonTypeCategory::Timestamp, (InvalidOid, None)),
        TIMESTAMPTZOID => (JsonTypeCategory::Timestamptz, (InvalidOid, None)),
        JSONOID | JSONBOID => (JsonTypeCategory::Json, resolve_output(typoid)?),
        _ => {
            if lsyscache::get_element_type(typoid)? != InvalidOid
                || typoid == ANYARRAYOID
                || typoid == ANYCOMPATIBLEARRAYOID
                || typoid == RECORDARRAYOID
            {
                (JsonTypeCategory::Array, (InvalidOid, None))
            } else if lsyscache::type_is_rowtype(typoid)? {
                (JsonTypeCategory::Composite, (InvalidOid, None))
            } else if typoid >= FirstNormalObjectId {
                let castfunc = fmgr_seams::find_json_cast_func::call(typoid)?;
                if castfunc != InvalidOid {
                    (
                        JsonTypeCategory::Cast,
                        (castfunc, Some(fmgr_seams::fmgr_info::call(castfunc)?)),
                    )
                } else {
                    (JsonTypeCategory::Other, resolve_output(typoid)?)
                }
            } else {
                (JsonTypeCategory::Other, resolve_output(typoid)?)
            }
        }
    };
    Ok(TypeCat {
        category,
        outfuncoid,
        outfunc,
    })
}

// C: OidOutputFunctionCall — the cstring result is only read before the next
// call through this flinfo, no arena copy needed.
fn output_call<'a>(cat: &'a mut TypeCat, mcx: Mcx<'_>, val: Datum) -> PgResult<&'a [u8]> {
    let flinfo = cat.outfunc.as_mut().expect("category with output function");
    let d = types_fmgr::function_call1_coll_in(flinfo, InvalidOid, mcx, val)?;
    // SAFETY: output functions return a NUL-terminated cstring datum.
    let bytes = unsafe { core::ffi::CStr::from_ptr((d.as_usize() as *const u8).cast()) }.to_bytes();
    Ok(bytes)
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

#[track_caller]
#[cold]
#[inline(never)]
fn timestamp_out_of_range() -> Box<PgError> {
    Box::new(
        PgError::error("timestamp out of range")
            .with_sqlstate(types_error::ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
    )
}

/// C: JsonEncodeDateTime with tzp=NULL. Returns the encoded length in `buf`.
pub fn json_encode_datetime(buf: &mut [u8; MAXDATELEN + 1], val: Datum, typid: Oid) -> PgResult<usize> {
    json_encode_datetime_tz(buf, val, typid, None)
}

/// C: JsonEncodeDateTime (json.c). TIMETZOID takes the TimeTzADT by pointer
/// datum (C ABI); `tzp` applies the zone shift on the TIMESTAMPTZOID arm.
pub fn json_encode_datetime_tz(
    buf: &mut [u8; MAXDATELEN + 1],
    val: Datum,
    typid: Oid,
    tzp: Option<i32>,
) -> PgResult<usize> {
    use types_core::catalog::{TIMEOID, TIMETZOID};
    let len = match typid {
        TIMEOID => {
            let time = val.as_i64();
            let mut tm = pg_tm::default();
            let mut fsec = 0;
            adt_date::time2tm(time, &mut tm, &mut fsec);
            adt_datetime::encode::EncodeTimeOnly(&tm, fsec, false, 0, USE_XSD_DATES, buf)
        }
        TIMETZOID => {
            // SAFETY: a TIMETZOID datum is a live pointer to TimeTzADT.
            let time = unsafe { &*(val.as_usize() as *const adt_date::TimeTzADT) };
            let mut tm = pg_tm::default();
            let mut fsec = 0;
            let mut tz = 0;
            adt_date::timetz2tm(time, &mut tm, &mut fsec, Some(&mut tz));
            adt_datetime::encode::EncodeTimeOnly(&tm, fsec, true, tz, USE_XSD_DATES, buf)
        }
        DATEOID => {
            let date = val.as_i32();
            if adt_date::DATE_NOT_FINITE(date) {
                adt_date::EncodeSpecialDate(date, buf)
            } else {
                let mut tm = pg_tm::default();
                adt_datetime::calendar::j2date(
                    date + POSTGRES_EPOCH_JDATE,
                    &mut tm.tm_year,
                    &mut tm.tm_mon,
                    &mut tm.tm_mday,
                );
                adt_datetime::encode::EncodeDateOnly(&tm, USE_XSD_DATES, buf)
            }
        }
        TIMESTAMPOID => {
            let timestamp = val.as_i64();
            if adt_timestamp::TIMESTAMP_NOT_FINITE(timestamp) {
                adt_timestamp::EncodeSpecialTimestamp(timestamp, buf)
            } else {
                let mut tm = pg_tm::default();
                let mut fsec = 0;
                adt_timestamp::timestamp2tm(timestamp, None, &mut tm, &mut fsec, None, None)
                    .map_err(|_| timestamp_out_of_range())?;
                adt_datetime::encode::EncodeDateTime(&mut tm, fsec, false, 0, None, USE_XSD_DATES, buf)
            }
        }
        TIMESTAMPTZOID => {
            let mut timestamp = val.as_i64();
            let mut tz = 0;
            if let Some(z) = tzp {
                tz = z;
                timestamp -= (z as i64) * adt_datetime::consts::USECS_PER_SEC;
            }
            if adt_timestamp::TIMESTAMP_NOT_FINITE(timestamp) {
                adt_timestamp::EncodeSpecialTimestamp(timestamp, buf)
            } else {
                let mut tm = pg_tm::default();
                let mut fsec = 0;
                let mut tzn: Option<&'static str> = None;
                adt_timestamp::timestamp2tm(
                    timestamp,
                    if tzp.is_some() { None } else { Some(&mut tz) },
                    &mut tm,
                    &mut fsec,
                    if tzp.is_some() { None } else { Some(&mut tzn) },
                    None,
                )
                .map_err(|_| timestamp_out_of_range())?;
                if tzp.is_some() {
                    tm.tm_isdst = 1;
                }
                adt_datetime::encode::EncodeDateTime(
                    &mut tm,
                    fsec,
                    true,
                    tz,
                    tzn.map(|s| s.as_bytes()),
                    USE_XSD_DATES,
                    buf,
                )
            }
        }
        other => panic!("unknown jsonb value datetime type oid {other}"),
    };
    Ok(len)
}

/// json_encode_datetime for callers that keep the encoded bytes (jsonb).
pub fn json_encode_datetime_in<'mcx>(
    mcx: Mcx<'mcx>,
    val: Datum,
    typid: Oid,
) -> PgResult<&'mcx [u8]> {
    let mut buf = [0u8; MAXDATELEN + 1];
    let len = json_encode_datetime(&mut buf, val, typid)?;
    Ok(mcx::slice_in(mcx, &buf[..len])?.leak())
}

fn append_quoted_datetime(
    result: &mut StringInfo<'_>,
    val: Datum,
    typid: Oid,
) -> PgResult<()> {
    let mut buf = [0u8; MAXDATELEN + 1];
    let len = json_encode_datetime(&mut buf, val, typid)?;
    result.append_byte(b'"')?;
    result.append_bytes(&buf[..len])?;
    result.append_byte(b'"')
}

/// C: datum_to_json_internal.
pub fn datum_to_json_internal(
    mcx: Mcx<'_>,
    result: &mut StringInfo<'_>,
    val: Datum,
    is_null: bool,
    cat: &mut TypeCat,
    key_scalar: bool,
) -> PgResult<()> {
    use JsonTypeCategory::*;
    check_stack_depth()?;

    if is_null {
        debug_assert!(!key_scalar);
        return result.append_bytes(b"null");
    }

    if key_scalar && matches!(cat.category, Array | Composite | Json | Jsonb | Cast) {
        return Err(invalid_param(
            "key value must be scalar, not array, composite, or json",
        ));
    }

    match cat.category {
        Array => array_to_json_internal(mcx, result, val, false),
        Composite => composite_to_json(mcx, result, val, false),
        Bool => {
            if key_scalar {
                result.append_bytes(if val.as_bool() { b"\"true\"" } else { b"\"false\"" })
            } else {
                result.append_bytes(if val.as_bool() { b"true" } else { b"false" })
            }
        }
        Numeric => {
            let outputstr = output_call(cat, mcx, val)?;
            // C: unquoted iff a valid JSON number (leading digit or -digit).
            let valid = matches!(outputstr.first(), Some(b'0'..=b'9'))
                || (outputstr.first() == Some(&b'-')
                    && matches!(outputstr.get(1), Some(b'0'..=b'9')));
            if !key_scalar && valid {
                result.append_bytes(outputstr)
            } else {
                result.append_byte(b'"')?;
                result.append_bytes(outputstr)?;
                result.append_byte(b'"')
            }
        }
        Date => append_quoted_datetime(result, val, DATEOID),
        Timestamp => append_quoted_datetime(result, val, TIMESTAMPOID),
        Timestamptz => append_quoted_datetime(result, val, TIMESTAMPTZOID),
        Json => {
            let outputstr = output_call(cat, mcx, val)?;
            result.append_bytes(outputstr)
        }
        Cast => {
            // outfunc is the cast function: returns json text, appended
            // verbatim (json.c JSONTYPE_CAST arm).
            let flinfo = cat.outfunc.as_mut().expect("cast function");
            let d = types_fmgr::function_call1_coll_in(flinfo, InvalidOid, mcx, val)?;
            // SAFETY: the cast to json returns a live text varlena datum.
            let image = unsafe { types_fmgr::datum_varlena_packed(d, mcx)? };
            result.append_bytes(image.data())
        }
        Jsonb | Null => panic!("datum_to_json_internal: unexpected category"),
        Other => {
            if matches!(cat.outfuncoid, F_TEXTOUT | F_VARCHAROUT | F_BPCHAROUT) {
                let image = detoast_datum(mcx, val)?;
                escape_json(result, varlena_payload(image))
            } else {
                let outputstr = output_call(cat, mcx, val)?;
                escape_json(result, outputstr)
            }
        }
    }
}

/// C: array_dim_to_json.
#[allow(clippy::too_many_arguments)]
fn array_dim_to_json(
    mcx: Mcx<'_>,
    result: &mut StringInfo<'_>,
    dim: usize,
    ndims: usize,
    dims: &[i32],
    vals: &[Datum],
    nulls: &[bool],
    valcount: &mut usize,
    cat: &mut TypeCat,
    use_line_feeds: bool,
) -> PgResult<()> {
    debug_assert!(dim < ndims);
    let sep: &[u8] = if use_line_feeds { b",\n " } else { b"," };
    result.append_byte(b'[')?;
    for i in 0..dims[dim] {
        if i > 0 {
            result.append_bytes(sep)?;
        }
        if dim + 1 == ndims {
            datum_to_json_internal(mcx, result, vals[*valcount], nulls[*valcount], cat, false)?;
            *valcount += 1;
        } else {
            array_dim_to_json(
                mcx, result, dim + 1, ndims, dims, vals, nulls, valcount, cat, false,
            )?;
        }
    }
    result.append_byte(b']')
}

/// C: array_to_json_internal.
pub fn array_to_json_internal(
    mcx: Mcx<'_>,
    result: &mut StringInfo<'_>,
    val: Datum,
    use_line_feeds: bool,
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
        return result.append_bytes(b"[]");
    }

    let (elmlen, elmbyval, elmalign) = lsyscache::get_typlenbyvalalign(element_type)?;
    let mut cat = json_categorize_type(element_type)?;
    let (elems, nulls) =
        arrayfuncs::deconstruct_array(mcx, flat, elmlen as i32, elmbyval, elmalign as u8, true)?;

    let mut count = 0usize;
    array_dim_to_json(
        mcx,
        result,
        0,
        ndim as usize,
        &dims[..ndim as usize],
        &elems,
        &nulls,
        &mut count,
        &mut cat,
        use_line_feeds,
    )
}

/// C: composite_to_json.
pub fn composite_to_json(
    mcx: Mcx<'_>,
    result: &mut StringInfo<'_>,
    val: Datum,
    use_line_feeds: bool,
) -> PgResult<()> {
    let sep: &[u8] = if use_line_feeds { b",\n " } else { b"," };
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
    let mut values: PgVec<'_, Datum> = mcx::vec_with_capacity_in(mcx, natts)?;
    values.resize(natts, Datum::null());
    let mut isnull: PgVec<'_, bool> = mcx::vec_with_capacity_in(mcx, natts)?;
    isnull.resize(natts, true);
    types_tuple::getattr::heap_deform_tuple(&tuple, &tupdesc, &mut values, &mut isnull);

    result.append_byte(b'{')?;
    let mut needsep = false;
    for i in 0..natts {
        let att = tupdesc.attr(i);
        if att.attisdropped {
            continue;
        }
        if needsep {
            result.append_bytes(sep)?;
        }
        needsep = true;
        escape_json(result, att.attname.name_str())?;
        result.append_byte(b':')?;
        if isnull[i] {
            let mut cat = TypeCat::null();
            datum_to_json_internal(mcx, result, values[i], true, &mut cat, false)?;
        } else {
            let mut cat = json_categorize_type(att.atttypid)?;
            datum_to_json_internal(mcx, result, values[i], false, &mut cat, false)?;
        }
    }
    result.append_byte(b'}')
}

/// C: add_json.
pub fn add_json(
    mcx: Mcx<'_>,
    result: &mut StringInfo<'_>,
    val: Datum,
    is_null: bool,
    val_type: Oid,
    key_scalar: bool,
) -> PgResult<()> {
    if val_type == InvalidOid {
        return Err(no_input_type());
    }
    let mut cat = if is_null {
        TypeCat::null()
    } else {
        json_categorize_type(val_type)?
    };
    datum_to_json_internal(mcx, result, val, is_null, &mut cat, key_scalar)
}

/// C: datum_to_json + to_json's categorize.
pub fn to_json<'mcx>(mcx: Mcx<'mcx>, val: Datum, val_type: Oid) -> PgResult<datum::Varlena<'mcx>> {
    if val_type == InvalidOid {
        return Err(no_input_type());
    }
    let mut cat = json_categorize_type(val_type)?;
    let mut result = StringInfo::new_in(mcx)?;
    datum_to_json_internal(mcx, &mut result, val, false, &mut cat, false)?;
    varlena::cstring_to_text(mcx, result.as_bytes())
}

/// C: to_json_is_immutable (json.c).
pub fn to_json_is_immutable(typoid: Oid) -> PgResult<bool> {
    use JsonTypeCategory::*;
    let cat = json_categorize_type(typoid)?;
    Ok(match cat.category {
        Bool | Json | Jsonb | Null => true,
        Date | Timestamp | Timestamptz => false,
        Array => false,
        Composite => false,
        // 'i' = PROVOLATILE_IMMUTABLE.
        Numeric | Cast | Other => lsyscache::func_volatile(cat.outfuncoid)? == b'i' as i8,
    })
}

/// datum_to_json over a compile-resolved category carrier
/// (execExprInterp.c ExecEvalJsonConstructor JSCTOR_JSON_SCALAR).
pub fn datum_to_json_cat<'mcx>(
    mcx: Mcx<'mcx>,
    val: Datum,
    cat: &mut TypeCat,
) -> PgResult<datum::Varlena<'mcx>> {
    let mut result = StringInfo::new_in(mcx)?;
    datum_to_json_internal(mcx, &mut result, val, false, cat, false)?;
    varlena::cstring_to_text(mcx, result.as_bytes())
}

#[track_caller]
#[cold]
#[inline(never)]
fn odd_argument_list() -> Box<PgError> {
    Box::new(
        PgError::error("argument list must have even number of elements")
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
            .with_hint(
                "The arguments of json_build_object() must consist of alternating keys and \
                 values.",
            ),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn duplicate_json_object_key(key: &[u8]) -> Box<PgError> {
    Box::new(
        PgError::error(alloc::format!(
            "duplicate JSON object key value: {}",
            String::from_utf8_lossy(key)
        ))
        .with_sqlstate(types_error::ERRCODE_DUPLICATE_JSON_OBJECT_KEY_VALUE),
    )
}

/// C: json_build_object_worker. The unique check dedups the rendered key
/// text (json.c:1290 pstrdup of the appended key, object_id always 0).
pub fn json_build_object_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
    unique_keys: bool,
) -> PgResult<datum::Varlena<'mcx>> {
    if args.len() % 2 != 0 {
        return Err(odd_argument_list());
    }
    let mut result = StringInfo::new_in(mcx)?;
    result.append_byte(b'{')?;
    let mut unique: Option<(mcx::PgFxHashMap<'mcx, &'mcx [u8], ()>, StringInfo<'mcx>)> =
        if unique_keys {
            Some((
                mcx::PgFxHashMap::with_hasher_in(Default::default(), mcx),
                StringInfo::new_in(mcx)?,
            ))
        } else {
            None
        };
    let mut sep: &[u8] = b"";
    let mut i = 0;
    while i < args.len() {
        let skip = absent_on_null && nulls[i + 1];
        if skip && unique.is_none() {
            i += 2;
            continue;
        }
        if nulls[i] {
            return Err(null_object_key());
        }
        let key_offset;
        if skip {
            let throwaway = &mut unique.as_mut().unwrap().1;
            throwaway.truncate(0);
            key_offset = 0;
            add_json(mcx, throwaway, args[i], false, types[i], true)?;
        } else {
            result.append_bytes(sep)?;
            sep = b", ";
            key_offset = result.len();
            add_json(mcx, &mut result, args[i], false, types[i], true)?;
        }
        if let Some((keys, throwaway)) = unique.as_mut() {
            let rendered: &[u8] = if skip {
                &throwaway.as_bytes()[key_offset..]
            } else {
                &result.as_bytes()[key_offset..]
            };
            let key: &'mcx [u8] = mcx::slice_in(mcx, rendered)?.leak();
            if keys.insert(key, ()).is_some() {
                return Err(duplicate_json_object_key(key));
            }
            if skip {
                i += 2;
                continue;
            }
        }
        result.append_bytes(b" : ")?;
        add_json(mcx, &mut result, args[i + 1], nulls[i + 1], types[i + 1], false)?;
        i += 2;
    }
    result.append_byte(b'}')?;
    varlena::cstring_to_text(mcx, result.as_bytes())
}

/// C: json_build_array_worker.
pub fn json_build_array_worker<'mcx>(
    mcx: Mcx<'mcx>,
    args: &[Datum],
    nulls: &[bool],
    types: &[Oid],
    absent_on_null: bool,
) -> PgResult<datum::Varlena<'mcx>> {
    let mut result = StringInfo::new_in(mcx)?;
    result.append_byte(b'[')?;
    let mut sep: &[u8] = b"";
    for i in 0..args.len() {
        if absent_on_null && nulls[i] {
            continue;
        }
        result.append_bytes(sep)?;
        sep = b", ";
        add_json(mcx, &mut result, args[i], nulls[i], types[i], false)?;
    }
    result.append_byte(b']')?;
    varlena::cstring_to_text(mcx, result.as_bytes())
}

#[track_caller]
#[cold]
#[inline(never)]
fn subscript_error(msg: &'static str) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(ERRCODE_ARRAY_SUBSCRIPT_ERROR))
}

// Text-datum payload from a deconstructed text[] element.
fn elem_payload<'mcx>(d: Datum) -> &'mcx [u8] {
    // SAFETY: non-null text element datums point into the flat array image,
    // which lives in the 'mcx arena the array was detoasted into.
    let pv = unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
    pv.data()
}

/// C: json_object (text[] key/value pairs).
pub fn json_object<'mcx>(mcx: Mcx<'mcx>, array: &'mcx [u8]) -> PgResult<datum::Varlena<'mcx>> {
    let ndims = arrayfuncs::arr_ndim(array);
    let dims = arrayfuncs::read_dims_lbounds(array).1;
    match ndims {
        0 => return varlena::cstring_to_text(mcx, b"{}"),
        1 => {
            if dims[0] % 2 != 0 {
                return Err(subscript_error("array must have even number of elements"));
            }
        }
        2 => {
            if dims[1] != 2 {
                return Err(subscript_error("array must have two columns"));
            }
        }
        _ => return Err(subscript_error("wrong number of array subscripts")),
    }

    let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(mcx, array, TEXTOID, true)?;
    let count = elems.len() / 2;
    let mut result = StringInfo::new_in(mcx)?;
    result.append_byte(b'{')?;
    for i in 0..count {
        if nulls[i * 2] {
            return Err(null_object_key());
        }
        if i > 0 {
            result.append_bytes(b", ")?;
        }
        escape_json(&mut result, elem_payload(elems[i * 2]))?;
        result.append_bytes(b" : ")?;
        if nulls[i * 2 + 1] {
            result.append_bytes(b"null")?;
        } else {
            escape_json(&mut result, elem_payload(elems[i * 2 + 1]))?;
        }
    }
    result.append_byte(b'}')?;
    varlena::cstring_to_text(mcx, result.as_bytes())
}

/// C: json_object_two_arg (text[] keys, text[] values).
pub fn json_object_two_arg<'mcx>(
    mcx: Mcx<'mcx>,
    key_array: &'mcx [u8],
    val_array: &'mcx [u8],
) -> PgResult<datum::Varlena<'mcx>> {
    let nkdims = arrayfuncs::arr_ndim(key_array);
    let nvdims = arrayfuncs::arr_ndim(val_array);
    if nkdims > 1 || nkdims != nvdims {
        return Err(subscript_error("wrong number of array subscripts"));
    }
    if nkdims == 0 {
        return varlena::cstring_to_text(mcx, b"{}");
    }
    let (key_elems, key_nulls) = arrayfuncs::deconstruct_array_builtin(mcx, key_array, TEXTOID, true)?;
    let (val_elems, val_nulls) = arrayfuncs::deconstruct_array_builtin(mcx, val_array, TEXTOID, true)?;
    if key_elems.len() != val_elems.len() {
        return Err(subscript_error("mismatched array dimensions"));
    }
    let mut result = StringInfo::new_in(mcx)?;
    result.append_byte(b'{')?;
    for i in 0..key_elems.len() {
        if key_nulls[i] {
            return Err(null_object_key());
        }
        if i > 0 {
            result.append_bytes(b", ")?;
        }
        escape_json(&mut result, elem_payload(key_elems[i]))?;
        result.append_bytes(b" : ")?;
        if val_nulls[i] {
            result.append_bytes(b"null")?;
        } else {
            escape_json(&mut result, elem_payload(val_elems[i]))?;
        }
    }
    result.append_byte(b'}')?;
    varlena::cstring_to_text(mcx, result.as_bytes())
}
