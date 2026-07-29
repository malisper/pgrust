//! fmgr wrappers (`fc_*`) + the `JSONB_BUILTINS` table for fmgr-core. The rest
//! of the jsonb surface (mutation family, jsonpath, subscripting, aggregates,
//! GIN, scalar casts, to_jsonb/build/object) stays loud via unported OIDs.

extern crate alloc;

use crate::container::{container_size, JsonbItem};
use crate::getfield::PathResult;
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::{Oid, TEXTOID};
use types_error::PgResult;
use types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction, PackedVarlena,
};
use varlena::VarPayload;

// Result images leak into the arming context (C palloc ownership).
pub(crate) fn image_result(v: PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    d
}

// C: PG_GETARG_JSONB_P — detoast; the payload is the root JsonbContainer.
// Short varlenas are expanded to an aligned copy like pg_detoast_datum: the
// container must start 4-aligned so embedded numeric digit arrays stay
// 2-aligned (mcx allocations are 8-aligned; VARHDRSZ keeps payloads at +4).
pub(crate) fn arg_jsonb<'a, 'mcx>(
    fcinfo: &'a Fcinfo,
    i: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<VarPayload<'a, 'mcx>> {
    // SAFETY: catalog arg i is a non-null jsonb varlena (strict functions only).
    let p = unsafe { fcinfo.arg_ptr(i) };
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let image = unsafe {
        core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
    };
    if image[0] & 0x01 == 0x01 && image[0] != 0x01 {
        let payload = &image[1..];
        let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 4 + payload.len())?;
        mcx::vec_append_bytes(&mut v, &(((4 + payload.len()) as u32) << 2).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, payload)?;
        return Ok(VarPayload::Detoasted(v));
    }
    varlena::open_image(mcx, image)
}

/// C: json_get_first_token (jsonfuncs.c), throw_error=false form:
/// None = lex error.
pub fn json_get_first_token(
    json: &[u8],
) -> PgResult<Option<adt_json::jsonapi::JsonToken>> {
    let mut lex = adt_json::jsonapi::JsonLex::new(json, mbutils::GetDatabaseEncoding());
    let r = lex.lex();
    if r != adt_json::jsonapi::JsonError::Success {
        return Ok(None);
    }
    Ok(Some(lex.token_type))
}

/// arg_jsonb over a bare Datum (executor step paths carry no fcinfo).
/// # Safety: `d` is a non-null jsonb varlena live for `'mcx`.
pub unsafe fn jsonb_payload_from_datum<'mcx>(
    mcx: Mcx<'mcx>,
    d: Datum,
) -> PgResult<VarPayload<'mcx, 'mcx>> {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — a live varlena readable through VARSIZE_ANY.
    let image = unsafe {
        core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p))
    };
    if image[0] & 0x01 == 0x01 && image[0] != 0x01 {
        let payload = &image[1..];
        let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 4 + payload.len())?;
        mcx::vec_append_bytes(&mut v, &(((4 + payload.len()) as u32) << 2).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, payload)?;
        return Ok(VarPayload::Detoasted(v));
    }
    varlena::open_image(mcx, image)
}

pub fn fc_jsonb_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of jsonb_in is a non-null cstring (strict fn).
    let (d, had_esc) = {
        let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
        let mcx = fcinfo.result_mcx();
        // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
        let esc = unsafe { fcinfo.soft_error_context() };
        let had_esc = esc.is_some();
        (crate::io::jsonb_in(mcx, s, esc)?.map(image_result), had_esc)
    };
    match d {
        Some(d) => Ok(d),
        None if had_esc => Ok(fcinfo.return_null()),
        None => panic!("jsonb_in: soft-error escape without an escontext"),
    }
}

pub fn fc_jsonb_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    Ok(cstring_result(crate::io::jsonb_out(mcx, jb.as_bytes())?))
}

pub fn fc_jsonb_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of jsonb_recv is a live &mut StringInfo (internal ABI).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    Ok(image_result(crate::io::jsonb_recv(mcx, buf)?))
}

pub fn fc_jsonb_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    Ok(varlena_result(crate::io::jsonb_send(mcx, jb.as_bytes())?))
}

pub fn fc_jsonb_typeof(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let name = crate::io::container_type_name(jb.as_bytes());
    Ok(varlena_result(varlena::cstring_to_text(
        mcx,
        name.as_bytes(),
    )?))
}

// C jsonb_gin.c gin_compare_jsonb: varstr_cmp(a, b, C_COLLATION_OID) over
// the two text keys — byte compare + length tiebreak (SQL-callable; the GIN
// opclass dispatch calls the core directly).
pub fn fc_gin_compare_jsonb(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: catalog args of gin_compare_jsonb are non-null text (strict fn).
    let a = unsafe { fcinfo.arg_varlena_packed(0)? };
    let b = unsafe { fcinfo.arg_varlena_packed(1)? };
    Ok(Datum::from_i32(crate::gin::gin_compare_jsonb(a.data(), b.data())))
}

pub fn fc_jsonb_object_field(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        let jb = arg_jsonb(fcinfo, 0, mcx)?;
        // SAFETY: catalog arg 1 is a non-null text varlena (strict fn).
        let key = unsafe { fcinfo.arg_varlena_packed(1)? };
        crate::getfield::object_field(mcx, jb.as_bytes(), key.data())?.map(image_result)
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_object_field_text(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        let jb = arg_jsonb(fcinfo, 0, mcx)?;
        // SAFETY: catalog arg 1 is a non-null text varlena (strict fn).
        let key = unsafe { fcinfo.arg_varlena_packed(1)? };
        crate::getfield::object_field_text(mcx, jb.as_bytes(), key.data())?.map(varlena_result)
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_array_element(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        let jb = arg_jsonb(fcinfo, 0, mcx)?;
        let element = fcinfo.arg_i32(1);
        crate::getfield::array_element(mcx, jb.as_bytes(), element)?.map(image_result)
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_array_element_text(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        let jb = arg_jsonb(fcinfo, 0, mcx)?;
        let element = fcinfo.arg_i32(1);
        crate::getfield::array_element_text(mcx, jb.as_bytes(), element)?.map(varlena_result)
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

// Text-array argument decomposed to payload slices borrowed from the image.
fn text_array_elems<'mcx>(
    fcinfo: &Fcinfo,
    i: usize,
    mcx: Mcx<'mcx>,
    skip_nulls: bool,
) -> PgResult<Option<PgVec<'mcx, &'mcx [u8]>>> {
    // SAFETY: catalog arg i is a non-null text[] (strict fn).
    let p = unsafe { fcinfo.arg_ptr(i) };
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    // C: PG_GETARG_ARRAYTYPE_P — a flat 4B-header image for the ARR_* reads.
    let array: &'mcx [u8] = detoast_seams::detoast_attr::call(mcx, raw)?.leak();
    if !skip_nulls && arrayfuncs::array_contains_nulls(array) {
        return Ok(None);
    }
    let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(mcx, array, TEXTOID, true)?;
    let mut out = mcx::vec_with_capacity_in(mcx, elems.len())?;
    for (d, isnull) in elems.iter().zip(nulls.iter()) {
        if *isnull {
            continue;
        }
        // SAFETY: non-null text element datums point into the flat image.
        let pv = unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) };
        out.push(pv.data());
    }
    Ok(Some(out))
}

fn extract_path(fcinfo: &mut Fcinfo, as_text: bool) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        let jb = arg_jsonb(fcinfo, 0, mcx)?;
        // C: get_jsonb_path_all — a null path element yields NULL.
        match text_array_elems(fcinfo, 1, mcx, false)? {
            None => None,
            Some(path) => match crate::getfield::get_element(mcx, jb.as_bytes(), &path, as_text)? {
                PathResult::Null => None,
                PathResult::Jsonb(v) => Some(image_result(v)),
                PathResult::Text(t) => Some(varlena_result(t)),
                PathResult::Input => {
                    let img = crate::build::item_to_jsonb_image(
                        mcx,
                        crate::container::JsonbItem::Binary(jb.as_bytes()),
                    )?;
                    Some(image_result(img))
                }
            },
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_extract_path(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    extract_path(fcinfo, false)
}

pub fn fc_jsonb_extract_path_text(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    extract_path(fcinfo, true)
}

pub fn fc_jsonb_exists(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    // SAFETY: catalog arg 1 is a non-null text varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(1)? };
    Ok(Datum::from_bool(crate::ops::exists_key(
        jb.as_bytes(),
        key.data(),
    )))
}

pub fn fc_jsonb_exists_any(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let keys = text_array_elems(fcinfo, 1, mcx, true)?.expect("skip_nulls returns Some");
    let payload = jb.as_bytes();
    Ok(Datum::from_bool(
        keys.iter().any(|k| crate::ops::exists_key(payload, k)),
    ))
}

pub fn fc_jsonb_exists_all(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let keys = text_array_elems(fcinfo, 1, mcx, true)?.expect("skip_nulls returns Some");
    let payload = jb.as_bytes();
    Ok(Datum::from_bool(
        keys.iter().all(|k| crate::ops::exists_key(payload, k)),
    ))
}

fn contains_worker(fcinfo: &mut Fcinfo, commute: bool) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let (vi, ti) = if commute { (1, 0) } else { (0, 1) };
    let val = arg_jsonb(fcinfo, vi, mcx)?;
    let tmpl = arg_jsonb(fcinfo, ti, mcx)?;
    Ok(Datum::from_bool(crate::ops::jsonb_contains(
        mcx,
        val.as_bytes(),
        tmpl.as_bytes(),
    )?))
}

pub fn fc_jsonb_contains(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    contains_worker(fcinfo, false)
}

pub fn fc_jsonb_contained(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    contains_worker(fcinfo, true)
}

fn cmp_args(fcinfo: &mut Fcinfo) -> PgResult<i32> {
    let mcx = fcinfo.result_mcx();
    let a = arg_jsonb(fcinfo, 0, mcx)?;
    let b = arg_jsonb(fcinfo, 1, mcx)?;
    crate::ops::compare_containers(mcx, a.as_bytes(), b.as_bytes())
}

pub fn fc_jsonb_eq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? == 0))
}

pub fn fc_jsonb_ne(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? != 0))
}

pub fn fc_jsonb_lt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? < 0))
}

pub fn fc_jsonb_gt(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? > 0))
}

pub fn fc_jsonb_le(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? <= 0))
}

pub fn fc_jsonb_ge(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_bool(cmp_args(fcinfo)? >= 0))
}

pub fn fc_jsonb_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(cmp_args(fcinfo)?))
}

pub fn fc_jsonb_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let payload = jb.as_bytes();
    if container_size(payload) == 0 {
        return Ok(Datum::from_i32(0));
    }
    Ok(Datum::from_i32(crate::ops::jsonb_hash(mcx, payload)? as i32))
}

pub fn fc_jsonb_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let seed = fcinfo.arg_i64(1) as u64;
    Ok(Datum::from_i64(
        crate::ops::jsonb_hash_extended(mcx, jb.as_bytes(), seed)? as i64,
    ))
}

// The flat text[] image of arg `i`, detoasted into `mcx`.
fn arg_flat_array<'mcx>(fcinfo: &Fcinfo, i: usize, mcx: Mcx<'mcx>) -> PgResult<&'mcx [u8]> {
    // SAFETY: catalog arg i is a non-null array varlena (checked by caller).
    let p = unsafe { fcinfo.arg_ptr(i) };
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    Ok(detoast_seams::detoast_attr::call(mcx, raw)?.leak())
}

#[cold]
#[inline(never)]
fn subscript_error() -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error("wrong number of array subscripts")
            .with_sqlstate(types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR),
    )
}

#[cold]
#[inline(never)]
fn invalid_param(msg: &'static str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

// Path elements with SQL NULLs preserved positionally (setPath semantics).
fn path_elems<'mcx>(
    mcx: Mcx<'mcx>,
    array: &'mcx [u8],
) -> PgResult<PgVec<'mcx, Option<&'mcx [u8]>>> {
    let (elems, nulls) = arrayfuncs::deconstruct_array_builtin(mcx, array, TEXTOID, true)?;
    let mut out = mcx::vec_with_capacity_in(mcx, elems.len())?;
    for (d, isnull) in elems.iter().zip(nulls.iter()) {
        if *isnull {
            out.push(None);
        } else {
            // SAFETY: non-null text element datums point into the flat image.
            let pv = unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) };
            out.push(Some(pv.data()));
        }
    }
    Ok(out)
}

// C: JsonbToJsonbValue — scalar roots unwrap, containers ride as jbvBinary.
pub(crate) fn root_item(payload: &[u8]) -> JsonbItem<'_> {
    match crate::io::extract_scalar(payload) {
        Some(v) => v,
        None => JsonbItem::Binary(payload),
    }
}

fn input_image<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum> {
    Ok(image_result(crate::build::item_to_jsonb_image(
        mcx,
        JsonbItem::Binary(payload),
    )?))
}

pub fn fc_jsonb_concat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb1 = arg_jsonb(fcinfo, 0, mcx)?;
    let jb2 = arg_jsonb(fcinfo, 1, mcx)?;
    let d = image_result(crate::mutate::concat(mcx, jb1.as_bytes(), jb2.as_bytes())?);
    Ok(d)
}

pub fn fc_jsonb_delete(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    // SAFETY: catalog arg 1 is a non-null text varlena (strict fn).
    let key = unsafe { fcinfo.arg_varlena_packed(1)? };
    let d = image_result(crate::mutate::delete_key(mcx, jb.as_bytes(), key.data())?);
    Ok(d)
}

pub fn fc_jsonb_delete_idx(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let idx = fcinfo.arg_i32(1);
    let d = image_result(crate::mutate::delete_idx(mcx, jb.as_bytes(), idx)?);
    Ok(d)
}

pub fn fc_jsonb_delete_array(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let payload = jb.as_bytes();
    let array = arg_flat_array(fcinfo, 1, mcx)?;
    if arrayfuncs::arr_ndim(array) > 1 {
        return Err(subscript_error());
    }
    if crate::container::container_is_scalar(payload) {
        return Err(invalid_param("cannot delete from scalar"));
    }
    if container_size(payload) == 0 {
        return input_image(mcx, payload);
    }
    let elems = path_elems(mcx, array)?;
    let mut keys: PgVec<'_, &[u8]> = mcx::vec_with_capacity_in(mcx, elems.len())?;
    for e in elems.iter().flatten() {
        keys.push(*e);
    }
    let d = image_result(crate::mutate::delete_keys(mcx, payload, keys.as_slice())?);
    Ok(d)
}

fn set_path_common<'mcx>(
    mcx: Mcx<'mcx>,
    payload: &'mcx [u8],
    array: &'mcx [u8],
    newval: Option<JsonbItem<'mcx>>,
    op_type: u32,
    scalar_msg: &'static str,
    skip_when_empty: bool,
) -> PgResult<Datum> {
    if arrayfuncs::arr_ndim(array) > 1 {
        return Err(subscript_error());
    }
    if crate::container::container_is_scalar(payload) {
        return Err(invalid_param(scalar_msg));
    }
    if skip_when_empty && container_size(payload) == 0 {
        return input_image(mcx, payload);
    }
    let path = path_elems(mcx, array)?;
    if path.is_empty() {
        return input_image(mcx, payload);
    }
    let args = crate::mutate::SetPathArgs {
        path: path.as_slice(),
        newval,
        op_type,
    };
    Ok(image_result(crate::mutate::set_path(mcx, payload, &args)?))
}

pub fn fc_jsonb_set(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let array = arg_flat_array(fcinfo, 1, mcx)?;
    let newjb = arg_jsonb(fcinfo, 2, mcx)?;
    let create = fcinfo.arg_bool(3);
    set_path_common(
        mcx,
        jb.as_bytes(),
        array,
        Some(root_item(newjb.as_bytes())),
        if create {
            crate::mutate::JB_PATH_CREATE
        } else {
            crate::mutate::JB_PATH_REPLACE
        },
        "cannot set path in scalar",
        !create,
    )
}

pub fn fc_jsonb_insert(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let array = arg_flat_array(fcinfo, 1, mcx)?;
    let newjb = arg_jsonb(fcinfo, 2, mcx)?;
    let after = fcinfo.arg_bool(3);
    set_path_common(
        mcx,
        jb.as_bytes(),
        array,
        Some(root_item(newjb.as_bytes())),
        if after {
            crate::mutate::JB_PATH_INSERT_AFTER
        } else {
            crate::mutate::JB_PATH_INSERT_BEFORE
        },
        "cannot set path in scalar",
        false,
    )
}

pub fn fc_jsonb_delete_path(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let array = arg_flat_array(fcinfo, 1, mcx)?;
    set_path_common(
        mcx,
        jb.as_bytes(),
        array,
        None,
        crate::mutate::JB_PATH_DELETE,
        "cannot delete path in scalar",
        true,
    )
}

const NULL_VALUE_TREATMENT_MSG: &str = "null_value_treatment must be \"delete_key\", \"return_target\", \"use_json_null\", or \"raise_exception\"";

pub fn fc_jsonb_set_lax(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    if fcinfo.argisnull(0) || fcinfo.argisnull(1) || fcinfo.argisnull(3) {
        return Ok(fcinfo.return_null());
    }
    if fcinfo.argisnull(4) {
        return Err(invalid_param(NULL_VALUE_TREATMENT_MSG));
    }
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let array = arg_flat_array(fcinfo, 1, mcx)?;
    let create = fcinfo.arg_bool(3);
    let set_op = if create {
        crate::mutate::JB_PATH_CREATE
    } else {
        crate::mutate::JB_PATH_REPLACE
    };
    if !fcinfo.argisnull(2) {
        let newjb = arg_jsonb(fcinfo, 2, mcx)?;
        return set_path_common(
            mcx,
            jb.as_bytes(),
            array,
            Some(root_item(newjb.as_bytes())),
            set_op,
            "cannot set path in scalar",
            !create,
        );
    }
    // SAFETY: arg 4 checked non-null above; it is a text varlena.
    let handling = unsafe { fcinfo.arg_varlena_packed(4)? };
    match handling.data() {
        b"raise_exception" => Err(Box::new(
            types_error::PgError::error("JSON value must not be null")
                .with_sqlstate(types_error::ERRCODE_NULL_VALUE_NOT_ALLOWED)
                .with_detail(
                    "Exception was raised because null_value_treatment is \"raise_exception\".",
                )
                .with_hint(
                    "To avoid, either change the null_value_treatment argument or ensure that \
                     an SQL NULL is not passed.",
                ),
        )),
        b"use_json_null" => set_path_common(
            mcx,
            jb.as_bytes(),
            array,
            Some(JsonbItem::Null),
            set_op,
            "cannot set path in scalar",
            !create,
        ),
        b"delete_key" => set_path_common(
            mcx,
            jb.as_bytes(),
            array,
            None,
            crate::mutate::JB_PATH_DELETE,
            "cannot delete path in scalar",
            true,
        ),
        b"return_target" => input_image(mcx, jb.as_bytes()),
        _ => Err(invalid_param(NULL_VALUE_TREATMENT_MSG)),
    }
}

pub fn fc_jsonb_pretty(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let payload = jb.as_bytes();
    let mut out = stringinfo::StringInfo::new_in(mcx)?;
    crate::io::jsonb_to_cstring_indent_into(mcx, &mut out, payload, payload.len() + 4)?;
    Ok(varlena_result(varlena::cstring_to_text(
        mcx,
        out.as_bytes(),
    )?))
}

// C: cannotCastJsonbValue.
#[cold]
#[inline(never)]
fn cast_error(kind: &'static str, sqltype: &'static str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error(alloc::format!("cannot cast jsonb {kind} to type {sqltype}"))
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

fn item_kind(item: &JsonbItem<'_>) -> &'static str {
    match item {
        JsonbItem::Null => "null",
        JsonbItem::String(_) => "string",
        JsonbItem::Numeric(_) => "numeric",
        JsonbItem::Bool(_) => "boolean",
        _ => "array or object",
    }
}

// C: JsonbExtractScalar + the cast family's shared null screening.
// Ok(None) = jsonb null → SQL NULL result.
fn cast_scalar<'a>(payload: &'a [u8], sqltype: &'static str) -> PgResult<Option<JsonbItem<'a>>> {
    let Some(v) = crate::io::extract_scalar(payload) else {
        let kind = if crate::container::container_is_array(payload) {
            "array"
        } else {
            "object"
        };
        return Err(cast_error(kind, sqltype));
    };
    if matches!(v, JsonbItem::Null) {
        return Ok(None);
    }
    Ok(Some(v))
}

pub fn fc_jsonb_bool(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        let jb = arg_jsonb(fcinfo, 0, mcx)?;
        match cast_scalar(jb.as_bytes(), "boolean")? {
            None => None,
            Some(JsonbItem::Bool(b)) => Some(Datum::from_bool(b)),
            Some(v) => return Err(cast_error(item_kind(&v), "boolean")),
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

fn cast_numeric_image<'a>(payload: &'a [u8], sqltype: &'static str) -> PgResult<Option<&'a [u8]>> {
    match cast_scalar(payload, sqltype)? {
        None => Ok(None),
        Some(JsonbItem::Numeric(img)) => Ok(Some(img)),
        Some(v) => Err(cast_error(item_kind(&v), sqltype)),
    }
}

pub fn fc_jsonb_numeric(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        let jb = arg_jsonb(fcinfo, 0, mcx)?;
        match cast_numeric_image(jb.as_bytes(), "numeric")? {
            // C: DatumGetNumericCopy — the image points into the jsonb body.
            Some(img) => Some(types_fmgr::byref_result(mcx, img)?),
            None => None,
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

macro_rules! jsonb_numeric_cast {
    ($fname:ident, $sqltype:literal, $conv:path, $mk:path) => {
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let d = {
                let mcx = fcinfo.result_mcx();
                let jb = arg_jsonb(fcinfo, 0, mcx)?;
                match cast_numeric_image(jb.as_bytes(), $sqltype)? {
                    Some(img) => Some($mk($conv(adt_numeric::Num::from_payload(&img[4..]))?)),
                    None => None,
                }
            };
            match d {
                Some(d) => Ok(d),
                None => Ok(fcinfo.return_null()),
            }
        }
    };
}

jsonb_numeric_cast!(fc_jsonb_int2, "smallint", adt_numeric::numeric_int2, Datum::from_i16);
jsonb_numeric_cast!(fc_jsonb_int4, "integer", adt_numeric::numeric_int4, Datum::from_i32);
jsonb_numeric_cast!(fc_jsonb_int8, "bigint", adt_numeric::numeric_int8, Datum::from_i64);
jsonb_numeric_cast!(fc_jsonb_float4, "real", adt_numeric::numeric_float4, Datum::from_f32);
jsonb_numeric_cast!(
    fc_jsonb_float8,
    "double precision",
    adt_numeric::numeric_float8,
    Datum::from_f64
);

pub fn fc_to_jsonb(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let val_type = funcapi::get_fn_expr_argtype(flinfo.as_deref(), 0);
    let mcx = fcinfo.result_mcx();
    let d = image_result(crate::tojsonb::to_jsonb(mcx, fcinfo.arg(0), val_type)?);
    Ok(d)
}

pub fn fc_jsonb_build_object(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        match funcapi::extract_variadic_args(mcx, flinfo.as_deref(), fcinfo, 0, true)? {
            None => None,
            Some(va) => Some(image_result(crate::tojsonb::jsonb_build_object_worker(
                mcx, &va.args, &va.nulls, &va.types, false, false,
            )?)),
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_build_object_noargs(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let d = image_result(crate::tojsonb::jsonb_build_object_worker(
        mcx,
        &[],
        &[],
        &[],
        false,
        false,
    )?);
    Ok(d)
}

pub fn fc_jsonb_build_array(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        match funcapi::extract_variadic_args(mcx, flinfo.as_deref(), fcinfo, 0, true)? {
            None => None,
            Some(va) => Some(image_result(crate::tojsonb::jsonb_build_array_worker(
                mcx, &va.args, &va.nulls, &va.types, false,
            )?)),
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_build_array_noargs(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let d = image_result(crate::tojsonb::jsonb_build_array_worker(
        mcx,
        &[],
        &[],
        &[],
        false,
    )?);
    Ok(d)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: true,
        func,
    }
}

const fn b_lax(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: false,
        retset: false,
        func,
    }
}

const fn srf_lax(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: false,
        retset: true,
        func,
    }
}

pub fn fc_jsonb_array_length(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    // Core factored to container::array_length for proofs/jsonb-probe.
    Ok(Datum::from_i32(
        crate::container::array_length(jb.as_bytes()).map_err(invalid_param_msg)?,
    ))
}

#[cold]
fn invalid_param_msg(msg: &str) -> Box<types_error::PgError> {
    Box::new(
        types_error::PgError::error(msg)
            .with_sqlstate(types_error::ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

pub fn fc_jsonb_object(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_flat_array(fcinfo, 0, mcx)?;
    Ok(image_result(crate::tojsonb::jsonb_object(mcx, array)?))
}

pub fn fc_jsonb_object_two_arg(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let keys = arg_flat_array(fcinfo, 0, mcx)?;
    let vals = arg_flat_array(fcinfo, 1, mcx)?;
    Ok(image_result(crate::tojsonb::jsonb_object_two_arg(mcx, keys, vals)?))
}

pub fn fc_jsonb_strip_nulls(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jb = arg_jsonb(fcinfo, 0, mcx)?;
    let strip_in_arrays = fcinfo.arg(1).as_bool();
    let d = image_result(crate::mutate::strip_nulls(mcx, jb.as_bytes(), strip_in_arrays)?);
    Ok(d)
}

// pg_proc.dat: all listed entries proisstrict except jsonb_set_lax; none retset.
pub const JSONB_BUILTINS: &[FmgrBuiltin] = &[
    b(3480, "gin_compare_jsonb", 2, fc_gin_compare_jsonb),
    b(3207, "jsonb_array_length", 1, fc_jsonb_array_length),
    b(3262, "jsonb_strip_nulls", 2, fc_jsonb_strip_nulls),
    b(3263, "jsonb_object", 1, fc_jsonb_object),
    b(3264, "jsonb_object_two_arg", 2, fc_jsonb_object_two_arg),
    b_lax(3209, "jsonb_populate_record", 2, crate::populate::fc_jsonb_populate_record),
    b_lax(6338, "jsonb_populate_record_valid", 2, crate::populate::fc_jsonb_populate_record_valid),
    b(3490, "jsonb_to_record", 1, crate::populate::fc_jsonb_to_record),
    srf_lax(3475, "jsonb_populate_recordset", 2, crate::populate::fc_jsonb_populate_recordset),
    srf_lax(3491, "jsonb_to_recordset", 1, crate::populate::fc_jsonb_to_recordset),
    b_lax(3960, "json_populate_record", 3, crate::populate::fc_json_populate_record),
    b(3204, "json_to_record", 1, crate::populate::fc_json_to_record),
    srf_lax(3961, "json_populate_recordset", 3, crate::populate::fc_json_populate_recordset),
    srf_lax(3205, "json_to_recordset", 1, crate::populate::fc_json_to_recordset),
    b(2580, "jsonb_float8", 1, fc_jsonb_float8),
    b(3301, "jsonb_concat", 2, fc_jsonb_concat),
    b(3302, "jsonb_delete", 2, fc_jsonb_delete),
    b(3303, "jsonb_delete", 2, fc_jsonb_delete_idx),
    b(3304, "jsonb_delete_path", 2, fc_jsonb_delete_path),
    b(3305, "jsonb_set", 4, fc_jsonb_set),
    b(3306, "jsonb_pretty", 1, fc_jsonb_pretty),
    b_lax(3271, "jsonb_build_array", 1, fc_jsonb_build_array),
    b_lax(3272, "jsonb_build_array_noargs", 0, fc_jsonb_build_array_noargs),
    b_lax(3273, "jsonb_build_object", 1, fc_jsonb_build_object),
    b_lax(3274, "jsonb_build_object_noargs", 0, fc_jsonb_build_object_noargs),
    b(3343, "jsonb_delete", 2, fc_jsonb_delete_array),
    b(3449, "jsonb_numeric", 1, fc_jsonb_numeric),
    b(3450, "jsonb_int2", 1, fc_jsonb_int2),
    b(3451, "jsonb_int4", 1, fc_jsonb_int4),
    b(3452, "jsonb_int8", 1, fc_jsonb_int8),
    b(3453, "jsonb_float4", 1, fc_jsonb_float4),
    b(3556, "jsonb_bool", 1, fc_jsonb_bool),
    b(3579, "jsonb_insert", 4, fc_jsonb_insert),
    b(3787, "to_jsonb", 1, fc_to_jsonb),
    b_lax(3265, "jsonb_agg_transfn", 2, crate::aggs::fc_jsonb_agg_transfn),
    b_lax(3266, "jsonb_agg_finalfn", 1, crate::aggs::fc_jsonb_agg_finalfn),
    b_lax(6283, "jsonb_agg_strict_transfn", 2, crate::aggs::fc_jsonb_agg_strict_transfn),
    b_lax(3268, "jsonb_object_agg_transfn", 3, crate::aggs::fc_jsonb_object_agg_transfn),
    b_lax(6285, "jsonb_object_agg_strict_transfn", 3, crate::aggs::fc_jsonb_object_agg_strict_transfn),
    b_lax(6286, "jsonb_object_agg_unique_transfn", 3, crate::aggs::fc_jsonb_object_agg_unique_transfn),
    b_lax(6287, "jsonb_object_agg_unique_strict_transfn", 3, crate::aggs::fc_jsonb_object_agg_unique_strict_transfn),
    b_lax(3269, "jsonb_object_agg_finalfn", 1, crate::aggs::fc_jsonb_object_agg_finalfn),
    b_lax(5054, "jsonb_set_lax", 5, fc_jsonb_set_lax),
    b(3210, "jsonb_typeof", 1, fc_jsonb_typeof),
    srf(3208, "jsonb_each", 1, crate::srfs::fc_jsonb_each),
    srf(3219, "jsonb_array_elements", 1, crate::srfs::fc_jsonb_array_elements),
    srf(3465, "jsonb_array_elements_text", 1, crate::srfs::fc_jsonb_array_elements_text),
    srf(3931, "jsonb_object_keys", 1, crate::srfs::fc_jsonb_object_keys),
    srf(3932, "jsonb_each_text", 1, crate::srfs::fc_jsonb_each_text),
    b(3214, "jsonb_object_field_text", 2, fc_jsonb_object_field_text),
    b(3215, "jsonb_array_element", 2, fc_jsonb_array_element),
    b(3216, "jsonb_array_element_text", 2, fc_jsonb_array_element_text),
    b(3217, "jsonb_extract_path", 2, fc_jsonb_extract_path),
    b(3416, "jsonb_hash_extended", 2, fc_jsonb_hash_extended),
    b(3478, "jsonb_object_field", 2, fc_jsonb_object_field),
    b(3803, "jsonb_send", 1, fc_jsonb_send),
    b(3804, "jsonb_out", 1, fc_jsonb_out),
    b(3805, "jsonb_recv", 1, fc_jsonb_recv),
    b(3806, "jsonb_in", 1, fc_jsonb_in),
    b(3940, "jsonb_extract_path_text", 2, fc_jsonb_extract_path_text),
    b(4038, "jsonb_ne", 2, fc_jsonb_ne),
    b(4039, "jsonb_lt", 2, fc_jsonb_lt),
    b(4040, "jsonb_gt", 2, fc_jsonb_gt),
    b(4041, "jsonb_le", 2, fc_jsonb_le),
    b(4042, "jsonb_ge", 2, fc_jsonb_ge),
    b(4043, "jsonb_eq", 2, fc_jsonb_eq),
    b(4044, "jsonb_cmp", 2, fc_jsonb_cmp),
    b(4045, "jsonb_hash", 1, fc_jsonb_hash),
    b(4046, "jsonb_contains", 2, fc_jsonb_contains),
    b(4047, "jsonb_exists", 2, fc_jsonb_exists),
    b(4048, "jsonb_exists_any", 2, fc_jsonb_exists_any),
    b(4049, "jsonb_exists_all", 2, fc_jsonb_exists_all),
    b(4050, "jsonb_contained", 2, fc_jsonb_contained),
];
