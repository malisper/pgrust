//! fmgr wrappers (`fc_*`) + the `JSONB_BUILTINS` table for fmgr-core. The rest
//! of the jsonb surface (mutation family, jsonpath, subscripting, aggregates,
//! GIN, scalar casts, to_jsonb/build/object) stays loud via unported OIDs.

extern crate alloc;

use crate::container::container_size;
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
fn image_result(v: PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    d
}

// C: PG_GETARG_JSONB_P — detoast; the payload is the root JsonbContainer.
// Short varlenas are expanded to an aligned copy like pg_detoast_datum: the
// container must start 4-aligned so embedded numeric digit arrays stay
// 2-aligned (mcx allocations are 8-aligned; VARHDRSZ keeps payloads at +4).
fn arg_jsonb<'a, 'mcx>(
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

// pg_proc.dat: all listed entries proisstrict, none retset.
pub const JSONB_BUILTINS: &[FmgrBuiltin] = &[
    b(3210, "jsonb_typeof", 1, fc_jsonb_typeof),
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
