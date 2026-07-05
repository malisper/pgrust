//! fmgr wrappers (`fc_*`) + the `JSON_BUILTINS` table for fmgr-core.
//! json_populate_record/recordset/to_record[set] and the _unique agg
//! variants stay loud through fmgr-core's unported-OID panic.

use crate::getpath::{get_worker, path_index};
use datum::Datum;
use mcx::{Mcx, PgVec};
use stringinfo::StringInfo;
use types_core::catalog::TEXTOID;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction, PackedVarlena,
};

pub fn fc_json_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of json_in is a non-null cstring (strict fn).
    let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
    let mcx = fcinfo.result_mcx();
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let had_esc = esc.is_some();
    match crate::json_in(mcx, s, esc)? {
        Some(v) => Ok(varlena_result(v)),
        None if had_esc => Ok(Datum::null()),
        None => panic!("json_in: soft-error escape without an escontext"),
    }
}

pub fn fc_json_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(cstring_result(crate::json_out(mcx, payload)?))
}

pub fn fc_json_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of json_recv is a live &mut StringInfo (internal ABI).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::json_recv(mcx, buf)?))
}

pub fn fc_json_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::json_send(mcx, payload)?))
}

pub fn fc_to_json(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let val_type = funcapi::get_fn_expr_argtype(flinfo.as_deref(), 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::tojson::to_json(
        mcx,
        fcinfo.arg(0),
        val_type,
    )?))
}

fn array_to_json_common(fcinfo: &mut Fcinfo, use_line_feeds: bool) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut result = StringInfo::new_in(mcx)?;
    crate::tojson::array_to_json_internal(mcx, &mut result, fcinfo.arg(0), use_line_feeds)?;
    Ok(varlena_result(varlena::cstring_to_text(
        mcx,
        result.as_bytes(),
    )?))
}

pub fn fc_array_to_json(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    array_to_json_common(fcinfo, false)
}

pub fn fc_array_to_json_pretty(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let use_line_feeds = fcinfo.arg_bool(1);
    array_to_json_common(fcinfo, use_line_feeds)
}

fn row_to_json_common(fcinfo: &mut Fcinfo, use_line_feeds: bool) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut result = StringInfo::new_in(mcx)?;
    crate::tojson::composite_to_json(mcx, &mut result, fcinfo.arg(0), use_line_feeds)?;
    Ok(varlena_result(varlena::cstring_to_text(
        mcx,
        result.as_bytes(),
    )?))
}

pub fn fc_row_to_json(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    row_to_json_common(fcinfo, false)
}

pub fn fc_row_to_json_pretty(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let use_line_feeds = fcinfo.arg_bool(1);
    row_to_json_common(fcinfo, use_line_feeds)
}

pub fn fc_json_build_object(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        match funcapi::extract_variadic_args(mcx, flinfo.as_deref(), fcinfo, 0, true)? {
            None => None,
            Some(va) => Some(varlena_result(crate::tojson::json_build_object_worker(
                mcx, &va.args, &va.nulls, &va.types, false, false,
            )?)),
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_json_build_object_noargs(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(mcx, b"{}")?))
}

pub fn fc_json_build_array(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        match funcapi::extract_variadic_args(mcx, flinfo.as_deref(), fcinfo, 0, true)? {
            None => None,
            Some(va) => Some(varlena_result(crate::tojson::json_build_array_worker(
                mcx, &va.args, &va.nulls, &va.types, false,
            )?)),
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_json_build_array_noargs(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(mcx, b"[]")?))
}

// The flat array image of arg `i`, detoasted into `mcx`.
fn arg_flat_array<'mcx>(fcinfo: &Fcinfo, i: usize, mcx: Mcx<'mcx>) -> PgResult<&'mcx [u8]> {
    // SAFETY: catalog arg i is a non-null array varlena (strict fn).
    let p = unsafe { fcinfo.arg_ptr(i) };
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let raw = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    Ok(detoast_seams::detoast_attr::call(mcx, raw)?.leak())
}

pub fn fc_json_object(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let array = arg_flat_array(fcinfo, 0, mcx)?;
    Ok(varlena_result(crate::tojson::json_object(mcx, array)?))
}

pub fn fc_json_object_two_arg(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let keys = arg_flat_array(fcinfo, 0, mcx)?;
    let vals = arg_flat_array(fcinfo, 1, mcx)?;
    Ok(varlena_result(crate::tojson::json_object_two_arg(
        mcx, keys, vals,
    )?))
}

fn object_field_common(fcinfo: &mut Fcinfo, normalize: bool) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog args are non-null varlenas (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        let key = unsafe { fcinfo.arg_varlena_packed(1)? };
        let names = [key.data()];
        get_worker(mcx, json.data(), Some(&names), None, 1, normalize)?.map(varlena_result)
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_json_object_field(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    object_field_common(fcinfo, false)
}

pub fn fc_json_object_field_text(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    object_field_common(fcinfo, true)
}

fn array_element_common(fcinfo: &mut Fcinfo, normalize: bool) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        let mut indexes = [fcinfo.arg_i32(1)];
        get_worker(mcx, json.data(), None, Some(&mut indexes), 1, normalize)?.map(varlena_result)
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_json_array_element(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    array_element_common(fcinfo, false)
}

pub fn fc_json_array_element_text(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    array_element_common(fcinfo, true)
}

/// C: get_path_all.
fn get_path_all(fcinfo: &mut Fcinfo, as_text: bool) -> PgResult<Datum> {
    let d = {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        let path = arg_flat_array(fcinfo, 1, mcx)?;
        if arrayfuncs::array_contains_nulls(path) {
            None
        } else {
            let (elems, _nulls) = arrayfuncs::deconstruct_array_builtin(mcx, path, TEXTOID, true)?;
            let npath = elems.len();
            let mut names: PgVec<'_, &[u8]> = mcx::vec_with_capacity_in(mcx, npath)?;
            let mut indexes: PgVec<'_, i32> = mcx::vec_with_capacity_in(mcx, npath)?;
            for d in elems.iter() {
                // SAFETY: non-null text element datums point into the flat image.
                let pv = unsafe { PackedVarlena::from_ptr(d.as_usize() as *const u8) };
                let payload = pv.data();
                names.push(payload);
                indexes.push(path_index(payload));
            }
            get_worker(
                mcx,
                json.data(),
                Some(&names),
                Some(&mut indexes),
                npath,
                as_text,
            )?
            .map(varlena_result)
        }
    };
    match d {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_json_extract_path(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    get_path_all(fcinfo, false)
}

pub fn fc_json_extract_path_text(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    get_path_all(fcinfo, true)
}

pub fn fc_json_array_length(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
    let json = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(Datum::from_i32(crate::funcs::json_array_length(
        mcx,
        json.data(),
    )?))
}

pub fn fc_json_strip_nulls(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
    let json = unsafe { fcinfo.arg_varlena_packed(0)? };
    let strip_in_arrays = if fcinfo.nargs() == 2 {
        fcinfo.arg_bool(1)
    } else {
        false
    };
    Ok(varlena_result(crate::funcs::json_strip_nulls(
        mcx,
        json.data(),
        strip_in_arrays,
    )?))
}

pub fn fc_json_typeof(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
    let json = unsafe { fcinfo.arg_varlena_packed(0)? };
    let t = crate::funcs::json_typeof(json.data())?;
    Ok(varlena_result(varlena::cstring_to_text(mcx, t.as_bytes())?))
}

pub fn fc_json_object_keys(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    crate::srfs::srf_drive(flinfo, fcinfo, "json_object_keys", |fcinfo| {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        crate::srfs::object_keys_rows(mcx, json.data())
    })
}

pub fn fc_json_each(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    crate::srfs::srf_drive(flinfo, fcinfo, "json_each", |fcinfo| {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        crate::srfs::each_rows(mcx, json.data(), false)
    })
}

pub fn fc_json_each_text(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    crate::srfs::srf_drive(flinfo, fcinfo, "json_each_text", |fcinfo| {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        crate::srfs::each_rows(mcx, json.data(), true)
    })
}

pub fn fc_json_array_elements(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    crate::srfs::srf_drive(flinfo, fcinfo, "json_array_elements", |fcinfo| {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        crate::srfs::elements_rows(mcx, json.data(), "json_array_elements", false)
    })
}

pub fn fc_json_array_elements_text(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    crate::srfs::srf_drive(flinfo, fcinfo, "json_array_elements_text", |fcinfo| {
        let mcx = fcinfo.result_mcx();
        // SAFETY: catalog arg 0 is a non-null json varlena (strict fn).
        let json = unsafe { fcinfo.arg_varlena_packed(0)? };
        crate::srfs::elements_rows(mcx, json.data(), "json_array_elements_text", true)
    })
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

// pg_proc.dat: strictness per proisstrict; retset SRFs per proretset.
pub const JSON_BUILTINS: &[FmgrBuiltin] = &[
    b(321, "json_in", 1, fc_json_in),
    b(322, "json_out", 1, fc_json_out),
    b(323, "json_recv", 1, fc_json_recv),
    b(324, "json_send", 1, fc_json_send),
    b(3153, "array_to_json", 1, fc_array_to_json),
    b(3154, "array_to_json_pretty", 2, fc_array_to_json_pretty),
    b(3155, "row_to_json", 1, fc_row_to_json),
    b(3156, "row_to_json_pretty", 2, fc_row_to_json_pretty),
    b(3176, "to_json", 1, fc_to_json),
    b_lax(3173, "json_agg_transfn", 2, crate::aggs::fc_json_agg_transfn),
    b_lax(3174, "json_agg_finalfn", 1, crate::aggs::fc_json_agg_finalfn),
    b_lax(6275, "json_agg_strict_transfn", 2, crate::aggs::fc_json_agg_strict_transfn),
    b_lax(3180, "json_object_agg_transfn", 3, crate::aggs::fc_json_object_agg_transfn),
    b_lax(6277, "json_object_agg_strict_transfn", 3, crate::aggs::fc_json_object_agg_strict_transfn),
    b_lax(6278, "json_object_agg_unique_transfn", 3, crate::aggs::fc_json_object_agg_unique_transfn),
    b_lax(6279, "json_object_agg_unique_strict_transfn", 3, crate::aggs::fc_json_object_agg_unique_strict_transfn),
    b_lax(3196, "json_object_agg_finalfn", 1, crate::aggs::fc_json_object_agg_finalfn),
    b_lax(3198, "json_build_array", 1, fc_json_build_array),
    b_lax(3199, "json_build_array_noargs", 0, fc_json_build_array_noargs),
    b_lax(3200, "json_build_object", 1, fc_json_build_object),
    b_lax(3201, "json_build_object_noargs", 0, fc_json_build_object_noargs),
    b(3202, "json_object", 1, fc_json_object),
    b(3203, "json_object_two_arg", 2, fc_json_object_two_arg),
    b(3261, "json_strip_nulls", 2, fc_json_strip_nulls),
    b(3947, "json_object_field", 2, fc_json_object_field),
    b(3948, "json_object_field_text", 2, fc_json_object_field_text),
    b(3949, "json_array_element", 2, fc_json_array_element),
    b(3950, "json_array_element_text", 2, fc_json_array_element_text),
    b(3951, "json_extract_path", 2, fc_json_extract_path),
    b(3953, "json_extract_path_text", 2, fc_json_extract_path_text),
    srf(3955, "json_array_elements", 1, fc_json_array_elements),
    b(3956, "json_array_length", 1, fc_json_array_length),
    srf(3957, "json_object_keys", 1, fc_json_object_keys),
    srf(3958, "json_each", 1, fc_json_each),
    srf(3959, "json_each_text", 1, fc_json_each_text),
    b(3968, "json_typeof", 1, fc_json_typeof),
    srf(3969, "json_array_elements_text", 1, fc_json_array_elements_text),
];
