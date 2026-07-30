//! misc.c slice: pg_input_is_valid / pg_input_error_info (soft-error probes).

pub mod builtins;
pub(crate) mod catalog_fk;
pub mod introspect;

#[cfg(test)]
mod tests;

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;
use core::ffi::CStr;

use ::datum::Datum;
use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::{unpack_sqlstate, PgError, PgResult};
use ::types_fmgr::{
    input_function_call_safe, ErrorSaveNode, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

// ValidIOData; the typname key replaces C's get_fn_expr_arg_stable constness
// probe (fn_expr unset on our paths) — byte-equal typname reuses the cache.
struct ValidIOData {
    typmod: i32,
    typioparam: Oid,
    inputproc: FmgrInfo,
    typname: String,
}

#[cold]
#[inline(never)]
fn null_flinfo(what: &str) -> ! {
    panic!("{what}: NULL flinfo")
}

fn input_is_valid_common(
    flinfo: &mut FmgrInfo,
    fcinfo: &Fcinfo,
    escontext: &mut ErrorSaveNode,
) -> PgResult<bool> {
    let mcx = fcinfo.result_mcx();

    // SAFETY: catalog args of pg_input_is_valid/pg_input_error_info are text
    // (strict functions; both non-null here).
    let typname_bytes = unsafe { fcinfo.arg_varlena_packed(1)? };
    let typname_bytes = typname_bytes.data();

    let need = match flinfo.fn_extra_ref::<ValidIOData>() {
        Some(v) => v.typname.as_bytes() != typname_bytes,
        None => true,
    };
    if need {
        let typname = String::from_utf8_lossy(typname_bytes);
        let (typoid, typmod) = parse_utilcmd::parseTypeString(mcx, &typname)?;
        let (typiofunc, typioparam) = lsyscache::getTypeInputInfo(typoid)?;
        let inputproc = fmgr_seams::fmgr_info::call(typiofunc)?;
        flinfo.set_fn_extra(ValidIOData {
            typmod,
            typioparam,
            inputproc,
            typname: typname.into_owned(),
        });
    }

    // SAFETY: arg 0 is a non-null text datum (strict function).
    let txt = unsafe { fcinfo.arg_varlena_packed(0)? };
    let txt = txt.data();
    let mut buf: PgVec<u8> = vec_with_capacity_in(mcx, txt.len() + 1)?;
    // SAFETY: single reserve above; copy fits the spare capacity exactly.
    unsafe {
        core::ptr::copy_nonoverlapping(txt.as_ptr(), buf.as_mut_ptr(), txt.len());
        buf.set_len(txt.len());
    }
    buf.push(0);
    // C text_to_cstring: an embedded NUL truncates the value.
    let cstr = CStr::from_bytes_until_nul(&buf).expect("NUL-terminated above");

    let v = flinfo.fn_extra_mut::<ValidIOData>().expect("populated above");
    let mut converted = Datum::null();
    input_function_call_safe(
        &mut v.inputproc,
        Some(cstr),
        v.typioparam,
        v.typmod,
        mcx,
        Some(escontext),
        &mut converted,
    )
}

pub fn fc_pg_input_is_valid(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let Some(flinfo) = flinfo else { null_flinfo("pg_input_is_valid") };
    let mut escontext = ErrorSaveNode::new(false);
    let ok = input_is_valid_common(flinfo, fcinfo, &mut escontext)?;
    Ok(Datum::from_bool(ok))
}

pub(crate) fn text_datum<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum> {
    let len = datum::varlena::VARHDRSZ + payload.len();
    let mut buf: PgVec<'mcx, u8> = vec_with_capacity_in(mcx, len)?;
    // SAFETY: single reserve above; header + payload fit exactly.
    unsafe {
        let hdr = datum::varlena::set_varsize_4b(len);
        core::ptr::copy_nonoverlapping(hdr.as_ptr(), buf.as_mut_ptr(), hdr.len());
        core::ptr::copy_nonoverlapping(
            payload.as_ptr(),
            buf.as_mut_ptr().add(hdr.len()),
            payload.len(),
        );
        buf.set_len(len);
    }
    let d = Datum::from_usize(buf.as_ptr() as usize);
    core::mem::forget(buf);
    Ok(d)
}

#[cold]
#[inline(never)]
pub(crate) fn not_row_type() -> Box<PgError> {
    Box::new(PgError::error("return type must be a row type"))
}

pub fn fc_pg_input_error_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let Some(flinfo) = flinfo else { null_flinfo("pg_input_error_info") };
    let mut escontext = ErrorSaveNode::new(true);
    let ok = input_is_valid_common(flinfo, fcinfo, &mut escontext)?;

    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(not_row_type());
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result has tupdesc");

    let mut values = [Datum::null(); 4];
    let mut isnull = [true; 4];
    if !ok {
        let err = escontext.ctx.take_error().expect("details_wanted saved the error");
        values[0] = text_datum(mcx, err.message.as_bytes())?;
        isnull[0] = false;
        if let Some(detail) = &err.detail {
            values[1] = text_datum(mcx, detail.as_bytes())?;
            isnull[1] = false;
        }
        if let Some(hint) = &err.hint {
            values[2] = text_datum(mcx, hint.as_bytes())?;
            isnull[2] = false;
        }
        values[3] = text_datum(mcx, &unpack_sqlstate(err.sqlstate))?;
        isnull[3] = false;
    }

    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &isnull)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}


// C `count_nulls` (misc.c); false = SQL NULL result (VARIADIC NULL array).
fn count_nulls(
    flinfo: Option<&FmgrInfo>,
    fcinfo: &Fcinfo,
) -> PgResult<Option<(i32, i32)>> {
    if funcapi::get_fn_expr_variadic(flinfo) {
        if fcinfo.argisnull(0) {
            return Ok(None);
        }
        let mcx = fcinfo.result_mcx();
        // SAFETY: arg 0 is a non-null array (varlena) datum.
        let p = unsafe { fcinfo.arg_ptr(0) };
        let total = arrayfuncs::foundation::varsize_any(p);
        // SAFETY: a live varlena of `total` bytes.
        let raw = unsafe { core::slice::from_raw_parts(p, total) };
        let arr = detoast_seams::detoast_attr::call(mcx, raw)?;
        let (ndim, dims, _lbs) = arrayfuncs::foundation::read_dims_lbounds(&arr);
        let nitems = arrayutils::array_get_n_items(ndim, &dims[..ndim.max(0) as usize])?;
        let mut count = 0;
        if let Some(off) = arrayfuncs::foundation::arr_nullbitmap_off(&arr) {
            let bitmap = &arr[off..];
            for i in 0..nitems as usize {
                if bitmap[i / 8] & (1 << (i % 8)) == 0 {
                    count += 1;
                }
            }
        }
        Ok(Some((nitems, count)))
    } else {
        let nargs = fcinfo.nargs();
        let mut count = 0;
        for i in 0..nargs {
            if fcinfo.argisnull(i) {
                count += 1;
            }
        }
        Ok(Some((nargs as i32, count)))
    }
}

pub fn fc_pg_num_nulls(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match count_nulls(flinfo.as_deref(), fcinfo)? {
        Some((_nargs, nulls)) => Ok(Datum::from_i32(nulls)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_num_nonnulls(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match count_nulls(flinfo.as_deref(), fcinfo)? {
        Some((nargs, nulls)) => Ok(Datum::from_i32(nargs - nulls)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_pg_typeof(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let _ = fcinfo;
    Ok(Datum::from_oid(funcapi::get_fn_expr_argtype(flinfo.as_deref(), 0)))
}

pub fn fc_current_query(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // C current_query (utils/adt/misc.c): debug_query_string or NULL. The
    // old read went through BackendLogContext, which no backend installs —
    // current_query() was NULL always (pinned by the dblink_local corpus:
    // dblink_current_query is an alias for this function).
    elog::with_debug_query_string(|q| match q {
        Some(q) => {
            let mcx = fcinfo.result_mcx();
            text_datum(mcx, q.as_bytes())
        }
        None => Ok(fcinfo.return_null()),
    })
}

pub fn fc_pg_basetype(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    const TYPTYPE_DOMAIN: i8 = b'd' as i8;
    let mut typid = fcinfo.arg(0).as_oid();
    loop {
        match syscache_seams::pg_type_base_shape::call(typid)? {
            None => return Ok(fcinfo.return_null()),
            Some(t) if t.typtype != TYPTYPE_DOMAIN => return Ok(Datum::from_oid(typid)),
            Some(t) => typid = t.typbasetype,
        }
    }
}

pub fn fc_pg_sleep(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    use types_storage::waiteventset::{WL_EXIT_ON_PM_DEATH, WL_LATCH_SET, WL_TIMEOUT};
    const WAIT_EVENT_PG_SLEEP: u32 = waitevent::PG_WAIT_TIMEOUT | 2;
    let secs = fcinfo.arg(0).as_f64();
    let now = || timestamp_seams::get_current_timestamp::call() as f64 / 1_000_000.0;
    // Stop time computed once: no delay accumulation across wakeups (C shape).
    let endtime = now() + secs;
    loop {
        postgres_seams::check_for_interrupts::call()?;
        let delay = endtime - now();
        let delay_ms: i64 = if delay >= 600.0 {
            600_000
        } else if delay > 0.0 {
            (delay * 1000.0).ceil() as i64
        } else {
            break;
        };
        latch_seams::wait_latch_my_latch::call(
            WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
            delay_ms,
            WAIT_EVENT_PG_SLEEP,
        );
        latch_seams::reset_latch_my_latch::call();
    }
    Ok(Datum::null())
}

pub fn fc_any_value_transfn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    fcinfo.isnull = fcinfo.args[0].isnull;
    Ok(fcinfo.arg(0))
}

#[track_caller]
#[cold]
#[inline(never)]
fn collations_not_supported(type_be: String) -> Box<PgError> {
    Box::new(
        PgError::error(format!("collations are not supported by type {type_be}"))
            .with_sqlstate(types_error::ERRCODE_DATATYPE_MISMATCH),
    )
}

pub fn fc_pg_collation_for(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typeid = funcapi::get_fn_expr_argtype(flinfo.as_deref(), 0);
    if typeid == types_core::InvalidOid {
        return Ok(fcinfo.return_null());
    }
    if !lsyscache::type_is_collatable(typeid)? && typeid != types_core::UNKNOWNOID {
        return Err(collations_not_supported(format_type::format_type_be(typeid)?));
    }
    let collid = fcinfo.get_collation();
    if collid == types_core::InvalidOid {
        return Ok(fcinfo.return_null());
    }
    let mcx = fcinfo.result_mcx();
    let name = ruleutils::generate_collation_name(mcx, collid)?;
    text_datum(mcx, name.as_bytes())
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

const fn bn(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: false, retset: false, func }
}

pub const MISC_BUILTINS: &[FmgrBuiltin] = &[
    bn(438, "pg_num_nulls", 1, fc_pg_num_nulls),
    bn(440, "pg_num_nonnulls", 1, fc_pg_num_nonnulls),
    // pg_proc.dat 1619 proisstrict=f: pg_typeof(NULL) still reports the type.
    bn(1619, "pg_typeof", 1, fc_pg_typeof),
    b(6210, "pg_input_is_valid", 2, fc_pg_input_is_valid),
    b(6211, "pg_input_error_info", 2, fc_pg_input_error_info),
    bn(817, "current_query", 0, fc_current_query),
    b(2626, "pg_sleep", 1, fc_pg_sleep),
    b(6292, "any_value_transfn", 2, fc_any_value_transfn),
    b(6315, "pg_basetype", 1, fc_pg_basetype),
    bn(3162, "pg_collation_for", 1, fc_pg_collation_for),
    b(2560, "pg_postmaster_start_time", 0, builtins::fc_pg_postmaster_start_time),
];
