//! misc.c slice: pg_input_is_valid / pg_input_error_info (soft-error probes).

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

fn text_datum<'mcx>(mcx: Mcx<'mcx>, payload: &[u8]) -> PgResult<Datum> {
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
fn not_row_type() -> Box<PgError> {
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

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const MISC_BUILTINS: &[FmgrBuiltin] = &[
    b(6210, "pg_input_is_valid", 2, fc_pg_input_is_valid),
    b(6211, "pg_input_error_info", 2, fc_pg_input_error_info),
];
