//! fmgr wrappers (`fc_*`) + the `JSONPATH_BUILTINS` table. The jsonpath
//! executor OIDs (@? / @@ / jsonb_path_*) live in adt_jsonpath_exec.

use ::datum::Datum;
use ::mcx::{Mcx, PgVec};
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};
use ::varlena::VarPayload;

fn image_result(v: PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    d
}

// C: PG_GETARG_JSONPATH_P — detoast to a 4B-header image (the node region
// holds int32 links and numeric varlenas, so it must start 4-aligned; short
// varlenas are expanded like pg_detoast_datum).
fn arg_jsonpath<'a, 'mcx>(
    fcinfo: &'a Fcinfo,
    i: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<VarPayload<'a, 'mcx>> {
    // SAFETY: catalog arg i is a non-null jsonpath varlena (strict fns only).
    let p = unsafe { fcinfo.arg_ptr(i) };
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let image =
        unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    if image[0] & 0x01 == 0x01 && image[0] != 0x01 {
        let payload = &image[1..];
        let mut v: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(mcx, 4 + payload.len())?;
        ::mcx::vec_append_bytes(&mut v, &(((4 + payload.len()) as u32) << 2).to_ne_bytes())?;
        ::mcx::vec_append_bytes(&mut v, payload)?;
        return Ok(VarPayload::Detoasted(v));
    }
    varlena::open_image(mcx, image)
}

// VarPayload yields the payload past the 4B header; the readers want the
// full image back.
fn full_image<'a>(jp: &'a VarPayload<'_, '_>) -> &'a [u8] {
    let payload = jp.as_bytes();
    // SAFETY: the payload slice sits 4 bytes into the varlena image built or
    // opened by arg_jsonpath; the header bytes are part of the same
    // allocation.
    unsafe { core::slice::from_raw_parts(payload.as_ptr().sub(4), payload.len() + 4) }
}

pub fn fc_jsonpath_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (d, had_esc) = {
        // SAFETY: catalog arg 0 of jsonpath_in is a non-null cstring (strict).
        let s = unsafe { fcinfo.arg_cstring(0) }.to_bytes();
        let mcx = fcinfo.result_mcx();
        // SAFETY: context, if set, rides per the ErrorSaveNode contract.
        let esc = unsafe { fcinfo.soft_error_context() };
        let had_esc = esc.is_some();
        (crate::path::jsonpath_in(mcx, s, esc)?.map(image_result), had_esc)
    };
    match d {
        Some(d) => Ok(d),
        None if had_esc => Ok(fcinfo.return_null()),
        None => panic!("jsonpath_in: soft-error escape without an escontext"),
    }
}

pub fn fc_jsonpath_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jp = arg_jsonpath(fcinfo, 0, mcx)?;
    Ok(cstring_result(crate::path::jsonpath_out(mcx, full_image(&jp))?))
}

pub fn fc_jsonpath_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of jsonpath_recv is a live &mut StringInfo.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let mcx = fcinfo.result_mcx();
    Ok(image_result(crate::path::jsonpath_recv(mcx, buf)?))
}

pub fn fc_jsonpath_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let jp = arg_jsonpath(fcinfo, 0, mcx)?;
    Ok(varlena_result(crate::path::jsonpath_send(mcx, full_image(&jp))?))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const JSONPATH_BUILTINS: &[FmgrBuiltin] = &[
    b(4001, "jsonpath_in", 1, fc_jsonpath_in),
    b(4002, "jsonpath_recv", 1, fc_jsonpath_recv),
    b(4003, "jsonpath_out", 1, fc_jsonpath_out),
    b(4004, "jsonpath_send", 1, fc_jsonpath_send),
];
