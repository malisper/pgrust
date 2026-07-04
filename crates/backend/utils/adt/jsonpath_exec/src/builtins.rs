//! fmgr wrappers (`fc_*`) + the `JSONPATH_EXEC_BUILTINS` table.

use crate::JsonPathVars;
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use varlena::VarPayload;

fn image_result(v: PgVec<'_, u8>) -> Datum {
    let d = Datum::from_usize(v.as_ptr() as usize);
    core::mem::forget(v);
    d
}

// C: PG_GETARG_JSONB_P / PG_GETARG_JSONPATH_P — detoast; short varlenas are
// expanded to an aligned 4B-header copy (containers hold int32 words and
// numeric varlenas).
fn arg_varlena<'a, 'mcx>(
    fcinfo: &'a Fcinfo,
    i: usize,
    mcx: Mcx<'mcx>,
) -> PgResult<VarPayload<'a, 'mcx>> {
    // SAFETY: catalog arg i is a non-null varlena (strict fns only).
    let p = unsafe { fcinfo.arg_ptr(i) };
    // SAFETY: a live varlena readable through its full VARSIZE_ANY.
    let image =
        unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    if image[0] & 0x01 == 0x01 && image[0] != 0x01 {
        let payload = &image[1..];
        let mut v: PgVec<'mcx, u8> = mcx::vec_with_capacity_in(mcx, 4 + payload.len())?;
        mcx::vec_append_bytes(&mut v, &(((4 + payload.len()) as u32) << 2).to_ne_bytes())?;
        mcx::vec_append_bytes(&mut v, payload)?;
        return Ok(VarPayload::Detoasted(v));
    }
    varlena::open_image(mcx, image)
}

// The jsonpath readers want the full image back (header included).
fn full_image<'a>(jp: &'a VarPayload<'_, '_>) -> &'a [u8] {
    let payload = jp.as_bytes();
    // SAFETY: the payload sits 4 bytes into the varlena image opened or built
    // by arg_varlena; the header bytes are part of the same allocation.
    unsafe { core::slice::from_raw_parts(payload.as_ptr().sub(4), payload.len() + 4) }
}

struct PathArgs<'a, 'mcx> {
    jb: VarPayload<'a, 'mcx>,
    jp: VarPayload<'a, 'mcx>,
    vars: Option<VarPayload<'a, 'mcx>>,
    silent: bool,
}

fn path_args<'a, 'mcx>(
    fcinfo: &'a Fcinfo,
    mcx: Mcx<'mcx>,
    four_args: bool,
) -> PgResult<PathArgs<'a, 'mcx>> {
    let jb = arg_varlena(fcinfo, 0, mcx)?;
    let jp = arg_varlena(fcinfo, 1, mcx)?;
    let (vars, silent) = if four_args {
        (Some(arg_varlena(fcinfo, 2, mcx)?), fcinfo.arg_bool(3))
    } else {
        (None, true)
    };
    Ok(PathArgs { jb, jp, vars, silent })
}

fn vars_of<'v, 'a, 'mcx>(vars: &'v Option<VarPayload<'a, 'mcx>>) -> JsonPathVars<'v, 'v> {
    match vars {
        None => JsonPathVars::None,
        Some(v) => JsonPathVars::Jsonb(v.as_bytes()),
    }
}

fn exists_common(fcinfo: &mut Fcinfo, four_args: bool, tz: bool) -> PgResult<Datum> {
    let res = {
        let mcx = fcinfo.result_mcx();
        let args = path_args(fcinfo, mcx, four_args)?;
        crate::jsonb_path_exists_core(
            mcx,
            args.jb.as_bytes(),
            full_image(&args.jp),
            vars_of(&args.vars),
            args.silent,
            tz,
        )?
    };
    match res {
        Some(b) => Ok(Datum::from_bool(b)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_path_exists(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    exists_common(fcinfo, true, false)
}

pub fn fc_jsonb_path_exists_tz(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    exists_common(fcinfo, true, true)
}

pub fn fc_jsonb_path_exists_opr(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    exists_common(fcinfo, false, false)
}

fn match_common(fcinfo: &mut Fcinfo, four_args: bool, tz: bool) -> PgResult<Datum> {
    let res = {
        let mcx = fcinfo.result_mcx();
        let args = path_args(fcinfo, mcx, four_args)?;
        crate::jsonb_path_match_core(
            mcx,
            args.jb.as_bytes(),
            full_image(&args.jp),
            vars_of(&args.vars),
            args.silent,
            tz,
        )?
    };
    match res {
        Some(b) => Ok(Datum::from_bool(b)),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_path_match(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match_common(fcinfo, true, false)
}

pub fn fc_jsonb_path_match_tz(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match_common(fcinfo, true, true)
}

pub fn fc_jsonb_path_match_opr(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    match_common(fcinfo, false, false)
}

fn query_common(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    tz: bool,
) -> PgResult<Datum> {
    let flinfo = flinfo.unwrap_or_else(|| panic!("jsonb_path_query: NULL flinfo"));
    if !flinfo.has_fn_extra() {
        let rows = {
            let mcx = fcinfo.result_mcx();
            let args = path_args(fcinfo, mcx, true)?;
            crate::jsonb_path_query_core(
                mcx,
                args.jb.as_bytes(),
                full_image(&args.jp),
                vars_of(&args.vars),
                args.silent,
                tz,
            )?
        };
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("SRF rows set at first call")
        .downcast_ref::<Vec<Vec<u8>>>()
        .expect("user_fctx is the row set");
    let mcx = fcinfo.result_mcx();
    match rows.get(idx) {
        Some(img) => {
            let d = byref_result(mcx, img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub fn fc_jsonb_path_query(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    query_common(flinfo, fcinfo, false)
}

pub fn fc_jsonb_path_query_tz(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    query_common(flinfo, fcinfo, true)
}

fn query_array_common(fcinfo: &mut Fcinfo, tz: bool) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let args = path_args(fcinfo, mcx, true)?;
    let img = crate::jsonb_path_query_array_core(
        mcx,
        args.jb.as_bytes(),
        full_image(&args.jp),
        vars_of(&args.vars),
        args.silent,
        tz,
    )?;
    Ok(image_result(img))
}

pub fn fc_jsonb_path_query_array(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    query_array_common(fcinfo, false)
}

pub fn fc_jsonb_path_query_array_tz(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    query_array_common(fcinfo, true)
}

fn query_first_common(fcinfo: &mut Fcinfo, tz: bool) -> PgResult<Datum> {
    let img = {
        let mcx = fcinfo.result_mcx();
        let args = path_args(fcinfo, mcx, true)?;
        crate::jsonb_path_query_first_core(
            mcx,
            args.jb.as_bytes(),
            full_image(&args.jp),
            vars_of(&args.vars),
            args.silent,
            tz,
        )?
        .map(image_result)
    };
    match img {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

pub fn fc_jsonb_path_query_first(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    query_first_common(fcinfo, false)
}

pub fn fc_jsonb_path_query_first_tz(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    query_first_common(fcinfo, true)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, retset: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset, func }
}

pub const JSONPATH_EXEC_BUILTINS: &[FmgrBuiltin] = &[
    b(1177, "jsonb_path_exists_tz", 4, false, fc_jsonb_path_exists_tz),
    b(1179, "jsonb_path_query_tz", 4, true, fc_jsonb_path_query_tz),
    b(1180, "jsonb_path_query_array_tz", 4, false, fc_jsonb_path_query_array_tz),
    b(2023, "jsonb_path_query_first_tz", 4, false, fc_jsonb_path_query_first_tz),
    b(2030, "jsonb_path_match_tz", 4, false, fc_jsonb_path_match_tz),
    b(4005, "jsonb_path_exists", 4, false, fc_jsonb_path_exists),
    b(4006, "jsonb_path_query", 4, true, fc_jsonb_path_query),
    b(4007, "jsonb_path_query_array", 4, false, fc_jsonb_path_query_array),
    b(4008, "jsonb_path_query_first", 4, false, fc_jsonb_path_query_first),
    b(4009, "jsonb_path_match", 4, false, fc_jsonb_path_match),
    b(4010, "jsonb_path_exists_opr", 2, false, fc_jsonb_path_exists_opr),
    b(4011, "jsonb_path_match_opr", 2, false, fc_jsonb_path_match_opr),
];
