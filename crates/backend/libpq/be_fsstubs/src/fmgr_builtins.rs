use datum::Datum;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

pub fn fc_lo_open(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let fd = crate::be_lo_open(fcinfo.result_mcx(), fcinfo.arg_oid(0), fcinfo.arg_i32(1))?;
    Ok(Datum::from_i32(fd))
}

pub fn fc_lo_close(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::be_lo_close(fcinfo.arg_i32(0))?))
}

pub fn fc_loread(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = crate::be_loread(fcinfo.result_mcx(), fcinfo.arg_i32(0), fcinfo.arg_i32(1))?;
    Ok(varlena_result(v))
}

pub fn fc_lowrite(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 1 is a non-null bytea (strict fn).
    let wbuf = unsafe { fcinfo.arg_varlena_packed(1)? };
    let n = crate::be_lowrite(fcinfo.result_mcx(), fcinfo.arg_i32(0), wbuf.data())?;
    Ok(Datum::from_i32(n))
}

pub fn fc_lo_lseek(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let r = crate::be_lo_lseek(
        fcinfo.result_mcx(),
        fcinfo.arg_i32(0),
        fcinfo.arg_i32(1),
        fcinfo.arg_i32(2),
    )?;
    Ok(Datum::from_i32(r))
}

pub fn fc_lo_lseek64(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let r = crate::be_lo_lseek64(
        fcinfo.result_mcx(),
        fcinfo.arg_i32(0),
        fcinfo.arg_i64(1),
        fcinfo.arg_i32(2),
    )?;
    Ok(Datum::from_i64(r))
}

pub fn fc_lo_creat(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // The mode argument is ignored (C: be_lo_creat).
    Ok(Datum::from_oid(crate::be_lo_creat(fcinfo.result_mcx())?))
}

pub fn fc_lo_create(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_oid(crate::be_lo_create(
        fcinfo.result_mcx(),
        fcinfo.arg_oid(0),
    )?))
}

pub fn fc_lo_tell(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::be_lo_tell(fcinfo.arg_i32(0))?))
}

pub fn fc_lo_tell64(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::be_lo_tell64(fcinfo.arg_i32(0))?))
}

pub fn fc_lo_unlink(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::be_lo_unlink(
        fcinfo.result_mcx(),
        fcinfo.arg_oid(0),
    )?))
}

pub fn fc_lo_import(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text (strict fn).
    let filename = unsafe { fcinfo.arg_varlena_packed(0)? };
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_oid(crate::be_lo_import(mcx, filename.data())?))
}

pub fn fc_lo_import_with_oid(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text (strict fn).
    let filename = unsafe { fcinfo.arg_varlena_packed(0)? };
    let oid = fcinfo.arg_oid(1);
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_oid(crate::be_lo_import_with_oid(
        mcx,
        filename.data(),
        oid,
    )?))
}

pub fn fc_lo_export(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 1 is a non-null text (strict fn).
    let filename = unsafe { fcinfo.arg_varlena_packed(1)? };
    let lobj_id = fcinfo.arg_oid(0);
    let mcx = fcinfo.result_mcx();
    Ok(Datum::from_i32(crate::be_lo_export(
        mcx,
        lobj_id,
        filename.data(),
    )?))
}

pub fn fc_lo_truncate(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::be_lo_truncate(
        fcinfo.result_mcx(),
        fcinfo.arg_i32(0),
        fcinfo.arg_i32(1),
    )?))
}

pub fn fc_lo_truncate64(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::be_lo_truncate64(
        fcinfo.result_mcx(),
        fcinfo.arg_i32(0),
        fcinfo.arg_i64(1),
    )?))
}

pub fn fc_lo_from_bytea(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 1 is a non-null bytea (strict fn).
    let data = unsafe { fcinfo.arg_varlena_packed(1)? };
    Ok(Datum::from_oid(crate::be_lo_from_bytea(
        fcinfo.result_mcx(),
        fcinfo.arg_oid(0),
        data.data(),
    )?))
}

pub fn fc_lo_get(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = crate::be_lo_get(fcinfo.result_mcx(), fcinfo.arg_oid(0))?;
    Ok(varlena_result(v))
}

pub fn fc_lo_get_fragment(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let v = crate::be_lo_get_fragment(
        fcinfo.result_mcx(),
        fcinfo.arg_oid(0),
        fcinfo.arg_i64(1),
        fcinfo.arg_i32(2),
    )?;
    Ok(varlena_result(v))
}

pub fn fc_lo_put(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 2 is a non-null bytea (strict fn).
    let data = unsafe { fcinfo.arg_varlena_packed(2)? };
    crate::be_lo_put(
        fcinfo.result_mcx(),
        fcinfo.arg_oid(0),
        fcinfo.arg_i64(1),
        data.data(),
    )?;
    Ok(Datum::null())
}

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, func: types_fmgr::PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

pub const FSSTUBS_BUILTINS: &[FmgrBuiltin] = &[
    b(715, "be_lo_create", 1, fc_lo_create),
    b(764, "be_lo_import", 1, fc_lo_import),
    b(765, "be_lo_export", 2, fc_lo_export),
    b(767, "be_lo_import_with_oid", 2, fc_lo_import_with_oid),
    b(952, "be_lo_open", 2, fc_lo_open),
    b(953, "be_lo_close", 1, fc_lo_close),
    b(954, "be_loread", 2, fc_loread),
    b(955, "be_lowrite", 2, fc_lowrite),
    b(956, "be_lo_lseek", 3, fc_lo_lseek),
    b(957, "be_lo_creat", 1, fc_lo_creat),
    b(958, "be_lo_tell", 1, fc_lo_tell),
    b(964, "be_lo_unlink", 1, fc_lo_unlink),
    b(1004, "be_lo_truncate", 2, fc_lo_truncate),
    b(3170, "be_lo_lseek64", 3, fc_lo_lseek64),
    b(3171, "be_lo_tell64", 1, fc_lo_tell64),
    b(3172, "be_lo_truncate64", 2, fc_lo_truncate64),
    b(3457, "be_lo_from_bytea", 2, fc_lo_from_bytea),
    b(3458, "be_lo_get", 1, fc_lo_get),
    b(3459, "be_lo_get_fragment", 3, fc_lo_get_fragment),
    b(3460, "be_lo_put", 3, fc_lo_put),
];
