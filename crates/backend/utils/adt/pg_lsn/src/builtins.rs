//! fmgr wrappers (`fc_*`) + `PG_LSN_BUILTINS` for fmgr-core.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

use crate::{XLogRecPtr, MAXPG_LSNLEN};
use adt_numeric::Num;

#[inline]
fn arg_lsn(fcinfo: &Fcinfo, i: usize) -> XLogRecPtr {
    fcinfo.arg_i64(i) as u64
}

// SAFETY: forwarded caller contract (strict fn, non-null numeric varlena).
unsafe fn num_arg(fcinfo: &Fcinfo, i: usize) -> PgResult<Num<'_>> {
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    let payload = if v.is_short() {
        v.data_expanded(fcinfo.result_mcx())?
    } else {
        v.data()
    };
    Ok(Num::from_payload(payload))
}

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the nameout precedent). The Datum aliases it until the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; MAXPG_LSNLEN + 1]> =
        const { core::cell::UnsafeCell::new([0; MAXPG_LSNLEN + 1]) };
}

pub fn fc_pg_lsn_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of the in-function is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    Ok(Datum::from_i64(crate::pg_lsn_in(&s, esc)? as i64))
}

pub fn fc_pg_lsn_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let lsn = arg_lsn(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::pg_lsn_out_into(lsn, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_pg_lsn_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_i64(crate::pg_lsn_recv(buf)? as i64))
}

pub fn fc_pg_lsn_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let lsn = arg_lsn(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::pg_lsn_send(mcx, lsn)?))
}

macro_rules! fc_lsn_cmp {
    ($($fc:ident: $op:tt;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            Ok(Datum::from_bool(arg_lsn(fcinfo, 0) $op arg_lsn(fcinfo, 1)))
        }
    )*};
}

fc_lsn_cmp! {
    fc_pg_lsn_eq: ==;
    fc_pg_lsn_ne: !=;
    fc_pg_lsn_lt: <;
    fc_pg_lsn_gt: >;
    fc_pg_lsn_le: <=;
    fc_pg_lsn_ge: >=;
}

pub fn fc_pg_lsn_larger(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(arg_lsn(fcinfo, 0).max(arg_lsn(fcinfo, 1)) as i64))
}

pub fn fc_pg_lsn_smaller(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(arg_lsn(fcinfo, 0).min(arg_lsn(fcinfo, 1)) as i64))
}

pub fn fc_pg_lsn_cmp(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i32(crate::pg_lsn_cmp_internal(
        arg_lsn(fcinfo, 0),
        arg_lsn(fcinfo, 1),
    )))
}

// C forwards to hashint8/hashint8extended (hashfunc.c).
pub fn fc_pg_lsn_hash(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let val = fcinfo.arg_i64(0);
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    Ok(Datum::from_u32(::hashfn::hash_bytes_uint32(lohalf)))
}

pub fn fc_pg_lsn_hash_extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let val = fcinfo.arg_i64(0);
    let seed = fcinfo.arg_i64(1) as u64;
    let lohalf = val as u32;
    let hihalf = (val >> 32) as u32;
    let lohalf = lohalf ^ if val >= 0 { hihalf } else { !hihalf };
    Ok(Datum::from_u64(::hashfn::hash_bytes_uint32_extended(lohalf, seed)))
}

fn img_result(fcinfo: &mut Fcinfo, img: &adt_numeric::NumericImage) -> PgResult<Datum> {
    ::types_fmgr::byref_result(fcinfo.result_mcx(), img.as_bytes())
}

pub fn fc_pg_lsn_mi(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let img = crate::pg_lsn_mi(arg_lsn(fcinfo, 0), arg_lsn(fcinfo, 1))?;
    img_result(fcinfo, &img)
}

pub fn fc_pg_lsn_pli(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null numeric varlena.
    let nbytes = unsafe { num_arg(fcinfo, 1)? };
    Ok(Datum::from_i64(crate::pg_lsn_pli(arg_lsn(fcinfo, 0), nbytes)? as i64))
}

pub fn fc_pg_lsn_mii(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 1 is a non-null numeric varlena.
    let nbytes = unsafe { num_arg(fcinfo, 1)? };
    Ok(Datum::from_i64(crate::pg_lsn_mii(arg_lsn(fcinfo, 0), nbytes)? as i64))
}

pub fn fc_numeric_pg_lsn(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: strict fn — arg 0 is a non-null numeric varlena.
    let num = unsafe { num_arg(fcinfo, 0)? };
    Ok(Datum::from_i64(crate::numeric_pg_lsn(num)? as i64))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_proc.dat rows for pg_lsn.c (+ numeric.c's numeric_pg_lsn 6103).
pub const PG_LSN_BUILTINS: &[FmgrBuiltin] = &[
    b(3229, "pg_lsn_in", 1, fc_pg_lsn_in),
    b(3230, "pg_lsn_out", 1, fc_pg_lsn_out),
    b(3231, "pg_lsn_lt", 2, fc_pg_lsn_lt),
    b(3232, "pg_lsn_le", 2, fc_pg_lsn_le),
    b(3233, "pg_lsn_eq", 2, fc_pg_lsn_eq),
    b(3234, "pg_lsn_ge", 2, fc_pg_lsn_ge),
    b(3235, "pg_lsn_gt", 2, fc_pg_lsn_gt),
    b(3236, "pg_lsn_ne", 2, fc_pg_lsn_ne),
    b(3237, "pg_lsn_mi", 2, fc_pg_lsn_mi),
    b(3238, "pg_lsn_recv", 1, fc_pg_lsn_recv),
    b(3239, "pg_lsn_send", 1, fc_pg_lsn_send),
    b(3251, "pg_lsn_cmp", 2, fc_pg_lsn_cmp),
    b(3252, "pg_lsn_hash", 1, fc_pg_lsn_hash),
    b(3413, "pg_lsn_hash_extended", 2, fc_pg_lsn_hash_extended),
    b(4187, "pg_lsn_larger", 2, fc_pg_lsn_larger),
    b(4188, "pg_lsn_smaller", 2, fc_pg_lsn_smaller),
    b(5022, "pg_lsn_pli", 2, fc_pg_lsn_pli),
    b(5024, "pg_lsn_mii", 2, fc_pg_lsn_mii),
    b(6103, "numeric_pg_lsn", 1, fc_numeric_pg_lsn),
];
