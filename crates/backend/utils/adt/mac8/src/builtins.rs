//! fmgr wrappers (`fc_*`) + `MAC8_BUILTINS` for fmgr-core.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction, MACADDR8_LEN,
};

use crate::{MacAddr8, MACADDR8_OUT_LEN};
use ::adt_mac::MacAddr;

fn arg_mac8(fcinfo: &Fcinfo, i: usize) -> MacAddr8 {
    // SAFETY: catalog args of these strict fns are non-null 8-byte macaddr8 blocks.
    let b = unsafe { fcinfo.arg_fixed(i, MACADDR8_LEN) };
    MacAddr8::from_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the nameout precedent). The Datum aliases it until the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; MACADDR8_OUT_LEN]> =
        const { core::cell::UnsafeCell::new([0; MACADDR8_OUT_LEN]) };
}

pub fn fc_macaddr8_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let addr = arg_mac8(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::macaddr8_out_into(&addr, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc_mac8_2 {
    ($($fc:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_mac8(fcinfo, 0), arg_mac8(fcinfo, 1));
            Ok(Datum::$conv(crate::$core(&a, &b)))
        }
    )*};
}

fc_mac8_2! {
    fc_macaddr8_eq: macaddr8_eq -> from_bool;
    fc_macaddr8_ne: macaddr8_ne -> from_bool;
    fc_macaddr8_lt: macaddr8_lt -> from_bool;
    fc_macaddr8_le: macaddr8_le -> from_bool;
    fc_macaddr8_gt: macaddr8_gt -> from_bool;
    fc_macaddr8_ge: macaddr8_ge -> from_bool;
    fc_macaddr8_cmp: macaddr8_cmp -> from_i32;
}

pub fn fc_hashmacaddr8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u32(crate::hashmacaddr8(&arg_mac8(fcinfo, 0))))
}

pub fn fc_hashmacaddr8extended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let seed = fcinfo.arg(1).as_u64();
    Ok(Datum::from_u64(crate::hashmacaddr8extended(
        &arg_mac8(fcinfo, 0),
        seed,
    )))
}

fn mac8_result(fcinfo: &mut Fcinfo, addr: &MacAddr8) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &addr.to_bytes())
}

pub fn fc_macaddr8_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of the in-function is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let addr = crate::macaddr8_in(&s, esc)?;
    mac8_result(fcinfo, &addr)
}

pub fn fc_macaddr8_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let addr = crate::macaddr8_recv(buf)?;
    mac8_result(fcinfo, &addr)
}

pub fn fc_macaddr8_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let addr = arg_mac8(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::macaddr8_send(mcx, &addr)?))
}

macro_rules! fc_mac8_1 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_mac8(fcinfo, 0);
            let r = crate::$core(&a);
            mac8_result(fcinfo, &r)
        }
    )*};
}

fc_mac8_1! {
    fc_macaddr8_trunc: macaddr8_trunc;
    fc_macaddr8_not: macaddr8_not;
    fc_macaddr8_set7bit: macaddr8_set7bit;
}

macro_rules! fc_mac8_bin {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_mac8(fcinfo, 0), arg_mac8(fcinfo, 1));
            let r = crate::$core(&a, &b);
            mac8_result(fcinfo, &r)
        }
    )*};
}

fc_mac8_bin! {
    fc_macaddr8_and: macaddr8_and;
    fc_macaddr8_or: macaddr8_or;
}

pub fn fc_macaddrtomacaddr8(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null 6-byte macaddr block (strict fn).
    let b = unsafe { fcinfo.arg_fixed(0, 6) };
    let a6 = MacAddr { a: b[0], b: b[1], c: b[2], d: b[3], e: b[4], f: b[5] };
    let r = crate::macaddrtomacaddr8(&a6);
    mac8_result(fcinfo, &r)
}

pub fn fc_macaddr8tomacaddr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_mac8(fcinfo, 0);
    let r = crate::macaddr8tomacaddr(&a)?;
    byref_result(fcinfo.result_mcx(), &r.to_bytes())
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

// pg_proc.dat rows (all proisstrict, none retset), OID-ascending.
pub const MAC8_BUILTINS: &[FmgrBuiltin] = &[
    b(328, "hashmacaddr8", 1, fc_hashmacaddr8),
    b(781, "hashmacaddr8extended", 2, fc_hashmacaddr8extended),
    b(3446, "macaddr8_recv", 1, fc_macaddr8_recv),
    b(3447, "macaddr8_send", 1, fc_macaddr8_send),
    b(4110, "macaddr8_in", 1, fc_macaddr8_in),
    b(4111, "macaddr8_out", 1, fc_macaddr8_out),
    b(4112, "macaddr8_trunc", 1, fc_macaddr8_trunc),
    b(4113, "macaddr8_eq", 2, fc_macaddr8_eq),
    b(4114, "macaddr8_lt", 2, fc_macaddr8_lt),
    b(4115, "macaddr8_le", 2, fc_macaddr8_le),
    b(4116, "macaddr8_gt", 2, fc_macaddr8_gt),
    b(4117, "macaddr8_ge", 2, fc_macaddr8_ge),
    b(4118, "macaddr8_ne", 2, fc_macaddr8_ne),
    b(4119, "macaddr8_cmp", 2, fc_macaddr8_cmp),
    b(4120, "macaddr8_not", 1, fc_macaddr8_not),
    b(4121, "macaddr8_and", 2, fc_macaddr8_and),
    b(4122, "macaddr8_or", 2, fc_macaddr8_or),
    b(4123, "macaddrtomacaddr8", 1, fc_macaddrtomacaddr8),
    b(4124, "macaddr8tomacaddr", 1, fc_macaddr8tomacaddr),
    b(4125, "macaddr8_set7bit", 1, fc_macaddr8_set7bit),
];
