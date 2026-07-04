//! fmgr wrappers (`fc_*`) + `MAC_BUILTINS` for fmgr-core. macaddr_in / recv /
//! send / not / and / or / trunc return by-reference values and need the frame
//! allocation/wire convention (the namein precedent); macaddr_sortsupport
//! needs the SortSupport node. Their value cores live in the crate root.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction, MACADDR_LEN,
};

use crate::{MacAddr, MACADDR_OUT_LEN};

fn arg_mac(fcinfo: &Fcinfo, i: usize) -> MacAddr {
    // SAFETY: catalog args of these strict fns are non-null 6-byte macaddr blocks.
    let b = unsafe { fcinfo.arg_fixed(i, MACADDR_LEN) };
    MacAddr::from_bytes([b[0], b[1], b[2], b[3], b[4], b[5]])
}

// C pallocs the cstring per row; the backend thread owns retained scratch
// (the nameout precedent). The Datum aliases it until the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; MACADDR_OUT_LEN]> =
        const { core::cell::UnsafeCell::new([0; MACADDR_OUT_LEN]) };
}

pub fn fc_macaddr_out(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let addr = arg_mac(fcinfo, 0);
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::macaddr_out_into(&addr, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc_mac2 {
    ($($fc:ident: $core:ident -> $conv:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_mac(fcinfo, 0), arg_mac(fcinfo, 1));
            Ok(Datum::$conv(crate::$core(&a, &b)))
        }
    )*};
}

fc_mac2! {
    fc_macaddr_eq: macaddr_eq -> from_bool;
    fc_macaddr_ne: macaddr_ne -> from_bool;
    fc_macaddr_lt: macaddr_lt -> from_bool;
    fc_macaddr_le: macaddr_le -> from_bool;
    fc_macaddr_gt: macaddr_gt -> from_bool;
    fc_macaddr_ge: macaddr_ge -> from_bool;
    fc_macaddr_cmp: macaddr_cmp -> from_i32;
}

pub fn fc_hashmacaddr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_u32(crate::hashmacaddr(&arg_mac(fcinfo, 0))))
}

pub fn fc_hashmacaddrextended(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let seed = fcinfo.arg(1).as_u64();
    Ok(Datum::from_u64(crate::hashmacaddrextended(
        &arg_mac(fcinfo, 0),
        seed,
    )))
}

fn mac_result(fcinfo: &mut Fcinfo, addr: &MacAddr) -> PgResult<Datum> {
    byref_result(fcinfo.result_mcx(), &addr.to_bytes())
}

pub fn fc_macaddr_in(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of the in-function is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract for this call.
    let esc = unsafe { fcinfo.soft_error_context() };
    let addr = crate::macaddr_in(&s, esc)?;
    mac_result(fcinfo, &addr)
}

pub fn fc_macaddr_recv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: recv arg0 is the live StringInfo pointer per the recv ABI.
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    let addr = crate::macaddr_recv(buf)?;
    mac_result(fcinfo, &addr)
}

pub fn fc_macaddr_send(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let addr = arg_mac(fcinfo, 0);
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::macaddr_send(mcx, &addr)?))
}

macro_rules! fc_mac1 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let a = arg_mac(fcinfo, 0);
            let r = crate::$core(&a);
            mac_result(fcinfo, &r)
        }
    )*};
}

fc_mac1! {
    fc_macaddr_trunc: macaddr_trunc;
    fc_macaddr_not: macaddr_not;
}

macro_rules! fc_mac_bin {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let (a, b) = (arg_mac(fcinfo, 0), arg_mac(fcinfo, 1));
            let r = crate::$core(&a, &b);
            mac_result(fcinfo, &r)
        }
    )*};
}

fc_mac_bin! {
    fc_macaddr_and: macaddr_and;
    fc_macaddr_or: macaddr_or;
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
// 3359 macaddr_sortsupport unregistered (SortSupport node frame absent; the
// uuid_sortsupport precedent).
pub const MAC_BUILTINS: &[FmgrBuiltin] = &[
    b(399, "hashmacaddr", 1, fc_hashmacaddr),
    b(436, "macaddr_in", 1, fc_macaddr_in),
    b(437, "macaddr_out", 1, fc_macaddr_out),
    b(753, "macaddr_trunc", 1, fc_macaddr_trunc),
    b(778, "hashmacaddrextended", 2, fc_hashmacaddrextended),
    b(830, "macaddr_eq", 2, fc_macaddr_eq),
    b(831, "macaddr_lt", 2, fc_macaddr_lt),
    b(832, "macaddr_le", 2, fc_macaddr_le),
    b(833, "macaddr_gt", 2, fc_macaddr_gt),
    b(834, "macaddr_ge", 2, fc_macaddr_ge),
    b(835, "macaddr_ne", 2, fc_macaddr_ne),
    b(836, "macaddr_cmp", 2, fc_macaddr_cmp),
    b(2494, "macaddr_recv", 1, fc_macaddr_recv),
    b(2495, "macaddr_send", 1, fc_macaddr_send),
    b(3144, "macaddr_not", 1, fc_macaddr_not),
    b(3145, "macaddr_and", 2, fc_macaddr_and),
    b(3146, "macaddr_or", 2, fc_macaddr_or),
];
