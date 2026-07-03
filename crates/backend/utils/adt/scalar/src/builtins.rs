use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

macro_rules! fc_oid2 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_bool(crate::$core(a.value.as_oid(), b.value.as_oid())))
        }
    )*};
}

fc_oid2! {
    fc_oideq: oideq;
    fc_oidne: oidne;
    fc_oidlt: oidlt;
    fc_oidle: oidle;
    fc_oidgt: oidgt;
    fc_oidge: oidge;
}

// Result Datum aliases the scratch: consume before the next out call.
std::thread_local! {
    static OUT_SCRATCH: core::cell::UnsafeCell<[u8; 16]> =
        const { core::cell::UnsafeCell::new([0; 16]) };
}

pub fn fc_xidout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_u32();
    OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = crate::xidout(v, buf);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

macro_rules! fc_xid2 {
    ($($fc:ident: $core:ident;)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            Ok(Datum::from_bool(crate::$core(a.value.as_u32(), b.value.as_u32())))
        }
    )*};
}

fc_xid2! {
    fc_xideq: xideq;
    fc_xidneq: xidneq;
}

pub fn fc_oidin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of oidin is cstring (typlen -2).
    let s = unsafe { fcinfo.arg_cstring(0) };
    let s = String::from_utf8_lossy(s.to_bytes());
    // SAFETY: context, if set, rides per the ErrorSaveNode contract.
    let esc = unsafe { fcinfo.soft_error_context() };
    let (v, _) = ::numutils::uint32in_subr(&s, false, "oid", esc)?;
    Ok(Datum::from_oid(v))
}

// C pallocs 12 bytes per row; the int.c retained-TLS out convention instead.
std::thread_local! {
    static OID_OUT_SCRATCH: core::cell::UnsafeCell<[u8; 12]> =
        const { core::cell::UnsafeCell::new([0; 12]) };
}

pub fn fc_oidout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let v = a.value.as_oid();
    OID_OUT_SCRATCH.with(|c| {
        // SAFETY: single-threaded backend; the sole live access is this call.
        let buf = unsafe { &mut *c.get() };
        let len = ::numutils::pg_ultoa_n(v, &mut buf[..11]);
        buf[len] = 0;
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

pub fn fc_oidrecv(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of oidrecv is internal (StringInfo).
    let buf = unsafe { fcinfo.arg_stringinfo(0) };
    Ok(Datum::from_oid(::pqformat::pq_getmsgint(buf, 4)? as u32))
}

pub fn fc_oidsend(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    let mcx = fcinfo.result_mcx();
    let mut buf = ::pqformat::pq_begintypsend(mcx)?;
    ::pqformat::pq_sendint32(&mut buf, a.value.as_oid())?;
    Ok(::types_fmgr::varlena_result(::pqformat::pq_endtypsend(buf)))
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
pub const SCALAR_BUILTINS: &[FmgrBuiltin] = &[
    b(51, "xidout", 1, fc_xidout),
    b(68, "xideq", 2, fc_xideq),
    b(3308, "xidneq", 2, fc_xidneq),
    b(184, "oideq", 2, fc_oideq),
    b(185, "oidne", 2, fc_oidne),
    b(716, "oidlt", 2, fc_oidlt),
    b(717, "oidle", 2, fc_oidle),
    b(1638, "oidgt", 2, fc_oidgt),
    b(1639, "oidge", 2, fc_oidge),
    b(1798, "oidin", 1, fc_oidin),
    b(1799, "oidout", 1, fc_oidout),
    b(2418, "oidrecv", 1, fc_oidrecv),
    b(2419, "oidsend", 1, fc_oidsend),
];
