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
];
