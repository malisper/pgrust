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
    b(184, "oideq", 2, fc_oideq),
    b(185, "oidne", 2, fc_oidne),
    b(716, "oidlt", 2, fc_oidlt),
    b(717, "oidle", 2, fc_oidle),
    b(1638, "oidgt", 2, fc_oidgt),
    b(1639, "oidge", 2, fc_oidge),
];
