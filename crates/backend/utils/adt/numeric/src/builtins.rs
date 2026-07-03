//! fmgr wrappers + registry for numeric.c's fmgr-callable rows. Currently the
//! int8-transtype sum transfns only; the NumericVar-repr rows land with the
//! numeric-datum unit.

use ::datum::Datum;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

// int8 is pass-by-value in this build (USE_FLOAT8_BYVAL), so C's
// modify-in-place AggCheckCallContext branch is compiled out.
#[inline]
pub fn int4_sum(oldsum: Option<i64>, newval: Option<i32>) -> Option<i64> {
    match (oldsum, newval) {
        (None, None) => None,
        (None, Some(v)) => Some(v as i64),
        (Some(s), None) => Some(s),
        (Some(s), Some(v)) => Some(s + v as i64),
    }
}

#[inline]
pub fn int2_sum(oldsum: Option<i64>, newval: Option<i16>) -> Option<i64> {
    int4_sum(oldsum, newval.map(i32::from))
}

macro_rules! fc_sum {
    ($($fc:ident: $core:ident($get:ident);)*) => {$(
        pub fn $fc(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let [a, b] = fcinfo.args_n::<2>();
            let oldsum = (!a.isnull).then(|| a.value.as_i64());
            let newval = (!b.isnull).then(|| b.value.$get());
            match $core(oldsum, newval) {
                Some(v) => Ok(Datum::from_i64(v)),
                None => {
                    fcinfo.isnull = true;
                    Ok(Datum::null())
                }
            }
        }
    )*};
}

fc_sum! {
    fc_int4_sum: int4_sum(as_i32);
    fc_int2_sum: int2_sum(as_i16);
}

const fn bn(foid: ::types_core::Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 2, strict: false, retset: false, func }
}

// pg_proc.dat rows (proisstrict 'f' per pg_proc.dat 1840/1841).
pub const NUMERIC_BUILTINS: &[FmgrBuiltin] = &[
    bn(1840, "int2_sum", fc_int2_sum),
    bn(1841, "int4_sum", fc_int4_sum),
];
