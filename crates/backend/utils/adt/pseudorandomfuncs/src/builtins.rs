use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    byref_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

pub fn fc_setseed(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    crate::setseed(fcinfo.arg_f64(0))?;
    Ok(Datum::null())
}

pub fn fc_drandom(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_f64(crate::drandom()))
}

pub fn fc_drandom_normal(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (mean, stddev) = (fcinfo.arg_f64(0), fcinfo.arg_f64(1));
    Ok(Datum::from_f64(crate::drandom_normal(mean, stddev)))
}

pub fn fc_int4random(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (rmin, rmax) = (fcinfo.arg_i32(0), fcinfo.arg_i32(1));
    Ok(Datum::from_i32(crate::int4random(rmin, rmax)?))
}

pub fn fc_int8random(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let (rmin, rmax) = (fcinfo.arg_i64(0), fcinfo.arg_i64(1));
    Ok(Datum::from_i64(crate::int8random(rmin, rmax)?))
}

pub fn fc_numeric_random(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args of numeric_random are non-null numerics (strict fn).
    let (rmin, rmax) = unsafe {
        (
            ::adt_numeric::builtins::num_arg(fcinfo, 0)?,
            ::adt_numeric::builtins::num_arg(fcinfo, 1)?,
        )
    };
    let img = crate::numeric_random(rmin, rmax)?;
    byref_result(fcinfo.result_mcx(), img.as_bytes())
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const PSEUDORANDOM_BUILTINS: &[FmgrBuiltin] = &[
    b(1598, "drandom", 0, fc_drandom),
    b(1599, "setseed", 1, fc_setseed),
    b(6212, "drandom_normal", 2, fc_drandom_normal),
    b(6339, "int4random", 2, fc_int4random),
    b(6340, "int8random", 2, fc_int8random),
    b(6341, "numeric_random", 2, fc_numeric_random),
];
