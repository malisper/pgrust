use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

fn arg_text<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a [u8]> {
    // SAFETY: strict text arg is a non-null live varlena.
    let pv = unsafe { fcinfo.arg_varlena_packed(i) }?;
    if pv.is_short() {
        Ok(pv.data_expanded(fcinfo.result_mcx())?)
    } else {
        Ok(pv.data())
    }
}

fn fc_gin_cmp_tslexeme(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_text(fcinfo, 0)?;
    let b = arg_text(fcinfo, 1)?;
    Ok(Datum::from_i32(crate::gin_cmp_tslexeme(a, b)))
}

fn fc_gin_cmp_prefix(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_text(fcinfo, 0)?;
    let b = arg_text(fcinfo, 1)?;
    Ok(Datum::from_i32(crate::gin_cmp_prefix(a, b)))
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const TSGINIDX_BUILTINS: &[FmgrBuiltin] = &[
    b(2700, "gin_cmp_prefix", 4, fc_gin_cmp_prefix),
    b(3724, "gin_cmp_tslexeme", 2, fc_gin_cmp_tslexeme),
];
