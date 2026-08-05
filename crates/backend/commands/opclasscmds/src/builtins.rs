use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo};

// amvalidate(oid) SQL function (amapi.c), pg_proc oid 338.
pub fn fc_amvalidate(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [arg] = fcinfo.args_n::<1>();
    Ok(Datum::from_bool(amapi::amvalidate(arg.value.as_oid())?))
}

pub static OPCLASS_BUILTINS: &[FmgrBuiltin] = &[FmgrBuiltin {
    foid: 338,
    name: "amvalidate",
    nargs: 1,
    strict: true,
    retset: false,
    func: fc_amvalidate,
}];
