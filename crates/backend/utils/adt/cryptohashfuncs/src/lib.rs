//! cryptohashfuncs.c MD5 half; SHA-2 rows stay unregistered (= loud) until a SHA-2 engine lands.

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

fn md5_common(fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text/bytea varlena; strict fn.
    let input = unsafe { fcinfo.arg_varlena_packed(0)? };
    let hexsum = pg_md5::pg_md5_hash(input.data());
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(mcx, &hexsum)?))
}

pub fn fc_md5_text(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    md5_common(fcinfo)
}

pub fn fc_md5_bytea(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    md5_common(fcinfo)
}

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 1, strict: true, retset: false, func }
}

pub const CRYPTOHASH_BUILTINS: &[FmgrBuiltin] = &[
    b(2311, "md5_text", fc_md5_text),
    b(2321, "md5_bytea", fc_md5_bytea),
];
