//! fmgr wrappers (`fc_*`) + `ENCODE_BUILTINS` for fmgr-core. Both results are
//! new by-ref varlenas built in the frame's armed context (result-mcx
//! convention, notes/fc-result-convention.md).

use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

pub fn fc_binary_encode(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null bytea (0) + text codec name (1); strict fn.
    let (data, name) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::binary_encode(
        mcx,
        data.data(),
        name.data(),
    )?))
}

pub fn fc_binary_decode(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text data (0) + text codec name (1); strict fn.
    let (data, name) = unsafe { (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?) };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::binary_decode(
        mcx,
        data.data(),
        name.data(),
    )?))
}

const fn b(foid: Oid, name: &'static str, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs: 2,
        strict: true,
        retset: false,
        func,
    }
}

pub const ENCODE_BUILTINS: &[FmgrBuiltin] = &[
    b(1946, "binary_encode", fc_binary_encode),
    b(1947, "binary_decode", fc_binary_decode),
];
