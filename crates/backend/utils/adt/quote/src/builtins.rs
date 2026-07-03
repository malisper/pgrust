use ::datum::Datum;
use ::types_core::Oid;
use ::types_error::PgResult;
use ::types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

pub fn fc_quote_ident(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is non-null text (strict fn).
    let t = unsafe { fcinfo.arg_varlena_packed(0)? };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::quote_ident(mcx, t.data())?))
}

pub fn fc_quote_literal(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is non-null text (strict fn).
    let t = unsafe { fcinfo.arg_varlena_packed(0)? };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::quote_literal(mcx, t.data())?))
}

pub fn fc_quote_nullable(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    if fcinfo.argisnull(0) {
        return Ok(varlena_result(varlena::cstring_to_text(mcx, b"NULL")?));
    }
    // SAFETY: nullness checked above; catalog arg 0 is text.
    let t = unsafe { fcinfo.arg_varlena_packed(0)? };
    Ok(varlena_result(crate::quote_literal(mcx, t.data())?))
}

const fn b(foid: Oid, name: &'static str, strict: bool, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs: 1, strict, retset: false, func }
}

// pg_proc.dat rows; 1285/1290 are the SQL-language anyelement variants.
pub const QUOTE_BUILTINS: &[FmgrBuiltin] = &[
    b(1282, "quote_ident", true, fc_quote_ident),
    b(1283, "quote_literal", true, fc_quote_literal),
    b(1289, "quote_nullable", false, fc_quote_nullable),
];
