//! fmgr wrappers (`fc_*`) + `ORACLE_COMPAT_BUILTINS` for fmgr-core. Only
//! ascii (1620) returns by value; every other row (870-885 case/pad/trim,
//! 401/881/882 one-arg trims, 1621 chr, 1622 repeat, 2015/6195/6196 bytea
//! trims, 6412 casefold) yields a text/bytea image and needs the frame
//! allocation convention (the varlena textin precedent) — value cores only.

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

pub fn fc_ascii(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of ascii is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0) }.data();
    Ok(Datum::from_i32(crate::ascii(payload)?))
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

pub const ORACLE_COMPAT_BUILTINS: &[FmgrBuiltin] = &[b(1620, "ascii", 1, fc_ascii)];
