//! fmgr wrappers (`fc_*`) + `ORACLE_COMPAT_BUILTINS` for fmgr-core. Text/bytea
//! results follow the result-mcx convention (notes/fc-result-convention.md).

use datum::Datum;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{
    varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};

pub fn fc_ascii(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of ascii is a non-null text varlena (strict fn).
    let payload = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    Ok(Datum::from_i32(crate::ascii(payload)?))
}

macro_rules! fc_case {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
            let s = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
            let mcx = fcinfo.result_mcx();
            Ok(varlena_result(crate::$core(mcx, s, fcinfo.get_collation())?))
        }
    )*};
}

fc_case! {
    fc_lower: lower;
    fc_upper: upper;
    fc_initcap: initcap;
    fc_casefold: casefold;
}

macro_rules! fc_trim1 {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog arg 0 is a non-null text/bpchar varlena (strict fn).
            let s = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
            let mcx = fcinfo.result_mcx();
            Ok(varlena_result(crate::$core(mcx, s)?))
        }
    )*};
}

fc_trim1! {
    fc_btrim1: btrim1;
    fc_ltrim1: ltrim1;
    fc_rtrim1: rtrim1;
}

macro_rules! fc_trim2 {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args are non-null text/bytea varlenas (strict fn).
            let (s, set) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(1)?)
            };
            let mcx = fcinfo.result_mcx();
            Ok(varlena_result(crate::$core(mcx, s.data(), set.data())?))
        }
    )*};
}

fc_trim2! {
    fc_btrim: btrim;
    fc_ltrim: ltrim;
    fc_rtrim: rtrim;
    fc_byteatrim: byteatrim;
    fc_bytealtrim: bytealtrim;
    fc_byteartrim: byteartrim;
}

macro_rules! fc_pad {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog args 0/2 are non-null text varlenas (strict fn).
            let (s, fill) = unsafe {
                (fcinfo.arg_varlena_packed(0)?, fcinfo.arg_varlena_packed(2)?)
            };
            let len = fcinfo.arg_i32(1);
            let mcx = fcinfo.result_mcx();
            Ok(varlena_result(crate::$core(mcx, s.data(), len, fill.data())?))
        }
    )*};
}

fc_pad! {
    fc_lpad: lpad;
    fc_rpad: rpad;
}

pub fn fc_translate(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog args are non-null text varlenas (strict fn).
    let (s, from, to) = unsafe {
        (
            fcinfo.arg_varlena_packed(0)?,
            fcinfo.arg_varlena_packed(1)?,
            fcinfo.arg_varlena_packed(2)?,
        )
    };
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::translate(
        mcx,
        s.data(),
        from.data(),
        to.data(),
    )?))
}

macro_rules! fc_text_n {
    ($($fname:ident: $core:ident;)*) => {$(
        pub fn $fname(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
            let s = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
            let n = fcinfo.arg_i32(1);
            let mcx = fcinfo.result_mcx();
            Ok(varlena_result(crate::$core(mcx, s, n)?))
        }
    )*};
}

fc_text_n! {
    fc_text_left: text_left;
    fc_text_right: text_right;
}

pub fn fc_text_reverse(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 is a non-null text varlena (strict fn).
    let s = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::text_reverse(mcx, s)?))
}

pub fn fc_chr(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::chr(mcx, fcinfo.arg_i32(0))?))
}

pub fn fc_repeat(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of repeat is a non-null text varlena (strict fn).
    let s = unsafe { fcinfo.arg_varlena_packed(0)? }.data();
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(crate::repeat(mcx, s, fcinfo.arg_i32(1))?))
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

// pg_proc.dat prosrc rows (all proisstrict, none retset); 401 = text(bpchar).
pub const ORACLE_COMPAT_BUILTINS: &[FmgrBuiltin] = &[
    b(401, "rtrim1", 1, fc_rtrim1),
    b(870, "lower", 1, fc_lower),
    b(871, "upper", 1, fc_upper),
    b(872, "initcap", 1, fc_initcap),
    b(873, "lpad", 3, fc_lpad),
    b(874, "rpad", 3, fc_rpad),
    b(875, "ltrim", 2, fc_ltrim),
    b(876, "rtrim", 2, fc_rtrim),
    b(878, "translate", 3, fc_translate),
    b(881, "ltrim1", 1, fc_ltrim1),
    b(882, "rtrim1", 1, fc_rtrim1),
    b(884, "btrim", 2, fc_btrim),
    b(885, "btrim1", 1, fc_btrim1),
    b(1620, "ascii", 1, fc_ascii),
    b(1621, "chr", 1, fc_chr),
    b(1622, "repeat", 2, fc_repeat),
    b(2015, "byteatrim", 2, fc_byteatrim),
    b(3060, "text_left", 2, fc_text_left),
    b(3061, "text_right", 2, fc_text_right),
    b(3062, "text_reverse", 1, fc_text_reverse),
    b(6195, "bytealtrim", 2, fc_bytealtrim),
    b(6196, "byteartrim", 2, fc_byteartrim),
    b(6412, "casefold", 1, fc_casefold),
];
