//! fmgr adapters for sequence.c's pg_proc surface; hosted here because
//! fmgr_core cannot depend on the DDL stack (one extra seam-slot load).
use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

fn fc_nextval_oid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_i64(crate::nextval_internal::call(a.value.as_oid(), true)?))
}

fn fc_currval_oid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a] = fcinfo.args_n::<1>();
    Ok(Datum::from_i64(crate::currval_internal::call(a.value.as_oid())?))
}

fn fc_lastval(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Ok(Datum::from_i64(crate::lastval_internal::call()?))
}

fn fc_setval_oid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b] = fcinfo.args_n::<2>();
    crate::do_setval::call(a.value.as_oid(), b.value.as_i64(), true)?;
    Ok(b.value)
}

fn fc_setval3_oid(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let [a, b, c] = fcinfo.args_n::<3>();
    crate::do_setval::call(a.value.as_oid(), b.value.as_i64(), c.value.as_bool())?;
    Ok(b.value)
}

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const SEQUENCE_BUILTINS: &[FmgrBuiltin] = &[
    b(1574, "nextval_oid", 1, fc_nextval_oid),
    b(1575, "currval_oid", 1, fc_currval_oid),
    b(1576, "setval_oid", 2, fc_setval_oid),
    b(1765, "setval3_oid", 3, fc_setval3_oid),
    b(2559, "lastval", 0, fc_lastval),
];
