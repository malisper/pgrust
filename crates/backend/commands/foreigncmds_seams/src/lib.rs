use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // fmgr builtin 2316 marshals here; fmgr_core cannot depend on the DDL stack.
    pub fn postgresql_fdw_validator(mcx: Mcx<'_>, options: Datum, catalog: Oid) -> PgResult<bool>
);

seam_core::seam!(
    pub fn get_fdw_routine_by_rel_id(mcx: Mcx<'_>, relid: Oid) -> PgResult<()>
);

seam_core::seam!(
    // get_foreign_data_wrapper_oid (foreign.c) — has_foreign_data_wrapper_privilege
    // name resolution (a direct adt_acl -> foreigncmds dep would cycle).
    pub fn get_foreign_data_wrapper_oid(fdwname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    // get_foreign_server_oid (foreign.c) — has_server_privilege name resolution.
    pub fn get_foreign_server_oid(servername: &str, missing_ok: bool) -> PgResult<Oid>
);

pub mod builtins {
    use super::*;
    use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

    fn fc_postgresql_fdw_validator(
        _flinfo: Option<&mut FmgrInfo>,
        fcinfo: &mut Fcinfo,
    ) -> PgResult<Datum> {
        let [a, b] = fcinfo.args_n::<2>();
        let r = postgresql_fdw_validator::call(fcinfo.result_mcx(), a.value, b.value.as_oid())?;
        Ok(Datum::from_bool(r))
    }

    const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
        FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
    }

    pub const FOREIGN_BUILTINS: &[FmgrBuiltin] =
        &[b(2316, "postgresql_fdw_validator", 2, fc_postgresql_fdw_validator)];
}
