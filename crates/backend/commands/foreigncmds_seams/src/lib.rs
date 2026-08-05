use datum::Datum;
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    // fmgr builtin 2316 marshals here; fmgr_core cannot depend on the DDL stack.
    pub fn postgresql_fdw_validator(mcx: Mcx<'_>, options: Datum, catalog: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // GetFdwRoutineByRelId (foreign.c) — the routine collapses to the provider
    // id (types_nodes::FdwKind); layer-owned callback tables key off it.
    pub fn get_fdw_routine_by_rel_id(mcx: Mcx<'_>, relid: Oid) -> PgResult<types_nodes::FdwKind>
);

seam_core::seam!(
    // GetFdwRoutineByServerId (foreign.c) — executor path for scanrelid == 0.
    pub fn get_fdw_routine_by_server_id(
        mcx: Mcx<'_>,
        serverid: Oid,
    ) -> PgResult<types_nodes::FdwKind>
);

seam_core::seam!(
    // GetForeignServerIdByRelId (foreign.c) — plancat populates RelOptInfo.serverid.
    pub fn get_foreign_server_id_by_rel_id(relid: Oid) -> PgResult<Oid>
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

seam_core::seam!(
    // pg_options_to_table (foreign.c) — fmgr builtin 2289 marshals here; the
    // materialized-SRF body lives with the DDL stack (funcapi dep).
    pub fn pg_options_to_table(
        flinfo: Option<&mut types_fmgr::FmgrInfo>,
        fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
    ) -> PgResult<Datum>
);

pub mod builtins {
    use super::*;
    use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

    fn fc_pg_options_to_table(
        flinfo: Option<&mut FmgrInfo>,
        fcinfo: &mut Fcinfo,
    ) -> PgResult<Datum> {
        pg_options_to_table::call(flinfo, fcinfo)
    }

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

    const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
        FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
    }

    pub const FOREIGN_BUILTINS: &[FmgrBuiltin] = &[
        b(2316, "postgresql_fdw_validator", 2, fc_postgresql_fdw_validator),
        srf(2289, "pg_options_to_table", 1, fc_pg_options_to_table),
    ];
}
