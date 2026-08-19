//! fmgr adapters for extension.c's pg_proc surface; hosted here because
//! fmgr_core cannot depend on the DDL stack (one extra seam-slot load).
use datum::Datum;
use types_error::PgResult;
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

fn fc_pg_available_extensions(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    crate::pg_available_extensions::call(flinfo, fcinfo)
}

fn fc_pg_available_extension_versions(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    crate::pg_available_extension_versions::call(flinfo, fcinfo)
}

fn fc_pg_extension_update_paths(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    crate::pg_extension_update_paths::call(flinfo, fcinfo)
}

fn fc_pg_extension_config_dump(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    crate::pg_extension_config_dump::call(flinfo, fcinfo)
}

fn fc_pg_get_loaded_modules(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    crate::pg_get_loaded_modules::call(flinfo, fcinfo)
}

const fn srf(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

pub const EXTENSION_BUILTINS: &[FmgrBuiltin] = &[
    srf(3082, "pg_available_extensions", 0, fc_pg_available_extensions),
    srf(3083, "pg_available_extension_versions", 0, fc_pg_available_extension_versions),
    srf(3084, "pg_extension_update_paths", 1, fc_pg_extension_update_paths),
    FmgrBuiltin {
        foid: 3086,
        name: "pg_extension_config_dump",
        nargs: 2,
        strict: true,
        retset: false,
        func: fc_pg_extension_config_dump,
    },
    srf(6353, "pg_get_loaded_modules", 0, fc_pg_get_loaded_modules),
];

#[cfg(test)]
mod tests {
    #[test]
    fn rows_match_canonical() {
        fmgr_core::assert_rows_match_canonical(super::EXTENSION_BUILTINS);
    }
}
