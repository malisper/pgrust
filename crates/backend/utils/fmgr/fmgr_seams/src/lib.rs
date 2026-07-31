use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;

seam_core::seam!(
    pub fn fmgr_info(function_id: Oid) -> PgResult<FmgrInfo>
);

seam_core::seam!(
    // pgrust-only (no C analogue): Some(builtin name) iff `flinfo` resolved
    // to fmgr's not-ported stub, i.e. invoking it can only raise the
    // feature-not-supported error. Lets eager resolvers (index-AM support
    // procs) fail at resolution time instead of mid-operation. Installed by
    // fmgr_core::init_seams; consumers must gate on is_installed() — test
    // mocks that install only fmgr_info leave this one empty.
    pub fn fmgr_info_not_ported_name(flinfo: &FmgrInfo) -> Option<&'static str>
);

seam_core::seam!(
    pub fn get_fn_expr_variadic(flinfo: &FmgrInfo) -> bool
);

seam_core::seam!(
    pub fn get_fn_expr_argtype(flinfo: &FmgrInfo, argnum: i16) -> Oid
);

seam_core::seam!(
    // C find_coercion_pathway(JSONOID, typoid, COERCION_EXPLICIT) narrowed to
    // json_categorize_type's use: the castfunc iff COERCION_PATH_FUNC, else
    // InvalidOid.
    pub fn find_json_cast_func(typoid: Oid) -> PgResult<Oid>
);
