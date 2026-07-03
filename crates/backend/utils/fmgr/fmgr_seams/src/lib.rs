use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;

seam_core::seam!(
    pub fn fmgr_info(function_id: Oid) -> PgResult<FmgrInfo>
);

seam_core::seam!(
    pub fn get_fn_expr_variadic(flinfo: &FmgrInfo) -> bool
);

seam_core::seam!(
    pub fn get_fn_expr_argtype(flinfo: &FmgrInfo, argnum: i16) -> Oid
);
