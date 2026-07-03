use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;

seam_core::seam!(
    // Resolve-once into the caller's carrier; startup/prepare frequency only.
    pub fn fmgr_info(function_id: Oid) -> PgResult<FmgrInfo>
);

seam_core::seam!(
    // get_fn_expr_variadic (fmgr.c); false when fn_expr is unset.
    pub fn get_fn_expr_variadic(flinfo: &FmgrInfo) -> bool
);

seam_core::seam!(
    // get_fn_expr_argtype (fmgr.c); InvalidOid when fn_expr is unset or
    // argnum is out of range.
    pub fn get_fn_expr_argtype(flinfo: &FmgrInfo, argnum: i16) -> Oid
);
