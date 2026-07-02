use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;

seam_core::seam!(
    // fmgr_info (fmgr.c): resolve-once into the caller's carrier; called at
    // startup/prepare frequency only, never per row.
    pub fn fmgr_info(function_id: Oid) -> PgResult<FmgrInfo>
);
