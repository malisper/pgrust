use types_core::Oid;
use types_error::PgResult;
use types_fmgr::FmgrInfo;

seam_core::seam!(
    // Resolve-once into the caller's carrier; startup/prepare frequency only.
    pub fn fmgr_info(function_id: Oid) -> PgResult<FmgrInfo>
);
