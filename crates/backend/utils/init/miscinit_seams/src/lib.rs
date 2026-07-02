use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn get_user_id_and_sec_context() -> (Oid, i32)
);

seam_core::seam!(
    pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32)
);

seam_core::seam!(
    pub fn get_user_name_from_id<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        noerr: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    pub fn is_bootstrap_processing_mode() -> bool
);

seam_core::seam!(
    // Recovery-only DatabasePath poke (ProcessCommittedInvalidationMessages).
    pub fn set_database_path(path: &str)
);

seam_core::seam!(
    pub fn clear_database_path()
);

seam_core::seam!(
    pub fn switch_to_shared_latch()
);

seam_core::seam!(
    pub fn switch_back_to_local_latch()
);

seam_core::seam!(
    pub fn get_user_id() -> Oid
);
