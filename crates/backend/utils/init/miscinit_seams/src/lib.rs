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

seam_core::seam!(
    pub fn get_session_user_id() -> Oid
);

seam_core::seam!(
    pub fn create_socket_lock_file(
        socketfile: &str,
        am_postmaster: bool,
        socket_dir: &str,
    ) -> PgResult<()>
);

seam_core::seam!(
    // InitializeSessionUserId(rolename, roleid, bypass_login_check) — the
    // pg_authid-syscache half deferred from the ported miscinit unit.
    pub fn initialize_session_user_id(
        rolename: Option<&str>,
        roleid: Oid,
        bypass_login_check: bool,
    ) -> PgResult<()>
);

seam_core::seam!(
    // has_rolreplication(roleid) (miscinit.c) — same deferred half.
    pub fn has_rolreplication(roleid: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // process_session_preload_libraries (miscinit.c) — same deferred half.
    pub fn process_session_preload_libraries() -> PgResult<()>
);

seam_core::seam!(
    // checkDataDir (miscinit.c) — deferred half of the ported miscinit unit.
    pub fn check_data_dir() -> PgResult<()>
);

seam_core::seam!(
    // process_shared_preload_libraries (miscinit.c) — same deferred half.
    pub fn process_shared_preload_libraries() -> PgResult<()>
);

seam_core::seam!(
    // preload_contrib boot GUC dispatch (hook-surface.md section 6 open Q2):
    // no C counterpart — compiled-in contrib's analog of processing
    // shared_preload_libraries.
    pub fn process_preload_contrib() -> PgResult<()>
);

seam_core::seam!(
    // process_shmem_requests (miscinit.c) — same deferred half.
    pub fn process_shmem_requests() -> PgResult<()>
);
