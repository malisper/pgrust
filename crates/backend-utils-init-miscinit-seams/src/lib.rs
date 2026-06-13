//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `CreateSocketLockFile(socketfile, amPostmaster, socketDir)` — create
    /// the interlock file for a Unix socket path and arrange for it to be
    /// removed at exit. Failure paths `ereport(FATAL)` inside
    /// `CreateLockFile`.
    pub fn create_socket_lock_file(
        socketfile: &str,
        am_postmaster: bool,
        socket_dir: &str,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `process_shmem_requests_in_progress` (miscinit.c) — true only while the
    /// postmaster is running registered `shmem_request_hook`s.
    pub fn process_shmem_requests_in_progress() -> bool
);

seam_core::seam!(
    /// `process_shared_preload_libraries_in_progress` (miscinit.c) — whether
    /// the backend is currently inside the `shared_preload_libraries`
    /// initialization window. A backend-local global read.
    pub fn process_shared_preload_libraries_in_progress() -> bool
);

seam_core::seam!(
    /// `IsBootstrapProcessingMode()` (miscadmin.h): `Mode ==
    /// BootstrapProcessing`. A plain global read — infallible.
    pub fn is_bootstrap_processing_mode() -> bool
);

seam_core::seam!(
    /// `GetUserIdAndSecContext(&userid, &sec_context)` (miscinit.c): the
    /// current user ID and security-context bitmask. Reads backend-local
    /// state; infallible.
    pub fn get_user_id_and_sec_context() -> (Oid, i32)
);

seam_core::seam!(
    /// `SetUserIdAndSecContext(userid, sec_context)` (miscinit.c): install a
    /// new current user ID and security-context bitmask. Writes
    /// backend-local state; infallible.
    pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32)
);

seam_core::seam!(
    /// `GetUserNameFromId(roleid, noerr)` (miscinit.c): the role name for
    /// `roleid`, copied into `mcx` (C: `pstrdup` in the current context).
    /// With `noerr = false` a missing role raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `noerr = true` it is
    /// `Ok(None)`. `Err` includes OOM from the copy and syscache lookup
    /// errors.
    pub fn get_user_name_from_id<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        noerr: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `InitPostmasterChild()` (`miscinit.c`): initialization common to all
    /// postmaster children — detangle the child from the postmaster (signal
    /// handling, process group, postmaster-death watch, etc.).
    pub fn init_postmaster_child()
);

seam_core::seam!(
    /// `GetUserId()` (miscinit.c): the current effective user id. Pure
    /// global read (asserts validity in C); cannot `ereport`.
    pub fn get_user_id() -> Oid
);

// ---- critical-section / interrupt brackets + superuser check (miscadmin.h) ----

seam_core::seam!(
    /// `START_CRIT_SECTION()` — bump `CritSectionCount`; an `ereport(ERROR)`
    /// inside a critical section is promoted to PANIC. Pure backend-local
    /// counter write.
    pub fn start_crit_section()
);
seam_core::seam!(
    /// `END_CRIT_SECTION()` — decrement `CritSectionCount`.
    pub fn end_crit_section()
);
seam_core::seam!(
    /// `HOLD_INTERRUPTS()` — increment `InterruptHoldoffCount`.
    pub fn hold_interrupts()
);
seam_core::seam!(
    /// `RESUME_INTERRUPTS()` — decrement `InterruptHoldoffCount`.
    pub fn resume_interrupts()
);
seam_core::seam!(
    /// `superuser_arg(roleid)` (superuser.c, reached via miscinit) — true if
    /// `roleid` has superuser privilege. Reads the catalog cache; pure for the
    /// twophase caller's purposes.
    pub fn superuser_arg(roleid: types_core::Oid) -> bool
);

// --- backend-utils-init-postinit consumers (miscinit.c) ---

seam_core::seam!(
    /// `GetSessionUserId()` (miscinit.c): the session user's role OID.
    pub fn get_session_user_id() -> types_core::Oid
);

seam_core::seam!(
    /// `InitializeSessionUserIdStandalone()` (miscinit.c): set the session user
    /// to the bootstrap superuser (standalone/aux processes). `Err` carries its
    /// `ereport` surface.
    pub fn initialize_session_user_id_standalone() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializeSessionUserId(rolename, roleid, bypass_login_check)`
    /// (miscinit.c): set the session user from name or OID, checking
    /// rolcanlogin/rolconnlimit. `Err` carries its `ereport(FATAL)` surface.
    pub fn initialize_session_user_id(
        rolename: Option<&str>,
        roleid: types_core::Oid,
        bypass_login_check: bool,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `InitializeSystemUser(authn_id, auth_method)` (miscinit.c): set the
    /// `system_user` SQL value from the authenticated identity and method.
    /// `Err` carries its `ereport` surface.
    pub fn initialize_system_user(
        authn_id: &str,
        auth_method: &str,
    ) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `has_rolreplication(roleid)` (miscinit.c): does the role have the
    /// REPLICATION attribute? `Err` carries its catcache `ereport` surface.
    pub fn has_rolreplication(roleid: types_core::Oid) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `ValidatePgVersion(path)` (miscinit.c): verify the database
    /// directory's PG_VERSION matches the server. `Err` carries its
    /// `ereport(FATAL)` surface.
    pub fn validate_pg_version(path: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SetDatabasePath(path)` (miscinit.c): record the database directory
    /// path globally. `Err` carries its OOM surface.
    pub fn set_database_path(path: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `process_session_preload_libraries()` (miscinit.c): load the libraries
    /// named by `session_preload_libraries`/`local_preload_libraries`. `Err`
    /// carries the loader's `ereport` surface.
    pub fn process_session_preload_libraries() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `pg_usleep(microsec)` (port; PostAuthDelay application): sleep the given
    /// number of microseconds.
    pub fn pg_usleep(microsec: i64)
);
