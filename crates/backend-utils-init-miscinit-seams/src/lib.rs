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

// ---- bootstrap-mode backend startup (miscinit.c) ----

seam_core::seam!(
    /// `InitStandaloneProcess(argv0)` (miscinit.c): set up the fake-shared
    /// state a standalone (non-postmaster) backend needs — `MyProcPid`,
    /// `MyStartTime`, shared-memory disposition, fake `LocalProcessControl`.
    /// Backend-local writes; infallible.
    pub fn init_standalone_process(argv0: &str)
);

seam_core::seam!(
    /// `checkDataDir()` (miscinit.c): validate the chosen data directory,
    /// `ereport(FATAL)` if missing or wrong permissions.
    pub fn check_data_dir() -> PgResult<()>
);

seam_core::seam!(
    /// `ChangeToDataDir()` (miscinit.c): `chdir()` into the data directory,
    /// `ereport(FATAL)` on failure.
    pub fn change_to_data_dir() -> PgResult<()>
);

seam_core::seam!(
    /// `CreateDataDirLockFile(amPostmaster)` (miscinit.c): create
    /// `postmaster.pid`, `ereport(FATAL)` if a conflicting lock file exists.
    pub fn create_data_dir_lock_file(am_postmaster: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `SetProcessingMode(BootstrapProcessing)` (miscadmin.h): set the `Mode`
    /// global. Backend-local write; infallible.
    pub fn set_processing_mode_bootstrap()
);

seam_core::seam!(
    /// `SetProcessingMode(NormalProcessing)` (miscadmin.h): set the `Mode`
    /// global. Backend-local write; infallible.
    pub fn set_processing_mode_normal()
);

seam_core::seam!(
    /// `IgnoreSystemIndexes = value` (the miscinit.c-owned global): force scans
    /// to ignore system indexes during bootstrap. Backend-local write.
    pub fn set_ignore_system_indexes(value: bool)
);
