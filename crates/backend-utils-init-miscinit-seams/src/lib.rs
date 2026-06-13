//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`).
//!
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly. Only `miscinit.c`'s own functions are declared here — outward
//! calls miscinit makes (syscache, guc, superuser, ...) live in their owners'
//! seam crates.

#![allow(non_snake_case)]

use mcx::{Mcx, PgString};
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `CreateSocketLockFile(socketfile, amPostmaster, socketDir)` — create the
    /// interlock file for a Unix socket path and arrange for it to be removed at
    /// exit. Failure paths `ereport(FATAL)` inside `CreateLockFile`.
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
    /// `process_shared_preload_libraries_in_progress` (miscinit.c) — whether the
    /// backend is currently inside the `shared_preload_libraries` initialization
    /// window. A backend-local global read.
    pub fn process_shared_preload_libraries_in_progress() -> bool
);

seam_core::seam!(
    /// `IsBootstrapProcessingMode()` (miscadmin.h): `Mode == BootstrapProcessing`.
    /// A plain global read — infallible.
    pub fn is_bootstrap_processing_mode() -> bool
);

seam_core::seam!(
    /// `GetUserIdAndSecContext(&userid, &sec_context)` (miscinit.c): the current
    /// user ID and security-context bitmask. Reads backend-local state;
    /// infallible.
    pub fn get_user_id_and_sec_context() -> (Oid, i32)
);

seam_core::seam!(
    /// `SetUserIdAndSecContext(userid, sec_context)` (miscinit.c): install a new
    /// current user ID and security-context bitmask. Writes backend-local state;
    /// infallible.
    pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32)
);

seam_core::seam!(
    /// `GetUserNameFromId(roleid, noerr)` (miscinit.c): the role name for
    /// `roleid`, copied into `mcx` (C: `pstrdup` in the current context). With
    /// `noerr = false` a missing role raises `ERRCODE_UNDEFINED_OBJECT`; with
    /// `noerr = true` it is `Ok(None)`. `Err` includes OOM and syscache errors.
    pub fn get_user_name_from_id<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        noerr: bool,
    ) -> PgResult<Option<PgString<'mcx>>>
);

seam_core::seam!(
    /// `InitPostmasterChild()` (`miscinit.c`): initialization common to all
    /// postmaster children — detangle the child from the postmaster (signal
    /// handling, process group, postmaster-death watch, etc.). Failure paths
    /// `elog/ereport(FATAL)`.
    pub fn init_postmaster_child() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `GetUserId()` (miscinit.c): the current effective user id. Pure global
    /// read (asserts validity in C); cannot `ereport`.
    pub fn get_user_id() -> Oid
);

// ---- critical-section / interrupt brackets + superuser check ----
//
// These are not miscinit.c's own functions (`START_CRIT_SECTION` etc. are
// miscadmin.h macros over globals.c counters; `superuser_arg` is superuser.c).
// They were declared here by an earlier consumer (twophase); miscinit installs
// them by delegating to their real owners until those owners land here, rather
// than break the existing call sites. New consumers should prefer the owners'
// own seam crates.

seam_core::seam!(
    /// `START_CRIT_SECTION()` (miscadmin.h) — `CritSectionCount++`.
    pub fn start_crit_section()
);
seam_core::seam!(
    /// `END_CRIT_SECTION()` (miscadmin.h) — `CritSectionCount--`.
    pub fn end_crit_section()
);
seam_core::seam!(
    /// `HOLD_INTERRUPTS()` (miscadmin.h) — `InterruptHoldoffCount++`.
    pub fn hold_interrupts()
);
seam_core::seam!(
    /// `RESUME_INTERRUPTS()` (miscadmin.h) — `InterruptHoldoffCount--`.
    pub fn resume_interrupts()
);
seam_core::seam!(
    /// `superuser_arg(roleid)` (superuser.c) — true if `roleid` has superuser
    /// privilege. Reads the catalog cache, so `Err` carries a lookup failure.
    pub fn superuser_arg(roleid: types_core::Oid) -> types_error::PgResult<bool>
);
