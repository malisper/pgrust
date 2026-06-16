//! Seam declarations for the `backend-utils-init-miscinit` unit
//! (`utils/init/miscinit.c`).
//!
//! The owning unit installs these from its `init_seams()`; until then a call
//! panics loudly. Only `miscinit.c`'s own functions are declared here — outward
//! calls miscinit makes (syscache, guc, superuser, ...) live in their owners'
//! seam crates.

#![allow(non_snake_case)]

extern crate alloc;
use alloc::string::String;

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
    /// `SwitchToSharedLatch()` (miscinit.c) — repoint `MyLatch` from the
    /// process-local latch to this backend's shared `&MyProc->procLatch`
    /// (called by `InitProcess`/`InitAuxiliaryProcess` after `OwnLatch`).
    /// Touches the FeBe wait set, which can `ereport(ERROR)`, hence `PgResult`.
    pub fn switch_to_shared_latch() -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SwitchBackToLocalLatch()` (miscinit.c) — repoint `MyLatch` back to the
    /// process-local latch (called by `ProcKill`/`AuxiliaryProcKill`).
    pub fn switch_back_to_local_latch() -> types_error::PgResult<()>
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
    /// `IsInitProcessingMode()` (miscadmin.h): `Mode == InitProcessing`.
    /// A plain global read — infallible.
    pub fn is_init_processing_mode() -> bool
);

seam_core::seam!(
    /// `InNoForceRLSOperation()` (miscinit.c): `SecurityRestrictionContext &
    /// SECURITY_NOFORCE_RLS`. A plain backend-local global read — infallible.
    pub fn in_no_force_rls_operation() -> bool
);

seam_core::seam!(
    /// `GetUserIdAndSecContext(&userid, &sec_context)` (miscinit.c): the
    /// current user ID and security-context bitmask. Reads backend-local
    /// state; infallible.
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
    /// Set the `DatabasePath` global (globals.c, owned via miscinit) to `path`.
    /// `ProcessCommittedInvalidationMessages` uses this during recovery to set
    /// `DatabasePath` directly (the comment in inval.c calls it "a quick hack")
    /// rather than [`set_database_path_once`] (`SetDatabasePath`), which is
    /// one-shot for normal backends.
    pub fn set_database_path(path: &str)
);

seam_core::seam!(
    /// Clear the `DatabasePath` global back to NULL (pairs with
    /// [`set_database_path`] in the recovery hack of
    /// `ProcessCommittedInvalidationMessages`).
    pub fn clear_database_path()
);

seam_core::seam!(
    /// Read the `DatabasePath` global (globals.c, owned via miscinit). Returns
    /// `None` when it is still NULL (no database selected yet — the C
    /// `DatabasePath != NULL` test relcache's init-file paths gate on),
    /// otherwise the owned path string.
    pub fn get_database_path() -> Option<String>
);

seam_core::seam!(
    /// `GetBackendTypeDesc(backendType)` (miscinit.c): the human-readable
    /// process-type description string for `backendType` (a static table
    /// lookup; the C returns a `const char *` into static text). Infallible.
    pub fn get_backend_type_desc(backend_type: types_core::init::BackendType) -> &'static str
);

// ---- critical-section / interrupt brackets + superuser check (miscadmin.h) ----

seam_core::seam!(
    /// `START_CRIT_SECTION()` (miscadmin.h) — `CritSectionCount++`.
    pub fn start_crit_section()
);

seam_core::seam!(
    /// `END_CRIT_SECTION()` (miscadmin.h) — `CritSectionCount--`.
    pub fn end_crit_section()
);

seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()` (miscadmin.h) — service any pending interrupt
    /// (query cancel, termination, recovery conflict). `Err` carries the
    /// `ProcessInterrupts` `ereport(ERROR/FATAL)`.
    pub fn check_for_interrupts() -> types_error::PgResult<()>
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

// ---- bootstrap-mode backend startup (miscinit.c) ----

seam_core::seam!(
    /// `InitStandaloneProcess(argv0)` (miscinit.c): set up the fake-shared
    /// state a standalone (non-postmaster) backend needs — `MyProcPid`,
    /// `MyStartTime`, shared-memory disposition, fake `LocalProcessControl`.
    /// `elog(FATAL)`s if the executable path cannot be located
    /// (`find_my_exec` failure), so `Err` carries that failure.
    pub fn init_standalone_process(argv0: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `checkDataDir()` (miscinit.c): validate the chosen data directory,
    /// `ereport(FATAL)` if missing or wrong permissions.
    pub fn check_data_dir() -> PgResult<()>
);

seam_core::seam!(
    /// `SetDataDir(dir)` (miscinit.c:440): set the `DataDir` global to the
    /// absolute form of `dir` (via `make_absolute_path`). `Err` carries the
    /// `make_absolute_path` OOM/FATAL surface.
    pub fn set_data_dir(dir: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `make_absolute_path(path)` (`src/port/path.c`): if `path` is relative,
    /// prepend the current working directory; otherwise return it unchanged. The
    /// owner (port/path.c) declares it in `backend-port-path-seams`; this is the
    /// miscinit-side re-export so consumers that already depend on miscinit-seams
    /// reach it without a separate dep. `Err` carries the OOM/`getcwd` FATAL.
    pub fn make_absolute_path(path: &str) -> PgResult<String>
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

seam_core::seam!(
    /// `IgnoreSystemIndexes` getter (the miscinit.c-owned global): whether
    /// system-table scans must skip their indexes (set during bootstrap / by
    /// the `ignore_system_indexes` GUC). genam's `systable_beginscan` reads it
    /// to decide the index-vs-heap path. Backend-local read; infallible.
    pub fn get_ignore_system_indexes() -> bool
);

seam_core::seam!(
    /// `bool has_rolreplication(Oid roleid)` (`miscinit.c:739`) — whether the
    /// role has the REPLICATION attribute. Superusers bypass the check; the
    /// non-superuser path does an `AUTHOID` syscache lookup, so it takes an
    /// `Mcx` and returns `PgResult` (the lookup / `superuser_arg` can
    /// `ereport(ERROR)`).
    pub fn has_rolreplication(mcx: mcx::Mcx<'_>, roleid: types_core::Oid) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `BackendType MyBackendType` (globals.c) — this process's backend type.
    pub fn my_backend_type() -> types_core::init::BackendType
);

seam_core::seam!(
    /// `bool IsBinaryUpgrade` (globals.c) — running a `pg_upgrade` binary
    /// upgrade.
    pub fn is_binary_upgrade() -> bool
);

seam_core::seam!(
    /// `InSecurityRestrictedOperation()` (miscinit.c) — true while a
    /// SECURITY_RESTRICTED_OPERATION context is in effect (e.g. inside an
    /// index expression). Pure read of `SecurityRestrictionContext`.
    pub fn in_security_restricted_operation() -> bool
);

seam_core::seam!(
    /// `InLocalUserIdChange()` (miscinit.c) — true while a SECURITY DEFINER
    /// local-userid context is in effect. Read by the GUC permission switch
    /// (`set_config_with_handle`, guc.c) for `GUC_NOT_WHILE_SEC_REST`.
    pub fn in_local_user_id_change() -> bool
);

seam_core::seam!(
    /// `superuser()` (superuser.c) — true if the *current* user
    /// (`GetUserId()`) has superuser privilege. Used by
    /// `fmgr_security_definer` to pick `PGC_SUSET` vs `PGC_USERSET` when
    /// applying a function's `proconfig` SET items. Equals
    /// `superuser_arg(GetUserId())`; reads the catalog cache, so it takes an
    /// `Mcx` and returns `PgResult` (the syscache read can `ereport(ERROR)`).
    pub fn superuser(mcx: mcx::Mcx<'_>) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `MyBackendType = B_WAL_SUMMARIZER` — set this backend's type to the WAL
    /// summarizer (globals.c stores `MyBackendType`; the WAL summarizer sets
    /// it before `AuxiliaryProcessMainCommon`). Plain backend-local write.
    pub fn set_my_backend_type_wal_summarizer()
);

seam_core::seam!(
    /// `AmWalSummarizerProcess()` (miscadmin.h): `MyBackendType ==
    /// B_WAL_SUMMARIZER`. Pure backend-local read.
    pub fn am_wal_summarizer_process() -> bool
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
        mcx: Mcx<'_>,
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
    )
);

seam_core::seam!(
    /// `ValidatePgVersion(path)` (miscinit.c): verify the database
    /// directory's PG_VERSION matches the server. `Err` carries its
    /// `ereport(FATAL)` surface.
    pub fn validate_pg_version(path: &str) -> types_error::PgResult<()>
);

seam_core::seam!(
    /// `SetDatabasePath(path)` (miscinit.c): record the database directory
    /// path globally, copying `path` into `TopMemoryContext`. This is the
    /// one-shot setter normal backends call once during `InitPostgres`; it is
    /// distinct from [`set_database_path`]/[`clear_database_path`], which are
    /// the inval.c recovery quick-hack that pokes `DatabasePath` directly.
    /// `Err` carries its OOM surface (the `MemoryContextStrdup`).
    pub fn set_database_path_once(path: &str)
);

seam_core::seam!(
    /// `process_session_preload_libraries()` (miscinit.c): load the libraries
    /// named by `session_preload_libraries`/`local_preload_libraries`. `Err`
    /// carries the loader's `ereport` surface.
    pub fn process_session_preload_libraries(mcx: Mcx<'_>) -> types_error::PgResult<()>
);

// ---------------------------------------------------------------------------
// Worker-bootstrap group used by the slot-sync worker entry point
// (`ReplSlotSyncWorkerMain`, slotsync.c). These mirror the auxiliary/background
// worker startup sequence; their true owners (proc.c, ps_status.c, postinit.c,
// interrupt.c, globals.c) are not yet ported, so a call panics loudly until
// installed.
// ---------------------------------------------------------------------------

seam_core::seam!(
    /// `MyBackendType = B_SLOTSYNC_WORKER` (miscadmin.h / globals.c).
    pub fn set_my_backend_type_slotsync() -> PgResult<()>
);

seam_core::seam!(
    /// `AmLogicalSlotSyncWorkerProcess()` (miscadmin.h): is this process the
    /// dedicated slot-sync worker (`B_SLOTSYNC_WORKER`)?
    pub fn am_logical_slot_sync_worker_process() -> bool
);

seam_core::seam!(
    /// `MyProcPid` (globals.c): this backend's process id.
    pub fn my_proc_pid() -> i32
);

seam_core::seam!(
    /// `init_ps_display("")` (ps_status.c): set up the process-title display.
    pub fn init_ps_display() -> PgResult<()>
);

seam_core::seam!(
    /// `InitProcess()` (proc.c): set up the PGPROC entry for this process.
    pub fn init_process() -> PgResult<()>
);

seam_core::seam!(
    /// `BaseInit()` (postinit.c): early per-backend subsystem initialization.
    pub fn base_init() -> PgResult<()>
);

seam_core::seam!(
    /// The slot-sync worker's signal-handler setup block (slotsync.c
    /// `ReplSlotSyncWorkerMain`): `pqsignal(...)`, `procsignal_sigusr1_handler`,
    /// etc. installed before unblocking signals.
    pub fn setup_signal_handlers() -> PgResult<()>
);

seam_core::seam!(
    /// `InitializeTimeouts()` (timeout.c): register the standard timeout
    /// handlers for this process.
    pub fn initialize_timeouts() -> PgResult<()>
);

seam_core::seam!(
    /// `BackgroundWorkerUnblockSignals()` / `sigprocmask(SIG_SETMASK, &UnBlockSig,
    /// NULL)` (slotsync.c): unblock signals once handlers are installed.
    pub fn unblock_signals() -> PgResult<()>
);

seam_core::seam!(
    /// `InitPostgres(dbname, InvalidOid, ..., NULL)` (postinit.c): bind this
    /// backend to the given database so `walrcv_exec` catalog queries can run.
    pub fn init_postgres(dbname: &str) -> PgResult<()>
);

seam_core::seam!(
    /// `MyBackendType = B_CHECKPOINTER` — set this backend's type to the
    /// checkpointer (globals.c stores `MyBackendType`; the checkpointer sets it
    /// before `AuxiliaryProcessMainCommon`). Plain backend-local write.
    pub fn set_my_backend_type_checkpointer()
);

seam_core::seam!(
    /// `MyBackendType = B_WAL_WRITER` — set this backend's type to the
    /// walwriter (globals.c stores `MyBackendType`; the walwriter sets it
    /// before `AuxiliaryProcessMainCommon`). Plain backend-local write.
    pub fn set_my_backend_type_wal_writer()
);

seam_core::seam!(
    /// `AmCheckpointerProcess()` (miscadmin.h): `MyBackendType ==
    /// B_CHECKPOINTER`. Pure backend-local read.
    pub fn am_checkpointer_process() -> bool
);

seam_core::seam!(
    /// `MyBackendType = B_BG_WRITER` — set this backend's type to the background
    /// writer (globals.c stores `MyBackendType`; the bgwriter sets it before
    /// `AuxiliaryProcessMainCommon`). Plain backend-local write.
    pub fn set_my_backend_type_bg_writer()
);

seam_core::seam!(
    /// `CritSectionCount > 0` (miscadmin.h `START_CRIT_SECTION` counter) — true
    /// when in a critical section. `CompactCheckpointerRequestQueue` checks this
    /// to avoid allocating inside one. Pure backend-local read.
    pub fn in_critical_section() -> bool
);
