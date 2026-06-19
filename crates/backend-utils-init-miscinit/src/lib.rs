//! Port of PostgreSQL's `src/backend/utils/init/miscinit.c` — miscellaneous
//! initialization support: the processing-mode and backend-type globals
//! (`Mode`/`MyBackendType`), the user-id / security-restriction state machine,
//! the system-user string, parallel-worker `ClientConnectionInfo` serialization,
//! common postmaster-child / standalone process startup, the interlock
//! lock-file machinery, the `PG_VERSION` check, and the library-preload /
//! shmem-request hooks.
//!
//! The C file keeps its state in process globals; a backend maps to one thread
//! here, so every one of those globals is a `thread_local!` (backend-private:
//! inherited at fork, diverging via SET / session state).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::cell::{Cell, RefCell};

use mcx::{Mcx, MemoryContext, PgString};
use types_core::catalog::BOOTSTRAP_SUPERUSERID;
use types_core::{
    BackendType, ProcessingMode, UserAuth, InvalidOid, Oid, SECURITY_LOCAL_USERID_CHANGE,
    SECURITY_NOFORCE_RLS, SECURITY_RESTRICTED_OPERATION, uaReject,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION, ERRCODE_TOO_MANY_CONNECTIONS,
    ERRCODE_UNDEFINED_OBJECT, ERROR, FATAL,
};

mod boot_paths;
mod fmgr_builtins;
mod lockfile;
mod process;
mod startup_paths;

pub use lockfile::{
    create_data_dir_lock_file, create_lock_file, create_socket_lock_file,
    touch_socket_lock_files, unlink_lock_files, AddToDataDirLockFile, RecheckDataDirLockFile,
};
pub use process::{
    ChangeToDataDir, InitPostmasterChild, InitProcessLocalLatch, InitStandaloneProcess,
    SwitchBackToLocalLatch, SwitchToSharedLatch, ValidatePgVersion, checkDataDir,
};

/// `src/backend/utils/init/miscinit.c` — the `errloc` filename for errors
/// raised here (matches the C `__FILE__`).
pub(crate) const MISCINIT_C: &str = "src/backend/utils/init/miscinit.c";

/// `PG_VERSION` (`src/include/pg_config.h`) — the server major-version string.
pub(crate) const PG_VERSION: &str = "18.3";

// ----------------------------------------------------------------
//  backend-private globals (thread_local; C file-scope statics)
// ----------------------------------------------------------------

thread_local! {
    /// `ProcessingMode Mode = InitProcessing;`
    static MODE: Cell<ProcessingMode> = const { Cell::new(ProcessingMode::InitProcessing) };
    /// `bool IgnoreSystemIndexes = false;`
    static IGNORE_SYSTEM_INDEXES: Cell<bool> = const { Cell::new(false) };

    /// `static Oid AuthenticatedUserId = InvalidOid;`
    static AUTHENTICATED_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    /// `static Oid SessionUserId = InvalidOid;`
    static SESSION_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    /// `static Oid OuterUserId = InvalidOid;`
    static OUTER_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    /// `static Oid CurrentUserId = InvalidOid;`
    static CURRENT_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    /// `static const char *SystemUser = NULL;`
    static SYSTEM_USER: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `static bool SessionUserIsSuperuser = false;`
    static SESSION_USER_IS_SUPERUSER: Cell<bool> = const { Cell::new(false) };
    /// `static int SecurityRestrictionContext = 0;`
    static SECURITY_RESTRICTION_CONTEXT: Cell<i32> = const { Cell::new(0) };
    /// `static bool SetRoleIsActive = false;`
    static SET_ROLE_IS_ACTIVE: Cell<bool> = const { Cell::new(false) };

    /// `ClientConnectionInfo MyClientConnectionInfo;`
    static MY_CLIENT_CONNECTION_INFO: RefCell<ClientConnectionInfo> =
        const { RefCell::new(ClientConnectionInfo::new()) };

    /// `char *session_preload_libraries_string = NULL;`
    static SESSION_PRELOAD_LIBRARIES: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *shared_preload_libraries_string = NULL;`
    static SHARED_PRELOAD_LIBRARIES: RefCell<Option<String>> = const { RefCell::new(None) };
    /// `char *local_preload_libraries_string = NULL;`
    static LOCAL_PRELOAD_LIBRARIES: RefCell<Option<String>> = const { RefCell::new(None) };

    /// `bool process_shared_preload_libraries_in_progress = false;`
    static SPL_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };
    /// `bool process_shared_preload_libraries_done = false;`
    static SPL_DONE: Cell<bool> = const { Cell::new(false) };
    /// `bool process_shmem_requests_in_progress = false;`
    static SHMEM_REQ_IN_PROGRESS: Cell<bool> = const { Cell::new(false) };

    /// `shmem_request_hook_type shmem_request_hook = NULL;` (miscinit.c:1841) —
    /// the optional shared-memory request hook a preloaded module registers in
    /// its `_PG_init`. NULL (None) unless a loaded module sets it.
    static SHMEM_REQUEST_HOOK: Cell<Option<fn() -> PgResult<()>>> = const { Cell::new(None) };
}

/// `shmem_request_hook != NULL` (miscinit.c) — whether a module has registered a
/// shared-memory request hook.
pub fn shmem_request_hook_present() -> bool {
    SHMEM_REQUEST_HOOK.with(|c| c.get().is_some())
}

/// Invoke the registered `shmem_request_hook()` (miscinit.c). Panics if called
/// with no hook registered (the caller guards with [`shmem_request_hook_present`],
/// mirroring C's `if (shmem_request_hook)`).
pub fn shmem_request_hook() -> PgResult<()> {
    match SHMEM_REQUEST_HOOK.with(Cell::get) {
        Some(hook) => hook(),
        None => panic!("shmem_request_hook() called with no hook registered"),
    }
}

/// Register a module's `shmem_request_hook` (the `shmem_request_hook = my_hook`
/// assignment a preloaded module makes in `_PG_init`).
pub fn set_shmem_request_hook(hook: fn() -> PgResult<()>) {
    SHMEM_REQUEST_HOOK.with(|c| c.set(Some(hook)));
}

/// Owned form of `ClientConnectionInfo` (`libpq/libpq-be.h`): backend-private
/// state synced to parallel workers. `authn_id` is C `char *` charged to
/// `TopMemoryContext`; here the backend owns it.
#[derive(Clone, Debug)]
pub struct ClientConnectionInfo {
    pub authn_id: Option<String>,
    pub auth_method: UserAuth,
}

impl ClientConnectionInfo {
    const fn new() -> Self {
        Self {
            authn_id: None,
            auth_method: uaReject,
        }
    }
}

// ----------------------------------------------------------------
//  processing-mode / backend-type globals
// ----------------------------------------------------------------

/// `GetProcessingMode()` (`miscadmin.h`).
pub fn GetProcessingMode() -> ProcessingMode {
    MODE.with(Cell::get)
}

/// `SetProcessingMode(mode)` (`miscadmin.h`): the C macro `Assert`s the value
/// is one of the three valid modes, then assigns.
pub fn SetProcessingMode(mode: ProcessingMode) {
    debug_assert!(matches!(
        mode,
        ProcessingMode::BootstrapProcessing
            | ProcessingMode::InitProcessing
            | ProcessingMode::NormalProcessing
    ));
    MODE.with(|c| c.set(mode));
}

/// `IsBootstrapProcessingMode()`.
pub fn IsBootstrapProcessingMode() -> bool {
    GetProcessingMode() == ProcessingMode::BootstrapProcessing
}

/// `IsInitProcessingMode()`.
pub fn IsInitProcessingMode() -> bool {
    GetProcessingMode() == ProcessingMode::InitProcessing
}

/// `IsNormalProcessingMode()`.
pub fn IsNormalProcessingMode() -> bool {
    GetProcessingMode() == ProcessingMode::NormalProcessing
}

/// `GetMyBackendType()` — read `MyBackendType`. The single canonical
/// `MyBackendType` global lives in globals.c (backend-utils-init-small); read it
/// through that owner's seam so every consumer sees the same value.
pub fn GetMyBackendType() -> BackendType {
    backend_utils_init_small_seams::my_backend_type::call()
}

/// `MyBackendType = ...` — assign the backend-type global (globals.c, owned by
/// backend-utils-init-small) through its setter seam.
pub fn SetMyBackendType(backend_type: BackendType) {
    backend_utils_init_small_seams::set_my_backend_type::call(backend_type);
}

/// `IgnoreSystemIndexes` getter (`miscinit.c:81`).
pub fn IgnoreSystemIndexes() -> bool {
    IGNORE_SYSTEM_INDEXES.with(Cell::get)
}

/// `IgnoreSystemIndexes` setter.
pub fn SetIgnoreSystemIndexes(ignore: bool) {
    IGNORE_SYSTEM_INDEXES.with(|c| c.set(ignore));
}

/// `AmRegularBackendProcess()` (`miscadmin.h`): true for a `B_BACKEND`.
fn am_regular_backend_process() -> bool {
    GetMyBackendType() == BackendType::Backend
}

/// `GetBackendTypeDesc(backendType)` (`miscinit.c:263`) — human-readable name.
///
/// The C marks each string with `gettext_noop` so callers can `_()`-localize;
/// translation is a project-wide deferral, so the untranslated English string
/// is returned. The default arm matches C's `"unknown process type"`.
pub fn GetBackendTypeDesc(backend_type: BackendType) -> &'static str {
    match backend_type {
        BackendType::Invalid => "not initialized",
        BackendType::Archiver => "archiver",
        BackendType::AutovacLauncher => "autovacuum launcher",
        BackendType::AutovacWorker => "autovacuum worker",
        BackendType::Backend => "client backend",
        BackendType::DeadEndBackend => "dead-end client backend",
        BackendType::BgWorker => "background worker",
        BackendType::BgWriter => "background writer",
        BackendType::Checkpointer => "checkpointer",
        BackendType::IoWorker => "io worker",
        BackendType::Logger => "logger",
        BackendType::SlotsyncWorker => "slotsync worker",
        BackendType::StandaloneBackend => "standalone backend",
        BackendType::Startup => "startup",
        BackendType::WalReceiver => "walreceiver",
        BackendType::WalSender => "walsender",
        BackendType::WalSummarizer => "walsummarizer",
        BackendType::WalWriter => "walwriter",
    }
}

// ----------------------------------------------------------------
//  database path / name support
// ----------------------------------------------------------------

/// `SetDatabasePath(path)` (`miscinit.c:333`) — set `DatabasePath` (held in
/// globals.c). C `Assert`s it happens only once per process.
pub fn SetDatabasePath(path: &str) {
    debug_assert!(backend_utils_init_small::globals::DatabasePath().is_none());
    backend_utils_init_small::globals::SetDatabasePath(Some(path.to_owned()));
}

/// `SetDataDir(dir)` (`miscinit.c:439`) — set `DataDir`, made absolute.
/// `make_absolute_path` (path.c) is the unported owner's helper.
pub fn SetDataDir(dir: &str) -> PgResult<()> {
    let new = backend_port_path_seams::make_absolute_path::call(dir)?;
    backend_utils_init_small::globals::SetDataDir(Some(new));
    Ok(())
}

// ----------------------------------------------------------------
//  User ID state
// ----------------------------------------------------------------

/// `GetUserId()` (`miscinit.c:519`) — the current effective user ID.
pub fn GetUserId() -> Oid {
    debug_assert!(CURRENT_USER_ID.with(Cell::get) != InvalidOid);
    CURRENT_USER_ID.with(Cell::get)
}

/// `GetOuterUserId()` (`miscinit.c:530`).
pub fn GetOuterUserId() -> Oid {
    debug_assert!(OUTER_USER_ID.with(Cell::get) != InvalidOid);
    OUTER_USER_ID.with(Cell::get)
}

/// `SetOuterUserId(userid, is_superuser)` (`miscinit.c:538`, `static`).
fn SetOuterUserId(userid: Oid, is_superuser: bool) -> PgResult<()> {
    debug_assert_eq!(SECURITY_RESTRICTION_CONTEXT.with(Cell::get), 0);
    debug_assert!(userid != InvalidOid);
    OUTER_USER_ID.with(|c| c.set(userid));
    // We force the effective user ID to match, too.
    CURRENT_USER_ID.with(|c| c.set(userid));
    // Also update the is_superuser GUC to match OuterUserId's property.
    backend_utils_misc_guc_seams::set_config_option_internal_dynamic_default::call(
        "is_superuser",
        if is_superuser { "on" } else { "off" },
    )
}

/// `GetSessionUserId()` (`miscinit.c:558`).
pub fn GetSessionUserId() -> Oid {
    debug_assert!(SESSION_USER_ID.with(Cell::get) != InvalidOid);
    SESSION_USER_ID.with(Cell::get)
}

/// `GetSessionUserIsSuperuser()` (`miscinit.c:565`).
pub fn GetSessionUserIsSuperuser() -> bool {
    debug_assert!(SESSION_USER_ID.with(Cell::get) != InvalidOid);
    SESSION_USER_IS_SUPERUSER.with(Cell::get)
}

/// `SetSessionUserId(userid, is_superuser)` (`miscinit.c:572`, `static`).
fn SetSessionUserId(userid: Oid, is_superuser: bool) {
    debug_assert_eq!(SECURITY_RESTRICTION_CONTEXT.with(Cell::get), 0);
    debug_assert!(userid != InvalidOid);
    SESSION_USER_ID.with(|c| c.set(userid));
    SESSION_USER_IS_SUPERUSER.with(|c| c.set(is_superuser));
}

/// `GetSystemUser()` (`miscinit.c:585`) — the `auth_method:authn_id` string.
pub fn GetSystemUser() -> Option<String> {
    SYSTEM_USER.with(|c| c.borrow().clone())
}

/// `GetAuthenticatedUserId()` (`miscinit.c:595`).
pub fn GetAuthenticatedUserId() -> Oid {
    debug_assert!(AUTHENTICATED_USER_ID.with(Cell::get) != InvalidOid);
    AUTHENTICATED_USER_ID.with(Cell::get)
}

/// `SetAuthenticatedUserId(userid)` (`miscinit.c:602`) — set the id and mark
/// the PGPROC entry (`MyProc->roleId = userid`).
pub fn SetAuthenticatedUserId(userid: Oid) -> PgResult<()> {
    debug_assert!(userid != InvalidOid);
    // call only once
    debug_assert!(AUTHENTICATED_USER_ID.with(Cell::get) == InvalidOid);
    AUTHENTICATED_USER_ID.with(|c| c.set(userid));
    // Also mark our PGPROC entry with the authenticated user id (atomic store).
    backend_storage_lmgr_proc_seams::set_my_proc_role_id::call(userid);
    Ok(())
}

/// `GetUserIdAndSecContext(&userid, &sec_context)` (`miscinit.c:662`) —
/// returned as a tuple. Never asserts; never errors.
pub fn GetUserIdAndSecContext() -> (Oid, i32) {
    (
        CURRENT_USER_ID.with(Cell::get),
        SECURITY_RESTRICTION_CONTEXT.with(Cell::get),
    )
}

/// `SetUserIdAndSecContext(userid, sec_context)` (`miscinit.c:669`).
pub fn SetUserIdAndSecContext(userid: Oid, sec_context: i32) {
    CURRENT_USER_ID.with(|c| c.set(userid));
    SECURITY_RESTRICTION_CONTEXT.with(|c| c.set(sec_context));
}

/// `InLocalUserIdChange()` (`miscinit.c:680`).
pub fn InLocalUserIdChange() -> bool {
    SECURITY_RESTRICTION_CONTEXT.with(Cell::get) & SECURITY_LOCAL_USERID_CHANGE != 0
}

/// `InSecurityRestrictedOperation()` (`miscinit.c:689`).
pub fn InSecurityRestrictedOperation() -> bool {
    SECURITY_RESTRICTION_CONTEXT.with(Cell::get) & SECURITY_RESTRICTED_OPERATION != 0
}

/// `InNoForceRLSOperation()` (`miscinit.c:698`).
pub fn InNoForceRLSOperation() -> bool {
    SECURITY_RESTRICTION_CONTEXT.with(Cell::get) & SECURITY_NOFORCE_RLS != 0
}

/// `GetUserIdAndContext(&userid, &sec_def_context)` (`miscinit.c:711`) —
/// obsolete pljava-compat accessor, returned as a tuple.
pub fn GetUserIdAndContext() -> (Oid, bool) {
    (CURRENT_USER_ID.with(Cell::get), InLocalUserIdChange())
}

/// `SetUserIdAndContext(userid, sec_def_context)` (`miscinit.c:718`) — obsolete
/// pljava-compat mutator. Throws the same error `SET ROLE` would inside a
/// security-restricted operation.
pub fn SetUserIdAndContext(userid: Oid, sec_def_context: bool) -> PgResult<()> {
    // We throw the same error SET ROLE would.
    if InSecurityRestrictedOperation() {
        return Err(PgError::new(
            ERROR,
            "cannot set parameter \"role\" within security-restricted operation",
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE));
    }
    CURRENT_USER_ID.with(|c| c.set(userid));
    SECURITY_RESTRICTION_CONTEXT.with(|c| {
        let mut v = c.get();
        if sec_def_context {
            v |= SECURITY_LOCAL_USERID_CHANGE;
        } else {
            v &= !SECURITY_LOCAL_USERID_CHANGE;
        }
        c.set(v);
    });
    Ok(())
}

/// `has_rolreplication(roleid)` (`miscinit.c:738`) — does the role have explicit
/// REPLICATION privilege? Superusers bypass the check.
pub fn has_rolreplication(mcx: Mcx<'_>, roleid: Oid) -> PgResult<bool> {
    // Superusers bypass all permission checking.
    if superuser_arg(roleid)? {
        return Ok(true);
    }
    Ok(
        backend_utils_cache_syscache_seams::lookup_authid_by_oid::call(mcx, roleid)?
            .map(|r| r.rolreplication)
            .unwrap_or(false),
    )
}

/// `superuser_arg(roleid)` (`superuser.c`) — does the role have superuser
/// privileges? An external owner (superuser.c) is not ported; routed through
/// its seam.
fn superuser_arg(roleid: Oid) -> PgResult<bool> {
    backend_utils_misc_superuser_seams::superuser_arg::call(roleid)
}

/// `superuser()` (`superuser.c`) — does the *current* user (`GetUserId()`) have
/// superuser privileges? `superuser() == superuser_arg(GetUserId())`. The
/// catalog read happens inside `superuser_arg`'s owner; the `Mcx` here mirrors
/// the C catalog-lookup surface.
fn superuser(_mcx: Mcx<'_>) -> PgResult<bool> {
    superuser_arg(GetUserId())
}

/// `InitializeSessionUserId(rolename, roleid, bypass_login_check)`
/// (`miscinit.c:760`) — initialize the user identity during normal backend
/// startup.
pub fn InitializeSessionUserId(
    mcx: Mcx<'_>,
    rolename: Option<&str>,
    roleid: Oid,
    bypass_login_check: bool,
) -> PgResult<()> {
    // In a parallel worker, ParallelWorkerMain already set our output variables
    // and we don't enforce rolcanlogin/rolconnlimit, nor scan the catalogs.
    if backend_access_transam_parallel::initializing_parallel_worker() {
        debug_assert!(bypass_login_check);
        return Ok(());
    }

    // Don't do scans if we're bootstrapping.
    debug_assert!(!IsBootstrapProcessingMode());

    // Make sure syscache entries are flushed for recent catalog changes, so we
    // can find roles created on-the-fly during authentication.
    backend_utils_cache_inval_seams::accept_invalidation_messages::call()?;

    // Look up the role, either by name if that's given or by OID if not.
    let role = if let Some(name) = rolename {
        backend_utils_cache_syscache_seams::lookup_authid_by_name::call(mcx, name)?.ok_or_else(
            || {
                PgError::new(FATAL, format!("role \"{name}\" does not exist"))
                    .with_sqlstate(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            },
        )?
    } else {
        backend_utils_cache_syscache_seams::lookup_authid_by_oid::call(mcx, roleid)?.ok_or_else(
            || {
                PgError::new(
                    FATAL,
                    format!("role with OID {roleid} does not exist"),
                )
                .with_sqlstate(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
            },
        )?
    };

    let resolved_roleid = role.oid;
    let rname = role.rolname.as_str().to_owned();
    let is_superuser = role.rolsuper;

    SetAuthenticatedUserId(resolved_roleid)?;

    // Set SessionUserId and related variables, including "role", via the GUC
    // mechanisms (PGC_BACKEND, PGC_S_OVERRIDE).
    backend_utils_misc_guc_seams::set_config_option_backend_override::call(
        "session_authorization",
        &rname,
    )?;

    // These next checks are not enforced when in standalone mode.
    if backend_utils_init_small::globals::IsUnderPostmaster() {
        // Is role allowed to login at all? (bypass_login_check overrides.)
        if !bypass_login_check && !role.rolcanlogin {
            return Err(PgError::new(
                FATAL,
                format!("role \"{rname}\" is not permitted to log in"),
            )
            .with_sqlstate(ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION));
        }

        // Check connection limit for this role. Enforced only for regular
        // backends, since other process types have their own PGPROC pools.
        if role.rolconnlimit >= 0
            && am_regular_backend_process()
            && !is_superuser
            && backend_storage_ipc_procarray_seams::count_user_backends::call(resolved_roleid)?
                > role.rolconnlimit
        {
            return Err(PgError::new(
                FATAL,
                format!("too many connections for role \"{rname}\""),
            )
            .with_sqlstate(ERRCODE_TOO_MANY_CONNECTIONS));
        }
    }

    Ok(())
}

/// `InitializeSessionUserIdStandalone()` (`miscinit.c:890`) — initialize user
/// identity during special backend startup (single-user, autovacuum, slotsync,
/// bgworkers).
pub fn InitializeSessionUserIdStandalone() -> PgResult<()> {
    // call only once
    debug_assert!(AUTHENTICATED_USER_ID.with(Cell::get) == InvalidOid);
    AUTHENTICATED_USER_ID.with(|c| c.set(BOOTSTRAP_SUPERUSERID));
    SetSessionAuthorization(BOOTSTRAP_SUPERUSERID, true)?;
    // We could do SetConfigOption("role"), but let's be consistent.
    SetCurrentRoleId(InvalidOid, false)
}

/// `InitializeSystemUser(authn_id, auth_method)` (`miscinit.c:924`) — build
/// `SystemUser` as `auth_method:authn_id`, stored in long-lived storage.
///
/// C builds the string with `psprintf` (transient current context), copies it
/// into `TopMemoryContext` with `MemoryContextStrdup`, then `pfree`s the working
/// buffer. The long-lived copy lives in process-lifetime storage; here that is
/// the backend-private `SYSTEM_USER` cell (an owned `String`, the
/// `MemoryContextStrdup(TopMemoryContext)` analog), so the construction is the
/// long-lived copy, not a transient palloc.
pub fn InitializeSystemUser(authn_id: &str, auth_method: &str) {
    // call only once; authn_id is non-NULL when this is called
    debug_assert!(SYSTEM_USER.with(|c| c.borrow().is_none()));
    let system_user = format!("{auth_method}:{authn_id}");
    SYSTEM_USER.with(|c| *c.borrow_mut() = Some(system_user));
}

/// `Datum system_user(PG_FUNCTION_ARGS)` (`miscinit.c:948`) — SQL `SYSTEM_USER`.
/// The fmgr/`Datum` glue is a project-wide deferral; the idiomatic form returns
/// the underlying owned string (or `None` for SQL NULL).
pub fn system_user() -> Option<String> {
    GetSystemUser()
}

/// `SetSessionAuthorization(userid, is_superuser)` (`miscinit.c:970`) — GUC
/// assign hook for `session_authorization`. Updates derived state only when
/// `!SetRoleIsActive`.
pub fn SetSessionAuthorization(userid: Oid, is_superuser: bool) -> PgResult<()> {
    SetSessionUserId(userid, is_superuser);
    if !SET_ROLE_IS_ACTIVE.with(Cell::get) {
        SetOuterUserId(userid, is_superuser)?;
    }
    Ok(())
}

/// `GetCurrentRoleId()` (`miscinit.c:985`) — the outer-level role ID, or
/// `InvalidOid` when the setting is logically `SET ROLE NONE`.
pub fn GetCurrentRoleId() -> Oid {
    if SET_ROLE_IS_ACTIVE.with(Cell::get) {
        OUTER_USER_ID.with(Cell::get)
    } else {
        InvalidOid
    }
}

/// `SetCurrentRoleId(roleid, is_superuser)` (`miscinit.c:1006`) — `SET ROLE`.
/// `InvalidOid` means `SET ROLE NONE`: revert to the session-user authorization.
pub fn SetCurrentRoleId(mut roleid: Oid, mut is_superuser: bool) -> PgResult<()> {
    if roleid == InvalidOid {
        // SET ROLE NONE.
        SET_ROLE_IS_ACTIVE.with(|c| c.set(false));

        // If SessionUserId hasn't been set yet, do nothing beyond updating
        // SetRoleIsActive; the eventual SetSessionAuthorization call updates
        // the derived state (needed during GUC initialization).
        if SESSION_USER_ID.with(Cell::get) == InvalidOid {
            return Ok(());
        }

        roleid = SESSION_USER_ID.with(Cell::get);
        is_superuser = SESSION_USER_IS_SUPERUSER.with(Cell::get);
    } else {
        SET_ROLE_IS_ACTIVE.with(|c| c.set(true));
    }

    SetOuterUserId(roleid, is_superuser)
}

/// `GetUserNameFromId(roleid, noerr)` (`miscinit.c:1038`) — role name for an
/// OID. Returns `None` for a nonexistent role when `noerr`, else errors.
pub fn GetUserNameFromId<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    noerr: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match backend_utils_cache_syscache_seams::lookup_authid_by_oid::call(mcx, roleid)? {
        Some(role) => Ok(Some(role.rolname)),
        None if noerr => Ok(None),
        None => Err(
            PgError::new(ERROR, format!("invalid role OID: {roleid}"))
                .with_sqlstate(ERRCODE_UNDEFINED_OBJECT),
        ),
    }
}

// ----------------------------------------------------------------
//  Client connection state shared with parallel workers
// ----------------------------------------------------------------

/// The serialized header is `SerializedClientConnectionInfo` =
/// `int32 authn_id_len` followed by `UserAuth auth_method` (`int` width).
const SERIALIZED_HEADER_LEN: usize = 8;

/// Install `MyClientConnectionInfo` (idiomatic replacement for assigning the
/// global struct directly).
pub fn set_client_connection_info(authn_id: Option<String>, auth_method: UserAuth) {
    MY_CLIENT_CONNECTION_INFO.with(|c| {
        let mut info = c.borrow_mut();
        info.authn_id = authn_id;
        info.auth_method = auth_method;
    });
}

/// Read a clone of `MyClientConnectionInfo`.
pub fn client_connection_info() -> ClientConnectionInfo {
    MY_CLIENT_CONNECTION_INFO.with(|c| c.borrow().clone())
}

/// `EstimateClientConnectionInfoSpace()` (`miscinit.c:1085`).
pub fn EstimateClientConnectionInfoSpace() -> usize {
    MY_CLIENT_CONNECTION_INFO.with(|c| {
        let info = c.borrow();
        let mut size = SERIALIZED_HEADER_LEN;
        if let Some(authn_id) = &info.authn_id {
            // strlen(authn_id) + 1 (NUL terminator)
            size += authn_id.len() + 1;
        }
        size
    })
}

/// `SerializeClientConnectionInfo(maxsize, start_address)` (`miscinit.c:1101`).
///
/// Layout: `int32 authn_id_len` (host order, `-1` if NULL), `UserAuth
/// auth_method` (host order), then, when present, the NUL-terminated `authn_id`.
/// C `Assert`s the buffer is large enough; here a too-small buffer is an error.
pub fn SerializeClientConnectionInfo(start_address: &mut [u8]) -> PgResult<()> {
    MY_CLIENT_CONNECTION_INFO.with(|c| {
        let info = c.borrow();

        let authn_id_len: i32 = match &info.authn_id {
            Some(authn_id) => authn_id.len() as i32,
            None => -1,
        };

        if start_address.len() < SERIALIZED_HEADER_LEN {
            return Err(PgError::error("client connection info buffer is too small"));
        }
        start_address[..4].copy_from_slice(&authn_id_len.to_ne_bytes());
        start_address[4..8].copy_from_slice(&info.auth_method.to_ne_bytes());

        // Copy authn_id (with NUL terminator) into the space after the struct.
        if authn_id_len >= 0 {
            let authn_id = info.authn_id.as_deref().unwrap_or("");
            let need = SERIALIZED_HEADER_LEN + authn_id.len() + 1;
            if start_address.len() < need {
                return Err(PgError::error("client connection info buffer is too small"));
            }
            let body = SERIALIZED_HEADER_LEN + authn_id.len();
            start_address[SERIALIZED_HEADER_LEN..body].copy_from_slice(authn_id.as_bytes());
            // include the NULL terminator to ease deserialization
            start_address[body] = 0;
        }
        Ok(())
    })
}

/// `RestoreClientConnectionInfo(conninfo)` (`miscinit.c:1134`).
pub fn RestoreClientConnectionInfo(conninfo: &[u8]) -> PgResult<()> {
    if conninfo.len() < SERIALIZED_HEADER_LEN {
        return Err(PgError::error("client connection info buffer is too small"));
    }

    let authn_id_len = i32::from_ne_bytes(conninfo[..4].try_into().unwrap());
    let auth_method = UserAuth::from_ne_bytes(conninfo[4..8].try_into().unwrap());

    let authn_id = if authn_id_len >= 0 {
        // The serialized form stores `strlen(authn_id)` bytes then a NUL.
        let len = authn_id_len as usize;
        let end = SERIALIZED_HEADER_LEN + len;
        if conninfo.len() < end + 1 {
            return Err(PgError::error("client connection info buffer is too small"));
        }
        let bytes = &conninfo[SERIALIZED_HEADER_LEN..end];
        let text = std::str::from_utf8(bytes)
            .map_err(|_| PgError::error("invalid serialized client authn_id"))?;
        Some(text.to_owned())
    } else {
        None
    };

    set_client_connection_info(authn_id, auth_method);
    Ok(())
}

// ----------------------------------------------------------------
//  Library preload support
// ----------------------------------------------------------------

/// Install the `session_preload_libraries` GUC string.
pub fn set_session_preload_libraries(libraries: Option<String>) {
    SESSION_PRELOAD_LIBRARIES.with(|c| *c.borrow_mut() = libraries);
}

/// Read the `session_preload_libraries_string` GUC backing store
/// (`*conf->variable` for the GUC engine).
pub fn get_session_preload_libraries() -> Option<String> {
    SESSION_PRELOAD_LIBRARIES.with(|c| c.borrow().clone())
}

/// Install the `shared_preload_libraries` GUC string.
pub fn set_shared_preload_libraries(libraries: Option<String>) {
    SHARED_PRELOAD_LIBRARIES.with(|c| *c.borrow_mut() = libraries);
}

/// Read the `shared_preload_libraries_string` GUC backing store
/// (`*conf->variable` for the GUC engine).
pub fn get_shared_preload_libraries() -> Option<String> {
    SHARED_PRELOAD_LIBRARIES.with(|c| c.borrow().clone())
}

/// Install the `local_preload_libraries` GUC string.
pub fn set_local_preload_libraries(libraries: Option<String>) {
    LOCAL_PRELOAD_LIBRARIES.with(|c| *c.borrow_mut() = libraries);
}

/// Read the `local_preload_libraries_string` GUC backing store
/// (`*conf->variable` for the GUC engine).
pub fn get_local_preload_libraries() -> Option<String> {
    LOCAL_PRELOAD_LIBRARIES.with(|c| c.borrow().clone())
}

/// `load_libraries(libraries, gucname, restricted)` (`miscinit.c:1850`, static)
/// — load the comma-separated library list. A list syntax error or load failure
/// is `ereport(LOG)` (non-fatal); `load_file` (dfmgr.c) is the unported owner.
fn load_libraries(mcx: Mcx<'_>, libraries: Option<&str>, gucname: &str, restricted: bool) -> PgResult<()> {
    let libraries = match libraries {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(()), // nothing to do
    };

    // Parse string into list of filename paths (SplitDirectoriesString).
    let elemlist = match backend_utils_adt_varlena_seams::split_directories_string::call(mcx, libraries)? {
        Some(list) => list,
        None => {
            // syntax error in list
            backend_utils_error::ereport(types_error::LOG)
                .errcode(types_error::ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("invalid list syntax in parameter \"{gucname}\""))
                .finish(types_error::ErrorLocation::new(MISCINIT_C, line!() as i32, "load_libraries"))?;
            return Ok(());
        }
    };

    for filename in elemlist {
        // If restricting, insert $libdir/plugins if not mentioned already.
        let restricted_name;
        let filename: &str = if restricted
            && backend_port_path_seams::first_dir_separator::call(&filename).is_none()
        {
            restricted_name = format!("$libdir/plugins/{filename}");
            &restricted_name
        } else {
            &filename
        };
        backend_utils_fmgr_dfmgr_seams::load_file::call(filename, restricted)?;
        // ereport(DEBUG1, "loaded library \"%s\"") — debug-level log.
    }
    Ok(())
}

/// `process_shared_preload_libraries()` (`miscinit.c:1902`).
pub fn process_shared_preload_libraries(mcx: Mcx<'_>) -> PgResult<()> {
    SPL_IN_PROGRESS.with(|c| c.set(true));
    let libraries = SHARED_PRELOAD_LIBRARIES.with(|c| c.borrow().clone());
    let result = load_libraries(mcx, libraries.as_deref(), "shared_preload_libraries", false);
    SPL_IN_PROGRESS.with(|c| c.set(false));
    SPL_DONE.with(|c| c.set(true));
    result
}

/// `process_session_preload_libraries()` (`miscinit.c:1916`).
pub fn process_session_preload_libraries(mcx: Mcx<'_>) -> PgResult<()> {
    let session = SESSION_PRELOAD_LIBRARIES.with(|c| c.borrow().clone());
    let local = LOCAL_PRELOAD_LIBRARIES.with(|c| c.borrow().clone());
    load_libraries(mcx, session.as_deref(), "session_preload_libraries", false)?;
    load_libraries(mcx, local.as_deref(), "local_preload_libraries", true)
}

/// `process_shmem_requests()` (`miscinit.c:1930`) — invoke the optional
/// `shmem_request_hook`, toggling the in-progress flag around the call.
pub fn process_shmem_requests() -> PgResult<()> {
    SHMEM_REQ_IN_PROGRESS.with(|c| c.set(true));
    let result = if backend_utils_fmgr_dfmgr_seams::shmem_request_hook_present::call() {
        backend_utils_fmgr_dfmgr_seams::shmem_request_hook::call()
    } else {
        Ok(())
    };
    SHMEM_REQ_IN_PROGRESS.with(|c| c.set(false));
    result
}

/// `process_shared_preload_libraries_in_progress` flag getter (`miscinit.c:1838`).
pub fn process_shared_preload_libraries_in_progress() -> bool {
    SPL_IN_PROGRESS.with(Cell::get)
}

/// `process_shared_preload_libraries_done` flag getter (`miscinit.c:1839`).
pub fn process_shared_preload_libraries_done() -> bool {
    SPL_DONE.with(Cell::get)
}

/// `process_shmem_requests_in_progress` flag getter (`miscinit.c:1842`).
pub fn process_shmem_requests_in_progress() -> bool {
    SHMEM_REQ_IN_PROGRESS.with(Cell::get)
}

/// `pg_bindtextdomain(domain)` (`miscinit.c:1939`) — `#ifdef ENABLE_NLS` is
/// inactive in this build, so the function is a compiled no-op. NLS/gettext is
/// a project-wide deferral.
pub fn pg_bindtextdomain(_domain: &str) {}

// ----------------------------------------------------------------
//  Seam installation
// ----------------------------------------------------------------

/// Install every seam declared in `backend-utils-init-miscinit-seams`.
pub fn init_seams() {
    use backend_utils_init_miscinit_seams as s;

    // Register this crate's SQL-callable builtins into the fmgr-core builtin
    // table (C: `fmgr_builtins[]`), so by-OID dispatch resolves them.
    fmgr_builtins::register_miscinit_builtins();

    s::create_socket_lock_file::set(|socketfile, am_postmaster, socket_dir| {
        create_socket_lock_file(socketfile, am_postmaster, socket_dir)
    });
    s::switch_to_shared_latch::set(crate::process::SwitchToSharedLatch);
    s::switch_back_to_local_latch::set(crate::process::SwitchBackToLocalLatch);
    s::process_shmem_requests_in_progress::set(process_shmem_requests_in_progress);

    // `process_shmem_requests()` lives in miscinit.c (this crate), not ipci.c;
    // install the single-user boot-driver seam (declared in
    // backend-tcop-postgres-seams) here, delegating to the real body.
    backend_tcop_postgres_seams::process_shmem_requests::set(process_shmem_requests);

    // `shmem_request_hook` (miscinit.c:1841) is owned here; the dfmgr-seams that
    // front the presence-check and invocation are installed by this crate.
    backend_utils_fmgr_dfmgr_seams::shmem_request_hook_present::set(shmem_request_hook_present);
    backend_utils_fmgr_dfmgr_seams::shmem_request_hook::set(shmem_request_hook);

    s::process_shared_preload_libraries_in_progress::set(
        process_shared_preload_libraries_in_progress,
    );
    s::process_shared_preload_libraries_done::set(process_shared_preload_libraries_done);
    s::is_bootstrap_processing_mode::set(IsBootstrapProcessingMode);
    s::is_init_processing_mode::set(IsInitProcessingMode);
    s::is_normal_processing_mode::set(IsNormalProcessingMode);
    s::get_user_id_and_sec_context::set(GetUserIdAndSecContext);
    s::set_user_id_and_sec_context::set(SetUserIdAndSecContext);
    s::get_user_name_from_id::set(|mcx, roleid, noerr| GetUserNameFromId(mcx, roleid, noerr));
    s::init_postmaster_child::set(InitPostmasterChild);
    s::get_user_id::set(GetUserId);
    // SetDataDir(dir) (miscinit.c:440) and the port/path.c make_absolute_path
    // re-export, consumed by guc.c SelectConfigFiles.
    s::set_data_dir::set(SetDataDir);
    s::make_absolute_path::set(|path| backend_port_path_seams::make_absolute_path::call(path));

    // Remaining miscinit.c-owned declarations (added by later consumers). Each
    // delegates to this crate's own function so the seam is no longer an
    // uninstalled panic. (process_shared_preload_libraries_in_progress is
    // already installed above.)
    s::in_no_force_rls_operation::set(InNoForceRLSOperation);
    s::in_security_restricted_operation::set(InSecurityRestrictedOperation);
    // ProcessUtility's CheckRestrictedOperation (utility.c) reads the same
    // predicate through the utility-out-seams copy; install it here (the body
    // is miscinit-owned), mirroring xact's cross-install of xact_read_only.
    backend_tcop_utility_out_seams::in_security_restricted_operation::set(
        InSecurityRestrictedOperation,
    );
    // plancache's GetCachedPlan revalidation slice (backendstate-pc-seams):
    // get_user_id is this crate's body; row_security reads the guc-tables slot
    // (the `plan_cache_mode` member is installed by plancache itself).
    backend_utils_misc_backendstate_pc_seams::get_user_id::set(|| Ok(GetUserId()));
    backend_utils_misc_backendstate_pc_seams::row_security::set(|| {
        Ok((backend_utils_misc_guc_tables::vars::row_security.get().get)())
    });
    s::in_local_user_id_change::set(InLocalUserIdChange);
    s::get_backend_type_desc::set(GetBackendTypeDesc);
    s::check_data_dir::set(crate::process::checkDataDir);
    s::change_to_data_dir::set(crate::process::ChangeToDataDir);
    s::create_data_dir_lock_file::set(crate::lockfile::create_data_dir_lock_file);
    s::add_to_data_dir_lock_file::set(crate::lockfile::AddToDataDirLockFile);
    // `RecheckDataDirLockFile()` (miscinit.c:1697) is bodied in this crate; the
    // postmaster's ServerLoop consumes it through backend-postmaster-postmaster-
    // seams. C returns bool and never longjmps (its only ereports are LOG); the
    // Rust port returns PgResult<bool>. Per the C contract ("return true if there
    // is any doubt: we do not want to cause a panic shutdown unnecessarily"), an
    // unexpected Err maps to true.
    backend_postmaster_postmaster_seams::recheck_data_dir_lock_file::set(|| {
        crate::lockfile::RecheckDataDirLockFile().unwrap_or(true)
    });
    s::set_processing_mode_bootstrap::set(|| {
        SetProcessingMode(ProcessingMode::BootstrapProcessing)
    });
    s::set_processing_mode_normal::set(|| SetProcessingMode(ProcessingMode::NormalProcessing));
    s::set_ignore_system_indexes::set(SetIgnoreSystemIndexes);
    s::get_ignore_system_indexes::set(IgnoreSystemIndexes);
    // MyBackendType lives in globals.c, but miscinit owns the accessor used by
    // the C macro; install the in-crate getter.
    s::my_backend_type::set(GetMyBackendType);
    // IsBinaryUpgrade lives in globals.c (init-small); bridge to its value until
    // that owner installs its own seam (same disposition as the crit/interrupt
    // brackets below).
    s::is_binary_upgrade::set(backend_utils_init_small::globals::IsBinaryUpgrade);

    // `TouchSocketLockFiles()` is a miscinit.c body that the postmaster's
    // ServerLoop calls through a seam declared on
    // backend-postmaster-postmaster-seams. The real owner is miscinit, so it
    // installs that postmaster-side seam slot here (delegating to its own fn),
    // mirroring the cross-crate-install pattern the syslogger owner uses for the
    // postmaster's logrotate seams. (RecheckDataDirLockFile is already installed
    // above.)
    backend_postmaster_postmaster_seams::touch_socket_lock_files::set(
        crate::lockfile::touch_socket_lock_files,
    );

    // Non-miscinit seams an earlier consumer declared here. Until their real
    // owners (globals.c counters, superuser.c) land in this repo, install thin
    // delegations to the owners' values so the existing call sites keep working.
    s::start_crit_section::set(|| {
        backend_utils_init_small::globals::SetCritSectionCount(
            backend_utils_init_small::globals::CritSectionCount() + 1,
        )
    });
    s::end_crit_section::set(|| {
        debug_assert!(backend_utils_init_small::globals::CritSectionCount() > 0);
        backend_utils_init_small::globals::SetCritSectionCount(
            backend_utils_init_small::globals::CritSectionCount() - 1,
        )
    });
    s::hold_interrupts::set(|| {
        backend_utils_init_small::globals::SetInterruptHoldoffCount(
            backend_utils_init_small::globals::InterruptHoldoffCount() + 1,
        )
    });
    s::resume_interrupts::set(|| {
        debug_assert!(backend_utils_init_small::globals::InterruptHoldoffCount() > 0);
        backend_utils_init_small::globals::SetInterruptHoldoffCount(
            backend_utils_init_small::globals::InterruptHoldoffCount() - 1,
        )
    });
    s::superuser_arg::set(superuser_arg);

    // The three owned seams whose declarations now mirror the C failure
    // surface (PgResult; Mcx for the catalog-lookup paths). Each delegates to
    // this crate's own function.
    s::init_standalone_process::set(crate::process::InitStandaloneProcess);
    s::has_rolreplication::set(has_rolreplication);
    s::superuser::set(superuser);

    // guc_funcs.c (SET/SHOW layer) reaches `superuser()` and `GetUserId()`
    // through its own outward seam crate (it depends only on
    // backend-utils-misc-guc-funcs-seams for cross-subsystem calls). Their real
    // owner is miscinit.c/superuser.c, so install them here. The guc_funcs seams
    // are bare (no Mcx / no PgResult), matching C's `bool superuser(void)` and
    // `Oid GetUserId(void)`; superuser's catalog read happens in superuser_arg's
    // owner behind a scratch context.
    backend_utils_misc_guc_funcs_seams::get_user_id::set(GetUserId);
    backend_utils_misc_guc_funcs_seams::superuser::set(|| {
        let scratch = MemoryContext::new("guc_funcs superuser seam");
        superuser(scratch.mcx()).expect("superuser() catalog lookup failed")
    });

    // DatabasePath direct set/clear (the recovery "quick hack" path in
    // ProcessCommittedInvalidationMessages) — bypasses SetDatabasePath's
    // one-shot Assert; both write the globals.c-owned DatabasePath.
    s::set_database_path::set(|path| {
        backend_utils_init_small::globals::SetDatabasePath(Some(path.to_owned()))
    });
    s::clear_database_path::set(|| backend_utils_init_small::globals::SetDatabasePath(None));
    // DatabasePath getter (relcache init-file path construction reads the
    // globals.c-owned DatabasePath; None mirrors the C `DatabasePath == NULL`).
    s::get_database_path::set(|| backend_utils_init_small::globals::DatabasePath());
    // MyProcPid getter (relcache's per-backend init-file temp name).
    s::my_proc_pid::set(|| backend_utils_init_small::globals::MyProcPid());

    // WAL summarizer backend-type set/test (MyBackendType lives in globals.c;
    // miscinit owns the accessor the C macros use).
    s::set_my_backend_type_wal_summarizer::set(|| {
        SetMyBackendType(BackendType::WalSummarizer)
    });
    s::am_wal_summarizer_process::set(|| GetMyBackendType() == BackendType::WalSummarizer);
    s::am_logical_slot_sync_worker_process::set(|| GetMyBackendType() == BackendType::SlotsyncWorker);

    // Checkpointer backend-type set/test (MyBackendType lives in globals.c).
    s::set_my_backend_type_checkpointer::set(|| SetMyBackendType(BackendType::Checkpointer));
    s::am_checkpointer_process::set(|| GetMyBackendType() == BackendType::Checkpointer);

    // Background writer backend-type set (MyBackendType lives in globals.c).
    s::set_my_backend_type_bg_writer::set(|| SetMyBackendType(BackendType::BgWriter));

    // Walwriter backend-type set (MyBackendType lives in globals.c).
    s::set_my_backend_type_wal_writer::set(|| SetMyBackendType(BackendType::WalWriter));

    // `CritSectionCount > 0` (the START_CRIT_SECTION counter in globals.c) —
    // CompactCheckpointerRequestQueue avoids allocating inside a crit section.
    s::in_critical_section::set(|| backend_utils_init_small::globals::CritSectionCount() > 0);

    // Pure-wiring installs (assemble/seam-wiring-guard): owner bodies match the
    // declared seam signatures exactly; the remaining miscinit seams either
    // diverge (extra Mcx / Result wrapper) or are mis-homed (pg_usleep lives in
    // port-pgsleep) and are tracked in DESIGN_DEBT.
    s::get_session_user_id::set(GetSessionUserId);
    s::initialize_session_user_id_standalone::set(InitializeSessionUserIdStandalone);
    s::validate_pg_version::set(ValidatePgVersion);
    s::initialize_session_user_id::set(InitializeSessionUserId);
    s::initialize_system_user::set(InitializeSystemUser);
    s::set_database_path_once::set(SetDatabasePath);
    s::process_session_preload_libraries::set(process_session_preload_libraries);

    // ---- parallel-worker bring-up: session/role/security-context restore -----
    //
    // ParallelWorkerMain (parallel.c) restores the leader's user/role/security
    // identity from the serialized FixedParallelState. The bodies are miscinit.c
    // functions; install the parallel-rt seam slots from the real owner. The
    // parallel-rt seam crate is a leaf (no cycle). `SetUserIdAndSecContext` is
    // `void` in C — wrap in `Ok`.
    {
        use backend_access_transam_parallel_rt_seams as rt;
        rt::set_authenticated_user_id::set(SetAuthenticatedUserId);
        rt::set_session_authorization::set(SetSessionAuthorization);
        rt::set_current_role_id::set(SetCurrentRoleId);
        rt::set_user_id_and_sec_context::set(|id, sec_context| {
            SetUserIdAndSecContext(id, sec_context);
            Ok(())
        });
        // `CHECK_FOR_INTERRUPTS()` — the parallel-rt seam crate collects this
        // decl (alongside ~15 other not-yet-ported owners) so the parallel
        // orchestration can call it; the real body is postgres.c's
        // `ProcessInterrupts`. Delegate to the interrupt owner's seam exactly as
        // the miscinit-homed `s::check_for_interrupts` below does. Reached on the
        // serial index-build / catalog paths (genam, catalog/index, namespace),
        // not just parallel.c.
        rt::check_for_interrupts::set(|| backend_tcop_postgres_seams::check_for_interrupts::call());
    }

    // ---- mis-homed slot-sync worker bootstrap group -----------------------
    //
    // These seams were declared on miscinit's seam crate by the slot-sync
    // worker consumer (`ReplSlotSyncWorkerMain`, slotsync.c), but none are
    // miscinit.c's own functions — each mirrors a step whose real body lives
    // in another (now-ported) owner. We install them here by delegating to the
    // real owner's seam, exactly as the WAL-summarizer / `my_proc_pid` lines
    // above delegate to globals. (slotsync is the *only* consumer of these
    // miscinit-homed copies; every other consumer already calls the real
    // owner's seam crate directly.)

    // `MyBackendType = B_SLOTSYNC_WORKER;` (slotsync.c:1464; globals.c) — the
    // exact analog of the installed WAL-summarizer setter above.
    s::set_my_backend_type_slotsync::set(|| {
        SetMyBackendType(BackendType::SlotsyncWorker);
        Ok(())
    });
    // `init_ps_display(NULL)` (slotsync.c:1466) — ps_status.c owner.
    s::init_ps_display::set(|| {
        backend_utils_misc_more_seams::init_ps_display::call(None);
        Ok(())
    });
    // `InitProcess()` (slotsync.c:1474) — proc.c owner.
    s::init_process::set(|| backend_storage_lmgr_proc_seams::init_process::call());
    // `BaseInit()` (slotsync.c:1479) — postinit.c owner.
    s::base_init::set(|| backend_utils_init_postinit_seams::base_init::call());
    // `InitializeTimeouts()` (slotsync.c:1535) — timeout.c owner.
    s::initialize_timeouts::set(|| {
        backend_utils_misc_timeout_seams::initialize_timeouts::call();
        Ok(())
    });
    // `sigprocmask(SIG_SETMASK, &UnBlockSig, NULL)` (slotsync.c:1543) —
    // the pqsignal owner's `unblock_signals` is the real primitive (also used
    // by bgworker).
    s::unblock_signals::set(|| {
        backend_libpq_pqsignal_seams::unblock_signals::call();
        Ok(())
    });
    // `InitPostgres(dbname, InvalidOid, NULL, InvalidOid, 0, NULL)`
    // (slotsync.c:1568) — postinit.c owner; dbname only, default username,
    // init_flags = 0.
    s::init_postgres::set(|dbname| {
        backend_utils_init_postinit_seams::init_postgres_by_name::call(Some(dbname), None, 0)
    });
    // `CHECK_FOR_INTERRUPTS()` (slotsync.c) — postgres.c / interrupt owner.
    s::check_for_interrupts::set(|| backend_tcop_postgres_seams::check_for_interrupts::call());

    // NOTE: `setup_signal_handlers` (the slotsync.c:1515-1522 `pqsignal(SIGHUP,
    // SignalHandlerForConfigReload)` ... block) is intentionally NOT installed
    // here — its handler bodies (SignalHandlerForConfigReload, die,
    // StatementCancelHandler, FloatExceptionHandler, procsignal_sigusr1_handler)
    // live in interrupt.c / postgres.c / procsignal.c, none of which is ported.
    // It is tracked in CONTRACT_RECONCILE_PENDING + DESIGN_DEBT (provider-unported).

    // ---- boot-prelude common/ startup helpers (see startup_paths.rs) ------
    //
    // Three tiny `common/`/`port/` functions the boot prelude reaches before
    // anything else, whose source files have no dedicated owner crate yet.
    // Bodied in `startup_paths` and homed here (the process-init crate the boot
    // path already routes through). Faithful non-Windows ports.
    common_path_seams::get_progname::set(startup_paths::get_progname);
    // get_share_path(my_exec_path) (common/path.c): the share-dir derivation the
    // tzdb (pgtz) and timezonesets (tzparser) reads resolve against.
    common_path_seams::get_share_path::set(boot_paths::get_share_path);
    backend_common_exec_seams::set_pglocale_pgservice::set(
        startup_paths::set_pglocale_pgservice,
    );
    // The path-computing tail of InitStandaloneProcess (find_my_exec /
    // get_pkglib_path), homed in `boot_paths` next to the other tiny
    // `common/`/`port/` boot helpers.
    backend_common_exec_seams::resolve_standalone_paths::set(
        boot_paths::resolve_standalone_paths,
    );
    // The small `src/port/path.c` + libc helpers miscinit's path/lock-file work
    // reaches, homed in `boot_paths`. (`post_port_number` is owned where the GUC
    // tables live and installed from there.)
    backend_port_path_seams::make_absolute_path::set(boot_paths::make_absolute_path);
    backend_port_path_seams::first_dir_separator::set(boot_paths::first_dir_separator_pub);
    backend_port_path_seams::getppid::set(boot_paths::getppid);
    backend_port_path_seams::pid_appears_live::set(boot_paths::pid_appears_live);
    backend_port_path_seams::touch_file_times::set(boot_paths::touch_file_times);
    // The `port/path.c` helpers that `commands/tablespace.c` reaches through the
    // tablespace-globals seam group, also homed in `boot_paths`.
    backend_commands_tablespace_globals_seams::canonicalize_path::set(
        boot_paths::canonicalize_path_pub,
    );
    backend_commands_tablespace_globals_seams::is_absolute_path::set(
        boot_paths::is_absolute_path_pub,
    );
    backend_commands_tablespace_globals_seams::path_is_prefix_of_path::set(
        boot_paths::path_is_prefix_of_path_pub,
    );
    backend_commands_tablespace_globals_seams::get_parent_directory::set(
        boot_paths::get_parent_directory_pub,
    );
    common_username_seams::get_user_name_or_exit::set(startup_paths::get_user_name_or_exit);

    // ---- GUC variable accessors (C `conf->variable` backing store) ----------
    //
    // These four GUCs are declared in guc_tables.c with `variable` pointing at
    // miscinit.c's own globals (`IgnoreSystemIndexes`,
    // `{session,shared,local}_preload_libraries_string`). The GUC engine reads
    // and writes each through the slot's accessors; install them so the engine
    // reaches this crate's backing store. (None of these are ControlFile-derived
    // — they are plain GUC globals set from postgresql.conf / SetConfigOption.)
    use backend_utils_misc_guc_tables::{vars, GucVarAccessors};
    vars::IgnoreSystemIndexes.install(GucVarAccessors {
        get: IgnoreSystemIndexes,
        set: SetIgnoreSystemIndexes,
    });
    vars::session_preload_libraries_string.install(GucVarAccessors {
        get: get_session_preload_libraries,
        set: set_session_preload_libraries,
    });
    vars::shared_preload_libraries_string.install(GucVarAccessors {
        get: get_shared_preload_libraries,
        set: set_shared_preload_libraries,
    });
    vars::local_preload_libraries_string.install(GucVarAccessors {
        get: get_local_preload_libraries,
        set: set_local_preload_libraries,
    });

    // Parallel-worker transfer of MyClientConnectionInfo. The bodies are owned
    // here; the seam decls live in parallel-rt-seams. The DSM chunk is an
    // `int32 authn_id_len` + `UserAuth auth_method` header (SERIALIZED_HEADER_LEN
    // bytes) followed, when `authn_id_len >= 0`, by `authn_id_len` bytes plus a
    // NUL — so the header bounds the restore read.
    {
        use backend_access_transam_parallel_rt_seams as rt;
        rt::estimate_client_connection_info_space::set(|| Ok(EstimateClientConnectionInfoSpace()));
        rt::serialize_client_connection_info::set(|len, space| {
            // SAFETY: `space` is the start of a `len`-byte chunk shm_toc_allocate
            // reserved for the ClientConnectionInfo (EstimateClientConnection-
            // InfoSpace sized it); the leader writes the whole chunk here. The
            // audited DSM-pointer primitive (cf. backend-utils-misc-guc).
            let buf = unsafe { core::slice::from_raw_parts_mut(space as *mut u8, len) };
            SerializeClientConnectionInfo(buf)
        });
        rt::restore_client_connection_info::set(|space| {
            // Read the fixed header, derive the total length from `authn_id_len`,
            // then form the bounded slice. SAFETY: `space` points at the
            // ClientConnectionInfo chunk the leader serialized; the header's
            // `authn_id_len` bounds the readable extent.
            let header =
                unsafe { core::slice::from_raw_parts(space as *const u8, SERIALIZED_HEADER_LEN) };
            let authn_id_len = i32::from_ne_bytes(header[..4].try_into().expect("4-byte len"));
            let total = if authn_id_len >= 0 {
                SERIALIZED_HEADER_LEN + authn_id_len as usize + 1
            } else {
                SERIALIZED_HEADER_LEN
            };
            let buf = unsafe { core::slice::from_raw_parts(space as *const u8, total) };
            RestoreClientConnectionInfo(buf)
        });
    }

    // matview.c reaches Get/SetUserIdAndSecContext (miscinit.c) through its
    // outward frontier seam crate; miscinit owns the bodies. Infallible in C.
    {
        use backend_commands_matview_deps_seams as m;
        m::get_user_id_and_sec_context::set(|| Ok(GetUserIdAndSecContext()));
        m::set_user_id_and_sec_context::set(|userid, sec_context| {
            SetUserIdAndSecContext(userid, sec_context);
            Ok(())
        });
    }
}

#[cfg(test)]
mod tests;
