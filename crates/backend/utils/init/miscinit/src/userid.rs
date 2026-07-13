use std::cell::Cell;

use mcx::{Mcx, PgString};
use types_core::{
    catalog::BOOTSTRAP_SUPERUSERID, BackendType, InvalidOid, Oid, SECURITY_LOCAL_USERID_CHANGE,
    SECURITY_NOFORCE_RLS, SECURITY_RESTRICTED_OPERATION,
};
use types_error::{
    PgError, PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_UNDEFINED_OBJECT, ERROR,
};

use crate::GetMyBackendType;

thread_local! {
    static AUTHENTICATED_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    static SESSION_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    static OUTER_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    static CURRENT_USER_ID: Cell<Oid> = const { Cell::new(InvalidOid) };
    // Set once, TopMemoryContext in C; leaked here.
    static SYSTEM_USER: Cell<Option<&'static str>> = const { Cell::new(None) };
    static SESSION_USER_IS_SUPERUSER: Cell<bool> = const { Cell::new(false) };
    static SECURITY_RESTRICTION_CONTEXT: Cell<i32> = const { Cell::new(0) };
    static SET_ROLE_IS_ACTIVE: Cell<bool> = const { Cell::new(false) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionIdentityState {
    pub authenticated_user_id: Oid,
    pub session_user_id: Oid,
    pub outer_user_id: Oid,
    pub current_user_id: Oid,
    pub system_user: Option<&'static str>,
    pub session_user_is_superuser: bool,
    pub security_restriction_context: i32,
    pub set_role_is_active: bool,
}

pub fn CaptureSessionIdentityState() -> SessionIdentityState {
    SessionIdentityState {
        authenticated_user_id: AUTHENTICATED_USER_ID.get(),
        session_user_id: SESSION_USER_ID.get(),
        outer_user_id: OUTER_USER_ID.get(),
        current_user_id: CURRENT_USER_ID.get(),
        system_user: SYSTEM_USER.get(),
        session_user_is_superuser: SESSION_USER_IS_SUPERUSER.get(),
        security_restriction_context: SECURITY_RESTRICTION_CONTEXT.get(),
        set_role_is_active: SET_ROLE_IS_ACTIVE.get(),
    }
}

pub fn ReplaceSessionIdentityState(state: SessionIdentityState) -> SessionIdentityState {
    let old = CaptureSessionIdentityState();
    AUTHENTICATED_USER_ID.set(state.authenticated_user_id);
    SESSION_USER_ID.set(state.session_user_id);
    OUTER_USER_ID.set(state.outer_user_id);
    CURRENT_USER_ID.set(state.current_user_id);
    SYSTEM_USER.set(state.system_user);
    SESSION_USER_IS_SUPERUSER.set(state.session_user_is_superuser);
    SECURITY_RESTRICTION_CONTEXT.set(state.security_restriction_context);
    SET_ROLE_IS_ACTIVE.set(state.set_role_is_active);
    if let Some(procno) = lmgr_proc::MyProc() {
        lmgr_proc::GetPGProcByNumber(procno)
            .roleId
            .store(state.authenticated_user_id, std::sync::atomic::Ordering::Relaxed);
    }
    old
}

pub fn GetUserId() -> Oid {
    debug_assert_ne!(CURRENT_USER_ID.get(), InvalidOid);
    CURRENT_USER_ID.get()
}

/// Retention park (wretain): clear the per-session identity TLS so a parked
/// pool standby carries no session identity (zero-leak) and the next claim's
/// set-once asserts (SetAuthenticatedUserId, InitializeSystemUser) hold.
pub fn ResetSessionIdentityForRetainedPark() {
    AUTHENTICATED_USER_ID.set(InvalidOid);
    SESSION_USER_ID.set(InvalidOid);
    OUTER_USER_ID.set(InvalidOid);
    CURRENT_USER_ID.set(InvalidOid);
    SESSION_USER_IS_SUPERUSER.set(false);
    SECURITY_RESTRICTION_CONTEXT.set(0);
    SET_ROLE_IS_ACTIVE.set(false);
    SYSTEM_USER.set(None);
}

pub fn GetOuterUserId() -> Oid {
    debug_assert_ne!(OUTER_USER_ID.get(), InvalidOid);
    OUTER_USER_ID.get()
}

fn SetOuterUserId(userid: Oid, is_superuser: bool) -> PgResult<()> {
    debug_assert_eq!(SECURITY_RESTRICTION_CONTEXT.get(), 0);
    debug_assert_ne!(userid, InvalidOid);
    OUTER_USER_ID.set(userid);
    // The effective user ID is forced to match; the is_superuser GUC follows.
    CURRENT_USER_ID.set(userid);
    guc_seams::set_config_option_internal_dynamic_default::call(
        "is_superuser",
        if is_superuser { "on" } else { "off" },
    )
}

pub fn GetSessionUserId() -> Oid {
    debug_assert_ne!(SESSION_USER_ID.get(), InvalidOid);
    SESSION_USER_ID.get()
}

pub fn GetSessionUserIsSuperuser() -> bool {
    debug_assert_ne!(SESSION_USER_ID.get(), InvalidOid);
    SESSION_USER_IS_SUPERUSER.get()
}

fn SetSessionUserId(userid: Oid, is_superuser: bool) {
    debug_assert_eq!(SECURITY_RESTRICTION_CONTEXT.get(), 0);
    debug_assert_ne!(userid, InvalidOid);
    SESSION_USER_ID.set(userid);
    SESSION_USER_IS_SUPERUSER.set(is_superuser);
}

pub fn GetSystemUser() -> Option<&'static str> {
    SYSTEM_USER.get()
}

pub fn GetAuthenticatedUserId() -> Oid {
    debug_assert_ne!(AUTHENTICATED_USER_ID.get(), InvalidOid);
    AUTHENTICATED_USER_ID.get()
}

pub fn SetAuthenticatedUserId(userid: Oid) {
    debug_assert_ne!(userid, InvalidOid);
    debug_assert_eq!(AUTHENTICATED_USER_ID.get(), InvalidOid);
    AUTHENTICATED_USER_ID.set(userid);

    // Also mark our PGPROC entry with the authenticated user id (atomic store).
    let procno = lmgr_proc::MyProc().expect("SetAuthenticatedUserId: MyProc is set");
    lmgr_proc::GetPGProcByNumber(procno)
        .roleId
        .store(userid, std::sync::atomic::Ordering::Relaxed);
}

// Never asserts/errors: Start/AbortTransaction save-restore through these
// while the value may still be invalid.
pub fn GetUserIdAndSecContext() -> (Oid, i32) {
    (CURRENT_USER_ID.get(), SECURITY_RESTRICTION_CONTEXT.get())
}

pub fn SetUserIdAndSecContext(userid: Oid, sec_context: i32) {
    CURRENT_USER_ID.set(userid);
    SECURITY_RESTRICTION_CONTEXT.set(sec_context);
}

pub fn InLocalUserIdChange() -> bool {
    SECURITY_RESTRICTION_CONTEXT.get() & SECURITY_LOCAL_USERID_CHANGE != 0
}

pub fn InSecurityRestrictedOperation() -> bool {
    SECURITY_RESTRICTION_CONTEXT.get() & SECURITY_RESTRICTED_OPERATION != 0
}

pub fn InNoForceRLSOperation() -> bool {
    SECURITY_RESTRICTION_CONTEXT.get() & SECURITY_NOFORCE_RLS != 0
}

// Obsolete pljava-compat pair.
pub fn GetUserIdAndContext() -> (Oid, bool) {
    (CURRENT_USER_ID.get(), InLocalUserIdChange())
}

pub fn SetUserIdAndContext(userid: Oid, sec_def_context: bool) -> PgResult<()> {
    if InSecurityRestrictedOperation() {
        return Err(PgError::new(
            ERROR,
            "cannot set parameter \"role\" within security-restricted operation",
        )
        .with_sqlstate(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .into());
    }
    CURRENT_USER_ID.set(userid);
    let ctx = SECURITY_RESTRICTION_CONTEXT.get();
    SECURITY_RESTRICTION_CONTEXT.set(if sec_def_context {
        ctx | SECURITY_LOCAL_USERID_CHANGE
    } else {
        ctx & !SECURITY_LOCAL_USERID_CHANGE
    });
    Ok(())
}

pub fn InitializeSessionUserId(
    rolename: Option<&str>,
    roleid: Oid,
    bypass_login_check: bool,
) -> PgResult<()> {
    // ParallelWorkerMain already set the user-id state; no catalog lookup
    // (the role may have been dropped) and no rolcanlogin/rolconnlimit.
    if parallel_seams::initializing_parallel_worker::is_installed()
        && parallel_seams::initializing_parallel_worker::call()
    {
        debug_assert!(bypass_login_check);
        return Ok(());
    }
    debug_assert!(!crate::IsBootstrapProcessingMode());

    inval_seams::accept_invalidation_messages::call()?;

    let form = match rolename {
        Some(rname) => {
            match syscache_seams::lookup_authid_session_by_rolname::call(rname)? {
                Some(f) => f,
                None => {
                    return elog::ereport(types_error::FATAL)
                        .errcode(types_error::ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                        .errmsg(format!("role \"{rname}\" does not exist"))
                        .finish(crate::process::loc(802, "InitializeSessionUserId"));
                }
            }
        }
        None => match syscache_seams::lookup_authid_session_by_oid::call(roleid)? {
            Some(f) => f,
            None => {
                return elog::ereport(types_error::FATAL)
                    .errcode(types_error::ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                    .errmsg(format!("role with OID {roleid} does not exist"))
                    .finish(crate::process::loc(810, "InitializeSessionUserId"));
            }
        },
    };

    let roleid = form.roleid;
    let rname = String::from_utf8_lossy(form.rolname.name_str()).into_owned();
    let is_superuser = form.rolsuper;

    SetAuthenticatedUserId(roleid);

    // SetSessionUserId/SetOuterUserId run via the session_authorization GUC
    // hooks, exactly C's SetConfigOption(..., PGC_BACKEND, PGC_S_OVERRIDE).
    guc_seams::set_config_option::call(
        "session_authorization",
        Some(&rname),
        types_guc::GucContext::PGC_BACKEND,
        types_guc::GucSource::PGC_S_OVERRIDE,
    )?;

    if init_small::globals::IsUnderPostmaster() {
        if !bypass_login_check && !form.rolcanlogin {
            return elog::ereport(types_error::FATAL)
                .errcode(types_error::ERRCODE_INVALID_AUTHORIZATION_SPECIFICATION)
                .errmsg(format!("role \"{rname}\" is not permitted to log in"))
                .finish(crate::process::loc(856, "InitializeSessionUserId"));
        }

        if form.rolconnlimit >= 0
            && GetMyBackendType() == BackendType::Backend
            && !is_superuser
            && procarray_seams::count_user_backends::call(roleid)? > form.rolconnlimit
        {
            return elog::ereport(types_error::FATAL)
                .errcode(types_error::ERRCODE_TOO_MANY_CONNECTIONS)
                .errmsg(format!("too many connections for role \"{rname}\""))
                .finish(crate::process::loc(877, "InitializeSessionUserId"));
        }
    }

    Ok(())
}

pub fn InitializeSessionUserIdStandalone() -> PgResult<()> {
    debug_assert!(
        !init_small::globals::IsUnderPostmaster()
            || matches!(
                GetMyBackendType(),
                BackendType::AutovacWorker | BackendType::SlotsyncWorker | BackendType::BgWorker
            )
    );
    debug_assert_eq!(AUTHENTICATED_USER_ID.get(), InvalidOid);
    AUTHENTICATED_USER_ID.set(BOOTSTRAP_SUPERUSERID);

    SetSessionAuthorization(BOOTSTRAP_SUPERUSERID, true)?;
    SetCurrentRoleId(InvalidOid, false)
}

pub fn InitializeSystemUser(authn_id: &str, auth_method: &str) {
    debug_assert!(SYSTEM_USER.get().is_none());
    SYSTEM_USER.set(Some(format!("{auth_method}:{authn_id}").leak()));
}

// session_authorization assign hook; commutative with SetCurrentRoleId, so
// derived state updates only when !SetRoleIsActive.
pub fn SetSessionAuthorization(userid: Oid, is_superuser: bool) -> PgResult<()> {
    SetSessionUserId(userid, is_superuser);
    if !SET_ROLE_IS_ACTIVE.get() {
        SetOuterUserId(userid, is_superuser)?;
    }
    Ok(())
}

pub fn GetCurrentRoleId() -> Oid {
    if SET_ROLE_IS_ACTIVE.get() {
        OUTER_USER_ID.get()
    } else {
        InvalidOid
    }
}

// SET ROLE; InvalidOid = SET ROLE NONE.
pub fn SetCurrentRoleId(mut roleid: Oid, mut is_superuser: bool) -> PgResult<()> {
    if roleid == InvalidOid {
        SET_ROLE_IS_ACTIVE.set(false);
        // Before SetSessionAuthorization runs, only the flag changes.
        if SESSION_USER_ID.get() == InvalidOid {
            return Ok(());
        }
        roleid = SESSION_USER_ID.get();
        is_superuser = SESSION_USER_IS_SUPERUSER.get();
    } else {
        SET_ROLE_IS_ACTIVE.set(true);
    }
    SetOuterUserId(roleid, is_superuser)
}

pub fn GetUserNameFromId<'mcx>(
    mcx: Mcx<'mcx>,
    roleid: Oid,
    noerr: bool,
) -> PgResult<Option<PgString<'mcx>>> {
    match syscache_seams::lookup_authid_rolname::call(mcx, roleid)? {
        Some(name) => Ok(Some(name)),
        None if noerr => Ok(None),
        None => Err(PgError::new(ERROR, format!("invalid role OID: {roleid}"))
            .with_sqlstate(ERRCODE_UNDEFINED_OBJECT)
            .into()),
    }
}
