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

pub fn GetUserId() -> Oid {
    debug_assert_ne!(CURRENT_USER_ID.get(), InvalidOid);
    CURRENT_USER_ID.get()
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
