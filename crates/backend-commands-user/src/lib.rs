#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// `CreateRole`/`AlterRole`/… faithfully take/return the same parameter set as
// the C callees; `PgError` is a large error type shared across the whole tree,
// so boxing it would diverge from every sibling crate's `Result` shape.
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]

//! `backend/commands/user.c` — CREATE / ALTER / DROP / RENAME ROLE, ALTER ROLE
//! … SET, GRANT/REVOKE role membership, DROP/REASSIGN OWNED, and the
//! `createrole_self_grant` GUC hooks.
//!
//! user.c's own decision logic lives in-crate: the role-attribute `DefElem`
//! option loop and its conflict detection, the per-attribute `pg_authid` tuple
//! assembly, every permission check, the empty-password clearing rule, the
//! implicit-self-grant + `createrole_self_grant` orchestration, the membership
//! add/del orchestration ([`AddRoleMems`]/[`DelRoleMems`]), the membership-loop
//! and grantor-circularity checks, the recursive-revoke planner, the grantor
//! inference/validation ([`check_role_grantor`]), the membership-authorization
//! check, and every `ereport`/`elog` (SQLSTATE + message + hint/detail).
//!
//! Genuine cross-subsystem externals cross the [`backend_commands_user_seams`]
//! seams; each panics until its owner installs a real implementation.
//!
//! The command entry points that reach an allocating cross-subsystem seam
//! (`CreateRole`/`AlterRole`/`GrantRole`/`DropOwnedObjects`/
//! `ReassignOwnedObjects`) and the membership helpers
//! (`AddRoleMems`/`DelRoleMems`/`check_role_*`) take `mcx: Mcx<'mcx>`:
//! the allocating cross-subsystem seams (`get_user_name_from_id` →
//! `GetUserNameFromId`'s `pstrdup`, `get_rolespec_name`, `encrypt_password`)
//! allocate their result in the caller's memory context, so they take `mcx`
//! and return `PgString<'mcx>`, matching the C palloc surface.
//!
//! ## Function inventory (user.c, PostgreSQL 18.3 — 21 functions)
//!
//! * `have_createrole_privilege`              — C 121-125 (static)
//! * `CreateRole`                             — C 131-608
//! * `AlterRole`                              — C 618-993
//! * `AlterRoleSet`                           — C 999-1083
//! * `DropRole`                               — C 1089-1328
//! * `RenameRole`                             — C 1333-1472
//! * `GrantRole`                              — C 1479-1575
//! * `DropOwnedObjects`                       — C 1582-1603
//! * `ReassignOwnedObjects`                   — C 1610-1642
//! * `roleSpecsToIds`                         — C 1651-1666
//! * `AddRoleMems`                            — C 1680-1965 (static)
//! * `DelRoleMems`                            — C 1978-2104 (static)
//! * `check_role_membership_authorization`    — C 2110-2173 (static)
//! * `check_role_grantor`                     — C 2204-2279 (static)
//! * `initialize_revoke_actions`              — C 2289-2302 (static)
//! * `plan_single_revoke`                     — C 2320-2379 (static)
//! * `plan_member_revoke`                     — C 2390-2407 (static)
//! * `plan_recursive_revoke`                  — C 2414-2499 (static)
//! * `InitGrantRoleOptions`                   — C 2504-2511 (static)
//! * `check_createrole_self_grant`            — C 2516-2564 (GUC check hook)
//! * `assign_createrole_self_grant`           — C 2569-2583 (GUC assign hook)

use backend_commands_user_seams as seam;
use backend_utils_error::ereport;
use mcx::Mcx;
use types_authid::{
    AuthIdUpdate, AuthMemForm, AuthMemUpdate, NewAuthMemRecord, NewAuthRecord, PasswordType,
    STATUS_OK,
};
use types_catalog::catalog_dependency::{InvalidObjectAddress, ObjectAddress};
use types_core::primitive::{InvalidOid, Oid, OidIsValid, TimestampTz};
use types_core::{
    AUTH_ID_OID_INDEX_ID, AUTH_ID_RELATION_ID, AUTH_MEM_OID_INDEX_ID, AUTH_MEM_RELATION_ID,
    BOOTSTRAP_SUPERUSERID, DATABASE_RELATION_ID, ROLE_PG_DATABASE_OWNER,
};
use types_error::{
    ErrorLevel, ErrorLocation, PgResult, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_GRANT_OPERATION, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_IN_USE,
    ERRCODE_RESERVED_NAME, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT,
};
use types_nodes::parsenodes::{DropBehavior, DROP_CASCADE, DROP_RESTRICT};
use types_parsenodes::{
    AccessPriv, AlterRoleSetStmt, AlterRoleStmt, CreateRoleStmt, DefElem, DropOwnedStmt,
    DropRoleStmt, GrantRoleStmt, Node, ParseState, ReassignOwnedStmt, RoleSpec, ROLESPEC_CSTRING,
    ROLESPEC_CURRENT_ROLE, ROLESTMT_GROUP, ROLESTMT_ROLE, ROLESTMT_USER,
};

const NOTICE: ErrorLevel = types_error::error::NOTICE;
const WARNING: ErrorLevel = types_error::error::WARNING;
const ERROR: ErrorLevel = types_error::error::ERROR;

/* -------------------------------------------------------------------------
 * Constants borrowed from PostgreSQL headers.
 * ------------------------------------------------------------------------- */

const AuthIdRelationId: Oid = AUTH_ID_RELATION_ID;
const AuthIdOidIndexId: Oid = AUTH_ID_OID_INDEX_ID;
const AuthMemRelationId: Oid = AUTH_MEM_RELATION_ID;
const AuthMemOidIndexId: Oid = AUTH_MEM_OID_INDEX_ID;
const DatabaseRelationId: Oid = DATABASE_RELATION_ID;

/* Lock modes (storage/lockdefs.h). */
const NoLock: i32 = 0;
const AccessShareLock: i32 = 1;
const RowExclusiveLock: i32 = 3;
const ShareUpdateExclusiveLock: i32 = 4;
const AccessExclusiveLock: i32 = 8;

/* GrantRoleOptions specified-bit flags (user.c). */
const GRANT_ROLE_SPECIFIED_ADMIN: u32 = 0x0001;
const GRANT_ROLE_SPECIFIED_INHERIT: u32 = 0x0002;
const GRANT_ROLE_SPECIFIED_SET: u32 = 0x0004;

/// `typedef struct GrantRoleOptions` (user.c:72-78).
#[derive(Clone, Copy, Debug)]
pub struct GrantRoleOptions {
    pub specified: u32,
    pub admin: bool,
    pub inherit: bool,
    pub set: bool,
}

/// `typedef enum RevokeRoleGrantAction` (user.c:60-67).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevokeRoleGrantAction {
    RRG_NOOP,
    RRG_REMOVE_ADMIN_OPTION,
    RRG_REMOVE_INHERIT_OPTION,
    RRG_REMOVE_SET_OPTION,
    RRG_DELETE_GRANT,
}
use RevokeRoleGrantAction::*;

/// `ErrorLocation` for `ereport(...).finish(...)` in this module.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("../src/backend/commands/user.c", 0, funcname)
}

/// `pg_popcount32(x)` — number of set bits (used in `plan_single_revoke`'s
/// assert).
fn pg_popcount32(x: u32) -> u32 {
    x.count_ones()
}

/* -------------------------------------------------------------------------
 * Owned node-tree value accessors.
 * ------------------------------------------------------------------------- */

/// `defel->defname`, or `""` when absent (the parser always fills it).
fn def_name(defel: &DefElem) -> &str {
    defel.defname.as_deref().unwrap_or("")
}

/// `defel->arg == NULL`.
fn defel_arg_is_null(defel: &DefElem) -> bool {
    defel.arg.is_none()
}

/// `strVal(defel->arg)`.
fn defel_str(defel: &DefElem) -> String {
    match defel.arg.as_deref().and_then(|n| n.as_string()) {
        Some(s) => s.sval.clone().unwrap_or_default(),
        _ => String::new(),
    }
}

/// `boolVal(defel->arg)`.
fn defel_bool(defel: &DefElem) -> bool {
    match defel.arg.as_deref().and_then(|n| n.as_boolean()) {
        Some(b) => b.boolval,
        _ => false,
    }
}

/// `intVal(defel->arg)`.
fn defel_int(defel: &DefElem) -> i32 {
    match defel.arg.as_deref().and_then(|n| n.as_integer()) {
        Some(i) => i.ival,
        _ => 0,
    }
}

/// `(List *) defel->arg` — the option's value is a `List *` of `RoleSpec`
/// nodes (addroleto, rolemembers, adminmembers).
fn defel_rolespec_list(defel: &DefElem) -> PgResult<Vec<RoleSpec>> {
    seam::def_get_rolespec_list::call(defel.clone())
}

/// Extract the contained `RoleSpec` from a node.
fn node_as_rolespec(node: &Node) -> Option<RoleSpec> {
    node.as_rolespec().cloned()
}

/// Walk a `Vec<Node>` of `RoleSpec` nodes into the owned `RoleSpec`s.
fn rolespecs(list: &[Node]) -> Vec<RoleSpec> {
    list.iter().filter_map(node_as_rolespec).collect()
}

/// Walk a `Vec<Node>` of `DefElem` nodes into references (the option loops).
fn def_elems(list: &[Node]) -> Vec<&DefElem> {
    list.iter().filter_map(|n| n.as_defelem()).collect()
}

/// Extract the contained `AccessPriv` from a node.
fn node_as_accesspriv(node: &Node) -> Option<&AccessPriv> {
    node.as_accesspriv()
}

/* =========================================================================
 * have_createrole_privilege   (C 121-125)
 * ========================================================================= */

/// `have_createrole_privilege` — true if the current user has CREATEROLE.
fn have_createrole_privilege() -> PgResult<bool> {
    seam::has_createrole_privilege::call(seam::get_user_id::call()?)
}

/* =========================================================================
 * CreateRole   (C 131-608)
 * ========================================================================= */

/// `CreateRole(pstate, stmt)` — CREATE ROLE/USER/GROUP.
pub fn CreateRole<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&ParseState>,
    stmt: &CreateRoleStmt,
) -> PgResult<Oid> {
    let currentUserId = seam::get_user_id::call()?;

    let mut password: Option<String> = None; /* user password */
    let mut issuper = false; /* Make the user a superuser? */
    let mut inherit = true; /* Auto inherit privileges? */
    let mut createrole = false; /* Can this user create roles? */
    let mut createdb = false; /* Can the user create databases? */
    let mut canlogin = false; /* Can this user login? */
    let mut isreplication = false; /* Is this a replication role? */
    let mut bypassrls = false; /* Is this a row security enabled role? */
    let mut connlimit: i32 = -1; /* maximum connections allowed */
    let mut addroleto: Vec<RoleSpec> = Vec::new(); /* roles to make this a member of */
    let mut rolemembers: Vec<RoleSpec> = Vec::new(); /* roles to be members of this role */
    let mut adminmembers: Vec<RoleSpec> = Vec::new(); /* roles to be admins of this role */
    let mut validUntil: Option<String> = None; /* time the login is valid until */
    let validUntil_datum: Option<TimestampTz>;

    let mut dpassword: Option<&DefElem> = None;
    let mut dissuper: Option<&DefElem> = None;
    let mut dinherit: Option<&DefElem> = None;
    let mut dcreaterole: Option<&DefElem> = None;
    let mut dcreatedb: Option<&DefElem> = None;
    let mut dcanlogin: Option<&DefElem> = None;
    let mut disreplication: Option<&DefElem> = None;
    let mut dconnlimit: Option<&DefElem> = None;
    let mut daddroleto: Option<&DefElem> = None;
    let mut drolemembers: Option<&DefElem> = None;
    let mut dadminmembers: Option<&DefElem> = None;
    let mut dvalidUntil: Option<&DefElem> = None;
    let mut dbypassRLS: Option<&DefElem> = None;

    let role = stmt.role.clone().unwrap_or_default();

    /* The defaults can vary depending on the original statement type */
    match stmt.stmt_type {
        ROLESTMT_ROLE => {}
        ROLESTMT_USER => {
            canlogin = true;
            /* may eventually want inherit to default to false here */
        }
        ROLESTMT_GROUP => {}
    }

    /* Extract options from the statement node tree */
    for defel in def_elems(&stmt.options) {
        let defname = def_name(defel);
        if defname == "password" {
            if dpassword.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dpassword = Some(defel);
        } else if defname == "sysid" {
            ereport(NOTICE)
                .errmsg("SYSID can no longer be specified")
                .finish(here("CreateRole"))?;
        } else if defname == "superuser" {
            if dissuper.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dissuper = Some(defel);
        } else if defname == "inherit" {
            if dinherit.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dinherit = Some(defel);
        } else if defname == "createrole" {
            if dcreaterole.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dcreaterole = Some(defel);
        } else if defname == "createdb" {
            if dcreatedb.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dcreatedb = Some(defel);
        } else if defname == "canlogin" {
            if dcanlogin.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dcanlogin = Some(defel);
        } else if defname == "isreplication" {
            if disreplication.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            disreplication = Some(defel);
        } else if defname == "connectionlimit" {
            if dconnlimit.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dconnlimit = Some(defel);
        } else if defname == "addroleto" {
            if daddroleto.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            daddroleto = Some(defel);
        } else if defname == "rolemembers" {
            if drolemembers.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            drolemembers = Some(defel);
        } else if defname == "adminmembers" {
            if dadminmembers.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dadminmembers = Some(defel);
        } else if defname == "validUntil" {
            if dvalidUntil.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dvalidUntil = Some(defel);
        } else if defname == "bypassrls" {
            if dbypassRLS.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dbypassRLS = Some(defel);
        } else {
            return ereport(ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
    }

    if let Some(d) = dpassword {
        if !defel_arg_is_null(d) {
            password = Some(defel_str(d));
        }
    }
    if let Some(d) = dissuper {
        issuper = defel_bool(d);
    }
    if let Some(d) = dinherit {
        inherit = defel_bool(d);
    }
    if let Some(d) = dcreaterole {
        createrole = defel_bool(d);
    }
    if let Some(d) = dcreatedb {
        createdb = defel_bool(d);
    }
    if let Some(d) = dcanlogin {
        canlogin = defel_bool(d);
    }
    if let Some(d) = disreplication {
        isreplication = defel_bool(d);
    }
    if let Some(d) = dconnlimit {
        connlimit = defel_int(d);
        if connlimit < -1 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("invalid connection limit: {connlimit}"))
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
    }
    if let Some(d) = daddroleto {
        addroleto = defel_rolespec_list(d)?;
    }
    if let Some(d) = drolemembers {
        rolemembers = defel_rolespec_list(d)?;
    }
    if let Some(d) = dadminmembers {
        adminmembers = defel_rolespec_list(d)?;
    }
    if let Some(d) = dvalidUntil {
        validUntil = Some(defel_str(d));
    }
    if let Some(d) = dbypassRLS {
        bypassrls = defel_bool(d);
    }

    /* Check some permissions first */
    if !seam::superuser_arg::call(currentUserId)? {
        if !seam::has_createrole_privilege::call(currentUserId)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to create role")
                .errdetail(format!(
                    "Only roles with the {} attribute may create roles.",
                    "CREATEROLE"
                ))
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
        if issuper {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to create role")
                .errdetail(format!(
                    "Only roles with the {} attribute may create roles with the {} attribute.",
                    "SUPERUSER", "SUPERUSER"
                ))
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
        if createdb && !seam::have_createdb_privilege::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to create role")
                .errdetail(format!(
                    "Only roles with the {} attribute may create roles with the {} attribute.",
                    "CREATEDB", "CREATEDB"
                ))
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
        if isreplication && !seam::has_rolreplication::call(currentUserId)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to create role")
                .errdetail(format!(
                    "Only roles with the {} attribute may create roles with the {} attribute.",
                    "REPLICATION", "REPLICATION"
                ))
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
        if bypassrls && !seam::has_bypassrls_privilege::call(currentUserId)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to create role")
                .errdetail(format!(
                    "Only roles with the {} attribute may create roles with the {} attribute.",
                    "BYPASSRLS", "BYPASSRLS"
                ))
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
    }

    /*
     * Check that the user is not trying to create a role in the reserved
     * "pg_" namespace.
     */
    if seam::is_reserved_name::call(role.clone())? {
        return ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("role name \"{role}\" is reserved"))
            .errdetail("Role names starting with \"pg_\" are reserved.")
            .finish(here("CreateRole"))
            .map(|()| InvalidOid);
    }

    /*
     * Check the pg_authid relation to be certain the role doesn't already
     * exist.
     */
    let pg_authid_rel = seam::table_open::call(AuthIdRelationId, RowExclusiveLock)?;

    if OidIsValid(seam::get_role_oid::call(role.clone(), true)?) {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("role \"{role}\" already exists"))
            .finish(here("CreateRole"))
            .map(|()| InvalidOid);
    }

    /* Convert validuntil to internal form */
    let validUntil_null;
    if let Some(vu) = &validUntil {
        validUntil_datum = Some(seam::timestamptz_in::call(vu.clone())?);
        validUntil_null = false;
    } else {
        validUntil_datum = None;
        validUntil_null = true;
    }

    /*
     * Call the password checking hook if there is one defined
     */
    if seam::has_check_password_hook::call()? {
        if let Some(pw) = &password {
            let ptype = seam::get_password_type::call(pw.clone())?;
            seam::call_check_password_hook::call(role.clone(), pw.clone(), ptype, validUntil_datum)?;
        }
    }

    /*
     * Build a tuple to insert
     */
    let rolpassword: Option<String>;
    if let Some(pw) = &password {
        /*
         * Don't allow an empty password.  By clearing the password when an
         * empty string is specified, the account is consistently locked for
         * all clients.
         */
        if pw.is_empty()
            || seam::plain_crypt_verify::call(role.clone(), pw.clone(), String::new())? == STATUS_OK
        {
            ereport(NOTICE)
                .errmsg("empty string is not a valid password, clearing password")
                .finish(here("CreateRole"))?;
            rolpassword = None;
        } else {
            /* Encrypt the password to the requested format. */
            let shadow_pass = seam::encrypt_password::call(
                mcx,
                seam::password_encryption::call()?,
                role.clone(),
                pw.clone(),
            )?;
            rolpassword = Some(shadow_pass.to_string());
        }
    } else {
        rolpassword = None;
    }

    /*
     * pg_largeobject_metadata contains pg_authid.oid's, so we use the
     * binary-upgrade override.
     */
    let roleid = if seam::is_binary_upgrade::call()? {
        let next = seam::take_binary_upgrade_next_pg_authid_oid::call()?;
        if !OidIsValid(next) {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("pg_authid OID value not set when in binary upgrade mode")
                .finish(here("CreateRole"))
                .map(|()| InvalidOid);
        }
        next
    } else {
        seam::get_new_oid_with_index::call(pg_authid_rel, AuthIdOidIndexId)?
    };

    let new_record = NewAuthRecord {
        rolname: role.clone(),
        rolsuper: issuper,
        rolinherit: inherit,
        rolcreaterole: createrole,
        rolcreatedb: createdb,
        rolcanlogin: canlogin,
        rolreplication: isreplication,
        rolconnlimit: connlimit,
        rolpassword,
        rolvaliduntil: validUntil_datum,
        rolbypassrls: bypassrls,
        oid: roleid,
    };
    let _ = validUntil_null;

    /*
     * Insert new record in the pg_authid table.
     */
    seam::insert_authid::call(pg_authid_rel, new_record)?;

    /*
     * Advance command counter so we can see new record; else tests in
     * AddRoleMems may fail.
     */
    if !addroleto.is_empty() || !adminmembers.is_empty() || !rolemembers.is_empty() {
        seam::command_counter_increment::call()?;
    }

    /* Default grant. */
    let mut popt = InitGrantRoleOptions();

    /*
     * Add the new role to the specified existing roles.
     */
    if !addroleto.is_empty() {
        let thisrole = make_cstring_rolespec(&role);
        let thisrole_list = vec![thisrole];
        let thisrole_oidlist = vec![roleid];

        for oldrole in &addroleto {
            let (oldroletup, oldroleform) = seam::get_rolespec_tuple::call(oldrole.clone())?;
            let oldroleid = oldroleform.oid;
            let oldrolename = oldroleform.rolname.clone();

            /* can only add this role to roles for which you have rights */
            check_role_membership_authorization(mcx, currentUserId, oldroleid, true)?;
            AddRoleMems(
                mcx,
                currentUserId,
                &oldrolename,
                oldroleid,
                &thisrole_list,
                &thisrole_oidlist,
                InvalidOid,
                &popt,
            )?;

            seam::release_sys_cache::call(oldroletup)?;
        }
    }

    /*
     * If the current user isn't a superuser, make them an admin of the new
     * role so that they can administer the new object they just created.
     */
    if !seam::superuser::call()? {
        let current_role = make_current_rolespec();
        let memberIds = vec![currentUserId];
        let memberSpecs = vec![current_role];

        let poptself = GrantRoleOptions {
            specified: GRANT_ROLE_SPECIFIED_ADMIN
                | GRANT_ROLE_SPECIFIED_INHERIT
                | GRANT_ROLE_SPECIFIED_SET,
            admin: true,
            inherit: false,
            set: false,
        };

        AddRoleMems(
            mcx,
            BOOTSTRAP_SUPERUSERID,
            &role,
            roleid,
            &memberSpecs,
            &memberIds,
            BOOTSTRAP_SUPERUSERID,
            &poptself,
        )?;

        /*
         * We must make the implicit grant visible to the code below, else the
         * additional grants will fail.
         */
        seam::command_counter_increment::call()?;

        /*
         * Because of the implicit grant above, a CREATEROLE user who creates a
         * role has the ability to grant that role back to themselves with the
         * INHERIT or SET options.  The createrole_self_grant GUC can be used to
         * make this happen automatically.
         */
        if seam::createrole_self_grant_enabled::call()? {
            let (specified, admin, sg_inherit, sg_set) = seam::createrole_self_grant_options::call()?;
            let self_grant_options = GrantRoleOptions {
                specified,
                admin,
                inherit: sg_inherit,
                set: sg_set,
            };
            AddRoleMems(
                mcx,
                currentUserId,
                &role,
                roleid,
                &memberSpecs,
                &memberIds,
                currentUserId,
                &self_grant_options,
            )?;
        }
    }

    /*
     * Add the specified members to this new role. adminmembers get the admin
     * option, rolemembers don't.
     *
     * NB: No permissions check is required here.
     */
    AddRoleMems(
        mcx,
        currentUserId,
        &role,
        roleid,
        &rolemembers,
        &roleSpecsToIds(&rolemembers)?,
        InvalidOid,
        &popt,
    )?;
    popt.specified |= GRANT_ROLE_SPECIFIED_ADMIN;
    popt.admin = true;
    AddRoleMems(
        mcx,
        currentUserId,
        &role,
        roleid,
        &adminmembers,
        &roleSpecsToIds(&adminmembers)?,
        InvalidOid,
        &popt,
    )?;

    /* Post creation hook for new role */
    seam::invoke_object_post_create_hook_authid::call(roleid)?;

    /*
     * Close pg_authid, but keep lock till commit.
     */
    seam::table_close::call(pg_authid_rel, NoLock)?;

    Ok(roleid)
}

/* =========================================================================
 * AlterRole   (C 618-993)
 * ========================================================================= */

/// `AlterRole(pstate, stmt)` — ALTER ROLE.
pub fn AlterRole<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&ParseState>,
    stmt: &AlterRoleStmt,
) -> PgResult<Oid> {
    let currentUserId = seam::get_user_id::call()?;

    let mut password: Option<String> = None; /* user password */
    let mut connlimit: i32 = -1; /* maximum connections allowed */
    let mut validUntil: Option<String> = None; /* time the login is valid until */

    let mut dpassword: Option<&DefElem> = None;
    let mut dissuper: Option<&DefElem> = None;
    let mut dinherit: Option<&DefElem> = None;
    let mut dcreaterole: Option<&DefElem> = None;
    let mut dcreatedb: Option<&DefElem> = None;
    let mut dcanlogin: Option<&DefElem> = None;
    let mut disreplication: Option<&DefElem> = None;
    let mut dconnlimit: Option<&DefElem> = None;
    let mut drolemembers: Option<&DefElem> = None;
    let mut dvalidUntil: Option<&DefElem> = None;
    let mut dbypassRLS: Option<&DefElem> = None;

    let stmt_role = stmt
        .role
        .as_deref()
        .and_then(node_as_rolespec)
        .unwrap_or_else(empty_rolespec);

    seam::check_rolespec_name::call(stmt_role.clone(), "Cannot alter reserved roles.".to_string())?;

    /* Extract options from the statement node tree */
    for defel in def_elems(&stmt.options) {
        let defname = def_name(defel);
        if defname == "password" {
            if dpassword.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dpassword = Some(defel);
        } else if defname == "superuser" {
            if dissuper.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dissuper = Some(defel);
        } else if defname == "inherit" {
            if dinherit.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dinherit = Some(defel);
        } else if defname == "createrole" {
            if dcreaterole.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dcreaterole = Some(defel);
        } else if defname == "createdb" {
            if dcreatedb.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dcreatedb = Some(defel);
        } else if defname == "canlogin" {
            if dcanlogin.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dcanlogin = Some(defel);
        } else if defname == "isreplication" {
            if disreplication.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            disreplication = Some(defel);
        } else if defname == "connectionlimit" {
            if dconnlimit.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dconnlimit = Some(defel);
        } else if defname == "rolemembers" && stmt.action != 0 {
            if drolemembers.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            drolemembers = Some(defel);
        } else if defname == "validUntil" {
            if dvalidUntil.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dvalidUntil = Some(defel);
        } else if defname == "bypassrls" {
            if dbypassRLS.is_some() {
                return seam::error_conflicting_def_elem::call(defel.clone(), pstate)
                    .map(|()| InvalidOid);
            }
            dbypassRLS = Some(defel);
        } else {
            return ereport(ERROR)
                .errmsg_internal(format!("option \"{defname}\" not recognized"))
                .finish(here("AlterRole"))
                .map(|()| InvalidOid);
        }
    }

    if let Some(d) = dpassword {
        if !defel_arg_is_null(d) {
            password = Some(defel_str(d));
        }
    }
    if let Some(d) = dconnlimit {
        connlimit = defel_int(d);
        if connlimit < -1 {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("invalid connection limit: {connlimit}"))
                .finish(here("AlterRole"))
                .map(|()| InvalidOid);
        }
    }
    if let Some(d) = dvalidUntil {
        validUntil = Some(defel_str(d));
    }

    /*
     * Scan the pg_authid relation to be certain the user exists.
     */
    let pg_authid_rel = seam::table_open::call(AuthIdRelationId, RowExclusiveLock)?;

    let (tuple, authform) = seam::get_rolespec_tuple::call(stmt_role)?;
    let rolename = authform.rolname.clone();
    let roleid = authform.oid;

    /* To mess with a superuser in any way you gotta be superuser. */
    if !seam::superuser::call()? && authform.rolsuper {
        return finish_alter_err(
            "permission denied to alter role",
            format!(
                "Only roles with the {} attribute may alter roles with the {} attribute.",
                "SUPERUSER", "SUPERUSER"
            ),
            "AlterRole",
        );
    }
    if !seam::superuser::call()? && dissuper.is_some() {
        return finish_alter_err(
            "permission denied to alter role",
            format!(
                "Only roles with the {} attribute may change the {} attribute.",
                "SUPERUSER", "SUPERUSER"
            ),
            "AlterRole",
        );
    }

    /*
     * Most changes to a role require that you both have CREATEROLE privileges
     * and also ADMIN OPTION on the role.
     */
    if !have_createrole_privilege()?
        || !seam::is_admin_of_role::call(seam::get_user_id::call()?, roleid)?
    {
        /* things an unprivileged user certainly can't do */
        if dinherit.is_some()
            || dcreaterole.is_some()
            || dcreatedb.is_some()
            || dcanlogin.is_some()
            || dconnlimit.is_some()
            || dvalidUntil.is_some()
            || disreplication.is_some()
            || dbypassRLS.is_some()
        {
            return finish_alter_err(
                "permission denied to alter role",
                format!(
                    "Only roles with the {} attribute and the {} option on role \"{}\" may alter this role.",
                    "CREATEROLE", "ADMIN", rolename
                ),
                "AlterRole",
            );
        }

        /* an unprivileged user can change their own password */
        if dpassword.is_some() && roleid != currentUserId {
            return finish_alter_err(
                "permission denied to alter role",
                format!(
                    "To change another role's password, the current user must have the {} attribute and the {} option on the role.",
                    "CREATEROLE", "ADMIN"
                ),
                "AlterRole",
            );
        }
    } else if !seam::superuser::call()? {
        /*
         * Even if you have both CREATEROLE and ADMIN OPTION on a role, you can
         * only change the CREATEDB, REPLICATION, or BYPASSRLS attributes if
         * they are set for your own role (or you are the superuser).
         */
        if dcreatedb.is_some() && !seam::have_createdb_privilege::call()? {
            return finish_alter_err(
                "permission denied to alter role",
                format!(
                    "Only roles with the {} attribute may change the {} attribute.",
                    "CREATEDB", "CREATEDB"
                ),
                "AlterRole",
            );
        }
        if disreplication.is_some() && !seam::has_rolreplication::call(currentUserId)? {
            return finish_alter_err(
                "permission denied to alter role",
                format!(
                    "Only roles with the {} attribute may change the {} attribute.",
                    "REPLICATION", "REPLICATION"
                ),
                "AlterRole",
            );
        }
        if dbypassRLS.is_some() && !seam::has_bypassrls_privilege::call(currentUserId)? {
            return finish_alter_err(
                "permission denied to alter role",
                format!(
                    "Only roles with the {} attribute may change the {} attribute.",
                    "BYPASSRLS", "BYPASSRLS"
                ),
                "AlterRole",
            );
        }
    }

    /* To add or drop members, you need ADMIN OPTION. */
    if drolemembers.is_some() && !seam::is_admin_of_role::call(currentUserId, roleid)? {
        return finish_alter_err(
            "permission denied to alter role",
            format!(
                "Only roles with the {} option on role \"{}\" may add or drop members.",
                "ADMIN", rolename
            ),
            "AlterRole",
        );
    }

    /* Convert validuntil to internal form */
    let validUntil_datum: Option<TimestampTz> = if dvalidUntil.is_some() {
        Some(seam::timestamptz_in::call(validUntil.clone().unwrap_or_default())?)
    } else {
        /* fetch existing setting in case hook needs it */
        seam::authid_validuntil::call(tuple)?
    };

    /*
     * Call the password checking hook if there is one defined
     */
    if seam::has_check_password_hook::call()? {
        if let Some(pw) = &password {
            let ptype = seam::get_password_type::call(pw.clone())?;
            seam::call_check_password_hook::call(rolename.clone(), pw.clone(), ptype, validUntil_datum)?;
        }
    }

    /*
     * Build an updated tuple, perusing the information just obtained
     */
    let mut upd = AuthIdUpdate::default();

    /* issuper/createrole/etc */
    if let Some(d) = dissuper {
        let should_be_super = defel_bool(d);

        if !should_be_super && roleid == BOOTSTRAP_SUPERUSERID {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("permission denied to alter role")
                .errdetail(format!(
                    "The bootstrap superuser must have the {} attribute.",
                    "SUPERUSER"
                ))
                .finish(here("AlterRole"))
                .map(|()| InvalidOid);
        }

        upd.rolsuper = Some(should_be_super);
    }

    if let Some(d) = dinherit {
        upd.rolinherit = Some(defel_bool(d));
    }
    if let Some(d) = dcreaterole {
        upd.rolcreaterole = Some(defel_bool(d));
    }
    if let Some(d) = dcreatedb {
        upd.rolcreatedb = Some(defel_bool(d));
    }
    if let Some(d) = dcanlogin {
        upd.rolcanlogin = Some(defel_bool(d));
    }
    if let Some(d) = disreplication {
        upd.rolreplication = Some(defel_bool(d));
    }
    if dconnlimit.is_some() {
        upd.rolconnlimit = Some(connlimit);
    }

    /* password */
    if let Some(pw) = &password {
        /* Like in CREATE USER, don't allow an empty password. */
        if pw.is_empty()
            || seam::plain_crypt_verify::call(rolename.clone(), pw.clone(), String::new())?
                == STATUS_OK
        {
            ereport(NOTICE)
                .errmsg("empty string is not a valid password, clearing password")
                .finish(here("AlterRole"))?;
            upd.rolpassword = Some(None);
        } else {
            /* Encrypt the password to the requested format. */
            let shadow_pass = seam::encrypt_password::call(
                mcx,
                seam::password_encryption::call()?,
                rolename.clone(),
                pw.clone(),
            )?;
            upd.rolpassword = Some(Some(shadow_pass.to_string()));
        }
    }

    /* unset password */
    if let Some(d) = dpassword {
        if defel_arg_is_null(d) {
            upd.rolpassword = Some(None);
        }
    }

    /* valid until */
    upd.rolvaliduntil = Some(validUntil_datum);

    if let Some(d) = dbypassRLS {
        upd.rolbypassrls = Some(defel_bool(d));
    }

    seam::update_authid::call(pg_authid_rel, tuple, upd)?;

    seam::invoke_object_post_alter_hook_authid::call(roleid)?;

    seam::release_sys_cache::call(tuple)?;

    let popt = InitGrantRoleOptions();

    /*
     * Advance command counter so we can see new record; else tests in
     * AddRoleMems may fail.
     */
    if let Some(d) = drolemembers {
        let rolemember_specs = defel_rolespec_list(d)?;

        seam::command_counter_increment::call()?;

        if stmt.action == 1 {
            /* add members to role */
            AddRoleMems(
                mcx,
                currentUserId,
                &rolename,
                roleid,
                &rolemember_specs,
                &roleSpecsToIds(&rolemember_specs)?,
                InvalidOid,
                &popt,
            )?;
        } else if stmt.action == -1 {
            /* drop members from role */
            DelRoleMems(
                mcx,
                currentUserId,
                &rolename,
                roleid,
                &rolemember_specs,
                &roleSpecsToIds(&rolemember_specs)?,
                InvalidOid,
                &popt,
                DROP_RESTRICT,
            )?;
        }
    }

    /*
     * Close pg_authid, but keep lock till commit.
     */
    seam::table_close::call(pg_authid_rel, NoLock)?;

    Ok(roleid)
}

/// Shared `ereport(ERROR, (errcode(INSUFFICIENT_PRIVILEGE), errmsg(msg),
/// errdetail(detail)))` returning `InvalidOid`.
fn finish_alter_err(msg: &str, detail: String, fname: &'static str) -> PgResult<Oid> {
    ereport(ERROR)
        .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
        .errmsg(msg.to_string())
        .errdetail(detail)
        .finish(here(fname))
        .map(|()| InvalidOid)
}

/* =========================================================================
 * AlterRoleSet   (C 999-1083)
 * ========================================================================= */

/// `AlterRoleSet(stmt)` — ALTER ROLE … SET.
pub fn AlterRoleSet(stmt: &AlterRoleSetStmt) -> PgResult<Oid> {
    let mut databaseid: Oid = InvalidOid;
    let mut roleid: Oid = InvalidOid;

    let stmt_role = stmt.role.as_deref().and_then(node_as_rolespec);

    if let Some(role_spec) = &stmt_role {
        seam::check_rolespec_name::call(role_spec.clone(), "Cannot alter reserved roles.".to_string())?;

        let (roletuple, roleform) = seam::get_rolespec_tuple::call(role_spec.clone())?;
        roleid = roleform.oid;

        /*
         * Obtain a lock on the role and make sure it didn't go away in the
         * meantime.
         */
        seam::shdep_lock_and_check_object::call(AuthIdRelationId, roleid)?;

        /*
         * To mess with a superuser you gotta be superuser; otherwise you need
         * CREATEROLE plus admin option on the target role; unless you're just
         * trying to change your own settings
         */
        if roleform.rolsuper {
            if !seam::superuser::call()? {
                return ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg("permission denied to alter role")
                    .errdetail(format!(
                        "Only roles with the {} attribute may alter roles with the {} attribute.",
                        "SUPERUSER", "SUPERUSER"
                    ))
                    .finish(here("AlterRoleSet"))
                    .map(|()| InvalidOid);
            }
        } else if (!have_createrole_privilege()?
            || !seam::is_admin_of_role::call(seam::get_user_id::call()?, roleid)?)
            && roleid != seam::get_user_id::call()?
        {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to alter role")
                .errdetail(format!(
                    "Only roles with the {} attribute and the {} option on role \"{}\" may alter this role.",
                    "CREATEROLE", "ADMIN", roleform.rolname
                ))
                .finish(here("AlterRoleSet"))
                .map(|()| InvalidOid);
        }

        seam::release_sys_cache::call(roletuple)?;
    }

    /* look up and lock the database, if specified */
    if let Some(dbname) = &stmt.database {
        databaseid = seam::get_database_oid::call(dbname.clone(), false)?;
        seam::shdep_lock_and_check_object::call(DatabaseRelationId, databaseid)?;

        if stmt_role.is_none() {
            /*
             * If no role is specified, then this is effectively the same as
             * ALTER DATABASE ... SET, so use the same permission check.
             */
            if !seam::object_ownercheck_database::call(databaseid, seam::get_user_id::call()?)? {
                seam::aclcheck_error_not_owner_database::call(dbname.clone())?;
            }
        }
    }

    if stmt_role.is_none() && stmt.database.is_none() {
        /* Must be superuser to alter settings globally. */
        if !seam::superuser::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to alter setting")
                .errdetail(format!(
                    "Only roles with the {} attribute may alter settings globally.",
                    "SUPERUSER"
                ))
                .finish(here("AlterRoleSet"))
                .map(|()| InvalidOid);
        }
    }

    let setstmt = stmt.setstmt.as_deref().cloned();
    seam::alter_setting::call(databaseid, roleid, setstmt)?;

    Ok(roleid)
}

/* =========================================================================
 * DropRole   (C 1089-1328)
 * ========================================================================= */

/// `DropRole(stmt)` — DROP ROLE.
pub fn DropRole(stmt: &DropRoleStmt) -> PgResult<()> {
    let mut role_oids: Vec<Oid> = Vec::new();

    if !have_createrole_privilege()? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to drop role")
            .errdetail(format!(
                "Only roles with the {} attribute and the {} option on the target roles may drop roles.",
                "CREATEROLE", "ADMIN"
            ))
            .finish(here("DropRole"));
    }

    /*
     * Scan the pg_authid relation to find the Oid of the role(s) to be deleted
     * and perform preliminary permissions and sanity checks.
     */
    let pg_authid_rel = seam::table_open::call(AuthIdRelationId, RowExclusiveLock)?;
    let pg_auth_members_rel = seam::table_open::call(AuthMemRelationId, RowExclusiveLock)?;

    for rolspec in rolespecs(&stmt.roles) {
        if rolspec.roletype != ROLESPEC_CSTRING {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg("cannot use special role specifier in DROP ROLE")
                .finish(here("DropRole"));
        }
        let role = rolspec.rolename.clone().unwrap_or_default();

        let tuple = match seam::authid_by_name::call(role.clone())? {
            Some(t) => t,
            None => {
                if !stmt.missing_ok {
                    return ereport(ERROR)
                        .errcode(ERRCODE_UNDEFINED_OBJECT)
                        .errmsg(format!("role \"{role}\" does not exist"))
                        .finish(here("DropRole"));
                } else {
                    ereport(NOTICE)
                        .errmsg(format!("role \"{role}\" does not exist, skipping"))
                        .finish(here("DropRole"))?;
                }
                continue;
            }
        };

        let roleform = seam::authid_form::call(tuple)?;
        let roleid = roleform.oid;

        if roleid == seam::get_user_id::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg("current user cannot be dropped")
                .finish(here("DropRole"));
        }
        if roleid == seam::get_outer_user_id::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg("current user cannot be dropped")
                .finish(here("DropRole"));
        }
        if roleid == seam::get_session_user_id::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_OBJECT_IN_USE)
                .errmsg("session user cannot be dropped")
                .finish(here("DropRole"));
        }

        /*
         * For safety's sake, we allow createrole holders to drop ordinary roles
         * but not superuser roles, and only if they also have ADMIN OPTION.
         */
        if roleform.rolsuper && !seam::superuser::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to drop role")
                .errdetail(format!(
                    "Only roles with the {} attribute may drop roles with the {} attribute.",
                    "SUPERUSER", "SUPERUSER"
                ))
                .finish(here("DropRole"));
        }
        if !seam::is_admin_of_role::call(seam::get_user_id::call()?, roleid)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to drop role")
                .errdetail(format!(
                    "Only roles with the {} attribute and the {} option on role \"{}\" may drop this role.",
                    "CREATEROLE", "ADMIN", roleform.rolname
                ))
                .finish(here("DropRole"));
        }

        /* DROP hook for the role being removed */
        seam::invoke_object_drop_hook_authid::call(roleid)?;

        /* Don't leak the syscache tuple */
        seam::release_sys_cache::call(tuple)?;

        /*
         * Lock the role, so nobody can add dependencies to her while we drop
         * her.  We keep the lock until the end of transaction.
         */
        seam::lock_shared_object_authid::call(roleid, AccessExclusiveLock)?;

        /*
         * If there is a pg_auth_members entry that has one of the roles to be
         * dropped as the roleid or member, it should be silently removed, but
         * if there is a pg_auth_members entry that has one of the roles to be
         * dropped as the grantor, the operation should fail.
         *
         * To make that work, we remove all pg_auth_members entries that can be
         * silently removed in this loop, and then below we'll make a second
         * pass over the list of roles to be removed and check for any remaining
         * dependencies.
         */
        seam::delete_authmem_by_roleid::call(pg_auth_members_rel, roleid)?;
        seam::delete_authmem_by_member::call(pg_auth_members_rel, roleid)?;

        /*
         * Advance command counter so that later iterations of this loop will
         * see the changes already made.
         */
        seam::command_counter_increment::call()?;

        /* Looks tentatively OK, add it to the list if not there yet. */
        if !role_oids.contains(&roleid) {
            role_oids.push(roleid);
        }
    }

    /*
     * Second pass over the roles to be removed.
     */
    for roleid in &role_oids {
        let roleid = *roleid;

        /*
         * Re-find the pg_authid tuple.
         */
        let tuple = match seam::authid_by_oid::call(roleid)? {
            Some(t) => t,
            None => {
                return ereport(ERROR)
                    .errmsg_internal(format!("could not find tuple for role {roleid}"))
                    .finish(here("DropRole"))
            }
        };
        let roleform = seam::authid_form::call(tuple)?;

        /*
         * Check for pg_shdepend entries depending on this role.
         */
        if let Some((detail, detail_log)) = seam::check_shared_dependencies::call(roleid)? {
            return ereport(ERROR)
                .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                .errmsg(format!(
                    "role \"{}\" cannot be dropped because some objects depend on it",
                    roleform.rolname
                ))
                .errdetail_internal(detail)
                .errdetail_log(detail_log)
                .finish(here("DropRole"));
        }

        /*
         * Remove the role from the pg_authid table
         */
        seam::delete_authid::call(pg_authid_rel, tuple)?;

        seam::release_sys_cache::call(tuple)?;

        /*
         * Remove any comments or security labels on this role.
         */
        seam::delete_shared_comments::call(roleid)?;
        seam::delete_shared_security_label::call(roleid)?;

        /*
         * Remove settings for this role.
         */
        seam::drop_setting::call(InvalidOid, roleid)?;
    }

    /*
     * Now we can clean up; but keep locks until commit.
     */
    seam::table_close::call(pg_auth_members_rel, NoLock)?;
    seam::table_close::call(pg_authid_rel, NoLock)?;

    Ok(())
}

/* =========================================================================
 * RenameRole   (C 1333-1472)
 * ========================================================================= */

/// `RenameRole(oldname, newname)` — rename a role.
pub fn RenameRole(oldname: &str, newname: &str) -> PgResult<ObjectAddress> {
    let rel = seam::table_open::call(AuthIdRelationId, RowExclusiveLock)?;

    let oldtuple = match seam::authid_by_name::call(oldname.to_string())? {
        Some(t) => t,
        None => {
            return ereport(ERROR)
                .errcode(ERRCODE_UNDEFINED_OBJECT)
                .errmsg(format!("role \"{oldname}\" does not exist"))
                .finish(here("RenameRole"))
                .map(|()| InvalidObjectAddress);
        }
    };

    let authform = seam::authid_form::call(oldtuple)?;
    let roleid = authform.oid;

    if roleid == seam::get_session_user_id::call()? {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("session user cannot be renamed")
            .finish(here("RenameRole"))
            .map(|()| InvalidObjectAddress);
    }
    if roleid == seam::get_outer_user_id::call()? {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg("current user cannot be renamed")
            .finish(here("RenameRole"))
            .map(|()| InvalidObjectAddress);
    }

    /*
     * Check that the user is not trying to rename a system role and not trying
     * to rename a role into the reserved "pg_" namespace.
     */
    if seam::is_reserved_name::call(authform.rolname.clone())? {
        return ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("role name \"{}\" is reserved", authform.rolname))
            .errdetail("Role names starting with \"pg_\" are reserved.")
            .finish(here("RenameRole"))
            .map(|()| InvalidObjectAddress);
    }

    if seam::is_reserved_name::call(newname.to_string())? {
        return ereport(ERROR)
            .errcode(ERRCODE_RESERVED_NAME)
            .errmsg(format!("role name \"{newname}\" is reserved"))
            .errdetail("Role names starting with \"pg_\" are reserved.")
            .finish(here("RenameRole"))
            .map(|()| InvalidObjectAddress);
    }

    /* make sure the new name doesn't exist */
    if seam::authid_exists_by_name::call(newname.to_string())? {
        return ereport(ERROR)
            .errcode(ERRCODE_DUPLICATE_OBJECT)
            .errmsg(format!("role \"{newname}\" already exists"))
            .finish(here("RenameRole"))
            .map(|()| InvalidObjectAddress);
    }

    /*
     * Only superusers can mess with superusers. Otherwise, a user with
     * CREATEROLE can rename a role for which they have ADMIN OPTION.
     */
    if authform.rolsuper {
        if !seam::superuser::call()? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to rename role")
                .errdetail(format!(
                    "Only roles with the {} attribute may rename roles with the {} attribute.",
                    "SUPERUSER", "SUPERUSER"
                ))
                .finish(here("RenameRole"))
                .map(|()| InvalidObjectAddress);
        }
    } else if !have_createrole_privilege()?
        || !seam::is_admin_of_role::call(seam::get_user_id::call()?, roleid)?
    {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to rename role")
            .errdetail(format!(
                "Only roles with the {} attribute and the {} option on role \"{}\" may rename this role.",
                "CREATEROLE", "ADMIN", authform.rolname
            ))
            .finish(here("RenameRole"))
            .map(|()| InvalidObjectAddress);
    }

    /* OK, construct the modified tuple */
    let datum = seam::authid_password::call(oldtuple)?;
    let clear_md5 = match &datum {
        Some(pw) => seam::get_password_type::call(pw.clone())? == PasswordType::Md5,
        None => false,
    };

    if clear_md5 {
        /* MD5 uses the username as salt, so just clear it on a rename */
        ereport(NOTICE)
            .errmsg("MD5 password cleared because of role rename")
            .finish(here("RenameRole"))?;
    }

    seam::rename_authid::call(rel, oldtuple, newname.to_string(), clear_md5)?;

    seam::invoke_object_post_alter_hook_authid::call(roleid)?;

    let address = ObjectAddressSet(AuthIdRelationId, roleid);

    seam::release_sys_cache::call(oldtuple)?;

    /*
     * Close pg_authid, but keep lock till commit.
     */
    seam::table_close::call(rel, NoLock)?;

    Ok(address)
}

/* =========================================================================
 * GrantRole   (C 1479-1575)
 * ========================================================================= */

/// `GrantRole(pstate, stmt)` — GRANT/REVOKE role membership.
pub fn GrantRole<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&ParseState>,
    stmt: &GrantRoleStmt,
) -> PgResult<()> {
    let currentUserId = seam::get_user_id::call()?;

    /* Parse options list. */
    let mut popt = InitGrantRoleOptions();
    for opt in def_elems(&stmt.opt) {
        let optval = seam::def_get_string::call(opt.clone())?;
        let defname = def_name(opt);

        let mut handled = false;
        if defname == "admin" {
            popt.specified |= GRANT_ROLE_SPECIFIED_ADMIN;
            if let Some(b) = parse_bool(&optval) {
                popt.admin = b;
                handled = true;
            }
        } else if defname == "inherit" {
            popt.specified |= GRANT_ROLE_SPECIFIED_INHERIT;
            if let Some(b) = parse_bool(&optval) {
                popt.inherit = b;
                handled = true;
            }
        } else if defname == "set" {
            popt.specified |= GRANT_ROLE_SPECIFIED_SET;
            if let Some(b) = parse_bool(&optval) {
                popt.set = b;
                handled = true;
            }
        } else {
            let _ = seam::parser_errposition::call(pstate, opt.location)?;
            return ereport(ERROR)
                .errcode(ERRCODE_SYNTAX_ERROR)
                .errmsg(format!("unrecognized role option \"{defname}\""))
                .finish(here("GrantRole"));
        }

        if handled {
            continue;
        }

        let _ = seam::parser_errposition::call(pstate, opt.location)?;
        return ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg(format!(
                "unrecognized value for role option \"{defname}\": \"{optval}\""
            ))
            .finish(here("GrantRole"));
    }

    /* Lookup OID of grantor, if specified. */
    let grantor = match stmt.grantor.as_deref().and_then(node_as_rolespec) {
        Some(g) => seam::get_rolespec_oid::call(g, false)?,
        None => InvalidOid,
    };

    let grantee_specs = rolespecs(&stmt.grantee_roles);
    let grantee_ids = roleSpecsToIds(&grantee_specs)?;

    /* AccessShareLock is enough since we aren't modifying pg_authid */
    let pg_authid_rel = seam::table_open::call(AuthIdRelationId, AccessShareLock)?;

    /*
     * Step through all of the granted roles and add, update, or remove entries
     * in pg_auth_members as appropriate.
     */
    for priv_ in &stmt.granted_roles {
        let Some(priv_ref) = node_as_accesspriv(priv_) else {
            return ereport(ERROR)
                .errmsg_internal("GrantRole: granted_roles element is not an AccessPriv")
                .finish(here("GrantRole"));
        };
        let rolename = priv_ref.priv_name.clone().unwrap_or_default();

        /* Must reject priv(columns) and ALL PRIVILEGES(columns) */
        if priv_ref.priv_name.is_none() || !priv_ref.cols.is_empty() {
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                .errmsg("column names cannot be included in GRANT/REVOKE ROLE")
                .finish(here("GrantRole"));
        }

        let roleid = seam::get_role_oid::call(rolename.clone(), false)?;
        check_role_membership_authorization(mcx, currentUserId, roleid, stmt.is_grant)?;
        if stmt.is_grant {
            AddRoleMems(
                mcx,
                currentUserId,
                &rolename,
                roleid,
                &grantee_specs,
                &grantee_ids,
                grantor,
                &popt,
            )?;
        } else {
            DelRoleMems(
                mcx,
                currentUserId,
                &rolename,
                roleid,
                &grantee_specs,
                &grantee_ids,
                grantor,
                &popt,
                stmt.behavior,
            )?;
        }
    }

    /*
     * Close pg_authid, but keep lock till commit.
     */
    seam::table_close::call(pg_authid_rel, NoLock)?;

    Ok(())
}

/* =========================================================================
 * DropOwnedObjects   (C 1582-1603)
 * ========================================================================= */

/// `DropOwnedObjects(stmt)` — DROP OWNED BY.
pub fn DropOwnedObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropOwnedStmt) -> PgResult<()> {
    let role_ids = roleSpecsToIds(&rolespecs(&stmt.roles))?;

    /* Check privileges */
    for roleid in &role_ids {
        let roleid = *roleid;
        if !seam::has_privs_of_role::call(seam::get_user_id::call()?, roleid)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to drop objects")
                .errdetail(format!(
                    "Only roles with privileges of role \"{}\" may drop objects owned by it.",
                    seam::get_user_name_from_id::call(mcx, roleid, false)?
                ))
                .finish(here("DropOwnedObjects"));
        }
    }

    /* Ok, do it */
    seam::shdep_drop_owned::call(role_ids, stmt.behavior)?;

    Ok(())
}

/* =========================================================================
 * ReassignOwnedObjects   (C 1610-1642)
 * ========================================================================= */

/// `ReassignOwnedObjects(stmt)` — REASSIGN OWNED BY.
pub fn ReassignOwnedObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &ReassignOwnedStmt) -> PgResult<()> {
    let role_ids = roleSpecsToIds(&rolespecs(&stmt.roles))?;

    /* Check privileges */
    for roleid in &role_ids {
        let roleid = *roleid;
        if !seam::has_privs_of_role::call(seam::get_user_id::call()?, roleid)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg("permission denied to reassign objects")
                .errdetail(format!(
                    "Only roles with privileges of role \"{}\" may reassign objects owned by it.",
                    seam::get_user_name_from_id::call(mcx, roleid, false)?
                ))
                .finish(here("ReassignOwnedObjects"));
        }
    }

    /* Must have privileges on the receiving side too */
    let newrole_spec = stmt
        .newrole
        .as_deref()
        .and_then(node_as_rolespec)
        .unwrap_or_else(empty_rolespec);
    let newrole = seam::get_rolespec_oid::call(newrole_spec, false)?;

    if !seam::has_privs_of_role::call(seam::get_user_id::call()?, newrole)? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg("permission denied to reassign objects")
            .errdetail(format!(
                "Only roles with privileges of role \"{}\" may reassign objects to it.",
                seam::get_user_name_from_id::call(mcx, newrole, false)?
            ))
            .finish(here("ReassignOwnedObjects"));
    }

    /* Ok, do it */
    seam::shdep_reassign_owned::call(role_ids, newrole)?;

    Ok(())
}

/* =========================================================================
 * roleSpecsToIds   (C 1651-1666)
 * ========================================================================= */

/// `roleSpecsToIds(memberNames)` — given a list of `RoleSpec`, generate a list
/// of role OIDs in the same order.  `ROLESPEC_PUBLIC` is not allowed.
pub fn roleSpecsToIds(memberNames: &[RoleSpec]) -> PgResult<Vec<Oid>> {
    let mut result: Vec<Oid> = Vec::new();
    for rolespec in memberNames {
        let roleid = seam::get_rolespec_oid::call(rolespec.clone(), false)?;
        result.push(roleid);
    }
    Ok(result)
}

/* =========================================================================
 * AddRoleMems   (C 1680-1965)
 * ========================================================================= */

/// `AddRoleMems` — add given members to the specified role.
fn AddRoleMems<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    rolename: &str,
    roleid: Oid,
    memberSpecs: &[RoleSpec],
    memberIds: &[Oid],
    grantorId: Oid,
    popt: &GrantRoleOptions,
) -> PgResult<()> {
    debug_assert_eq!(memberSpecs.len(), memberIds.len());

    /* Validate grantor (and resolve implicit grantor if not specified). */
    let grantorId = check_role_grantor(mcx, currentUserId, roleid, grantorId, true)?;

    let pg_authmem_rel = seam::table_open::call(AuthMemRelationId, RowExclusiveLock)?;

    /*
     * Only allow changes to this role by one backend at a time, so that we can
     * check integrity constraints like the lack of circular ADMIN OPTION grants
     * without fear of race conditions.
     */
    seam::lock_shared_object_authid::call(roleid, ShareUpdateExclusiveLock)?;

    /* Preliminary sanity checks. */
    for (memberRole, iditem) in memberSpecs.iter().zip(memberIds.iter()) {
        let memberid = *iditem;

        /*
         * pg_database_owner is never a role member.
         */
        if memberid == ROLE_PG_DATABASE_OWNER {
            let name = seam::get_rolespec_name::call(mcx, memberRole.clone())?;
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!("role \"{name}\" cannot be a member of any role"))
                .finish(here("AddRoleMems"));
        }

        /*
         * Refuse creation of membership loops, including the trivial case where
         * a role is made a member of itself.
         */
        if seam::is_member_of_role_nosuper::call(roleid, memberid)? {
            let name = seam::get_rolespec_name::call(mcx, memberRole.clone())?;
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                .errmsg(format!("role \"{rolename}\" is a member of role \"{name}\""))
                .finish(here("AddRoleMems"));
        }
    }

    /*
     * Disallow attempts to grant ADMIN OPTION back to a user who granted it to
     * you, similar to what check_circularity does for ACLs.
     */
    if popt.admin && grantorId != BOOTSTRAP_SUPERUSERID {
        /* Get the list of members for this role. */
        let (memlist, members) = seam::authmem_list_by_role::call(roleid)?;

        /*
         * Figure out what would happen if we removed all existing grants to
         * every role to which we've been asked to make a new grant.
         */
        let mut actions = initialize_revoke_actions(&members);
        for iditem in memberIds.iter() {
            let memberid = *iditem;
            if memberid == BOOTSTRAP_SUPERUSERID {
                seam::release_sys_cache_list::call(memlist)?;
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                    .errmsg(format!(
                        "{} option cannot be granted back to your own grantor",
                        "ADMIN"
                    ))
                    .finish(here("AddRoleMems"));
            }
            plan_member_revoke(&members, &mut actions, memberid);
        }

        /*
         * If the result would be that the grantor role would no longer have the
         * ability to perform the grant, then the proposed grant would create a
         * circularity.
         */
        let mut i = 0;
        while i < members.len() {
            let authmem_form = members[i];
            if actions[i] == RRG_NOOP && authmem_form.member == grantorId && authmem_form.admin_option
            {
                break;
            }
            i += 1;
        }
        if i >= members.len() {
            seam::release_sys_cache_list::call(memlist)?;
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_GRANT_OPERATION)
                .errmsg(format!(
                    "{} option cannot be granted back to your own grantor",
                    "ADMIN"
                ))
                .finish(here("AddRoleMems"));
        }

        seam::release_sys_cache_list::call(memlist)?;
    }

    /* Now perform the catalog updates. */
    for (memberRole, iditem) in memberSpecs.iter().zip(memberIds.iter()) {
        let memberid = *iditem;

        /* Find any existing tuple */
        let authmem_tuple = seam::authmem_by_keys::call(roleid, memberid, grantorId)?;

        /*
         * If we found a tuple, update it with new option values, unless there
         * are no changes, in which case issue a WARNING.  If we didn't find a
         * tuple, just insert one.
         */
        if let Some(authmem_tuple) = authmem_tuple {
            let authmem_form = seam::authmem_form::call(authmem_tuple)?;
            let mut update = AuthMemUpdate::default();
            let mut at_least_one_change = false;

            if (popt.specified & GRANT_ROLE_SPECIFIED_ADMIN) != 0
                && authmem_form.admin_option != popt.admin
            {
                update.admin_option = Some(popt.admin);
                at_least_one_change = true;
            }

            if (popt.specified & GRANT_ROLE_SPECIFIED_INHERIT) != 0
                && authmem_form.inherit_option != popt.inherit
            {
                update.inherit_option = Some(popt.inherit);
                at_least_one_change = true;
            }

            if (popt.specified & GRANT_ROLE_SPECIFIED_SET) != 0
                && authmem_form.set_option != popt.set
            {
                update.set_option = Some(popt.set);
                at_least_one_change = true;
            }

            if !at_least_one_change {
                let mname = seam::get_rolespec_name::call(mcx, memberRole.clone())?;
                ereport(NOTICE)
                    .errmsg(format!(
                        "role \"{}\" has already been granted membership in role \"{}\" by role \"{}\"",
                        mname,
                        rolename,
                        seam::get_user_name_from_id::call(mcx, grantorId, false)?
                    ))
                    .finish(here("AddRoleMems"))?;
                seam::release_sys_cache::call(authmem_tuple)?;
                continue;
            }

            seam::update_authmem_by_tuple::call(pg_authmem_rel, authmem_tuple, update)?;

            seam::release_sys_cache::call(authmem_tuple)?;
        } else {
            /*
             * The values for admin/set can be taken directly from 'popt'.
             */
            let admin_option = popt.admin;
            let set_option = popt.set;

            /*
             * If the user specified a value for the inherit option, use whatever
             * was specified.  Otherwise, set the default value based on the
             * role-level property.
             */
            let inherit_option = if (popt.specified & GRANT_ROLE_SPECIFIED_INHERIT) != 0 {
                popt.inherit
            } else {
                let mrtup = match seam::authid_by_oid::call(memberid)? {
                    Some(t) => t,
                    None => {
                        return ereport(ERROR)
                            .errmsg_internal(format!("cache lookup failed for role {memberid}"))
                            .finish(here("AddRoleMems"))
                    }
                };
                let mrform = seam::authid_form::call(mrtup)?;
                let v = mrform.rolinherit;
                seam::release_sys_cache::call(mrtup)?;
                v
            };

            /* get an OID for the new row and insert it */
            let objectId = seam::get_new_oid_with_index::call(pg_authmem_rel, AuthMemOidIndexId)?;
            let rec = NewAuthMemRecord {
                oid: objectId,
                roleid,
                member: memberid,
                grantor: grantorId,
                admin_option,
                inherit_option,
                set_option,
            };

            /*
             * Insert it; the seam then performs
             * `updateAclDependencies(... 1, {grantorId})`.
             */
            seam::insert_authmem::call(pg_authmem_rel, rec)?;
        }

        /* CCI after each change, in case there are duplicates in list */
        seam::command_counter_increment::call()?;
    }

    /*
     * Close pg_authmem, but keep lock till commit.
     */
    seam::table_close::call(pg_authmem_rel, NoLock)?;

    Ok(())
}

/* =========================================================================
 * DelRoleMems   (C 1978-2104)
 * ========================================================================= */

/// `DelRoleMems` — remove given members from the specified role.
fn DelRoleMems<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    rolename: &str,
    roleid: Oid,
    memberSpecs: &[RoleSpec],
    memberIds: &[Oid],
    grantorId: Oid,
    popt: &GrantRoleOptions,
    behavior: DropBehavior,
) -> PgResult<()> {
    debug_assert_eq!(memberSpecs.len(), memberIds.len());

    /* Validate grantor (and resolve implicit grantor if not specified). */
    let grantorId = check_role_grantor(mcx, currentUserId, roleid, grantorId, false)?;

    let pg_authmem_rel = seam::table_open::call(AuthMemRelationId, RowExclusiveLock)?;

    /*
     * Only allow changes to this role by one backend at a time.
     */
    seam::lock_shared_object_authid::call(roleid, ShareUpdateExclusiveLock)?;

    let (memlist, members) = seam::authmem_list_by_role::call(roleid)?;
    let mut actions = initialize_revoke_actions(&members);

    /*
     * We may need to recurse to dependent privileges if DROP_CASCADE was
     * specified, or refuse to perform the operation if dependent privileges
     * exist and DROP_RESTRICT was specified.
     */
    for (memberRole, iditem) in memberSpecs.iter().zip(memberIds.iter()) {
        let memberid = *iditem;

        if !plan_single_revoke(&members, &mut actions, memberid, grantorId, popt, behavior)? {
            let mname = seam::get_rolespec_name::call(mcx, memberRole.clone())?;
            ereport(WARNING)
                .errmsg(format!(
                    "role \"{}\" has not been granted membership in role \"{}\" by role \"{}\"",
                    mname,
                    rolename,
                    seam::get_user_name_from_id::call(mcx, grantorId, false)?
                ))
                .finish(here("DelRoleMems"))?;
            continue;
        }
    }

    /*
     * We now know what to do with each catalog tuple: it should either be left
     * alone, deleted, or just have an option flag cleared.
     */
    for i in 0..members.len() {
        if actions[i] == RRG_NOOP {
            continue;
        }

        if actions[i] == RRG_DELETE_GRANT {
            /*
             * Remove the entry altogether, after first removing its
             * dependencies.
             */
            seam::delete_authmem_in_list::call(pg_authmem_rel, memlist, i)?;
        } else {
            /* Just turn off the specified option */
            let mut update = AuthMemUpdate::default();

            if actions[i] == RRG_REMOVE_ADMIN_OPTION {
                update.admin_option = Some(false);
            } else if actions[i] == RRG_REMOVE_INHERIT_OPTION {
                update.inherit_option = Some(false);
            } else if actions[i] == RRG_REMOVE_SET_OPTION {
                update.set_option = Some(false);
            } else {
                seam::release_sys_cache_list::call(memlist)?;
                return ereport(ERROR)
                    .errmsg_internal("unknown role revoke action")
                    .finish(here("DelRoleMems"));
            }

            seam::update_authmem::call(pg_authmem_rel, memlist, i, update)?;
        }
    }

    seam::release_sys_cache_list::call(memlist)?;

    /*
     * Close pg_authmem, but keep lock till commit.
     */
    seam::table_close::call(pg_authmem_rel, NoLock)?;

    Ok(())
}

/* =========================================================================
 * check_role_membership_authorization   (C 2110-2173)
 * ========================================================================= */

/// `check_role_membership_authorization` — verify currentUserId may modify the
/// membership list for roleid.  Throws an error if not.
fn check_role_membership_authorization<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    roleid: Oid,
    is_grant: bool,
) -> PgResult<()> {
    /*
     * The charter of pg_database_owner is to have exactly one, implicit,
     * situation-dependent member.
     */
    if is_grant && roleid == ROLE_PG_DATABASE_OWNER {
        return ereport(ERROR)
            .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
            .errmsg(format!(
                "role \"{}\" cannot have explicit members",
                seam::get_user_name_from_id::call(mcx, roleid, false)?
            ))
            .finish(here("check_role_membership_authorization"));
    }

    /* To mess with a superuser role, you gotta be superuser. */
    if seam::superuser_arg::call(roleid)? {
        if !seam::superuser_arg::call(currentUserId)? {
            if is_grant {
                return ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!(
                        "permission denied to grant role \"{}\"",
                        seam::get_user_name_from_id::call(mcx, roleid, false)?
                    ))
                    .errdetail(format!(
                        "Only roles with the {} attribute may grant roles with the {} attribute.",
                        "SUPERUSER", "SUPERUSER"
                    ))
                    .finish(here("check_role_membership_authorization"));
            } else {
                return ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!(
                        "permission denied to revoke role \"{}\"",
                        seam::get_user_name_from_id::call(mcx, roleid, false)?
                    ))
                    .errdetail(format!(
                        "Only roles with the {} attribute may revoke roles with the {} attribute.",
                        "SUPERUSER", "SUPERUSER"
                    ))
                    .finish(here("check_role_membership_authorization"));
            }
        }
    } else {
        /*
         * Otherwise, must have admin option on the role to be changed.
         */
        if !seam::is_admin_of_role::call(currentUserId, roleid)? {
            if is_grant {
                return ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!(
                        "permission denied to grant role \"{}\"",
                        seam::get_user_name_from_id::call(mcx, roleid, false)?
                    ))
                    .errdetail(format!(
                        "Only roles with the {} option on role \"{}\" may grant this role.",
                        "ADMIN",
                        seam::get_user_name_from_id::call(mcx, roleid, false)?
                    ))
                    .finish(here("check_role_membership_authorization"));
            } else {
                return ereport(ERROR)
                    .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                    .errmsg(format!(
                        "permission denied to revoke role \"{}\"",
                        seam::get_user_name_from_id::call(mcx, roleid, false)?
                    ))
                    .errdetail(format!(
                        "Only roles with the {} option on role \"{}\" may revoke this role.",
                        "ADMIN",
                        seam::get_user_name_from_id::call(mcx, roleid, false)?
                    ))
                    .finish(here("check_role_membership_authorization"));
            }
        }
    }

    Ok(())
}

/* =========================================================================
 * check_role_grantor   (C 2204-2279)
 * ========================================================================= */

/// `check_role_grantor` — sanity-check, or infer, the grantor for a GRANT or
/// REVOKE statement targeting a role.  Returns the OID to record as grantor.
fn check_role_grantor<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    roleid: Oid,
    grantorId: Oid,
    is_grant: bool,
) -> PgResult<Oid> {
    /* If the grantor ID was not specified, pick one to use. */
    if !OidIsValid(grantorId) {
        /*
         * Grants where the grantor is recorded as the bootstrap superuser do
         * not depend on any other existing grants.
         */
        if seam::superuser_arg::call(currentUserId)? {
            return Ok(BOOTSTRAP_SUPERUSERID);
        }

        /*
         * Otherwise, the grantor must either have ADMIN OPTION on the role or
         * inherit the privileges of a role which does.
         */
        let grantorId = seam::select_best_admin::call(currentUserId, roleid)?;
        if !OidIsValid(grantorId) {
            return ereport(ERROR)
                .errmsg_internal("no possible grantors")
                .finish(here("check_role_grantor"))
                .map(|()| InvalidOid);
        }
        return Ok(grantorId);
    }

    /*
     * If an explicit grantor is specified, it must be a role whose privileges
     * the current user possesses.
     */
    if is_grant {
        if !seam::has_privs_of_role::call(currentUserId, grantorId)? {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied to grant privileges as role \"{}\"",
                    seam::get_user_name_from_id::call(mcx, grantorId, false)?
                ))
                .errdetail(format!(
                    "Only roles with privileges of role \"{}\" may grant privileges as this role.",
                    seam::get_user_name_from_id::call(mcx, grantorId, false)?
                ))
                .finish(here("check_role_grantor"))
                .map(|()| InvalidOid);
        }

        if grantorId != BOOTSTRAP_SUPERUSERID
            && seam::select_best_admin::call(grantorId, roleid)? != grantorId
        {
            return ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
                .errmsg(format!(
                    "permission denied to grant privileges as role \"{}\"",
                    seam::get_user_name_from_id::call(mcx, grantorId, false)?
                ))
                .errdetail(format!(
                    "The grantor must have the {} option on role \"{}\".",
                    "ADMIN",
                    seam::get_user_name_from_id::call(mcx, roleid, false)?
                ))
                .finish(here("check_role_grantor"))
                .map(|()| InvalidOid);
        }
    } else if !seam::has_privs_of_role::call(currentUserId, grantorId)? {
        return ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied to revoke privileges granted by role \"{}\"",
                seam::get_user_name_from_id::call(mcx, grantorId, false)?
            ))
            .errdetail(format!(
                "Only roles with privileges of role \"{}\" may revoke privileges granted by this role.",
                seam::get_user_name_from_id::call(mcx, grantorId, false)?
            ))
            .finish(here("check_role_grantor"))
            .map(|()| InvalidOid);
    }

    /*
     * If a grantor was specified explicitly, always attribute the grant to that
     * role (unless we error out above).
     */
    Ok(grantorId)
}

/* =========================================================================
 * initialize_revoke_actions   (C 2289-2302)
 * ========================================================================= */

/// `initialize_revoke_actions(memlist)` — build a vector of RRG_NOOP actions,
/// one per member grant.
fn initialize_revoke_actions(members: &[AuthMemForm]) -> Vec<RevokeRoleGrantAction> {
    /* C returns NULL for an empty list; an empty Vec is equivalent here. */
    vec![RRG_NOOP; members.len()]
}

/* =========================================================================
 * plan_single_revoke   (C 2320-2379)
 * ========================================================================= */

/// `plan_single_revoke` — figure out what we would need to do to revoke a grant
/// (or just an option), recording the plan in `actions`.  Returns true if the
/// matching grant was found in the list.
fn plan_single_revoke(
    members: &[AuthMemForm],
    actions: &mut [RevokeRoleGrantAction],
    member: Oid,
    grantor: Oid,
    popt: &GrantRoleOptions,
    behavior: DropBehavior,
) -> PgResult<bool> {
    debug_assert!(pg_popcount32(popt.specified) <= 1);

    for i in 0..members.len() {
        let authmem_form = members[i];

        if authmem_form.member == member && authmem_form.grantor == grantor {
            if (popt.specified & GRANT_ROLE_SPECIFIED_INHERIT) != 0 {
                /*
                 * Revoking the INHERIT option doesn't change anything for
                 * dependent privileges, so we don't need to recurse.
                 */
                actions[i] = RRG_REMOVE_INHERIT_OPTION;
            } else if (popt.specified & GRANT_ROLE_SPECIFIED_SET) != 0 {
                /* Here too, no need to recurse. */
                actions[i] = RRG_REMOVE_SET_OPTION;
            } else {
                /*
                 * Revoking the grant entirely, or ADMIN option on a grant,
                 * implicates dependent privileges, so we may need to recurse.
                 */
                let revoke_admin_option_only = (popt.specified & GRANT_ROLE_SPECIFIED_ADMIN) != 0;
                plan_recursive_revoke(members, actions, i, revoke_admin_option_only, behavior)?;
            }
            return Ok(true);
        }
    }

    Ok(false)
}

/* =========================================================================
 * plan_member_revoke   (C 2390-2407)
 * ========================================================================= */

/// `plan_member_revoke` — figure out what we would need to do to revoke all
/// grants to a given member, recording the plan in `actions`.
fn plan_member_revoke(members: &[AuthMemForm], actions: &mut [RevokeRoleGrantAction], member: Oid) {
    for i in 0..members.len() {
        let authmem_form = members[i];
        if authmem_form.member == member {
            /*
             * `plan_member_revoke` always passes DROP_CASCADE, so the recursive
             * planner never raises; the PgResult is therefore always Ok here.
             */
            let _ = plan_recursive_revoke(members, actions, i, false, DROP_CASCADE);
        }
    }
}

/* =========================================================================
 * plan_recursive_revoke   (C 2414-2499)
 * ========================================================================= */

/// `plan_recursive_revoke` — workhorse for figuring out recursive revocation of
/// role grants (similar to `recursive_revoke` for ACLs).
fn plan_recursive_revoke(
    members: &[AuthMemForm],
    actions: &mut [RevokeRoleGrantAction],
    index: usize,
    revoke_admin_option_only: bool,
    behavior: DropBehavior,
) -> PgResult<()> {
    /* If it's already been done, we can just return. */
    if actions[index] == RRG_DELETE_GRANT {
        return Ok(());
    }
    if actions[index] == RRG_REMOVE_ADMIN_OPTION && revoke_admin_option_only {
        return Ok(());
    }

    /* Locate tuple data. */
    let authmem_form = members[index];

    /*
     * If the existing tuple does not have admin_option set, then we do not need
     * to recurse.
     */
    if !revoke_admin_option_only {
        actions[index] = RRG_DELETE_GRANT;
        if !authmem_form.admin_option {
            return Ok(());
        }
    } else {
        if !authmem_form.admin_option {
            return Ok(());
        }
        actions[index] = RRG_REMOVE_ADMIN_OPTION;
    }

    /* Determine whether the member would still have ADMIN OPTION. */
    let mut would_still_have_admin_option = false;
    for i in 0..members.len() {
        let am_cascade_form = members[i];
        if am_cascade_form.member == authmem_form.member
            && am_cascade_form.admin_option
            && actions[i] == RRG_NOOP
        {
            would_still_have_admin_option = true;
            break;
        }
    }

    /* If the member would still have ADMIN OPTION, we need not recurse. */
    if would_still_have_admin_option {
        return Ok(());
    }

    /*
     * Recurse to grants that are not yet slated for deletion which have this
     * member as the grantor.
     */
    for i in 0..members.len() {
        let am_cascade_form = members[i];
        if am_cascade_form.grantor == authmem_form.member && actions[i] != RRG_DELETE_GRANT {
            if behavior == DROP_RESTRICT {
                return ereport(ERROR)
                    .errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                    .errmsg("dependent privileges exist")
                    .errhint("Use CASCADE to revoke them too.")
                    .finish(here("plan_recursive_revoke"));
            }

            plan_recursive_revoke(members, actions, i, false, behavior)?;
        }
    }

    Ok(())
}

/* =========================================================================
 * InitGrantRoleOptions   (C 2504-2511)
 * ========================================================================= */

/// `InitGrantRoleOptions` — initialize a GrantRoleOptions with default values.
fn InitGrantRoleOptions() -> GrantRoleOptions {
    GrantRoleOptions {
        specified: 0,
        admin: false,
        inherit: false,
        set: true,
    }
}

/* =========================================================================
 * check_createrole_self_grant   (C 2516-2564) — GUC check hook
 * ========================================================================= */

/// `check_createrole_self_grant` — GUC check_hook for `createrole_self_grant`.
/// Returns `Some(options_bits)` (the `*extra`) on success, or `None` on a
/// syntax/keyword error (the C returns false after `GUC_check_errdetail`).
pub fn check_createrole_self_grant(newval: &str) -> PgResult<Option<u32>> {
    /* Need a modifiable copy of string + SplitIdentifierString. */
    let elemlist = match seam::split_identifier_string::call(newval.to_string())? {
        Some(list) => list,
        None => {
            /* syntax error in list */
            seam::guc_check_errdetail::call("List syntax is invalid.".to_string());
            return Ok(None);
        }
    };

    let mut options: u32 = 0;
    for tok in &elemlist {
        if tok.eq_ignore_ascii_case("SET") {
            options |= GRANT_ROLE_SPECIFIED_SET;
        } else if tok.eq_ignore_ascii_case("INHERIT") {
            options |= GRANT_ROLE_SPECIFIED_INHERIT;
        } else {
            seam::guc_check_errdetail::call(format!("Unrecognized key word: \"{tok}\"."));
            return Ok(None);
        }
    }

    Ok(Some(options))
}

/* =========================================================================
 * assign_createrole_self_grant   (C 2569-2583) — GUC assign hook
 * ========================================================================= */

/// `assign_createrole_self_grant` — GUC assign_hook for `createrole_self_grant`.
/// Returns the `(enabled, GrantRoleOptions)` the C stores in
/// `createrole_self_grant_enabled` / `createrole_self_grant_options`.
pub fn assign_createrole_self_grant(options: u32) -> (bool, GrantRoleOptions) {
    let enabled = options != 0;
    let opts = GrantRoleOptions {
        specified: GRANT_ROLE_SPECIFIED_ADMIN | GRANT_ROLE_SPECIFIED_INHERIT | GRANT_ROLE_SPECIFIED_SET,
        admin: false,
        inherit: (options & GRANT_ROLE_SPECIFIED_INHERIT) != 0,
        set: (options & GRANT_ROLE_SPECIFIED_SET) != 0,
    };
    (enabled, opts)
}

/* -------------------------------------------------------------------------
 * Local helpers
 * ------------------------------------------------------------------------- */

/// `parse_bool(value, &result)` — `Some(b)` on success, `None` if the string is
/// not a recognized boolean.  Pure computation (utils/adt/bool.c semantics).
fn parse_bool(value: &str) -> Option<bool> {
    /* Mirror PostgreSQL's `parse_bool_with_len` recognized spellings. */
    match value.to_ascii_lowercase().as_str() {
        "t" | "true" | "y" | "yes" | "on" | "1" => Some(true),
        "f" | "false" | "n" | "no" | "off" | "0" => Some(false),
        _ => None,
    }
}

/// `ObjectAddressSet(addr, class, object)` — sets `objectSubId = 0`.
fn ObjectAddressSet(class_id: Oid, object_id: Oid) -> ObjectAddress {
    ObjectAddress {
        classId: class_id,
        objectId: object_id,
        objectSubId: 0,
    }
}

/// An empty `ROLESPEC_CSTRING` RoleSpec, used where the C would dereference a
/// `RoleSpec *` that the parser always supplies (defensive fallback only).
fn empty_rolespec() -> RoleSpec {
    RoleSpec {
        roletype: ROLESPEC_CSTRING,
        rolename: None,
        location: -1,
    }
}

/// `makeNode(RoleSpec); thisrole->roletype = ROLESPEC_CSTRING; rolename = name;`
fn make_cstring_rolespec(name: &str) -> RoleSpec {
    RoleSpec {
        roletype: ROLESPEC_CSTRING,
        rolename: Some(name.to_string()),
        location: -1,
    }
}

/// `makeNode(RoleSpec); current_role->roletype = ROLESPEC_CURRENT_ROLE;`
fn make_current_rolespec() -> RoleSpec {
    RoleSpec {
        roletype: ROLESPEC_CURRENT_ROLE,
        rolename: None,
        location: -1,
    }
}

/* -------------------------------------------------------------------------
 * Seam installation.
 * ------------------------------------------------------------------------- */

/// Install this crate's seams. `commands/user.c`'s own functions are called
/// by `tcop`'s utility dispatch via a direct dependency, so there are no
/// inward seams to install here.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
