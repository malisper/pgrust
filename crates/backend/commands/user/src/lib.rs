//! user.c. Password verifiers (SCRAM/MD5) are unported: PASSWORD <string> is
//! a loud panic, PASSWORD NULL and the empty-password NOTICE path work.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]

use std::cell::Cell;

use cache_syscache::cacheinfo::{AUTHMEMROLEMEM, AUTHNAME, AUTHOID};
use cache_syscache::{
    ReleaseSysCache, ReleaseSysCacheList, SearchSysCache1, SearchSysCache3, SearchSysCacheList1,
    SysCacheGetAttr, SysCacheGetAttrNotNull, SysCacheKey,
};
use catcache::CatCTuple;
use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::catalog::{BOOTSTRAP_SUPERUSERID, ROLE_PG_DATABASE_OWNER};
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::{
    ErrorLocation, PgError, PgResult, SqlState, ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST,
    ERRCODE_DUPLICATE_OBJECT, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INSUFFICIENT_PRIVILEGE,
    ERRCODE_INVALID_GRANT_OPERATION, ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_OBJECT_IN_USE,
    ERRCODE_RESERVED_NAME, ERRCODE_SYNTAX_ERROR, ERRCODE_UNDEFINED_OBJECT, NOTICE, WARNING,
};
use types_guc::GucSource;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{
    AlterRoleSetStmt, AlterRoleStmt, CreateRoleStmt, DefElem, DropBehavior, DropOwnedStmt,
    DropRoleStmt, GrantRoleStmt, ReassignOwnedStmt, RoleSpec, RoleSpecType, RoleStmtType,
};
use types_nodes::NodeTag;
use types_rel::{
    AccessExclusiveLock, AccessShareLock, NoLock, Relation, RowExclusiveLock,
    ShareUpdateExclusiveLock,
};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_tuple::{HeapTupleData, ItemPointerData, NameData};

#[cfg(test)]
mod tests;

const Natts_pg_authid: usize = 12;
const Anum_pg_authid_oid: i32 = 1;
const Anum_pg_authid_rolname: i32 = 2;
const Anum_pg_authid_rolsuper: i32 = 3;
const Anum_pg_authid_rolinherit: i32 = 4;
const Anum_pg_authid_rolcreaterole: i32 = 5;
const Anum_pg_authid_rolcreatedb: i32 = 6;
const Anum_pg_authid_rolcanlogin: i32 = 7;
const Anum_pg_authid_rolreplication: i32 = 8;
const Anum_pg_authid_rolbypassrls: i32 = 9;
const Anum_pg_authid_rolconnlimit: i32 = 10;
const Anum_pg_authid_rolpassword: i32 = 11;
const Anum_pg_authid_rolvaliduntil: i32 = 12;

const Natts_pg_auth_members: usize = 7;
const Anum_pg_auth_members_oid: i32 = 1;
const Anum_pg_auth_members_roleid: i32 = 2;
const Anum_pg_auth_members_member: i32 = 3;
const Anum_pg_auth_members_grantor: i32 = 4;
const Anum_pg_auth_members_admin_option: i32 = 5;
const Anum_pg_auth_members_inherit_option: i32 = 6;
const Anum_pg_auth_members_set_option: i32 = 7;

const Anum_pg_shdescription_objoid: i32 = 1;
const Anum_pg_shdescription_classoid: i32 = 2;
const Anum_pg_shseclabel_objoid: i32 = 1;
const Anum_pg_shseclabel_classoid: i32 = 2;

pub const GRANT_ROLE_SPECIFIED_ADMIN: u32 = 0x0001;
pub const GRANT_ROLE_SPECIFIED_INHERIT: u32 = 0x0002;
pub const GRANT_ROLE_SPECIFIED_SET: u32 = 0x0004;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrantRoleOptions {
    pub specified: u32,
    pub admin: bool,
    pub inherit: bool,
    pub set: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RevokeRoleGrantAction {
    Noop,
    RemoveAdminOption,
    RemoveInheritOption,
    RemoveSetOption,
    DeleteGrant,
}

#[derive(Clone, Copy)]
pub struct AuthMemRow {
    pub tid: ItemPointerData,
    pub oid: Oid,
    pub roleid: Oid,
    pub member: Oid,
    pub grantor: Oid,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}

thread_local! {
    static CREATEROLE_SELF_GRANT_ENABLED: Cell<bool> = const { Cell::new(false) };
    static CREATEROLE_SELF_GRANT_OPTIONS: Cell<GrantRoleOptions> = const {
        Cell::new(GrantRoleOptions { specified: 0, admin: false, inherit: false, set: false })
    };
}

fn err(msg: String, sqlstate: SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg).with_sqlstate(sqlstate))
}

fn err_detail(msg: &str, detail: String, sqlstate: SqlState) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(sqlstate).with_detail(detail))
}

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

fn notice(msg: String, func: &'static str) -> PgResult<()> {
    elog::ereport(NOTICE).errmsg(msg).finish(loc(func))
}

fn warning(msg: String, func: &'static str) -> PgResult<()> {
    elog::ereport(WARNING).errmsg(msg).finish(loc(func))
}

fn get_user_name_from_id<'mcx>(mcx: Mcx<'mcx>, roleid: Oid) -> PgResult<String> {
    Ok(miscinit::GetUserNameFromId(mcx, roleid, false)?
        .expect("GetUserNameFromId(noerr=false) returned None")
        .as_str()
        .to_owned())
}

fn name_attr(cache_id: i32, tuple: &CatCTuple, attnum: i32) -> PgResult<String> {
    let d = SysCacheGetAttrNotNull(cache_id, tuple, attnum)?;
    // SAFETY: a Name column inside the held tuple: 64 bytes, NUL-terminated.
    let cs = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    Ok(cs.to_string_lossy().into_owned())
}

fn authid_tuple(roleid: Oid) -> PgResult<CatCTuple> {
    match SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? {
        Some(t) => Ok(t),
        None => Err(Box::new(PgError::error(format!("cache lookup failed for role {roleid}")))),
    }
}

fn authid_bool_attr(roleid: Oid, attnum: i32) -> PgResult<bool> {
    match SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))? {
        Some(tuple) => {
            let result = SysCacheGetAttrNotNull(AUTHOID, &tuple, attnum)?.as_bool();
            ReleaseSysCache(tuple);
            Ok(result)
        }
        None => Ok(false),
    }
}

// has_createrole_privilege (aclchk.c).
fn has_createrole_privilege(roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    authid_bool_attr(roleid, Anum_pg_authid_rolcreaterole)
}

fn have_createrole_privilege() -> PgResult<bool> {
    has_createrole_privilege(miscinit::GetUserId())
}

// have_createdb_privilege (dbcommands.c).
fn have_createdb_privilege() -> PgResult<bool> {
    if superuser::superuser()? {
        return Ok(true);
    }
    authid_bool_attr(miscinit::GetUserId(), Anum_pg_authid_rolcreatedb)
}

// has_rolreplication (miscinit.c).
fn has_rolreplication(roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    authid_bool_attr(roleid, Anum_pg_authid_rolreplication)
}

// has_bypassrls_privilege (aclchk.c).
fn has_bypassrls_privilege(roleid: Oid) -> PgResult<bool> {
    if superuser::superuser_arg(roleid)? {
        return Ok(true);
    }
    authid_bool_attr(roleid, Anum_pg_authid_rolbypassrls)
}

// get_rolespec_oid (acl.c).
fn get_rolespec_oid(role: &RoleSpec<'_>, missing_ok: bool) -> PgResult<Oid> {
    use RoleSpecType::*;
    match role.roletype {
        ROLESPEC_CSTRING => adt_acl::get_role_oid(role.rolename.unwrap_or_default(), missing_ok),
        ROLESPEC_CURRENT_ROLE | ROLESPEC_CURRENT_USER => Ok(miscinit::GetUserId()),
        ROLESPEC_SESSION_USER => Ok(miscinit::GetSessionUserId()),
        ROLESPEC_PUBLIC => {
            Err(err("role \"public\" does not exist".into(), ERRCODE_UNDEFINED_OBJECT))
        }
    }
}

// get_rolespec_tuple (acl.c).
fn get_rolespec_tuple(role: &RoleSpec<'_>) -> PgResult<CatCTuple> {
    use RoleSpecType::*;
    match role.roletype {
        ROLESPEC_CSTRING => {
            let name = role.rolename.unwrap_or_default();
            match SearchSysCache1(AUTHNAME, SysCacheKey::Str(name))? {
                Some(t) => Ok(t),
                None => {
                    Err(err(format!("role \"{name}\" does not exist"), ERRCODE_UNDEFINED_OBJECT))
                }
            }
        }
        ROLESPEC_CURRENT_ROLE | ROLESPEC_CURRENT_USER => authid_tuple(miscinit::GetUserId()),
        ROLESPEC_SESSION_USER => authid_tuple(miscinit::GetSessionUserId()),
        ROLESPEC_PUBLIC => {
            Err(err("role \"public\" does not exist".into(), ERRCODE_UNDEFINED_OBJECT))
        }
    }
}

// get_rolespec_name (acl.c).
fn get_rolespec_name(role: &RoleSpec<'_>) -> PgResult<String> {
    let tuple = get_rolespec_tuple(role)?;
    let name = name_attr(tuple.cache_id(), &tuple, Anum_pg_authid_rolname)?;
    ReleaseSysCache(tuple);
    Ok(name)
}

// check_rolespec_name (acl.c).
fn check_rolespec_name(role: &RoleSpec<'_>, detail_msg: &str) -> PgResult<()> {
    if role.roletype != RoleSpecType::ROLESPEC_CSTRING {
        return Ok(());
    }
    let rolename = role.rolename.unwrap_or_default();
    if catalog::IsReservedName(rolename) {
        return Err(err_detail(
            &format!("role name \"{rolename}\" is reserved"),
            detail_msg.to_string(),
            ERRCODE_RESERVED_NAME,
        ));
    }
    Ok(())
}

fn bool_arg(d: &DefElem<'_>) -> bool {
    d.arg.expect("DefElem Boolean arg").as_boolean().expect("Boolean").boolval
}

fn int_arg(d: &DefElem<'_>) -> i32 {
    d.arg.expect("DefElem Integer arg").as_integer().expect("Integer").ival
}

fn str_arg<'a>(d: &DefElem<'a>) -> &'a str {
    d.arg.expect("DefElem String arg").as_string().expect("String").sval
}

fn list_arg<'a>(d: &DefElem<'a>) -> &'a NodeList<'a> {
    d.arg.expect("DefElem List arg").as_list().expect("List")
}

// defGetString (define.c), the arms role options can produce.
fn def_get_string(def: &DefElem<'_>) -> PgResult<String> {
    let defname = def.defname.unwrap_or("");
    let Some(arg) = def.arg else {
        return Err(err(format!("{defname} requires a parameter"), ERRCODE_SYNTAX_ERROR));
    };
    Ok(match arg.node_tag() {
        NodeTag::T_Integer => arg.as_integer().unwrap().ival.to_string(),
        NodeTag::T_Float => arg.as_float().unwrap().fval.to_string(),
        NodeTag::T_Boolean => {
            (if arg.as_boolean().unwrap().boolval { "true" } else { "false" }).to_string()
        }
        NodeTag::T_String => arg.as_string().unwrap().sval.to_string(),
        t => panic!("defGetString (define.c): {t:?} arg arm unported"),
    })
}

fn conflicting_def_elem() -> Box<PgError> {
    err("conflicting or redundant options".into(), ERRCODE_SYNTAX_ERROR)
}

fn oid_key(attno: i32, oid: Oid) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno as AttrNumber;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = 0;
    key.sk_func = fmgr_seams::fmgr_info::call(types_core::fmgr::F_OIDEQ)
        .unwrap_or_else(|e| panic!("fmgr_info(F_OIDEQ) failed: {e:?}"));
    key.sk_argument = Datum::from_oid(oid);
    key
}

fn authmem_getattr(tup: &HeapTupleData<'_>, attnum: i32) -> Datum {
    let td = match catcache::cache_tupdesc(AUTHMEMROLEMEM) {
        Some(td) => td,
        None => {
            catcache::InitCatCachePhase2(AUTHMEMROLEMEM, false)
                .expect("catcache phase-2 init for pg_auth_members");
            catcache::cache_tupdesc(AUTHMEMROLEMEM).expect("phase-2 init left no tupdesc")
        }
    };
    let mut isnull = false;
    // SAFETY: fixed NOT NULL pg_auth_members column under the cache's
    // descriptor.
    let d = unsafe { types_tuple::heap_getattr(tup, attnum, td, &mut isnull) };
    debug_assert!(!isnull);
    d
}

// Snapshot of SearchSysCacheList1(AUTHMEMROLEMEM, roleid): all grants of
// roleid, in C's memlist order.
fn authmem_rows_for_role<'mcx>(mcx: Mcx<'mcx>, roleid: Oid) -> PgResult<PgVec<'mcx, AuthMemRow>> {
    let memlist = SearchSysCacheList1(AUTHMEMROLEMEM, SysCacheKey::Value(Datum::from_oid(roleid)))?;
    let n = memlist.n_members() as usize;
    let mut rows: PgVec<'mcx, AuthMemRow> = mcx::vec_with_capacity_in(mcx, n)?;
    for m in 0..n {
        let member = memlist.member(m);
        let tuple = member.tuple();
        rows.push(AuthMemRow {
            tid: tuple.t_self,
            oid: authmem_getattr(&tuple, Anum_pg_auth_members_oid).as_oid(),
            roleid: authmem_getattr(&tuple, Anum_pg_auth_members_roleid).as_oid(),
            member: authmem_getattr(&tuple, Anum_pg_auth_members_member).as_oid(),
            grantor: authmem_getattr(&tuple, Anum_pg_auth_members_grantor).as_oid(),
            admin_option: authmem_getattr(&tuple, Anum_pg_auth_members_admin_option).as_bool(),
            inherit_option: authmem_getattr(&tuple, Anum_pg_auth_members_inherit_option).as_bool(),
            set_option: authmem_getattr(&tuple, Anum_pg_auth_members_set_option).as_bool(),
        });
    }
    ReleaseSysCacheList(memlist);
    Ok(rows)
}

pub fn InitGrantRoleOptions() -> GrantRoleOptions {
    GrantRoleOptions { specified: 0, admin: false, inherit: false, set: true }
}

pub fn createrole_self_grant_enabled() -> bool {
    CREATEROLE_SELF_GRANT_ENABLED.get()
}

fn createrole_self_grant_options() -> GrantRoleOptions {
    CREATEROLE_SELF_GRANT_OPTIONS.get()
}

// CREATE ROLE
pub fn CreateRole<'mcx, 'a>(mcx: Mcx<'mcx>, stmt: &CreateRoleStmt<'a>) -> PgResult<Oid> {
    let currentUserId = miscinit::GetUserId();
    let role = stmt.role.expect("CreateRoleStmt.role");

    let mut canlogin = stmt.stmt_type == RoleStmtType::ROLESTMT_USER;

    let mut dpassword: Option<&'a DefElem<'a>> = None;
    let mut dissuper: Option<&'a DefElem<'a>> = None;
    let mut dinherit: Option<&'a DefElem<'a>> = None;
    let mut dcreaterole: Option<&'a DefElem<'a>> = None;
    let mut dcreatedb: Option<&'a DefElem<'a>> = None;
    let mut dcanlogin: Option<&'a DefElem<'a>> = None;
    let mut disreplication: Option<&'a DefElem<'a>> = None;
    let mut dconnlimit: Option<&'a DefElem<'a>> = None;
    let mut daddroleto: Option<&'a DefElem<'a>> = None;
    let mut drolemembers: Option<&'a DefElem<'a>> = None;
    let mut dadminmembers: Option<&'a DefElem<'a>> = None;
    let mut dvalidUntil: Option<&'a DefElem<'a>> = None;
    let mut dbypassRLS: Option<&'a DefElem<'a>> = None;

    for cell in stmt.options.iter() {
        let defel = cell.as_def_elem().expect("CreateRole options: DefElem list");
        let slot: &mut Option<&'a DefElem<'a>> = match defel.defname.unwrap_or("") {
            "password" => &mut dpassword,
            "sysid" => {
                notice("SYSID can no longer be specified".into(), "CreateRole")?;
                continue;
            }
            "superuser" => &mut dissuper,
            "inherit" => &mut dinherit,
            "createrole" => &mut dcreaterole,
            "createdb" => &mut dcreatedb,
            "canlogin" => &mut dcanlogin,
            "isreplication" => &mut disreplication,
            "connectionlimit" => &mut dconnlimit,
            "addroleto" => &mut daddroleto,
            "rolemembers" => &mut drolemembers,
            "adminmembers" => &mut dadminmembers,
            "validUntil" => &mut dvalidUntil,
            "bypassrls" => &mut dbypassRLS,
            other => {
                return Err(Box::new(PgError::error(format!(
                    "option \"{other}\" not recognized"
                ))))
            }
        };
        if slot.is_some() {
            return Err(conflicting_def_elem());
        }
        *slot = Some(defel);
    }

    let password = dpassword.and_then(|d| d.arg).map(|a| a.as_string().expect("String").sval);
    let issuper = dissuper.map(bool_arg).unwrap_or(false);
    let inherit = dinherit.map(bool_arg).unwrap_or(true);
    let createrole = dcreaterole.map(bool_arg).unwrap_or(false);
    let createdb = dcreatedb.map(bool_arg).unwrap_or(false);
    if let Some(d) = dcanlogin {
        canlogin = bool_arg(d);
    }
    let isreplication = disreplication.map(bool_arg).unwrap_or(false);
    let mut connlimit = -1;
    if let Some(d) = dconnlimit {
        connlimit = int_arg(d);
        if connlimit < -1 {
            return Err(err(
                format!("invalid connection limit: {connlimit}"),
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
    }
    let nil = NodeList::nil();
    let addroleto = daddroleto.map(list_arg).unwrap_or(&nil);
    let rolemembers = drolemembers.map(list_arg).unwrap_or(&nil);
    let adminmembers = dadminmembers.map(list_arg).unwrap_or(&nil);
    let validUntil = dvalidUntil.map(str_arg);
    let bypassrls = dbypassRLS.map(bool_arg).unwrap_or(false);

    if !superuser::superuser_arg(currentUserId)? {
        if !has_createrole_privilege(currentUserId)? {
            return Err(err_detail(
                "permission denied to create role",
                "Only roles with the CREATEROLE attribute may create roles.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        if issuper {
            return Err(err_detail(
                "permission denied to create role",
                "Only roles with the SUPERUSER attribute may create roles with the SUPERUSER attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        if createdb && !have_createdb_privilege()? {
            return Err(err_detail(
                "permission denied to create role",
                "Only roles with the CREATEDB attribute may create roles with the CREATEDB attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        if isreplication && !has_rolreplication(currentUserId)? {
            return Err(err_detail(
                "permission denied to create role",
                "Only roles with the REPLICATION attribute may create roles with the REPLICATION attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        if bypassrls && !has_bypassrls_privilege(currentUserId)? {
            return Err(err_detail(
                "permission denied to create role",
                "Only roles with the BYPASSRLS attribute may create roles with the BYPASSRLS attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    if catalog::IsReservedName(role) {
        return Err(err_detail(
            &format!("role name \"{role}\" is reserved"),
            "Role names starting with \"pg_\" are reserved.".into(),
            ERRCODE_RESERVED_NAME,
        ));
    }

    let pg_authid_rel = table::table_open(mcx, catalog::AuthIdRelationId, RowExclusiveLock)?;

    if adt_acl::get_role_oid(role, true)? != InvalidOid {
        return Err(err(format!("role \"{role}\" already exists"), ERRCODE_DUPLICATE_OBJECT));
    }

    let (validUntil_datum, validUntil_null) = match validUntil {
        Some(s) => (Datum::from_i64(adt_timestamp::timestamptz_in(s, -1, None)?), false),
        None => (Datum::null(), true),
    };

    let mut new_record = [Datum::null(); Natts_pg_authid];
    let mut new_record_nulls = [false; Natts_pg_authid];

    let mut rolname = NameData::default();
    rolname.namestrcpy(role);
    new_record[(Anum_pg_authid_rolname - 1) as usize] =
        Datum::from_usize(rolname.data.as_ptr() as usize);
    new_record[(Anum_pg_authid_rolsuper - 1) as usize] = Datum::from_bool(issuper);
    new_record[(Anum_pg_authid_rolinherit - 1) as usize] = Datum::from_bool(inherit);
    new_record[(Anum_pg_authid_rolcreaterole - 1) as usize] = Datum::from_bool(createrole);
    new_record[(Anum_pg_authid_rolcreatedb - 1) as usize] = Datum::from_bool(createdb);
    new_record[(Anum_pg_authid_rolcanlogin - 1) as usize] = Datum::from_bool(canlogin);
    new_record[(Anum_pg_authid_rolreplication - 1) as usize] = Datum::from_bool(isreplication);
    new_record[(Anum_pg_authid_rolconnlimit - 1) as usize] = Datum::from_i32(connlimit);

    let mut _shadow_pass_text = None;
    match password {
        // CVE-2017-7546 arm: a supplied verifier OF the empty string also clears.
        Some(p) => {
            if p.is_empty()
                || crypt::plain_crypt_verify(mcx, role, p, "", &mut None)? == crypt::STATUS_OK
            {
                notice(
                    "empty string is not a valid password, clearing password".into(),
                    "CreateRole",
                )?;
                new_record_nulls[(Anum_pg_authid_rolpassword - 1) as usize] = true;
            } else {
                let shadow_pass = crypt::encrypt_password(
                    mcx,
                    crypt::PasswordType::from_guc(password_encryption()),
                    role,
                    p,
                )?;
                let t = varlena::cstring_to_text(mcx, shadow_pass.as_str().as_bytes())?;
                new_record[(Anum_pg_authid_rolpassword - 1) as usize] =
                    Datum::from_usize(t.as_bytes().as_ptr() as usize);
                _shadow_pass_text = Some(t);
            }
        }
        None => new_record_nulls[(Anum_pg_authid_rolpassword - 1) as usize] = true,
    }

    new_record[(Anum_pg_authid_rolvaliduntil - 1) as usize] = validUntil_datum;
    new_record_nulls[(Anum_pg_authid_rolvaliduntil - 1) as usize] = validUntil_null;

    new_record[(Anum_pg_authid_rolbypassrls - 1) as usize] = Datum::from_bool(bypassrls);

    let roleid = catalog::GetNewOidWithIndex(
        mcx,
        &pg_authid_rel,
        catalog::AuthIdOidIndexId,
        Anum_pg_authid_oid as AttrNumber,
    )?;
    new_record[(Anum_pg_authid_oid - 1) as usize] = Datum::from_oid(roleid);

    let mut tuple =
        heaptuple::heap_form_tuple(mcx, pg_authid_rel.descr(), &new_record, &new_record_nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &pg_authid_rel, &mut tuple)?;

    if !addroleto.is_nil() || !adminmembers.is_nil() || !rolemembers.is_nil() {
        xact::CommandCounterIncrement()?;
    }

    let mut popt = InitGrantRoleOptions();

    if !addroleto.is_nil() {
        let thisrole =
            RoleSpec { roletype: RoleSpecType::ROLESPEC_CSTRING, rolename: Some(role), location: -1 };
        for cell in addroleto.iter() {
            let oldrole = cell.as_role_spec().expect("RoleSpec");
            let oldroletup = get_rolespec_tuple(oldrole)?;
            let oldroleid =
                SysCacheGetAttrNotNull(oldroletup.cache_id(), &oldroletup, Anum_pg_authid_oid)?
                    .as_oid();
            let oldrolename =
                name_attr(oldroletup.cache_id(), &oldroletup, Anum_pg_authid_rolname)?;

            check_role_membership_authorization(mcx, currentUserId, oldroleid, true)?;
            AddRoleMems(
                mcx,
                currentUserId,
                &oldrolename,
                oldroleid,
                &[&thisrole],
                &[roleid],
                InvalidOid,
                &popt,
            )?;

            ReleaseSysCache(oldroletup);
        }
    }

    if !superuser::superuser()? {
        let current_role =
            RoleSpec { roletype: RoleSpecType::ROLESPEC_CURRENT_ROLE, rolename: None, location: -1 };
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
            role,
            roleid,
            &[&current_role],
            &[currentUserId],
            BOOTSTRAP_SUPERUSERID,
            &poptself,
        )?;

        xact::CommandCounterIncrement()?;

        if createrole_self_grant_enabled() {
            AddRoleMems(
                mcx,
                currentUserId,
                role,
                roleid,
                &[&current_role],
                &[currentUserId],
                currentUserId,
                &createrole_self_grant_options(),
            )?;
        }
    }

    let member_specs = role_spec_refs(mcx, rolemembers)?;
    let member_ids = roleSpecsToIds(mcx, rolemembers)?;
    AddRoleMems(mcx, currentUserId, role, roleid, &member_specs, &member_ids, InvalidOid, &popt)?;

    popt.specified |= GRANT_ROLE_SPECIFIED_ADMIN;
    popt.admin = true;
    let admin_specs = role_spec_refs(mcx, adminmembers)?;
    let admin_ids = roleSpecsToIds(mcx, adminmembers)?;
    AddRoleMems(mcx, currentUserId, role, roleid, &admin_specs, &admin_ids, InvalidOid, &popt)?;

    pg_authid_rel.close(NoLock)?;

    Ok(roleid)
}

// ALTER ROLE
pub fn AlterRole<'mcx, 'a>(mcx: Mcx<'mcx>, stmt: &AlterRoleStmt<'a>) -> PgResult<Oid> {
    let currentUserId = miscinit::GetUserId();

    check_rolespec_name(stmt.role, "Cannot alter reserved roles.")?;

    let mut dpassword: Option<&'a DefElem<'a>> = None;
    let mut dissuper: Option<&'a DefElem<'a>> = None;
    let mut dinherit: Option<&'a DefElem<'a>> = None;
    let mut dcreaterole: Option<&'a DefElem<'a>> = None;
    let mut dcreatedb: Option<&'a DefElem<'a>> = None;
    let mut dcanlogin: Option<&'a DefElem<'a>> = None;
    let mut disreplication: Option<&'a DefElem<'a>> = None;
    let mut dconnlimit: Option<&'a DefElem<'a>> = None;
    let mut drolemembers: Option<&'a DefElem<'a>> = None;
    let mut dvalidUntil: Option<&'a DefElem<'a>> = None;
    let mut dbypassRLS: Option<&'a DefElem<'a>> = None;

    for cell in stmt.options.iter() {
        let defel = cell.as_def_elem().expect("AlterRole options: DefElem list");
        let defname = defel.defname.unwrap_or("");
        let slot: &mut Option<&'a DefElem<'a>> = match defname {
            "password" => &mut dpassword,
            "superuser" => &mut dissuper,
            "inherit" => &mut dinherit,
            "createrole" => &mut dcreaterole,
            "createdb" => &mut dcreatedb,
            "canlogin" => &mut dcanlogin,
            "isreplication" => &mut disreplication,
            "connectionlimit" => &mut dconnlimit,
            "rolemembers" if stmt.action != 0 => &mut drolemembers,
            "validUntil" => &mut dvalidUntil,
            "bypassrls" => &mut dbypassRLS,
            other => {
                return Err(Box::new(PgError::error(format!(
                    "option \"{other}\" not recognized"
                ))))
            }
        };
        if slot.is_some() {
            return Err(conflicting_def_elem());
        }
        *slot = Some(defel);
    }

    let password = dpassword.and_then(|d| d.arg).map(|a| a.as_string().expect("String").sval);
    let mut connlimit = -1;
    if let Some(d) = dconnlimit {
        connlimit = int_arg(d);
        if connlimit < -1 {
            return Err(err(
                format!("invalid connection limit: {connlimit}"),
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
    }
    let validUntil = dvalidUntil.map(str_arg);

    let pg_authid_rel = table::table_open(mcx, catalog::AuthIdRelationId, RowExclusiveLock)?;

    let tuple = get_rolespec_tuple(stmt.role)?;
    let cache_id = tuple.cache_id();
    let rolename = name_attr(cache_id, &tuple, Anum_pg_authid_rolname)?;
    let roleid = SysCacheGetAttrNotNull(cache_id, &tuple, Anum_pg_authid_oid)?.as_oid();
    let rolsuper = SysCacheGetAttrNotNull(cache_id, &tuple, Anum_pg_authid_rolsuper)?.as_bool();

    if !superuser::superuser()? && rolsuper {
        return Err(err_detail(
            "permission denied to alter role",
            "Only roles with the SUPERUSER attribute may alter roles with the SUPERUSER attribute.".into(),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }
    if !superuser::superuser()? && dissuper.is_some() {
        return Err(err_detail(
            "permission denied to alter role",
            "Only roles with the SUPERUSER attribute may change the SUPERUSER attribute.".into(),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    if !have_createrole_privilege()? || !adt_acl::is_admin_of_role(miscinit::GetUserId(), roleid)? {
        if dinherit.is_some()
            || dcreaterole.is_some()
            || dcreatedb.is_some()
            || dcanlogin.is_some()
            || dconnlimit.is_some()
            || dvalidUntil.is_some()
            || disreplication.is_some()
            || dbypassRLS.is_some()
        {
            return Err(err_detail(
                "permission denied to alter role",
                format!(
                    "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{rolename}\" may alter this role."
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }

        if dpassword.is_some() && roleid != currentUserId {
            return Err(err_detail(
                "permission denied to alter role",
                "To change another role's password, the current user must have the CREATEROLE attribute and the ADMIN option on the role.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    } else if !superuser::superuser()? {
        if dcreatedb.is_some() && !have_createdb_privilege()? {
            return Err(err_detail(
                "permission denied to alter role",
                "Only roles with the CREATEDB attribute may change the CREATEDB attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        if disreplication.is_some() && !has_rolreplication(currentUserId)? {
            return Err(err_detail(
                "permission denied to alter role",
                "Only roles with the REPLICATION attribute may change the REPLICATION attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        if dbypassRLS.is_some() && !has_bypassrls_privilege(currentUserId)? {
            return Err(err_detail(
                "permission denied to alter role",
                "Only roles with the BYPASSRLS attribute may change the BYPASSRLS attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    if drolemembers.is_some() && !adt_acl::is_admin_of_role(currentUserId, roleid)? {
        return Err(err_detail(
            "permission denied to alter role",
            format!(
                "Only roles with the ADMIN option on role \"{rolename}\" may add or drop members."
            ),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let (validUntil_datum, validUntil_null) = match validUntil {
        Some(s) => (Datum::from_i64(adt_timestamp::timestamptz_in(s, -1, None)?), false),
        None => SysCacheGetAttr(cache_id, &tuple, Anum_pg_authid_rolvaliduntil)?,
    };

    let mut new_record = [Datum::null(); Natts_pg_authid];
    let mut new_record_nulls = [false; Natts_pg_authid];
    let mut new_record_repl = [false; Natts_pg_authid];

    if let Some(d) = dissuper {
        let should_be_super = bool_arg(d);
        if !should_be_super && roleid == BOOTSTRAP_SUPERUSERID {
            return Err(err_detail(
                "permission denied to alter role",
                "The bootstrap superuser must have the SUPERUSER attribute.".into(),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }
        new_record[(Anum_pg_authid_rolsuper - 1) as usize] = Datum::from_bool(should_be_super);
        new_record_repl[(Anum_pg_authid_rolsuper - 1) as usize] = true;
    }

    if let Some(d) = dinherit {
        new_record[(Anum_pg_authid_rolinherit - 1) as usize] = Datum::from_bool(bool_arg(d));
        new_record_repl[(Anum_pg_authid_rolinherit - 1) as usize] = true;
    }
    if let Some(d) = dcreaterole {
        new_record[(Anum_pg_authid_rolcreaterole - 1) as usize] = Datum::from_bool(bool_arg(d));
        new_record_repl[(Anum_pg_authid_rolcreaterole - 1) as usize] = true;
    }
    if let Some(d) = dcreatedb {
        new_record[(Anum_pg_authid_rolcreatedb - 1) as usize] = Datum::from_bool(bool_arg(d));
        new_record_repl[(Anum_pg_authid_rolcreatedb - 1) as usize] = true;
    }
    if let Some(d) = dcanlogin {
        new_record[(Anum_pg_authid_rolcanlogin - 1) as usize] = Datum::from_bool(bool_arg(d));
        new_record_repl[(Anum_pg_authid_rolcanlogin - 1) as usize] = true;
    }
    if let Some(d) = disreplication {
        new_record[(Anum_pg_authid_rolreplication - 1) as usize] = Datum::from_bool(bool_arg(d));
        new_record_repl[(Anum_pg_authid_rolreplication - 1) as usize] = true;
    }
    if dconnlimit.is_some() {
        new_record[(Anum_pg_authid_rolconnlimit - 1) as usize] = Datum::from_i32(connlimit);
        new_record_repl[(Anum_pg_authid_rolconnlimit - 1) as usize] = true;
    }

    let mut _shadow_pass_text = None;
    match password {
        // CVE-2017-7546 arm: a supplied verifier OF the empty string also clears.
        Some(p) => {
            if p.is_empty()
                || crypt::plain_crypt_verify(mcx, &rolename, p, "", &mut None)?
                    == crypt::STATUS_OK
            {
                notice(
                    "empty string is not a valid password, clearing password".into(),
                    "AlterRole",
                )?;
                new_record_nulls[(Anum_pg_authid_rolpassword - 1) as usize] = true;
                new_record_repl[(Anum_pg_authid_rolpassword - 1) as usize] = true;
            } else {
                let shadow_pass = crypt::encrypt_password(
                    mcx,
                    crypt::PasswordType::from_guc(password_encryption()),
                    &rolename,
                    p,
                )?;
                let t = varlena::cstring_to_text(mcx, shadow_pass.as_str().as_bytes())?;
                new_record[(Anum_pg_authid_rolpassword - 1) as usize] =
                    Datum::from_usize(t.as_bytes().as_ptr() as usize);
                new_record_repl[(Anum_pg_authid_rolpassword - 1) as usize] = true;
                _shadow_pass_text = Some(t);
            }
        }
        None => {}
    }

    if let Some(d) = dpassword {
        if d.arg.is_none() {
            new_record_repl[(Anum_pg_authid_rolpassword - 1) as usize] = true;
            new_record_nulls[(Anum_pg_authid_rolpassword - 1) as usize] = true;
        }
    }

    new_record[(Anum_pg_authid_rolvaliduntil - 1) as usize] = validUntil_datum;
    new_record_nulls[(Anum_pg_authid_rolvaliduntil - 1) as usize] = validUntil_null;
    new_record_repl[(Anum_pg_authid_rolvaliduntil - 1) as usize] = true;

    if let Some(d) = dbypassRLS {
        new_record[(Anum_pg_authid_rolbypassrls - 1) as usize] = Datum::from_bool(bool_arg(d));
        new_record_repl[(Anum_pg_authid_rolbypassrls - 1) as usize] = true;
    }

    let otid = tuple.tuple().t_self;
    let mut new_tuple = heaptuple::heap_modify_tuple(
        mcx,
        &tuple.tuple(),
        pg_authid_rel.descr(),
        &new_record,
        &new_record_nulls,
        &new_record_repl,
    )?;
    catalog_indexing::CatalogTupleUpdate(mcx, &pg_authid_rel, &otid, &mut new_tuple)?;

    ReleaseSysCache(tuple);

    let popt = InitGrantRoleOptions();

    if let Some(d) = drolemembers {
        let rolemembers = list_arg(d);

        xact::CommandCounterIncrement()?;

        let member_specs = role_spec_refs(mcx, rolemembers)?;
        let member_ids = roleSpecsToIds(mcx, rolemembers)?;
        if stmt.action == 1 {
            AddRoleMems(
                mcx,
                currentUserId,
                &rolename,
                roleid,
                &member_specs,
                &member_ids,
                InvalidOid,
                &popt,
            )?;
        } else if stmt.action == -1 {
            DelRoleMems(
                mcx,
                currentUserId,
                &rolename,
                roleid,
                &member_specs,
                &member_ids,
                InvalidOid,
                &popt,
                DropBehavior::DROP_RESTRICT,
            )?;
        }
    }

    pg_authid_rel.close(NoLock)?;

    Ok(roleid)
}

// ALTER ROLE ... SET
pub fn AlterRoleSet<'mcx>(mcx: Mcx<'mcx>, stmt: &AlterRoleSetStmt<'_>) -> PgResult<Oid> {
    let mut databaseid = InvalidOid;
    let mut roleid = InvalidOid;

    if let Some(role) = stmt.role {
        check_rolespec_name(role, "Cannot alter reserved roles.")?;

        let tuple = get_rolespec_tuple(role)?;
        let cache_id = tuple.cache_id();
        let rolename = name_attr(cache_id, &tuple, Anum_pg_authid_rolname)?;
        roleid = SysCacheGetAttrNotNull(cache_id, &tuple, Anum_pg_authid_oid)?.as_oid();
        let rolsuper = SysCacheGetAttrNotNull(cache_id, &tuple, Anum_pg_authid_rolsuper)?.as_bool();

        pg_shdepend::shdepLockAndCheckObject(mcx, catalog::AuthIdRelationId, roleid)?;

        if rolsuper {
            if !superuser::superuser()? {
                return Err(err_detail(
                    "permission denied to alter role",
                    "Only roles with the SUPERUSER attribute may alter roles with the SUPERUSER attribute.".into(),
                    ERRCODE_INSUFFICIENT_PRIVILEGE,
                ));
            }
        } else if (!have_createrole_privilege()?
            || !adt_acl::is_admin_of_role(miscinit::GetUserId(), roleid)?)
            && roleid != miscinit::GetUserId()
        {
            return Err(err_detail(
                "permission denied to alter role",
                format!(
                    "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{rolename}\" may alter this role."
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    if let Some(database) = stmt.database {
        databaseid = dbcommands_seams::get_database_oid::call(mcx, database, false)?;
        pg_shdepend::shdepLockAndCheckObject(mcx, types_core::catalog::DATABASE_RELATION_ID, databaseid)?;

        if stmt.role.is_none()
            && !aclchk::object_ownercheck(types_core::catalog::DATABASE_RELATION_ID, databaseid, miscinit::GetUserId())?
        {
            aclchk::aclcheck_error(
                aclchk::ACLCHECK_NOT_OWNER,
                types_nodes::parsenodes::ObjectType::OBJECT_DATABASE,
                database,
            )?;
        }
    }

    if stmt.role.is_none() && stmt.database.is_none() && !superuser::superuser()? {
        return Err(err_detail(
            "permission denied to alter setting",
            "Only roles with the SUPERUSER attribute may alter settings globally.".into(),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    pg_db_role_setting::AlterSetting(mcx, databaseid, roleid, stmt.setstmt)?;

    Ok(roleid)
}

// RenameRole (user.c)
pub fn RenameRole<'mcx>(mcx: Mcx<'mcx>, oldname: &str, newname: &str) -> PgResult<Oid> {
    let rel = table::table_open(mcx, catalog::AuthIdRelationId, RowExclusiveLock)?;

    let Some(oldtuple) = SearchSysCache1(AUTHNAME, SysCacheKey::Str(oldname))? else {
        return Err(err(format!("role \"{oldname}\" does not exist"), ERRCODE_UNDEFINED_OBJECT));
    };

    let roleid = SysCacheGetAttrNotNull(AUTHNAME, &oldtuple, Anum_pg_authid_oid)?.as_oid();
    let rolsuper = SysCacheGetAttrNotNull(AUTHNAME, &oldtuple, Anum_pg_authid_rolsuper)?.as_bool();
    let curname = name_attr(AUTHNAME, &oldtuple, Anum_pg_authid_rolname)?;

    if roleid == miscinit::GetSessionUserId() {
        return Err(err("session user cannot be renamed".into(), ERRCODE_FEATURE_NOT_SUPPORTED));
    }
    if roleid == miscinit::GetOuterUserId() {
        return Err(err("current user cannot be renamed".into(), ERRCODE_FEATURE_NOT_SUPPORTED));
    }

    if catalog::IsReservedName(&curname) {
        return Err(err_detail(
            &format!("role name \"{curname}\" is reserved"),
            "Role names starting with \"pg_\" are reserved.".into(),
            ERRCODE_RESERVED_NAME,
        ));
    }
    if catalog::IsReservedName(newname) {
        return Err(err_detail(
            &format!("role name \"{newname}\" is reserved"),
            "Role names starting with \"pg_\" are reserved.".into(),
            ERRCODE_RESERVED_NAME,
        ));
    }

    if adt_acl::get_role_oid(newname, true)? != InvalidOid {
        return Err(err(format!("role \"{newname}\" already exists"), ERRCODE_DUPLICATE_OBJECT));
    }

    if rolsuper {
        if !superuser::superuser()? {
            return Err(err_detail(
                "permission denied to rename role",
                "Only roles with the SUPERUSER attribute may rename roles with the SUPERUSER attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    } else if !have_createrole_privilege()?
        || !adt_acl::is_admin_of_role(miscinit::GetUserId(), roleid)?
    {
        return Err(err_detail(
            "permission denied to rename role",
            format!(
                "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{curname}\" may rename this role."
            ),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let mut repl_val = [Datum::null(); Natts_pg_authid];
    let mut repl_null = [false; Natts_pg_authid];
    let mut repl_repl = [false; Natts_pg_authid];

    let mut rolname = NameData::default();
    rolname.namestrcpy(newname);
    repl_val[(Anum_pg_authid_rolname - 1) as usize] =
        Datum::from_usize(rolname.data.as_ptr() as usize);
    repl_repl[(Anum_pg_authid_rolname - 1) as usize] = true;

    let (password_datum, password_null) =
        SysCacheGetAttr(AUTHNAME, &oldtuple, Anum_pg_authid_rolpassword)?;
    if !password_null {
        let p = password_datum.as_usize() as *const u8;
        // SAFETY: a live varlena readable through its full VARSIZE_ANY.
        let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
        let payload = varlena::open_image(mcx, image)?;
        let shadow_pass =
            core::str::from_utf8(payload.as_bytes()).expect("rolpassword is server-encoding text");
        // MD5 uses the username as salt, so it is cleared on a rename.
        if crypt::get_password_type(shadow_pass) == crypt::PasswordType::Md5 {
            repl_repl[(Anum_pg_authid_rolpassword - 1) as usize] = true;
            repl_null[(Anum_pg_authid_rolpassword - 1) as usize] = true;
            notice("MD5 password cleared because of role rename".into(), "RenameRole")?;
        }
    }

    let otid = oldtuple.tuple().t_self;
    let mut newtuple = heaptuple::heap_modify_tuple(
        mcx,
        &oldtuple.tuple(),
        rel.descr(),
        &repl_val,
        &repl_null,
        &repl_repl,
    )?;
    catalog_indexing::CatalogTupleUpdate(mcx, &rel, &otid, &mut newtuple)?;

    ReleaseSysCache(oldtuple);

    rel.close(NoLock)?;

    Ok(roleid)
}

// DROP ROLE
pub fn DropRole<'mcx>(mcx: Mcx<'mcx>, stmt: &DropRoleStmt<'_>) -> PgResult<()> {
    if !have_createrole_privilege()? {
        return Err(err_detail(
            "permission denied to drop role",
            "Only roles with the CREATEROLE attribute and the ADMIN option on the target roles may drop roles.".into(),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    let pg_authid_rel = table::table_open(mcx, catalog::AuthIdRelationId, RowExclusiveLock)?;
    let pg_auth_members_rel = table::table_open(mcx, catalog::AuthMemRelationId, RowExclusiveLock)?;

    let mut role_oids: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, stmt.roles.len())?;

    for cell in stmt.roles.iter() {
        let rolspec = cell.as_role_spec().expect("RoleSpec");
        if rolspec.roletype != RoleSpecType::ROLESPEC_CSTRING {
            return Err(err(
                "cannot use special role specifier in DROP ROLE".into(),
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        let role = rolspec.rolename.unwrap_or_default();

        let Some(tuple) = SearchSysCache1(AUTHNAME, SysCacheKey::Str(role))? else {
            if !stmt.missing_ok {
                return Err(err(
                    format!("role \"{role}\" does not exist"),
                    ERRCODE_UNDEFINED_OBJECT,
                ));
            }
            notice(format!("role \"{role}\" does not exist, skipping"), "DropRole")?;
            continue;
        };

        let roleid = SysCacheGetAttrNotNull(AUTHNAME, &tuple, Anum_pg_authid_oid)?.as_oid();
        let rolsuper = SysCacheGetAttrNotNull(AUTHNAME, &tuple, Anum_pg_authid_rolsuper)?.as_bool();
        let rolname = name_attr(AUTHNAME, &tuple, Anum_pg_authid_rolname)?;

        if roleid == miscinit::GetUserId() || roleid == miscinit::GetOuterUserId() {
            return Err(err("current user cannot be dropped".into(), ERRCODE_OBJECT_IN_USE));
        }
        if roleid == miscinit::GetSessionUserId() {
            return Err(err("session user cannot be dropped".into(), ERRCODE_OBJECT_IN_USE));
        }

        if rolsuper && !superuser::superuser()? {
            return Err(err_detail(
                "permission denied to drop role",
                "Only roles with the SUPERUSER attribute may drop roles with the SUPERUSER attribute.".into(),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
        if !adt_acl::is_admin_of_role(miscinit::GetUserId(), roleid)? {
            return Err(err_detail(
                "permission denied to drop role",
                format!(
                    "Only roles with the CREATEROLE attribute and the ADMIN option on role \"{rolname}\" may drop this role."
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }

        ReleaseSysCache(tuple);

        lmgr::LockSharedObject(catalog::AuthIdRelationId, roleid, 0, AccessExclusiveLock)?;

        // Both membership directions are silently removed; grantor-only
        // references are caught by checkSharedDependencies in the second pass.
        delete_auth_members(
            mcx,
            &pg_auth_members_rel,
            Anum_pg_auth_members_roleid,
            catalog::AuthMemRoleMemIndexId,
            roleid,
        )?;
        delete_auth_members(
            mcx,
            &pg_auth_members_rel,
            Anum_pg_auth_members_member,
            catalog::AuthMemMemRoleIndexId,
            roleid,
        )?;

        xact::CommandCounterIncrement()?;

        if !role_oids.contains(&roleid) {
            role_oids.push(roleid);
        }
    }

    for i in 0..role_oids.len() {
        let roleid = role_oids[i];

        let Some(tuple) = SearchSysCache1(AUTHOID, SysCacheKey::Value(Datum::from_oid(roleid)))?
        else {
            return Err(Box::new(PgError::error(format!(
                "could not find tuple for role {roleid}"
            ))));
        };
        let rolname = name_attr(AUTHOID, &tuple, Anum_pg_authid_rolname)?;

        if let Some((detail, detail_log)) =
            pg_shdepend::checkSharedDependencies(mcx, catalog::AuthIdRelationId, roleid)?
        {
            return Err(Box::new(
                PgError::error(format!(
                    "role \"{rolname}\" cannot be dropped because some objects depend on it"
                ))
                .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                .with_detail(detail.as_str().to_owned())
                .with_detail_log(detail_log.as_str().to_owned()),
            ));
        }

        let tid = tuple.tuple().t_self;
        catalog_indexing::CatalogTupleDelete(&pg_authid_rel, &tid)?;

        ReleaseSysCache(tuple);

        DeleteSharedComments(mcx, roleid, catalog::AuthIdRelationId)?;
        DeleteSharedSecurityLabel(mcx, roleid, catalog::AuthIdRelationId)?;

        pg_db_role_setting::DropSetting(mcx, InvalidOid, roleid)?;
    }

    pg_auth_members_rel.close(NoLock)?;
    pg_authid_rel.close(NoLock)
}

fn delete_auth_members<'mcx>(
    mcx: Mcx<'mcx>,
    pg_auth_members_rel: &Relation<'mcx>,
    attno: i32,
    index_id: Oid,
    roleid: Oid,
) -> PgResult<()> {
    let keys = [oid_key(attno, roleid)];
    let desc = pg_auth_members_rel.descr();
    let mut scan =
        genam::systable_beginscan(mcx, pg_auth_members_rel, index_id, true, None, &keys)?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: pg_auth_members oid column under the relation's descriptor.
        let oid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_auth_members_oid, desc, &mut isnull)
        }
        .as_oid();
        debug_assert!(!isnull);
        let tid = tup.t_self;
        pg_shdepend::deleteSharedDependencyRecordsFor(mcx, catalog::AuthMemRelationId, oid, 0)?;
        catalog_indexing::CatalogTupleDelete(pg_auth_members_rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)
}

// DeleteSharedComments (comment.c).
fn DeleteSharedComments<'mcx>(mcx: Mcx<'mcx>, oid: Oid, classoid: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, catalog::SharedDescriptionRelationId, RowExclusiveLock)?;
    let keys = [
        oid_key(Anum_pg_shdescription_objoid, oid),
        oid_key(Anum_pg_shdescription_classoid, classoid),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        catalog::SharedDescriptionObjIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

// DeleteSharedSecurityLabel (seclabel.c).
fn DeleteSharedSecurityLabel<'mcx>(mcx: Mcx<'mcx>, objectId: Oid, classId: Oid) -> PgResult<()> {
    let rel = table::table_open(mcx, catalog::SharedSecLabelRelationId, RowExclusiveLock)?;
    let keys = [
        oid_key(Anum_pg_shseclabel_objoid, objectId),
        oid_key(Anum_pg_shseclabel_classoid, classId),
    ];
    let mut scan = genam::systable_beginscan(
        mcx,
        &rel,
        catalog::SharedSecLabelObjectIndexId,
        true,
        None,
        &keys,
    )?;
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let tid = tup.t_self;
        catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)
}

// GRANT/REVOKE role TO/FROM role
pub fn GrantRole<'mcx, 'a>(mcx: Mcx<'mcx>, stmt: &GrantRoleStmt<'a>) -> PgResult<()> {
    let currentUserId = miscinit::GetUserId();

    let mut popt = InitGrantRoleOptions();
    for cell in stmt.opt.iter() {
        let opt = cell.as_def_elem().expect("GrantRole options: DefElem list");
        let defname = opt.defname.unwrap_or("");
        let optval = def_get_string(opt)?;

        let parsed = adt_bool::parse_bool(&optval);
        match defname {
            "admin" => {
                popt.specified |= GRANT_ROLE_SPECIFIED_ADMIN;
                if let Some(v) = parsed {
                    popt.admin = v;
                    continue;
                }
            }
            "inherit" => {
                popt.specified |= GRANT_ROLE_SPECIFIED_INHERIT;
                if let Some(v) = parsed {
                    popt.inherit = v;
                    continue;
                }
            }
            "set" => {
                popt.specified |= GRANT_ROLE_SPECIFIED_SET;
                if let Some(v) = parsed {
                    popt.set = v;
                    continue;
                }
            }
            _ => {
                return Err(err(
                    format!("unrecognized role option \"{defname}\""),
                    ERRCODE_SYNTAX_ERROR,
                ))
            }
        }

        return Err(err(
            format!("unrecognized value for role option \"{defname}\": \"{optval}\""),
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }

    let grantor = match stmt.grantor {
        Some(g) => get_rolespec_oid(g, false)?,
        None => InvalidOid,
    };

    let grantee_specs = role_spec_refs(mcx, &stmt.grantee_roles)?;
    let grantee_ids = roleSpecsToIds(mcx, &stmt.grantee_roles)?;

    // AccessShareLock is enough since we aren't modifying pg_authid.
    let pg_authid_rel = table::table_open(mcx, catalog::AuthIdRelationId, AccessShareLock)?;

    for cell in stmt.granted_roles.iter() {
        let privnode = cell.as_access_priv().expect("AccessPriv");
        let Some(rolename) = privnode.priv_name.filter(|_| privnode.cols.is_nil()) else {
            return Err(err(
                "column names cannot be included in GRANT/REVOKE ROLE".into(),
                ERRCODE_INVALID_GRANT_OPERATION,
            ));
        };

        let roleid = adt_acl::get_role_oid(rolename, false)?;
        check_role_membership_authorization(mcx, currentUserId, roleid, stmt.is_grant)?;
        if stmt.is_grant {
            AddRoleMems(
                mcx,
                currentUserId,
                rolename,
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
                rolename,
                roleid,
                &grantee_specs,
                &grantee_ids,
                grantor,
                &popt,
                stmt.behavior,
            )?;
        }
    }

    pg_authid_rel.close(NoLock)
}

// DROP OWNED BY
pub fn DropOwnedObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &DropOwnedStmt<'_>) -> PgResult<()> {
    let role_ids = roleSpecsToIds(mcx, &stmt.roles)?;

    for &roleid in role_ids.iter() {
        if !adt_acl::has_privs_of_role(miscinit::GetUserId(), roleid)? {
            return Err(err_detail(
                "permission denied to drop objects",
                format!(
                    "Only roles with privileges of role \"{}\" may drop objects owned by it.",
                    get_user_name_from_id(mcx, roleid)?
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    pg_shdepend::shdepDropOwned(mcx, &role_ids, stmt.behavior)
}

// REASSIGN OWNED BY
pub fn ReassignOwnedObjects<'mcx>(mcx: Mcx<'mcx>, stmt: &ReassignOwnedStmt<'_>) -> PgResult<()> {
    let role_ids = roleSpecsToIds(mcx, &stmt.roles)?;

    for &roleid in role_ids.iter() {
        if !adt_acl::has_privs_of_role(miscinit::GetUserId(), roleid)? {
            return Err(err_detail(
                "permission denied to reassign objects",
                format!(
                    "Only roles with privileges of role \"{}\" may reassign objects owned by it.",
                    get_user_name_from_id(mcx, roleid)?
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    }

    let newrole = get_rolespec_oid(stmt.newrole, false)?;

    if !adt_acl::has_privs_of_role(miscinit::GetUserId(), newrole)? {
        return Err(err_detail(
            "permission denied to reassign objects",
            format!(
                "Only roles with privileges of role \"{}\" may reassign objects to it.",
                get_user_name_from_id(mcx, newrole)?
            ),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    pg_shdepend::shdepReassignOwned(mcx, &role_ids, newrole)
}

fn role_spec_refs<'mcx, 'a>(
    mcx: Mcx<'mcx>,
    list: &NodeList<'a>,
) -> PgResult<PgVec<'mcx, &'a RoleSpec<'a>>> {
    let mut result: PgVec<'mcx, &'a RoleSpec<'a>> = mcx::vec_with_capacity_in(mcx, list.len())?;
    for cell in list.iter() {
        result.push(cell.as_role_spec().expect("RoleSpec"));
    }
    Ok(result)
}

pub fn roleSpecsToIds<'mcx>(
    mcx: Mcx<'mcx>,
    memberNames: &NodeList<'_>,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, memberNames.len())?;
    for cell in memberNames.iter() {
        let rolespec = cell.as_role_spec().expect("RoleSpec");
        result.push(get_rolespec_oid(rolespec, false)?);
    }
    Ok(result)
}

fn AddRoleMems<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    rolename: &str,
    roleid: Oid,
    memberSpecs: &[&RoleSpec<'_>],
    memberIds: &[Oid],
    grantorId: Oid,
    popt: &GrantRoleOptions,
) -> PgResult<()> {
    debug_assert_eq!(memberSpecs.len(), memberIds.len());

    let grantorId = check_role_grantor(mcx, currentUserId, roleid, grantorId, true)?;

    let pg_authmem_rel = table::table_open(mcx, catalog::AuthMemRelationId, RowExclusiveLock)?;

    lmgr::LockSharedObject(catalog::AuthIdRelationId, roleid, 0, ShareUpdateExclusiveLock)?;

    for (memberRole, &memberid) in memberSpecs.iter().zip(memberIds) {
        // pg_database_owner is never a role member.
        if memberid == ROLE_PG_DATABASE_OWNER {
            return Err(err(
                format!(
                    "role \"{}\" cannot be a member of any role",
                    get_rolespec_name(memberRole)?
                ),
                ERRCODE_FEATURE_NOT_SUPPORTED,
            ));
        }

        // Membership loops, superuserness ignored so superuser-privileged
        // roles can still be granted.
        if adt_acl::is_member_of_role_nosuper(roleid, memberid)? {
            return Err(err(
                format!(
                    "role \"{rolename}\" is a member of role \"{}\"",
                    get_rolespec_name(memberRole)?
                ),
                ERRCODE_INVALID_GRANT_OPERATION,
            ));
        }
    }

    // Grant chains must remain acyclic: refuse ADMIN OPTION granted back to a
    // role the grantor's own ADMIN OPTION depends on.
    if popt.admin && grantorId != BOOTSTRAP_SUPERUSERID {
        let members = authmem_rows_for_role(mcx, roleid)?;
        let mut actions = initialize_revoke_actions(mcx, members.len())?;

        for &memberid in memberIds {
            if memberid == BOOTSTRAP_SUPERUSERID {
                return Err(err(
                    "ADMIN option cannot be granted back to your own grantor".into(),
                    ERRCODE_INVALID_GRANT_OPERATION,
                ));
            }
            plan_member_revoke(&members, &mut actions, memberid)?;
        }

        let grantor_survives = members.iter().enumerate().any(|(i, m)| {
            actions[i] == RevokeRoleGrantAction::Noop
                && m.member == grantorId
                && m.admin_option
        });
        if !grantor_survives {
            return Err(err(
                "ADMIN option cannot be granted back to your own grantor".into(),
                ERRCODE_INVALID_GRANT_OPERATION,
            ));
        }
    }

    for (memberRole, &memberid) in memberSpecs.iter().zip(memberIds) {
        let mut new_record = [Datum::null(); Natts_pg_auth_members];
        let mut new_record_nulls = [false; Natts_pg_auth_members];
        let mut new_record_repl = [false; Natts_pg_auth_members];

        new_record[(Anum_pg_auth_members_roleid - 1) as usize] = Datum::from_oid(roleid);
        new_record[(Anum_pg_auth_members_member - 1) as usize] = Datum::from_oid(memberid);
        new_record[(Anum_pg_auth_members_grantor - 1) as usize] = Datum::from_oid(grantorId);

        let existing = SearchSysCache3(
            AUTHMEMROLEMEM,
            SysCacheKey::Value(Datum::from_oid(roleid)),
            SysCacheKey::Value(Datum::from_oid(memberid)),
            SysCacheKey::Value(Datum::from_oid(grantorId)),
        )?;

        if let Some(authmem_tuple) = existing {
            let cur_admin =
                SysCacheGetAttrNotNull(AUTHMEMROLEMEM, &authmem_tuple, Anum_pg_auth_members_admin_option)?
                    .as_bool();
            let cur_inherit = SysCacheGetAttrNotNull(
                AUTHMEMROLEMEM,
                &authmem_tuple,
                Anum_pg_auth_members_inherit_option,
            )?
            .as_bool();
            let cur_set =
                SysCacheGetAttrNotNull(AUTHMEMROLEMEM, &authmem_tuple, Anum_pg_auth_members_set_option)?
                    .as_bool();

            let mut at_least_one_change = false;
            if popt.specified & GRANT_ROLE_SPECIFIED_ADMIN != 0 && cur_admin != popt.admin {
                new_record[(Anum_pg_auth_members_admin_option - 1) as usize] =
                    Datum::from_bool(popt.admin);
                new_record_repl[(Anum_pg_auth_members_admin_option - 1) as usize] = true;
                at_least_one_change = true;
            }
            if popt.specified & GRANT_ROLE_SPECIFIED_INHERIT != 0 && cur_inherit != popt.inherit {
                new_record[(Anum_pg_auth_members_inherit_option - 1) as usize] =
                    Datum::from_bool(popt.inherit);
                new_record_repl[(Anum_pg_auth_members_inherit_option - 1) as usize] = true;
                at_least_one_change = true;
            }
            if popt.specified & GRANT_ROLE_SPECIFIED_SET != 0 && cur_set != popt.set {
                new_record[(Anum_pg_auth_members_set_option - 1) as usize] =
                    Datum::from_bool(popt.set);
                new_record_repl[(Anum_pg_auth_members_set_option - 1) as usize] = true;
                at_least_one_change = true;
            }

            if !at_least_one_change {
                notice(
                    format!(
                        "role \"{}\" has already been granted membership in role \"{rolename}\" by role \"{}\"",
                        get_rolespec_name(memberRole)?,
                        get_user_name_from_id(mcx, grantorId)?
                    ),
                    "AddRoleMems",
                )?;
                ReleaseSysCache(authmem_tuple);
                continue;
            }

            let otid = authmem_tuple.tuple().t_self;
            let mut tuple = heaptuple::heap_modify_tuple(
                mcx,
                &authmem_tuple.tuple(),
                pg_authmem_rel.descr(),
                &new_record,
                &new_record_nulls,
                &new_record_repl,
            )?;
            catalog_indexing::CatalogTupleUpdate(mcx, &pg_authmem_rel, &otid, &mut tuple)?;

            ReleaseSysCache(authmem_tuple);
        } else {
            new_record[(Anum_pg_auth_members_admin_option - 1) as usize] =
                Datum::from_bool(popt.admin);
            new_record[(Anum_pg_auth_members_set_option - 1) as usize] =
                Datum::from_bool(popt.set);

            // Unspecified INHERIT defaults to the member's role-level
            // rolinherit property.
            let inherit = if popt.specified & GRANT_ROLE_SPECIFIED_INHERIT != 0 {
                popt.inherit
            } else {
                let mrtup = authid_tuple(memberid)?;
                let rolinherit =
                    SysCacheGetAttrNotNull(mrtup.cache_id(), &mrtup, Anum_pg_authid_rolinherit)?
                        .as_bool();
                ReleaseSysCache(mrtup);
                rolinherit
            };
            new_record[(Anum_pg_auth_members_inherit_option - 1) as usize] =
                Datum::from_bool(inherit);

            let objectId = catalog::GetNewOidWithIndex(
                mcx,
                &pg_authmem_rel,
                catalog::AuthMemOidIndexId,
                Anum_pg_auth_members_oid as AttrNumber,
            )?;
            new_record[(Anum_pg_auth_members_oid - 1) as usize] = Datum::from_oid(objectId);

            let mut tuple = heaptuple::heap_form_tuple(
                mcx,
                pg_authmem_rel.descr(),
                &new_record,
                &new_record_nulls,
            )?;
            catalog_indexing::CatalogTupleInsert(mcx, &pg_authmem_rel, &mut tuple)?;

            pg_shdepend::updateAclDependencies(
                mcx,
                catalog::AuthMemRelationId,
                objectId,
                0,
                InvalidOid,
                &[],
                &[grantorId],
            )?;
        }

        // CCI after each change, in case there are duplicates in list.
        xact::CommandCounterIncrement()?;
    }

    pg_authmem_rel.close(NoLock)
}

fn DelRoleMems<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    rolename: &str,
    roleid: Oid,
    memberSpecs: &[&RoleSpec<'_>],
    memberIds: &[Oid],
    grantorId: Oid,
    popt: &GrantRoleOptions,
    behavior: DropBehavior,
) -> PgResult<()> {
    debug_assert_eq!(memberSpecs.len(), memberIds.len());

    let grantorId = check_role_grantor(mcx, currentUserId, roleid, grantorId, false)?;

    let pg_authmem_rel = table::table_open(mcx, catalog::AuthMemRelationId, RowExclusiveLock)?;

    lmgr::LockSharedObject(catalog::AuthIdRelationId, roleid, 0, ShareUpdateExclusiveLock)?;

    let members = authmem_rows_for_role(mcx, roleid)?;
    let mut actions = initialize_revoke_actions(mcx, members.len())?;

    for (memberRole, &memberid) in memberSpecs.iter().zip(memberIds) {
        if !plan_single_revoke(&members, &mut actions, memberid, grantorId, popt, behavior)? {
            warning(
                format!(
                    "role \"{}\" has not been granted membership in role \"{rolename}\" by role \"{}\"",
                    get_rolespec_name(memberRole)?,
                    get_user_name_from_id(mcx, grantorId)?
                ),
                "DelRoleMems",
            )?;
            continue;
        }
    }

    for (i, m) in members.iter().enumerate() {
        match actions[i] {
            RevokeRoleGrantAction::Noop => continue,
            RevokeRoleGrantAction::DeleteGrant => {
                pg_shdepend::deleteSharedDependencyRecordsFor(
                    mcx,
                    catalog::AuthMemRelationId,
                    m.oid,
                    0,
                )?;
                catalog_indexing::CatalogTupleDelete(&pg_authmem_rel, &m.tid)?;
            }
            action => {
                // C heap_modify_tuple's the cached tuple; an identical row
                // image with the option cleared is rebuilt here instead.
                let mut row = *m;
                match action {
                    RevokeRoleGrantAction::RemoveAdminOption => row.admin_option = false,
                    RevokeRoleGrantAction::RemoveInheritOption => row.inherit_option = false,
                    RevokeRoleGrantAction::RemoveSetOption => row.set_option = false,
                    _ => unreachable!(),
                }
                let values = [
                    Datum::from_oid(row.oid),
                    Datum::from_oid(row.roleid),
                    Datum::from_oid(row.member),
                    Datum::from_oid(row.grantor),
                    Datum::from_bool(row.admin_option),
                    Datum::from_bool(row.inherit_option),
                    Datum::from_bool(row.set_option),
                ];
                let nulls = [false; Natts_pg_auth_members];
                let mut tuple =
                    heaptuple::heap_form_tuple(mcx, pg_authmem_rel.descr(), &values, &nulls)?;
                catalog_indexing::CatalogTupleUpdate(mcx, &pg_authmem_rel, &m.tid, &mut tuple)?;
            }
        }
    }

    pg_authmem_rel.close(NoLock)
}

fn check_role_membership_authorization<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    roleid: Oid,
    is_grant: bool,
) -> PgResult<()> {
    // pg_database_owner has exactly one implicit, situation-dependent member.
    if is_grant && roleid == ROLE_PG_DATABASE_OWNER {
        return Err(err(
            format!(
                "role \"{}\" cannot have explicit members",
                get_user_name_from_id(mcx, roleid)?
            ),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ));
    }

    if superuser::superuser_arg(roleid)? {
        if !superuser::superuser_arg(currentUserId)? {
            let name = get_user_name_from_id(mcx, roleid)?;
            let verb = if is_grant { "grant" } else { "revoke" };
            return Err(err_detail(
                &format!("permission denied to {verb} role \"{name}\""),
                format!(
                    "Only roles with the SUPERUSER attribute may {verb} roles with the SUPERUSER attribute."
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    } else if !adt_acl::is_admin_of_role(currentUserId, roleid)? {
        let name = get_user_name_from_id(mcx, roleid)?;
        let verb = if is_grant { "grant" } else { "revoke" };
        return Err(err_detail(
            &format!("permission denied to {verb} role \"{name}\""),
            format!("Only roles with the ADMIN option on role \"{name}\" may {verb} this role."),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    Ok(())
}

fn check_role_grantor<'mcx>(
    mcx: Mcx<'mcx>,
    currentUserId: Oid,
    roleid: Oid,
    grantorId: Oid,
    is_grant: bool,
) -> PgResult<Oid> {
    if grantorId == InvalidOid {
        if superuser::superuser_arg(currentUserId)? {
            return Ok(BOOTSTRAP_SUPERUSERID);
        }
        let grantorId = adt_acl::select_best_admin(currentUserId, roleid)?;
        if grantorId == InvalidOid {
            return Err(Box::new(PgError::error("no possible grantors".to_string())));
        }
        return Ok(grantorId);
    }

    if is_grant {
        if !adt_acl::has_privs_of_role(currentUserId, grantorId)? {
            let name = get_user_name_from_id(mcx, grantorId)?;
            return Err(err_detail(
                &format!("permission denied to grant privileges as role \"{name}\""),
                format!(
                    "Only roles with privileges of role \"{name}\" may grant privileges as this role."
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }

        if grantorId != BOOTSTRAP_SUPERUSERID
            && adt_acl::select_best_admin(grantorId, roleid)? != grantorId
        {
            return Err(err_detail(
                &format!(
                    "permission denied to grant privileges as role \"{}\"",
                    get_user_name_from_id(mcx, grantorId)?
                ),
                format!(
                    "The grantor must have the ADMIN option on role \"{}\".",
                    get_user_name_from_id(mcx, roleid)?
                ),
                ERRCODE_INSUFFICIENT_PRIVILEGE,
            ));
        }
    } else if !adt_acl::has_privs_of_role(currentUserId, grantorId)? {
        let name = get_user_name_from_id(mcx, grantorId)?;
        return Err(err_detail(
            &format!("permission denied to revoke privileges granted by role \"{name}\""),
            format!(
                "Only roles with privileges of role \"{name}\" may revoke privileges granted by this role."
            ),
            ERRCODE_INSUFFICIENT_PRIVILEGE,
        ));
    }

    Ok(grantorId)
}

fn initialize_revoke_actions<'mcx>(
    mcx: Mcx<'mcx>,
    n: usize,
) -> PgResult<PgVec<'mcx, RevokeRoleGrantAction>> {
    let mut actions: PgVec<'mcx, RevokeRoleGrantAction> = mcx::vec_with_capacity_in(mcx, n)?;
    actions.resize(n, RevokeRoleGrantAction::Noop);
    Ok(actions)
}

pub fn plan_single_revoke(
    members: &[AuthMemRow],
    actions: &mut [RevokeRoleGrantAction],
    member: Oid,
    grantor: Oid,
    popt: &GrantRoleOptions,
    behavior: DropBehavior,
) -> PgResult<bool> {
    debug_assert!(popt.specified.count_ones() <= 1);

    for i in 0..members.len() {
        if members[i].member == member && members[i].grantor == grantor {
            if popt.specified & GRANT_ROLE_SPECIFIED_INHERIT != 0 {
                actions[i] = RevokeRoleGrantAction::RemoveInheritOption;
            } else if popt.specified & GRANT_ROLE_SPECIFIED_SET != 0 {
                actions[i] = RevokeRoleGrantAction::RemoveSetOption;
            } else {
                let revoke_admin_option_only = popt.specified & GRANT_ROLE_SPECIFIED_ADMIN != 0;
                plan_recursive_revoke(members, actions, i, revoke_admin_option_only, behavior)?;
            }
            return Ok(true);
        }
    }

    Ok(false)
}

pub fn plan_member_revoke(
    members: &[AuthMemRow],
    actions: &mut [RevokeRoleGrantAction],
    member: Oid,
) -> PgResult<()> {
    for i in 0..members.len() {
        if members[i].member == member {
            plan_recursive_revoke(members, actions, i, false, DropBehavior::DROP_CASCADE)?;
        }
    }
    Ok(())
}

pub fn plan_recursive_revoke(
    members: &[AuthMemRow],
    actions: &mut [RevokeRoleGrantAction],
    index: usize,
    revoke_admin_option_only: bool,
    behavior: DropBehavior,
) -> PgResult<()> {
    if actions[index] == RevokeRoleGrantAction::DeleteGrant {
        return Ok(());
    }
    if actions[index] == RevokeRoleGrantAction::RemoveAdminOption && revoke_admin_option_only {
        return Ok(());
    }

    let target = members[index];

    if !revoke_admin_option_only {
        actions[index] = RevokeRoleGrantAction::DeleteGrant;
        if !target.admin_option {
            return Ok(());
        }
    } else {
        if !target.admin_option {
            return Ok(());
        }
        actions[index] = RevokeRoleGrantAction::RemoveAdminOption;
    }

    let would_still_have_admin_option = members.iter().enumerate().any(|(i, m)| {
        m.member == target.member && m.admin_option && actions[i] == RevokeRoleGrantAction::Noop
    });
    if would_still_have_admin_option {
        return Ok(());
    }

    for i in 0..members.len() {
        if members[i].grantor == target.member
            && actions[i] != RevokeRoleGrantAction::DeleteGrant
        {
            if behavior == DropBehavior::DROP_RESTRICT {
                return Err(Box::new(
                    PgError::error("dependent privileges exist".to_string())
                        .with_sqlstate(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST)
                        .with_hint("Use CASCADE to revoke them too."),
                ));
            }
            plan_recursive_revoke(members, actions, i, false, behavior)?;
        }
    }

    Ok(())
}

// GUC check_hook for createrole_self_grant.
pub fn check_createrole_self_grant(
    newval: &mut Option<String>,
    extra: &mut Option<guc_tables::GucHookExtra>,
    _source: GucSource,
) -> PgResult<bool> {
    let value = newval.clone().unwrap_or_default();
    let ctx = mcx::MemoryContext::new("check_createrole_self_grant");
    let Some(elemlist) = varlena::split_identifier_string(
        ctx.mcx(),
        &value,
        b',',
        mbutils::GetDatabaseEncoding(),
    )?
    else {
        guc::GUC_check_errdetail("List syntax is invalid.");
        return Ok(false);
    };

    let mut options: u32 = 0;
    for tok in &elemlist {
        if tok.eq_ignore_ascii_case("SET") {
            options |= GRANT_ROLE_SPECIFIED_SET;
        } else if tok.eq_ignore_ascii_case("INHERIT") {
            options |= GRANT_ROLE_SPECIFIED_INHERIT;
        } else {
            guc::GUC_check_errdetail(format!("Unrecognized key word: \"{tok}\"."));
            return Ok(false);
        }
    }

    *extra = Some(Box::new(options));
    Ok(true)
}

// GUC assign_hook for createrole_self_grant.
pub fn assign_createrole_self_grant(
    _newval: Option<&str>,
    extra: Option<&guc_tables::GucHookExtra>,
) {
    let options = extra.and_then(|e| e.downcast_ref::<u32>()).copied().unwrap_or(0);
    CREATEROLE_SELF_GRANT_ENABLED.set(options != 0);
    CREATEROLE_SELF_GRANT_OPTIONS.set(GrantRoleOptions {
        specified: GRANT_ROLE_SPECIFIED_ADMIN
            | GRANT_ROLE_SPECIFIED_INHERIT
            | GRANT_ROLE_SPECIFIED_SET,
        admin: false,
        inherit: options & GRANT_ROLE_SPECIFIED_INHERIT != 0,
        set: options & GRANT_ROLE_SPECIFIED_SET != 0,
    });
}

// int Password_encryption = PASSWORD_TYPE_SCRAM_SHA_256 (user.c:85).
thread_local! {
    static PASSWORD_ENCRYPTION: Cell<i32> = const { Cell::new(2) };
}

fn password_encryption() -> i32 {
    PASSWORD_ENCRYPTION.with(Cell::get)
}

pub fn init_seams() {
    guc_tables::hooks::check_createrole_self_grant.install(check_createrole_self_grant);
    guc_tables::hooks::assign_createrole_self_grant.install(assign_createrole_self_grant);
    guc_tables::vars::Password_encryption.install(guc_tables::GucVarAccessors {
        get: password_encryption,
        set: |v| PASSWORD_ENCRYPTION.with(|c| c.set(v)),
    });
}
