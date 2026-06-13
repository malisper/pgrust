//! Seam declarations for `commands/user.c`'s cross-subsystem callees.
//!
//! Every outward call from `backend-commands-user` crosses one of these seams:
//! the user-id/superuser substrate, the acl.c role-membership helpers,
//! role-OID/RoleSpec resolution, the catalog open/scan/insert/update/delete +
//! syscache, the lock manager, dependency/shdepend/comment/seclabel/setting
//! helpers, the object-access hooks, the password/crypt subsystem, the
//! `defGetString`/`errorConflictingDefElem`/`parser_errposition` value layer,
//! the `timestamptz_in` VALID UNTIL parse, and the
//! `IsBinaryUpgrade`/`Password_encryption`/`createrole_self_grant` globals.
//!
//! Each owner installs its real implementation when it lands; until then a
//! call panics loudly with the seam path. The signatures mirror each C
//! function's failure surface (`PgResult<_>` where the C path can `ereport` at
//! ERROR+).

use mcx::{Mcx, PgString};
use seam_core::seam;
use types_authid::{
    AuthIdForm, AuthIdUpdate, AuthMemForm, AuthMemUpdate, CatCListHandle, NewAuthMemRecord,
    NewAuthRecord, PasswordType, TupleHandle,
};
use types_core::primitive::{Oid, TimestampTz};
use types_error::PgResult;
use types_nodes::parsenodes::DropBehavior;
use types_parsenodes::{DefElem, Node, ParseState, RoleSpec};

/* --- user-id / superuser substrate (miscinit.c, superuser.c) --- */

seam!(
    /// `GetUserId()`.
    pub fn get_user_id() -> PgResult<Oid>
);
seam!(
    /// `GetOuterUserId()`.
    pub fn get_outer_user_id() -> PgResult<Oid>
);
seam!(
    /// `GetSessionUserId()`.
    pub fn get_session_user_id() -> PgResult<Oid>
);
seam!(
    /// `superuser()`.
    pub fn superuser() -> PgResult<bool>
);
seam!(
    /// `superuser_arg(roleid)`.
    pub fn superuser_arg(roleid: Oid) -> PgResult<bool>
);
seam!(
    /// `GetUserNameFromId(roleid, noerr)` — the role name `pstrdup`'d into the
    /// caller's context (`mcx`).
    pub fn get_user_name_from_id<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        noerr: bool,
    ) -> PgResult<PgString<'mcx>>
);

/* --- acl.c role-membership / privilege helpers --- */

seam!(
    /// `has_createrole_privilege(roleid)`.
    pub fn has_createrole_privilege(roleid: Oid) -> PgResult<bool>
);
seam!(
    /// `have_createdb_privilege()` (dbcommands.c).
    pub fn have_createdb_privilege() -> PgResult<bool>
);
seam!(
    /// `has_rolreplication(roleid)`.
    pub fn has_rolreplication(roleid: Oid) -> PgResult<bool>
);
seam!(
    /// `has_bypassrls_privilege(roleid)`.
    pub fn has_bypassrls_privilege(roleid: Oid) -> PgResult<bool>
);
seam!(
    /// `is_admin_of_role(member, role)`.
    pub fn is_admin_of_role(member: Oid, role: Oid) -> PgResult<bool>
);
seam!(
    /// `has_privs_of_role(member, role)`.
    pub fn has_privs_of_role(member: Oid, role: Oid) -> PgResult<bool>
);
seam!(
    /// `is_member_of_role_nosuper(member, role)`.
    pub fn is_member_of_role_nosuper(member: Oid, role: Oid) -> PgResult<bool>
);
seam!(
    /// `select_best_admin(memberId, roleId)` — the OID to record as grantor, or
    /// `InvalidOid` if none.
    pub fn select_best_admin(member_id: Oid, role_id: Oid) -> PgResult<Oid>
);

/* --- role-OID / RoleSpec resolution (acl.c, dbcommands.c) --- */

seam!(
    /// `get_role_oid(rolename, missing_ok)`.
    pub fn get_role_oid(rolename: String, missing_ok: bool) -> PgResult<Oid>
);
seam!(
    /// `get_rolespec_oid(role, missing_ok)`.
    pub fn get_rolespec_oid(role: RoleSpec, missing_ok: bool) -> PgResult<Oid>
);
seam!(
    /// `get_rolespec_name(role)` — the resolved role name allocated in `mcx`
    /// (C: `pstrdup`).
    pub fn get_rolespec_name<'mcx>(mcx: Mcx<'mcx>, role: RoleSpec) -> PgResult<PgString<'mcx>>
);
seam!(
    /// `check_rolespec_name(role, detail_msg)` — reject reserved RoleSpecs.
    pub fn check_rolespec_name(role: RoleSpec, detail_msg: String) -> PgResult<()>
);
seam!(
    /// `IsReservedName(name)`.
    pub fn is_reserved_name(name: String) -> PgResult<bool>
);
seam!(
    /// `get_database_oid(dbname, missing_ok)`.
    pub fn get_database_oid(dbname: String, missing_ok: bool) -> PgResult<Oid>
);
seam!(
    /// `object_ownercheck(DatabaseRelationId, dboid, roleid)`.
    pub fn object_ownercheck_database(dboid: Oid, roleid: Oid) -> PgResult<bool>
);
seam!(
    /// `aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_DATABASE, dbname)` — always raises.
    pub fn aclcheck_error_not_owner_database(dbname: String) -> PgResult<()>
);

/* --- catalog / cache mutation --- */

seam!(
    /// `CommandCounterIncrement()`.
    pub fn command_counter_increment() -> PgResult<()>
);
seam!(
    /// `SearchSysCacheExists1(AUTHNAME, name)`.
    pub fn authid_exists_by_name(rolename: String) -> PgResult<bool>
);
seam!(
    /// `table_open(relid, lockmode)` — opens + locks the relation and returns
    /// its OID identity (the same `relid`).
    pub fn table_open(relid: Oid, lockmode: i32) -> PgResult<Oid>
);
seam!(
    /// `table_close(rel, lockmode)` — `rel` is the OID identity from `table_open`.
    pub fn table_close(rel: Oid, lockmode: i32) -> PgResult<()>
);
seam!(
    /// `GetNewOidWithIndex(rel, indexId, oidColumn)`.
    pub fn get_new_oid_with_index(rel: Oid, index_id: Oid) -> PgResult<Oid>
);
seam!(
    /// `SearchSysCache1(AUTHNAME, name)` — `Some(handle)` if found.
    pub fn authid_by_name(rolename: String) -> PgResult<Option<TupleHandle>>
);
seam!(
    /// `SearchSysCache1(AUTHOID, oid)` — `Some(handle)` if found.
    pub fn authid_by_oid(roleid: Oid) -> PgResult<Option<TupleHandle>>
);
seam!(
    /// `GETSTRUCT(tuple)` for a cached `pg_authid` tuple.
    pub fn authid_form(tuple: TupleHandle) -> PgResult<AuthIdForm>
);
seam!(
    /// The stored password text, or `None` if NULL.
    pub fn authid_password(tuple: TupleHandle) -> PgResult<Option<String>>
);
seam!(
    /// The stored valid-until value, or `None` if NULL.
    pub fn authid_validuntil(tuple: TupleHandle) -> PgResult<Option<TimestampTz>>
);
seam!(
    /// `ReleaseSysCache(tuple)`.
    pub fn release_sys_cache(tuple: TupleHandle) -> PgResult<()>
);
seam!(
    /// `get_rolespec_tuple(role)` — locate the role and return its
    /// `Form_pg_authid` view.
    pub fn get_rolespec_tuple(role: RoleSpec) -> PgResult<(TupleHandle, AuthIdForm)>
);
seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` of a fresh `pg_authid` row.
    pub fn insert_authid(rel: Oid, rec: NewAuthRecord) -> PgResult<()>
);
seam!(
    /// `heap_modify_tuple` + `CatalogTupleUpdate` of the located `pg_authid` tuple.
    pub fn update_authid(rel: Oid, tuple: TupleHandle, upd: AuthIdUpdate) -> PgResult<()>
);
seam!(
    /// `CatalogTupleDelete(pg_authid_rel, &tuple->t_self)`.
    pub fn delete_authid(rel: Oid, tuple: TupleHandle) -> PgResult<()>
);
seam!(
    /// The `RenameRole` rename of `rolname` (and optional MD5 clear).
    pub fn rename_authid(
        rel: Oid,
        tuple: TupleHandle,
        newname: String,
        clear_md5: bool,
    ) -> PgResult<()>
);
seam!(
    /// `SearchSysCache3(AUTHMEMROLEMEM, roleid, member, grantor)`.
    pub fn authmem_by_keys(
        roleid: Oid,
        member: Oid,
        grantor: Oid,
    ) -> PgResult<Option<TupleHandle>>
);
seam!(
    /// `GETSTRUCT(tuple)` for a cached/scanned `pg_auth_members` tuple.
    pub fn authmem_form(tuple: TupleHandle) -> PgResult<AuthMemForm>
);
seam!(
    /// `SearchSysCacheList1(AUTHMEMROLEMEM, roleid)` — the list handle and the
    /// `Form_pg_auth_members` view of every member entry, in order.
    pub fn authmem_list_by_role(
        roleid: Oid,
    ) -> PgResult<(CatCListHandle, Vec<AuthMemForm>)>
);
seam!(
    /// `ReleaseSysCacheList(list)`.
    pub fn release_sys_cache_list(list: CatCListHandle) -> PgResult<()>
);
seam!(
    /// `heap_form_tuple` + `CatalogTupleInsert` of a fresh `pg_auth_members` row
    /// (the provider then runs `updateAclDependencies(... 1, {grantorId})`).
    pub fn insert_authmem(rel: Oid, rec: NewAuthMemRecord) -> PgResult<()>
);
seam!(
    /// `heap_modify_tuple` + `CatalogTupleUpdate` of the i-th list member tuple.
    pub fn update_authmem(
        rel: Oid,
        list: CatCListHandle,
        index: usize,
        upd: AuthMemUpdate,
    ) -> PgResult<()>
);
seam!(
    /// `heap_modify_tuple` + `CatalogTupleUpdate` of a single syscache-located
    /// `pg_auth_members` tuple.
    pub fn update_authmem_by_tuple(
        rel: Oid,
        tuple: TupleHandle,
        upd: AuthMemUpdate,
    ) -> PgResult<()>
);
seam!(
    /// `deleteSharedDependencyRecordsFor` then `CatalogTupleDelete` of the i-th
    /// list member tuple.
    pub fn delete_authmem_in_list(
        rel: Oid,
        list: CatCListHandle,
        index: usize,
    ) -> PgResult<()>
);
seam!(
    /// `systable` scan of `pg_auth_members` by `roleid`; returns the count removed.
    pub fn delete_authmem_by_roleid(rel: Oid, roleid: Oid) -> PgResult<usize>
);
seam!(
    /// `systable` scan of `pg_auth_members` by `member`; returns the count removed.
    pub fn delete_authmem_by_member(rel: Oid, memberid: Oid) -> PgResult<usize>
);

/* --- lock manager, shdepend, comments, seclabel, settings --- */

seam!(
    /// `LockSharedObject(AuthIdRelationId, roleid, 0, lockmode)`.
    pub fn lock_shared_object_authid(roleid: Oid, lockmode: i32) -> PgResult<()>
);
seam!(
    /// `shdepLockAndCheckObject(classId, objid)`.
    pub fn shdep_lock_and_check_object(class_id: Oid, objid: Oid) -> PgResult<()>
);
seam!(
    /// `checkSharedDependencies(AuthIdRelationId, roleid, &detail, &detail_log)`.
    pub fn check_shared_dependencies(roleid: Oid) -> PgResult<Option<(String, String)>>
);
seam!(
    /// `DeleteSharedComments(roleid, AuthIdRelationId)`.
    pub fn delete_shared_comments(roleid: Oid) -> PgResult<()>
);
seam!(
    /// `DeleteSharedSecurityLabel(roleid, AuthIdRelationId)`.
    pub fn delete_shared_security_label(roleid: Oid) -> PgResult<()>
);
seam!(
    /// `DropSetting(databaseid, roleid)`.
    pub fn drop_setting(databaseid: Oid, roleid: Oid) -> PgResult<()>
);
seam!(
    /// `AlterSetting(databaseid, roleid, setstmt)` — the SET/RESET subcommand is
    /// the `VariableSetStmt`, carried as the owned `Node` (`None` => C `NULL`).
    pub fn alter_setting(
        databaseid: Oid,
        roleid: Oid,
        setstmt: Option<Node>,
    ) -> PgResult<()>
);
seam!(
    /// `shdepDropOwned(role_ids, behavior)`.
    pub fn shdep_drop_owned(role_ids: Vec<Oid>, behavior: DropBehavior) -> PgResult<()>
);
seam!(
    /// `shdepReassignOwned(role_ids, newrole)`.
    pub fn shdep_reassign_owned(role_ids: Vec<Oid>, newrole: Oid) -> PgResult<()>
);

/* --- object-access hooks --- */

seam!(
    /// `InvokeObjectPostCreateHook(AuthIdRelationId, roleid, 0)`.
    pub fn invoke_object_post_create_hook_authid(roleid: Oid) -> PgResult<()>
);
seam!(
    /// `InvokeObjectPostAlterHook(AuthIdRelationId, roleid, 0)`.
    pub fn invoke_object_post_alter_hook_authid(roleid: Oid) -> PgResult<()>
);
seam!(
    /// `InvokeObjectDropHook(AuthIdRelationId, roleid, 0)`.
    pub fn invoke_object_drop_hook_authid(roleid: Oid) -> PgResult<()>
);

/* --- password / crypt subsystem (auth-scram.c, crypt.c) --- */

seam!(
    /// `encrypt_password(Password_encryption, role, password)` — the encrypted
    /// password string allocated in the caller's context (`mcx`).
    pub fn encrypt_password<'mcx>(
        mcx: Mcx<'mcx>,
        password_encryption: i32,
        role: String,
        password: String,
    ) -> PgResult<PgString<'mcx>>
);
seam!(
    /// `plain_crypt_verify(role, shadow_pass, client_pass, &logdetail)` — the
    /// `STATUS_OK`/`STATUS_ERROR` int.
    pub fn plain_crypt_verify(
        role: String,
        shadow_pass: String,
        client_pass: String,
    ) -> PgResult<i32>
);
seam!(
    /// Whether a `check_password_hook` is installed.
    pub fn has_check_password_hook() -> PgResult<bool>
);
seam!(
    /// `get_password_type(shadow_pass)`.
    pub fn get_password_type(shadow_pass: String) -> PgResult<PasswordType>
);
seam!(
    /// `(*check_password_hook)(...)`.
    pub fn call_check_password_hook(
        username: String,
        password: String,
        password_type: PasswordType,
        validuntil: Option<TimestampTz>,
    ) -> PgResult<()>
);
seam!(
    /// `DirectFunctionCall3(timestamptz_in, value, InvalidOid, -1)`.
    pub fn timestamptz_in(value: String) -> PgResult<TimestampTz>
);

/* --- DefElem value layer / parser error positioning --- */

seam!(
    /// `defGetString(opt)`.
    pub fn def_get_string(opt: DefElem) -> PgResult<String>
);
seam!(
    /// `(List *) defel->arg` materialized as the owned list of `RoleSpec` nodes.
    pub fn def_get_rolespec_list(defel: DefElem) -> PgResult<Vec<RoleSpec>>
);
seam!(
    /// `errorConflictingDefElem(defel, pstate)` — always raises a syntax error.
    pub fn error_conflicting_def_elem(
        defel: DefElem,
        pstate: Option<&ParseState>,
    ) -> PgResult<()>
);
seam!(
    /// `parser_errposition(pstate, location)`.
    pub fn parser_errposition(pstate: Option<&ParseState>, location: i32) -> PgResult<i32>
);

/* --- backend globals (set at startup / by GUC assign hooks) --- */

seam!(
    /// `IsBinaryUpgrade`.
    pub fn is_binary_upgrade() -> PgResult<bool>
);
seam!(
    /// `binary_upgrade_next_pg_authid_oid`, consumed (reset to `InvalidOid`).
    pub fn take_binary_upgrade_next_pg_authid_oid() -> PgResult<Oid>
);
seam!(
    /// The `Password_encryption` GUC.
    pub fn password_encryption() -> PgResult<i32>
);
seam!(
    /// `createrole_self_grant_enabled`.
    pub fn createrole_self_grant_enabled() -> PgResult<bool>
);
seam!(
    /// The parsed `createrole_self_grant_options` (specified-bits, admin, inherit, set).
    pub fn createrole_self_grant_options() -> PgResult<(u32, bool, bool, bool)>
);
seam!(
    /// `SplitIdentifierString(rawstring, ',', &elemlist)` — `Some(tokens)` on
    /// success, `None` on a list-syntax error.
    pub fn split_identifier_string(rawstring: String) -> PgResult<Option<Vec<String>>>
);
seam!(
    /// `GUC_check_errdetail(fmt, ...)` — records the detail string for the
    /// in-progress GUC check-hook failure (the GUC machinery owns the
    /// `GUC_check_errdetail_string` slot). Infallible (just stashes the text).
    pub fn guc_check_errdetail(detail: String)
);
