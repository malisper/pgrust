//! Role-membership cache and queries (`utils/adt/acl.c`).
//!
//! `initialize_acl`/`RoleMembershipCacheCallback` set up and invalidate the
//! per-backend `cached_roles` lists; `roles_list_append`/`roles_is_member_of`
//! build the transitive membership set (with a Bloom-filter fast path past
//! `ROLES_LIST_BLOOM_THRESHOLD`). The public predicates (`has_privs_of_role`,
//! `member_can_set_role`, `check_can_set_role`, `is_member_of_role`,
//! `is_member_of_role_nosuper`, `is_admin_of_role`, `select_best_admin`,
//! `select_best_grantor`) and the rolespec resolvers (`get_role_oid`,
//! `get_role_oid_or_public`, `get_rolespec_oid`, `get_rolespec_tuple`,
//! `get_rolespec_name`, `check_rolespec_name`) sit on top.

use types_acl::{AclMode, RoleRecurseType};
use types_core::Oid;
use types_error::PgResult;

/// `ROLES_LIST_BLOOM_THRESHOLD` (acl.c) — membership-list size past which a
/// Bloom filter is built to speed up membership tests.
pub const ROLES_LIST_BLOOM_THRESHOLD: usize = 1024;

/// `initialize_acl` (acl.c) — register the syscache invalidation callback and
/// reset the per-backend membership cache.
pub fn initialize_acl() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::initialize_acl")
}

/// `RoleMembershipCacheCallback` (acl.c) — syscache invalidation callback that
/// flushes the cached membership lists.
pub fn role_membership_cache_callback(_cacheid: i32, _hashvalue: u32) {
    todo!("scaffold: backend-utils-adt-acl::role_membership::role_membership_cache_callback")
}

/// `roles_list_append` (acl.c) — append `role` to the working list, building or
/// updating the Bloom filter once past the threshold.
pub fn roles_list_append(_role: Oid) -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::roles_list_append")
}

/// `roles_is_member_of` (acl.c) — the transitive set of roles `roleid` belongs
/// to under `type`. `admin_of`/`admin_role` is the optional admin-search out
/// param. Returns the cached OID list.
pub fn roles_is_member_of(
    _roleid: Oid,
    _ty: RoleRecurseType,
    _admin_of: Oid,
    _admin_role: &mut Oid,
) -> PgResult<&'static [Oid]> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::roles_is_member_of")
}

/// `has_privs_of_role` (acl.c) — does `member` inherit privileges of `role`?
pub fn has_privs_of_role(_member: Oid, _role: Oid) -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::has_privs_of_role")
}

/// `member_can_set_role` (acl.c) — may `member` `SET ROLE` to `role`?
pub fn member_can_set_role(_member: Oid, _role: Oid) -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::member_can_set_role")
}

/// `check_can_set_role` (acl.c) — error unless `member` may `SET ROLE` to
/// `role`.
pub fn check_can_set_role(_member: Oid, _role: Oid) -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::check_can_set_role")
}

/// `is_member_of_role` (acl.c) — is `member` a member of `role` (any path)?
pub fn is_member_of_role(_member: Oid, _role: Oid) -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::is_member_of_role")
}

/// `is_member_of_role_nosuper` (acl.c) — membership test ignoring superuser.
pub fn is_member_of_role_nosuper(_member: Oid, _role: Oid) -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::is_member_of_role_nosuper")
}

/// `is_admin_of_role` (acl.c) — may `member` administer membership in `role`?
pub fn is_admin_of_role(_member: Oid, _role: Oid) -> PgResult<bool> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::is_admin_of_role")
}

/// `select_best_admin` (acl.c) — pick an admin role through which `member` can
/// administer `role`, or `InvalidOid`.
pub fn select_best_admin(_member: Oid, _role: Oid) -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::select_best_admin")
}

/// `select_best_grantor` (acl.c) — choose the grantor role and grantable
/// privileges for a GRANT performed by `role_id`. Returns
/// `(grantor_id, grant_option_mode)`.
pub fn select_best_grantor(
    _role_id: Oid,
    _privileges: AclMode,
    _owner_id: Oid,
) -> PgResult<(Oid, AclMode)> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::select_best_grantor")
}

/// `get_role_oid` (acl.c) — resolve a role name to its OID, honoring
/// `missing_ok`.
pub fn get_role_oid(_rolname: &str, _missing_ok: bool) -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::get_role_oid")
}

/// `get_role_oid_or_public` (acl.c) — like `get_role_oid`, mapping "public" to
/// `ACL_ID_PUBLIC`.
pub fn get_role_oid_or_public(_rolname: &str) -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::get_role_oid_or_public")
}

/// `get_rolespec_oid` (acl.c) — resolve a parser `RoleSpec` to a role OID.
/// `role` is the C `RoleSpec` node (scaffold placeholder until `types-nodes`
/// is threaded through this family).
pub fn get_rolespec_oid(_missing_ok: bool) -> PgResult<Oid> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::get_rolespec_oid")
}

/// `get_rolespec_tuple` (acl.c) — fetch the `pg_authid` tuple for a `RoleSpec`.
pub fn get_rolespec_tuple() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::get_rolespec_tuple")
}

/// `get_rolespec_name` (acl.c) — the role name a `RoleSpec` resolves to.
pub fn get_rolespec_name() -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::get_rolespec_name")
}

/// `check_rolespec_name` (acl.c) — reject reserved role names with `detail_msg`.
pub fn check_rolespec_name(_detail_msg: &str) -> PgResult<()> {
    todo!("scaffold: backend-utils-adt-acl::role_membership::check_rolespec_name")
}
