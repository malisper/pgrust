//! Seam declarations for the `backend-utils-adt-acl` unit
//! (`utils/adt/acl.c`): role-membership checks.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use nodes::parsenodes::RoleSpec;

seam_core::seam!(
    /// `member_can_set_role(member, role)` (acl.c): whether `member` is
    /// permitted to `SET ROLE` to `role` (superuser, or membership with the
    /// SET option). Performs catalog/syscache lookups, which can
    /// `ereport(ERROR)`.
    pub fn member_can_set_role(member: Oid, role: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `get_role_oid(rolname, missing_ok)` (acl.c): the OID of the role with
    /// the given name. With `missing_ok = false` a missing role raises
    /// `ERRCODE_UNDEFINED_OBJECT` (`Err`); with `missing_ok = true` it is
    /// `Ok(InvalidOid)`.
    pub fn get_role_oid(rolname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `check_can_set_role(member, role)` (acl.c): error unless `member` may
    /// `SET ROLE` to `role`. Raises `ERRCODE_INSUFFICIENT_PRIVILEGE`
    /// ("must be able to SET ROLE \"%s\"") otherwise.
    pub fn check_can_set_role(member: Oid, role: Oid) -> PgResult<()>
);

seam_core::seam!(
    /// `get_rolespec_oid(role, missing_ok)` (acl.c): resolve a `RoleSpec` to a
    /// role OID. With `missing_ok = false` a missing/invalid role raises
    /// (`Err`); with `missing_ok = true` a missing CSTRING role returns
    /// `InvalidOid`.
    pub fn get_rolespec_oid(role: &RoleSpec<'_>, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    /// `has_privs_of_role(member, role)` (acl.c): whether `member` has the
    /// privileges of `role` (is a member with `inherit`, or is the role).
    /// Catalog/syscache lookups can `ereport(ERROR)`.
    pub fn has_privs_of_role(member: Oid, role: Oid) -> PgResult<bool>
);

seam_core::seam!(
    /// `has_bypassrls_privilege(roleid)` (acl.c): whether `roleid` has the
    /// BYPASSRLS attribute (superusers always do). Performs catalog/syscache
    /// lookups, which can `ereport(ERROR)`.
    pub fn has_bypassrls_privilege(roleid: Oid) -> PgResult<bool>
);

// NOTE: `object_ownercheck` (catalog/aclchk.c) was a mis-homed OUTWARD seam
// here (adt-acl merely called it); it is canonically declared in
// `backend-catalog-aclchk-seams` and installed by the ported aclchk owner.
// The lone consumer of this duplicate slot (ri-triggers) was re-pointed to the
// canonical seam, so this declaration was removed.

seam_core::seam!(
    /// `initialize_acl()` (acl.c): set up the ACL framework (role membership
    /// cache). `Err` carries its `ereport` surface.
    pub fn initialize_acl() -> types_error::PgResult<()>
);
