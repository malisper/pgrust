//! Seam declarations for the `backend-utils-adt-acl` unit
//! (`utils/adt/acl.c`): role-membership checks.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;
use types_nodes::parsenodes::RoleSpec;

seam_core::seam!(
    /// `member_can_set_role(member, role)` (acl.c): whether `member` is
    /// permitted to `SET ROLE` to `role` (superuser, or membership with the
    /// SET option). Performs catalog/syscache lookups, which can
    /// `ereport(ERROR)`.
    pub fn member_can_set_role(member: Oid, role: Oid) -> PgResult<bool>
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
