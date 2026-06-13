//! Seam declarations for the `backend-utils-adt-acl` unit
//! (`utils/adt/acl.c`): role-membership checks.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    /// `member_can_set_role(member, role)` (acl.c): whether `member` is
    /// permitted to `SET ROLE` to `role` (superuser, or membership with the
    /// SET option). Performs catalog/syscache lookups, which can
    /// `ereport(ERROR)`.
    pub fn member_can_set_role(member: Oid, role: Oid) -> PgResult<bool>
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

seam_core::seam!(
    /// `object_ownercheck(classid, objectid, roleid)` (catalog/aclchk.c):
    /// whether `roleid` owns the catalog object. Catalog lookup; can
    /// `ereport(ERROR)`.
    pub fn object_ownercheck(classid: Oid, objectid: Oid, roleid: Oid) -> PgResult<bool>
);
