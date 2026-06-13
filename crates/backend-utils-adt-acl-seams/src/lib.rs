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

// --- backend-utils-init-postinit consumers (acl.c) ---

seam_core::seam!(
    /// `has_privs_of_role(member, role)` (acl.c): does `member` have the
    /// privileges of `role` (directly or transitively, INHERIT)? `Err` carries
    /// its catcache `ereport` surface.
    pub fn has_privs_of_role(
        member: types_core::Oid,
        role: types_core::Oid,
    ) -> types_error::PgResult<bool>
);

seam_core::seam!(
    /// `initialize_acl()` (acl.c): set up the ACL framework (role membership
    /// cache). `Err` carries its `ereport` surface.
    pub fn initialize_acl() -> types_error::PgResult<()>
);
