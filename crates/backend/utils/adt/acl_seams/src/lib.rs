use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn member_can_set_role(member: Oid, role: Oid) -> PgResult<bool>
);

seam_core::seam!(
    // initialize_acl (acl.c): registers role-membership-cache inval callbacks.
    pub fn initialize_acl() -> PgResult<()>
);

seam_core::seam!(
    // has_privs_of_role(member, role) (acl.c).
    pub fn has_privs_of_role(member: Oid, role: Oid) -> PgResult<bool>
);
