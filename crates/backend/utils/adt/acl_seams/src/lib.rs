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

seam_core::seam!(
    // get_role_oid(rolname, missing_ok) (acl.c); InvalidOid when missing_ok.
    pub fn get_role_oid(rolname: &str, missing_ok: bool) -> PgResult<Oid>
);

seam_core::seam!(
    // is_member_of_role_nosuper(member, role) (acl.c).
    pub fn is_member_of_role_nosuper(member: Oid, role: Oid) -> PgResult<bool>
);
