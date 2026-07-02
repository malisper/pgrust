use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn member_can_set_role(member: Oid, role: Oid) -> PgResult<bool>
);
