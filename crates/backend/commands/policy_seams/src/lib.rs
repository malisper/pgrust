use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn remove_policy_by_id<'mcx>(mcx: Mcx<'mcx>, policy_id: Oid) -> PgResult<()>
);

seam_core::seam!(
    pub fn remove_role_from_object_policy<'mcx>(
        mcx: Mcx<'mcx>,
        roleid: Oid,
        classid: Oid,
        policy_id: Oid,
    ) -> PgResult<bool>
);
