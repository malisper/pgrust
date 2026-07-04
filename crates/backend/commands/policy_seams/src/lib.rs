use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;

seam_core::seam!(
    pub fn remove_policy_by_id<'mcx>(mcx: Mcx<'mcx>, policy_id: Oid) -> PgResult<()>
);
