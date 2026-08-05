use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn at_eoxact_large_object(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eosubxact_large_object(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    ) -> PgResult<()>
);
