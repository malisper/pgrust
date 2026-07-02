use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn pre_commit_on_commit_actions() -> PgResult<()>
);

seam_core::seam!(
    pub fn at_eoxact_on_commit_actions(is_commit: bool)
);

seam_core::seam!(
    pub fn at_eosubxact_on_commit_actions(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    )
);
