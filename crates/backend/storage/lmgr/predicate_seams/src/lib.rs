use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn pre_commit_check_for_serialization_failure() -> PgResult<()>
);

seam_core::seam!(
    pub fn register_predicate_locking_xid(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn at_prepare_predicate_locks() -> PgResult<()>
);

seam_core::seam!(
    pub fn post_prepare_predicate_locks(xid: TransactionId) -> PgResult<()>
);
