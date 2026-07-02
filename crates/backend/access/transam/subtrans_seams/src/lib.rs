use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    pub fn sub_trans_set_parent(xid: TransactionId, parent: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    pub fn sub_trans_get_topmost_transaction(xid: TransactionId) -> PgResult<TransactionId>
);
