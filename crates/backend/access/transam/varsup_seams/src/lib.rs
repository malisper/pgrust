use types_core::{FullTransactionId, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    // GetNewTransactionId(isSubXact) (varsup.c).
    pub fn get_new_transaction_id(is_subxact: bool) -> PgResult<FullTransactionId>
);

seam_core::seam!(
    pub fn read_next_transaction_id() -> TransactionId
);

seam_core::seam!(
    // AdvanceNextFullTransactionIdPastXid (varsup.c); redo-only.
    pub fn advance_next_full_transaction_id_past_xid(xid: TransactionId)
);
