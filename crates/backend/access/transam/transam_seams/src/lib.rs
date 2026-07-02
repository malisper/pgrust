use types_core::{TransactionId, XLogRecPtr};
use types_error::PgResult;

seam_core::seam!(
    pub fn transaction_id_commit_tree<'a>(xid: TransactionId, children: &'a [TransactionId]) -> PgResult<()>
);

seam_core::seam!(
    pub fn transaction_id_async_commit_tree<'a>(
        xid: TransactionId,
        children: &'a [TransactionId],
        lsn: XLogRecPtr,
    ) -> PgResult<()>
);

seam_core::seam!(
    pub fn transaction_id_abort_tree<'a>(xid: TransactionId, children: &'a [TransactionId]) -> PgResult<()>
);

seam_core::seam!(
    pub fn transaction_id_latest<'a>(mainxid: TransactionId, children: &'a [TransactionId]) -> TransactionId
);

seam_core::seam!(
    pub fn transaction_id_did_commit(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    pub fn transaction_id_did_abort(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    // TransactionIdGetCommitLSN (transam.c).
    pub fn transaction_id_get_commit_lsn(xid: TransactionId) -> PgResult<XLogRecPtr>
);
