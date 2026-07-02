use types_core::{RepOriginId, TimestampTz, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    pub fn transaction_tree_set_commit_ts_data<'a>(
        xid: TransactionId,
        children: &'a [TransactionId],
        timestamp: TimestampTz,
        node_id: RepOriginId,
    ) -> PgResult<()>
);
