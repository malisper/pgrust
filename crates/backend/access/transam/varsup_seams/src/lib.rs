use types_core::{FullTransactionId, Oid, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    // GetNewTransactionId(isSubXact) (varsup.c).
    pub fn get_new_transaction_id(is_subxact: bool) -> PgResult<FullTransactionId>
);

seam_core::seam!(
    // ReadNextTransactionId (transam.h); PgResult: takes XidGenLock shared.
    pub fn read_next_transaction_id() -> PgResult<TransactionId>
);

seam_core::seam!(
    // ReadNextFullTransactionId (transam.h); PgResult: takes XidGenLock
    // shared. The epoch-qualified horizon consumers need to map a bare 32-bit
    // xid back to the FullTransactionId it actually names (recent-past window),
    // rather than aliasing a recycled xid across wraparound.
    pub fn read_next_full_transaction_id() -> PgResult<FullTransactionId>
);

seam_core::seam!(
    // AdvanceNextFullTransactionIdPastXid (varsup.c); redo-only. PgResult:
    // takes XidGenLock, whose acquire carries C's ereport surface.
    pub fn advance_next_full_transaction_id_past_xid(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    // AdvanceOldestClogXid (varsup.c); clog truncation + redo.
    pub fn advance_oldest_clog_xid(oldest_datfrozenxid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    // SetTransactionIdLimit (varsup.c); StartupXLOG + vacuum wraparound limits.
    pub fn set_transaction_id_limit(oldest_datfrozenxid: TransactionId, oldest_datoid: Oid) -> PgResult<()>
);
