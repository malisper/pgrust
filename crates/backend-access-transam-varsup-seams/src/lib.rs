//! Seam declarations for the `backend-access-transam-varsup` unit
//! (`access/transam/varsup.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::xact::FullTransactionId;
use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `ReadNextFullTransactionId()` — the next full xid to be assigned.
    pub fn read_next_full_transaction_id() -> FullTransactionId
);

seam_core::seam!(
    /// `AdvanceNextFullTransactionIdPastXid(xid)` (varsup.c): bump
    /// `TransamVariables->nextXid` past `xid` if it is not already, so a
    /// recovered prepared transaction's subxids don't collide with future
    /// assignments. Takes `XidGenLock`; the SLRU extension it triggers can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn advance_next_full_xid_past_xid(xid: TransactionId) -> PgResult<()>
);
