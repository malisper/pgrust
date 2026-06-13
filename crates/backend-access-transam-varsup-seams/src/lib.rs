//! Seam declarations for the `backend-access-transam-varsup` unit
//! (`access/transam/varsup.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{FullTransactionId, TransactionId};
use types_error::PgResult;

seam_core::seam!(
    /// `ReadNextFullTransactionId()` — the next full xid to be assigned.
    pub fn read_next_full_transaction_id() -> FullTransactionId
);

seam_core::seam!(
    /// `GetNewTransactionId(isSubXact)` — allocate the next FullTransactionId,
    /// record it in PGPROC and pg_subtrans. `ereport(ERROR)`s during recovery,
    /// in parallel mode, and near XID wraparound.
    pub fn get_new_transaction_id(is_subxact: bool) -> PgResult<FullTransactionId>
);

seam_core::seam!(
    /// `ReadNextTransactionId()` (`access/transam.h`) — read
    /// `TransamVariables->nextXid` (the xid part).
    pub fn read_next_transaction_id() -> TransactionId
);

seam_core::seam!(
    /// `AdvanceNextFullTransactionIdPastXid(xid)` — used during redo to keep
    /// nextXid beyond any XID mentioned in WAL.
    pub fn advance_next_full_transaction_id_past_xid(xid: TransactionId)
);

seam_core::seam!(
    /// `AdvanceNextFullTransactionIdPastXid(xid)` (varsup.c): bump
    /// `TransamVariables->nextXid` past `xid` if it is not already, so a
    /// recovered prepared transaction's subxids don't collide with future
    /// assignments. Takes `XidGenLock`; the SLRU extension it triggers can
    /// `ereport(ERROR)`, carried on `Err`.
    pub fn advance_next_full_xid_past_xid(xid: TransactionId) -> PgResult<()>
);
