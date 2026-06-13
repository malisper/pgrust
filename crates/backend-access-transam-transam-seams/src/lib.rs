//! Seam declarations for the `backend-access-transam-transam` unit
//! (`access/transam/transam.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `TransactionIdDidCommit(xid)` — clog lookup; can `ereport(ERROR)` on
    /// clog I/O failure.
    pub fn transaction_id_did_commit(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    /// `TransactionIdDidAbort(xid)` — clog lookup; can `ereport(ERROR)` on
    /// clog I/O failure.
    pub fn transaction_id_did_abort(xid: TransactionId) -> PgResult<bool>
);

seam_core::seam!(
    /// `TransactionIdPrecedes(id1, id2)` — modulo-2^31 circular comparison.
    pub fn transaction_id_precedes(id1: TransactionId, id2: TransactionId) -> bool
);
