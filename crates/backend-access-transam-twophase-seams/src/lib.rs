//! Seam declarations for the `backend-access-transam-twophase` unit
//! (`access/transam/twophase.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `StandbyTransactionIdIsPrepared(xid)` — true if `xid` is a prepared
    /// transaction known to this standby.
    pub fn standby_transaction_id_is_prepared(xid: TransactionId) -> PgResult<bool>
);
