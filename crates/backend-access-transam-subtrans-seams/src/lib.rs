//! Seam declarations for the `backend-access-transam-subtrans` unit
//! (`access/transam/subtrans.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::TransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `SubTransGetParent(xid)` (subtrans.c): the immediate parent xid
    /// recorded in pg_subtrans for a subtransaction, or
    /// `InvalidTransactionId` if none is recorded (e.g. the post-startup
    /// window where pg_subtrans was zeroed). The SLRU page read can
    /// `ereport(ERROR)` on I/O failure, carried on `Err`.
    pub fn sub_trans_get_parent(xid: TransactionId) -> PgResult<TransactionId>
);

seam_core::seam!(
    /// `SubTransSetParent(xid, parent)` (subtrans.c): record `parent` as the
    /// immediate parent of subtransaction `xid` in pg_subtrans (idempotent if
    /// the same parent is already recorded). The SLRU page write can
    /// `ereport(ERROR)` on I/O failure, carried on `Err`.
    pub fn sub_trans_set_parent(xid: TransactionId, parent: TransactionId) -> PgResult<()>
);
