//! Seam declarations for the `backend-storage-lmgr-lmgr` unit
//! (`storage/lmgr/lmgr.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::{TransactionId, VirtualTransactionId};
use types_error::PgResult;

seam_core::seam!(
    /// `XactLockTableInsert(xid)` — take ExclusiveLock on the transaction
    /// XID. Lock acquisition can `ereport(ERROR)` (out of shared memory).
    pub fn xact_lock_table_insert(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `XactLockTableDelete(xid)` — release the subtransaction XID lock.
    pub fn xact_lock_table_delete(xid: TransactionId) -> PgResult<()>
);

seam_core::seam!(
    /// `VirtualXactLockTableInsert(vxid)` — lock our virtual transaction id
    /// before advertising it in the proc array.
    pub fn virtual_xact_lock_table_insert(vxid: VirtualTransactionId) -> PgResult<()>
);
