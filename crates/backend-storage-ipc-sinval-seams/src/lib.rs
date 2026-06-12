//! Seam declarations for the `backend-storage-ipc-sinval` unit
//! (`storage/ipc/sinval.c` + `sinvaladt.c`). The owning unit installs these
//! from its `init_seams()` when it lands; until then a call panics loudly.

use types_core::LocalTransactionId;

seam_core::seam!(
    /// `GetNextLocalTransactionId()` (sinvaladt.c) — assign the next backend-
    /// local transaction id.
    pub fn get_next_local_transaction_id() -> LocalTransactionId
);
