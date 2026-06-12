//! Seam declarations for the `backend-access-transam-varsup` unit
//! (`access/transam/varsup.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

use types_core::xact::FullTransactionId;

seam_core::seam!(
    /// `ReadNextFullTransactionId()` — the next full xid to be assigned.
    pub fn read_next_full_transaction_id() -> FullTransactionId
);
