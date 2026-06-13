//! Seam declarations for the `backend-storage-ipc-procarray` unit
//! (`storage/ipc/procarray.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::primitive::TransactionId;

seam_core::seam!(
    /// `GetOldestSafeDecodingTransactionId(catalogOnly)` (procarray.c) — the
    /// oldest xid from which it is safe to start decoding. Caller holds
    /// `ProcArrayLock`; pure scan, infallible.
    pub fn get_oldest_safe_decoding_transaction_id(catalog_only: bool) -> TransactionId
);
