//! Seam declarations for the `backend-storage-ipc-procarray` unit
//! (`storage/ipc/procarray.c`), as consumed by logical decoding.
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

#![allow(non_snake_case)]

use types_core::primitive::TransactionId;

seam_core::seam!(
    /// `GetOldestSafeDecodingTransactionId(catalogOnly)`.
    pub fn GetOldestSafeDecodingTransactionId(catalog_only: bool) -> TransactionId
);
seam_core::seam!(
    /// `LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE)`.
    pub fn ProcArrayLock_acquire_exclusive()
);
seam_core::seam!(
    /// `LWLockRelease(ProcArrayLock)`.
    pub fn ProcArrayLock_release()
);
seam_core::seam!(
    /// `MyProc->statusFlags |= PROC_IN_LOGICAL_DECODING;
    /// ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;`
    /// performed while holding `ProcArrayLock`.
    pub fn mark_proc_in_logical_decoding()
);
