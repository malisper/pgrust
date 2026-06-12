//! Seam declarations for the `backend-storage-ipc-sinval` unit
//! (`storage/ipc/sinval.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `SharedInvalidMessageCounter` (sinval.c): the running count of shared
    /// invalidation messages this backend has processed. Pure global read.
    pub fn shared_invalid_message_counter() -> u64
);
