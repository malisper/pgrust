//! Seam declarations for the `backend-utils-time-snapmgr` unit
//! (`utils/time/snapmgr.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

use types_core::TransactionId;

seam_core::seam!(
    /// Read the `TransactionXmin` global (snapmgr.c): the oldest xid still
    /// considered running by the backend's snapshots. A pure backend-local
    /// read; cannot `ereport`.
    pub fn transaction_xmin() -> TransactionId
);
