//! Seam declarations for the `backend-access-transam-xlog` unit
//! (`access/transam/xlog.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.

seam_core::seam!(
    /// `RecoveryInProgress()` (xlog.c): true while hot-standby recovery is
    /// running. Reads backend-local + shared state; cannot `ereport`.
    pub fn recovery_in_progress() -> bool
);
