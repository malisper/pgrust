//! Seam declarations for the `backend-access-transam-xlogrecovery` unit
//! (`access/transam/xlogrecovery.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `InRecovery` (`access/xlogutils.h`, owned by xlogrecovery.c) — true
    /// while this process is performing WAL replay (the startup process's
    /// local flag, distinct from the shared `RecoveryInProgress()`). Pure
    /// read of the owner's per-backend flag at the point of use; the
    /// zero-arg-getter shape is recorded in DESIGN_DEBT.md.
    pub fn in_recovery() -> bool
);
