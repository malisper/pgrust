//! Seam declarations for the `backend-replication-logical-launcher` unit
//! (`replication/logical/launcher.c`). The owning unit installs these from
//! its `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_ApplyLauncher(isCommit)` — wake/forget logical-rep launcher
    /// work queued in this transaction.
    pub fn at_eoxact_apply_launcher(is_commit: bool)
);
