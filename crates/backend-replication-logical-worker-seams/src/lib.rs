//! Seam declarations for the `backend-replication-logical-worker` unit
//! (`replication/logical/worker.c`). The owning unit installs these from its
//! `init_seams()` when it lands; until then a call panics loudly.

seam_core::seam!(
    /// `AtEOXact_LogicalRepWorkers(isCommit)`.
    pub fn at_eoxact_logical_rep_workers(is_commit: bool)
);
