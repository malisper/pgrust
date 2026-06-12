//! Seam declarations for the `backend-replication-logical-applyparallelworker`
//! unit (`replication/logical/applyparallelworker.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `HandleParallelApplyMessageInterrupt()` (applyparallelworker.c) — the
    /// PROCSIG_PARALLEL_APPLY_MESSAGE arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_parallel_apply_message_interrupt()
);
