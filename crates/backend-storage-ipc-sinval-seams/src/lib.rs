//! Seam declarations for the `backend-storage-ipc-sinval` unit
//! (`storage/ipc/sinval.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `HandleCatchupInterrupt()` (sinval.c) — the PROCSIG_CATCHUP_INTERRUPT
    /// arm of `procsignal_sigusr1_handler`. Signal-handler-safe flag
    /// flipping; infallible.
    pub fn handle_catchup_interrupt()
);
