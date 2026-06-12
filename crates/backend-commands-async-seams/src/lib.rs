//! Seam declarations for the `backend-commands-async` unit
//! (`commands/async.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `HandleNotifyInterrupt()` (async.c) — the PROCSIG_NOTIFY_INTERRUPT
    /// arm of `procsignal_sigusr1_handler`. Signal-handler-safe flag
    /// flipping; infallible.
    pub fn handle_notify_interrupt()
);
