//! Seam declarations for the `backend-replication-walsender` unit
//! (`replication/walsender.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `HandleWalSndInitStopping()` (walsender.c) — the
    /// PROCSIG_WALSND_INIT_STOPPING arm of `procsignal_sigusr1_handler`.
    /// Signal-handler-safe flag flipping; infallible.
    pub fn handle_wal_snd_init_stopping()
);

seam_core::seam!(
    /// `am_walsender` (walsender.c global) — true in a WAL-sender backend.
    /// Pure read of backend-local state.
    pub fn am_walsender() -> bool
);

seam_core::seam!(
    /// `am_db_walsender` (walsender.c global) — true in a database-connected
    /// (logical-replication) WAL sender. Pure read.
    pub fn am_db_walsender() -> bool
);

seam_core::seam!(
    /// `am_walsender = value` (walsender.c global) — set by the startup-packet
    /// `replication` parameter handling.
    pub fn set_am_walsender(value: bool)
);

seam_core::seam!(
    /// `am_db_walsender = value` (walsender.c global).
    pub fn set_am_db_walsender(value: bool)
);
