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

// --- backend-utils-init-postinit consumers (walsender.c) ---

seam_core::seam!(
    /// `am_walsender` (walsender.c global): is this a WAL sender process?
    pub fn am_walsender() -> bool
);

seam_core::seam!(
    /// `am_db_walsender` (walsender.c global): is this a database-connected
    /// (logical) WAL sender?
    pub fn am_db_walsender() -> bool
);

seam_core::seam!(
    /// `max_wal_senders` (walsender.c GUC).
    pub fn max_wal_senders() -> i32
);
