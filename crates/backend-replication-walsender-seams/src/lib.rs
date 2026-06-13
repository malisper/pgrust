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
    /// `GetStandbyFlushRecPtr(TimeLineID *tli)` (walsender.c) — the most recent
    /// WAL position safe to send from this standby (max of replay and
    /// same-timeline receive). The slotsync caller passes `NULL` for `tli`.
    /// Pure shmem reads; infallible.
    pub fn get_standby_flush_rec_ptr() -> types_core::primitive::XLogRecPtr
);
