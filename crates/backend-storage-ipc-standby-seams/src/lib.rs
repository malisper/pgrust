//! Seam declarations for the `backend-storage-ipc-standby` unit
//! (`storage/ipc/standby.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `ShutdownRecoveryTransactionEnvironment()` (standby.c) — mark tracked
    /// in-progress transactions finished and release recovery locks at
    /// startup-process exit. Safe to call redundantly; its error surface is
    /// WARNING/PANIC only, never `ereport(ERROR)`.
    pub fn shutdown_recovery_transaction_environment()
);

seam_core::seam!(
    /// `StandbyDeadLockHandler()` (standby.c) — `STANDBY_DEADLOCK_TIMEOUT`
    /// expiry callback; runs in signal-handler context.
    pub fn standby_dead_lock_handler()
);

seam_core::seam!(
    /// `StandbyTimeoutHandler()` (standby.c) — `STANDBY_TIMEOUT` expiry
    /// callback; runs in signal-handler context.
    pub fn standby_timeout_handler()
);

seam_core::seam!(
    /// `StandbyLockTimeoutHandler()` (standby.c) — `STANDBY_LOCK_TIMEOUT`
    /// expiry callback; runs in signal-handler context.
    pub fn standby_lock_timeout_handler()
);
