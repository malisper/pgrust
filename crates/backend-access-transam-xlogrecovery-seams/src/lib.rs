//! Seam declarations for the `backend-access-transam-xlogrecovery` unit
//! (`access/transam/xlogrecovery.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// `WakeupRecovery()` (xlogrecovery.c) — set the recovery-wakeup latch so
    /// the redo loop notices new state. Safe in signal-handler context.
    pub fn wakeup_recovery()
);

seam_core::seam!(
    /// Read the `PrimaryConnInfo` GUC string (xlogrecovery.c). Returns an
    /// owned snapshot of the backend-local value (never NULL in C; the
    /// boot value is `""`).
    pub fn primary_conninfo() -> String
);

seam_core::seam!(
    /// Read the `PrimarySlotName` GUC string (xlogrecovery.c). Returns an
    /// owned snapshot of the backend-local value.
    pub fn primary_slot_name() -> String
);

seam_core::seam!(
    /// Read the `wal_receiver_create_temp_slot` GUC bool (xlogrecovery.c).
    pub fn wal_receiver_create_temp_slot() -> bool
);

seam_core::seam!(
    /// `StartupRequestWalReceiverRestart()` (xlogrecovery.c) — flag that the
    /// walreceiver must be restarted because a critical option changed.
    pub fn startup_request_wal_receiver_restart()
);
