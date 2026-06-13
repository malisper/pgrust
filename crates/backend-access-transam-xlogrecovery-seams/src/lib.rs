//! Seam declarations for the `backend-access-transam-xlogrecovery` unit
//! (`access/transam/xlogrecovery.c`): the recovery-side globals slotsync reads.
//!
//! The owning unit installs these from its `init_seams()` when it lands; until
//! then a call panics loudly.
//!
//! `PrimaryConnInfo` / `PrimarySlotName` / `StandbyMode` are recovery-config
//! globals declared in xlogrecovery.c. They are surfaced as reads of the
//! owner's state (the recovery worker reads them ambiently in C); the owner
//! installs the actual read when it lands.

seam_core::seam!(
    /// `PrimaryConnInfo` (xlogrecovery.c) — the `primary_conninfo` recovery
    /// setting, or `None` when unset.
    pub fn primary_conn_info() -> Option<String>
);

seam_core::seam!(
    /// `PrimarySlotName` (xlogrecovery.c) — the `primary_slot_name` recovery
    /// setting, or `None` when unset.
    pub fn primary_slot_name() -> Option<String>
);

seam_core::seam!(
    /// `StandbyMode` (xlogrecovery.c) — whether the server is in standby mode.
    pub fn standby_mode() -> bool
);
