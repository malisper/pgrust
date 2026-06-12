//! Seam declarations for the `backend-access-transam-xlogutils` unit
//! (`access/transam/xlogutils.c`).
//!
//! The owning unit installs these from its `init_seams()` when it lands;
//! until then a call panics loudly.

seam_core::seam!(
    /// Read `standbyState` (xlogutils.c), the startup process's
    /// `HotStandbyState` recovery-tracking state.
    pub fn standby_state() -> types_wal::HotStandbyState
);
